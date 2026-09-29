# backend/routers/webhook.py
# ================================================================
# Upstox Order Update Webhook
#
# Register this URL in your Upstox app's webhook settings:
#   POST  https://yourdomain.com/api/webhook/upstox
#
# Upstox sends a signed HTTPS POST for every order state change:
#   open → trigger_pending → complete / rejected / cancelled
#
# This endpoint:
#   1. Optionally verifies an HMAC-SHA256 signature (only when
#      WEBHOOK_ENFORCE_SIGNATURE=true — Upstox postbacks are unsigned)
#   2. Normalises the payload (handles V2 and V3 field names)
#   3. Identifies which user owns this order (by order tag or
#      scanning active subscriptions via Redis)
#   4. Writes the update to Redis via order_store.publish_order_update
#   5. Publishes an ORDER_UPDATE event on the Redis event bus so the
#      dashboard live feed updates instantly
#
# SIGNATURES: Upstox order-update postbacks do NOT carry a signature
# or "Postback Secret", so verification is OFF by default. A missing
# signature must never drop a real order update (that would stall fill
# detection and SL tracking). Set WEBHOOK_ENFORCE_SIGNATURE=true only
# if postbacks are signed by a proxy you control.
#
# IMPORTANT: this endpoint MUST be publicly reachable over HTTPS.
# For local dev, use ngrok: ngrok http 8000
# ================================================================

import hashlib
import hmac
import json
import time
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Request, HTTPException, Header

from backend.config import get_settings
from backend.services.redis_client import get_redis
from backend.services.order_store import publish_order_update
from backend.services.event_bus import publish_event_sync

router = APIRouter(prefix="/api/webhook", tags=["webhook"])
settings = get_settings()


# ================================================================
# SIGNATURE VERIFICATION
# ================================================================

def _verify_signature(raw_body: bytes, sig_header: Optional[str]) -> bool:
    """
    Optional HMAC-SHA256 verification.

    Upstox order-update postbacks are UNSIGNED — there is no postback
    secret — so by default this returns True and the update is always
    processed. Verification only runs when WEBHOOK_ENFORCE_SIGNATURE
    is true (e.g. a signing proxy you control in front of Upstox).

    Returns False only when enforcement is on and the signature is
    missing or does not match.
    """
    enforce = bool(getattr(settings, "WEBHOOK_ENFORCE_SIGNATURE", False))
    secret = getattr(settings, "WEBHOOK_SECRET", "")
    if not enforce:
        if sig_header:
            print("[webhook] ℹ️ Signature header present but verification "
                  "is disabled (WEBHOOK_ENFORCE_SIGNATURE not set) — accepting")
        return True
    if not secret:
        print("[webhook] ❌ Enforcement is on but WEBHOOK_SECRET is empty — "
              "cannot verify signature")
        return False
    if not sig_header:
        print("[webhook] ❌ Signature verification failed: no signature header")
        return False
    expected = hmac.new(
        secret.encode(), raw_body, hashlib.sha256
    ).hexdigest()
    # Some senders prefix with "sha256=" — strip if present
    received = sig_header.replace("sha256=", "").strip()
    match = hmac.compare_digest(expected, received)
    if match:
        print("[webhook] ✅ Signature verification passed successfully.")
    else:
        print(f"[webhook] ❌ Signature verification failed: mismatch.\n"
              f"  Received signature: '{received}'\n"
              f"  Expected signature: '{expected}'\n"
              f"  Payload size: {len(raw_body)} bytes")
    return match


# ================================================================
# PAYLOAD NORMALISATION
# ================================================================

_STATUS_MAP = {
    "complete":         "complete",
    "filled":           "complete",
    "traded":           "complete",
    "rejected":         "rejected",
    "cancelled":        "cancelled",
    "open":             "open",
    "trigger_pending":  "trigger_pending",
    "modify_pending":   "open",
    "cancel_pending":   "open",
    "error":            "rejected",
}


def _normalise(raw: dict) -> dict:
    """
    Upstox webhook payloads differ slightly between API v2 and v3
    and between order types. This produces a consistent flat dict
    regardless of version.
    """
    # Upstox v3 wraps payload in {"type":"order_update","data":{...}}
    data = raw.get("data", raw)
    if isinstance(data, str):
        try: data = json.loads(data)
        except Exception: data = raw

    def _get(*keys):
        for k in keys:
            v = data.get(k)
            if v is not None and v != "":
                return v
        return None

    raw_status = str(_get("status", "order_status", "") or "").lower().strip()
    status     = _STATUS_MAP.get(raw_status, raw_status)

    avg_price  = _get("average_price", "avg_price", "fill_price", "executed_price")
    try: avg_price = float(avg_price) if avg_price else 0.0
    except: avg_price = 0.0

    qty_filled = _get("filled_quantity", "traded_quantity", "quantity")
    try: qty_filled = int(qty_filled) if qty_filled else 0
    except: qty_filled = 0

    order_type = str(_get("order_type", "type") or "").upper()
    side       = str(_get("transaction_type", "side", "order_side") or "").upper()

    return {
        "order_id":        str(_get("order_id", "id") or ""),
        "status":          status,
        "average_price":   avg_price,
        "qty_filled":      qty_filled,
        "instrument_key":  str(_get("instrument_key", "instrument_token", "tradingsymbol") or ""),
        "trading_symbol":  str(_get("tradingsymbol", "trading_symbol") or ""),
        "order_type":      order_type,
        "side":            side,
        "trigger_price":   float(_get("trigger_price", "sl_trigger") or 0),
        "tag":             str(_get("tag", "order_tag") or ""),
        "exchange_order_id": str(_get("exchange_order_id", "exchange_id") or ""),
        "exchange_time":   str(_get("exchange_timestamp", "exchange_time") or ""),
        "placed_by":       str(_get("placed_by", "client_id") or ""),
        "message":         str(_get("status_message", "message", "rejection_reason") or ""),
        "received_at":     datetime.utcnow().isoformat() + "Z",
        "raw":             raw,   # keep original for audit
    }


# ================================================================
# USER RESOLUTION
# ================================================================

async def _resolve_user_id(order_data: dict) -> Optional[int]:
    """
    Try to identify which user owns this order. Two strategies:
    1. The `tag` field — we set tag="algo_bot" on every order, but
       some implementations encode user_id like "algo_bot:42". If
       your _place_order uses tag=f"algo_bot:{user_id}", extract it.
    2. Scan active bot statuses in Redis (all running bots) and
       match instrument_key or order_id against their active engines.
       This is a fallback — tag-based resolution is preferred.

    Returns the user_id int, or None if unresolvable (the order
    update is stored without a user_id — still useful for fill
    price lookups by order_id).
    """
    tag = order_data.get("tag", "")
    # Strategy 1 — tag encodes user_id: "algo_bot:42"
    if ":" in tag:
        try:
            parts = tag.split(":")
            return int(parts[-1])
        except (ValueError, IndexError):
            pass

    # Strategy 2 — scan running bots' order id sets in Redis
    from backend.services.state_store import get_all_bot_statuses
    statuses = await get_all_bot_statuses()
    order_id = order_data.get("order_id", "")
    from backend.services.redis_client import get_redis
    r = get_redis()
    for user_id in statuses.keys():
        # Check if this order_id is registered under this user
        key = f"bot:orders:{user_id}"
        if await r.sismember(key, order_id):
            return user_id

    return None


# ================================================================
# WEBHOOK ENDPOINT
# ================================================================

@router.post("/upstox")
async def upstox_webhook(
    request: Request,
    x_upstox_signature: Optional[str]           = Header(None),
    x_upstox_webhook_signature: Optional[str]   = Header(None),
):
    """
    Receives order update POSTs from Upstox.

    Register in your Upstox app:
      URL: https://yourdomain.com/api/webhook/upstox
      Events: ORDER_UPDATE  (select all order events)
    """
    raw_body = await request.body()
    # Extract signature header with multiple fallbacks
    sig = (
        request.headers.get("x-upstox-signature")
        or request.headers.get("x-upstox-webhook-signature")
        or x_upstox_signature
        or x_upstox_webhook_signature
    )
    
    print(f"[webhook] 📥 Received webhook POST request. Body length: {len(raw_body)} bytes. Headers: {dict(request.headers)}")

    if not _verify_signature(raw_body, sig):
        raise HTTPException(status_code=403, detail="Invalid webhook signature")

    try:
        raw = json.loads(raw_body)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    # Upstox sometimes sends an array of events
    events = raw if isinstance(raw, list) else [raw]

    for event_raw in events:
        try:
            order_data = _normalise(event_raw)
        except Exception as e:
            print(f"[webhook] normalise error: {e}")
            continue

        order_id = order_data.get("order_id")
        if not order_id:
            continue

        # Resolve user_id and attach it
        user_id = await _resolve_user_id(order_data)
        if user_id:
            order_data["user_id"] = user_id

        # Write to Redis — unblocks wait_for_fill_sync in the worker
        await publish_order_update(order_data)

        # Push to dashboard live feed via event bus
        if user_id:
            from fastapi.concurrency import run_in_threadpool
            await run_in_threadpool(
                publish_event_sync, user_id,
                {"event": "ORDER_UPDATE", **order_data}
            )
            # Also publish on the push-notification channel so the
            # worker's _order_update_push_loop can send a push to the
            # user's registered devices.
            import json as _json
            push_data = {"user_id": user_id, **order_data}
            await get_redis().publish("order_update_push",
                                      _json.dumps(push_data, default=str))

        print(f"[webhook] order_id={order_id} status={order_data['status']} "
              f"user={user_id} sym={order_data.get('trading_symbol')} "
              f"fill=₹{order_data.get('average_price')}")

    # Upstox expects a 200 response — any non-200 triggers retries
    return {"ok": True}


@router.get("/upstox/test")
async def webhook_test():
    """
    Public health-check — confirm the webhook URL is reachable.
    Point Upstox's 'Test Webhook' button at this path.
    """
    return {"status": "ok", "endpoint": "/api/webhook/upstox",
            "time": datetime.utcnow().isoformat() + "Z"}
