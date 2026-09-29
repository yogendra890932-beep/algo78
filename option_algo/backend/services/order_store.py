# backend/services/order_store.py
# ================================================================
# Redis-backed order update store.
#
# Upstox sends webhook POSTs to /api/webhook/upstox every time an
# order changes state (open → trigger_pending → complete/rejected).
# The web process receives these and writes normalised order-update
# dicts into Redis keys; the worker-side SymbolEngine reads them
# instead of polling get_order_details on every tick.
#
# Key layout:
#   order_update:{order_id}     -> JSON order snapshot, TTL=2h
#   order_updates:{user_id}     -> Redis LIST of recent order_ids
#                                   (most recent first), TTL=2h
#                                   (for dashboard order history)
#
# The ENGINE side (worker process) uses:
#   wait_for_fill_sync(order_id, timeout)  -> fill_price or None
#   get_order_update_sync(order_id)        -> dict or None
#   is_order_filled_sync(order_id)         -> bool
#
# The WEB side (webhook endpoint) uses:
#   publish_order_update(order_data)       -> async write + pub/sub
#
# Both read/write the same keys — worker via sync client,
# web via async client.
# ================================================================

import json
import time as _time
from typing import Optional

from backend.services.redis_client import get_redis, get_redis_sync

ORDER_TTL_SEC   = 7200   # 2 hours — covers a full trading session


# ── STATUS NORMALISATION ─────────────────────────────────────────
# Upstox uses mixed case and varying field names across API versions.

FILLED_STATUSES   = {"complete", "filled", "traded"}
REJECTED_STATUSES = {"rejected", "cancelled", "error", "failed"}

# Last broker rejection reason per order_id. wait_for_fill_sync records
# the reason here so callers (execution layer / engines) can surface WHY
# an order failed instead of just seeing a bare None.
_LAST_REJECTION: dict = {}


def _rejection_reason(update: dict) -> str:
    """Pull the human-readable reason out of a normalised order update."""
    if not isinstance(update, dict):
        return ""
    msg = update.get("message")
    if not msg:
        raw = update.get("raw")
        if isinstance(raw, dict):
            msg = (raw.get("status_message") or raw.get("message")
                   or raw.get("rejection_reason"))
    return str(msg or "").strip()


def _note_rejection(order_id: str, where: str, update: dict) -> None:
    reason = _rejection_reason(update)
    _LAST_REJECTION[order_id] = reason
    print(f"[worker-store] ❌ {where}: Order rejected/cancelled"
          + (f" — reason: {reason}" if reason else ""))


def get_last_rejection(order_id: str) -> str:
    """Broker rejection reason for order_id, or '' if none recorded."""
    return _LAST_REJECTION.get(str(order_id), "")


def _normalise_status(raw: str) -> str:
    return raw.lower().strip().replace(" ", "_")


def is_filled_status(status: str) -> bool:
    return _normalise_status(status) in FILLED_STATUSES


def is_rejected_status(status: str) -> bool:
    return _normalise_status(status) in REJECTED_STATUSES


# ── WEB SIDE (async) — write on webhook receipt ──────────────────

async def publish_order_update(order_data: dict):
    """
    Called by the webhook endpoint after signature verification.
    Writes the normalised order dict to Redis and publishes on the
    order_updates pub/sub channel so the worker is notified
    immediately (no polling needed).
    """
    order_id = str(order_data.get("order_id", ""))
    if not order_id:
        print("[webhook-store] ⚠️ No order_id in order_data, cannot publish update.")
        return

    r   = get_redis()
    key = f"order_update:{order_id}"
    res_set = await r.set(key, json.dumps(order_data, default=str), ex=ORDER_TTL_SEC)
    print(f"[webhook-store] 💾 Redis SET key={key} success={res_set}")

    # Publish on per-order channel — worker's wait_for_fill_sync
    # subscribes to this channel during the fill-wait window.
    notify_channel = f"order_notify:{order_id}"
    res_pub = await r.publish(notify_channel, json.dumps(order_data, default=str))
    print(f"[webhook-store] 📣 Redis PUBLISH channel={notify_channel} subscribers_notified={res_pub} payload={order_data}")

    # Also append to user's recent order list (for dashboard display)
    user_id = order_data.get("user_id")
    if user_id:
        list_key = f"order_updates:{user_id}"
        await r.lpush(list_key, order_id)
        await r.ltrim(list_key, 0, 49)   # keep last 50 per user
        await r.expire(list_key, ORDER_TTL_SEC)


async def get_recent_order_updates(user_id: int, limit: int = 20) -> list:
    r        = get_redis()
    list_key = f"order_updates:{user_id}"
    ids      = await r.lrange(list_key, 0, limit - 1)
    result   = []
    for oid in ids:
        raw = await r.get(f"order_update:{oid}")
        if raw:
            try: result.append(json.loads(raw))
            except Exception: pass
    return result


# ── WORKER SIDE (sync) — read during order placement / SL check ──

def get_order_update_sync(order_id: str) -> Optional[dict]:
    raw = get_redis_sync().get(f"order_update:{order_id}")
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return None


def is_order_filled_sync(order_id: str) -> bool:
    """
    Instant check — no blocking. Returns True only if a filled
    webhook has already arrived. Falls back to False if no webhook
    has been received yet (caller should then poll or wait).
    """
    update = get_order_update_sync(order_id)
    if not update:
        return False
    return is_filled_status(update.get("status", ""))


def wait_for_fill_sync(order_id: str, timeout: float = 15.0) -> Optional[float]:
    """
    REPLACES the polling loop in _get_fill_price for live orders.
    Subscribes to the order's Redis pub/sub channel and waits up to
    `timeout` seconds for a 'complete' webhook. Returns the
    average_price on fill, None on timeout/rejection.

    Falls back gracefully: if no webhook arrives in time (e.g.
    Upstox webhook is not configured), callers can still use
    get_order_details as a last resort.
    """
    notify_channel = f"order_notify:{order_id}"
    print(f"[worker-store] ⏱️ wait_for_fill_sync started for order_id={order_id}, timeout={timeout}s")
    
    r      = get_redis_sync()
    pubsub = r.pubsub()
    
    # 1. Subscribe FIRST to avoid missing any messages published in the race window
    print(f"[worker-store] 🔔 Subscribing to Redis channel: {notify_channel}")
    pubsub.subscribe(notify_channel)
    
    # 2. Check fast path immediately after subscribing
    print(f"[worker-store] 🔍 Checking fast path in Redis state store for order_id={order_id}")
    update = get_order_update_sync(order_id)
    if update:
        status = update.get("status", "")
        print(f"[worker-store] ⚡ Fast path check: found order update in Redis. status={status}")
        if is_filled_status(status):
            fill_price = float(update.get("average_price") or update.get("fill_price", 0)) or None
            print(f"[worker-store] ✅ Fast path match: Order filled @ ₹{fill_price}. Unsubscribing.")
            try:
                pubsub.unsubscribe(notify_channel)
                pubsub.close()
            except Exception:
                pass
            return fill_price
        if is_rejected_status(status):
            _note_rejection(order_id, "Fast path match", update)
            try:
                pubsub.unsubscribe(notify_channel)
                pubsub.close()
            except Exception:
                pass
            return None

    # 3. Message loop with polling fallback
    deadline = _time.time() + timeout
    fill_price = None
    rejected = False
    print(f"[worker-store] 🔁 Entering Pub/Sub wait loop for order_id={order_id} (deadline={_time.strftime('%H:%M:%S', _time.localtime(deadline))})")
    
    try:
        while _time.time() < deadline:
            msg = pubsub.get_message(ignore_subscribe_messages=True, timeout=0.5)
            if msg is None:
                # Polling fallback: check Redis key directly inside loop in case Pub/Sub connection dropped
                update = get_order_update_sync(order_id)
                if update:
                    status = update.get("status", "")
                    if is_filled_status(status):
                        fill_price = float(update.get("average_price") or update.get("fill_price", 0)) or None
                        print(f"[worker-store] ✅ Loop fallback check: Order filled @ ₹{fill_price} inside wait loop.")
                        break
                    if is_rejected_status(status):
                        _note_rejection(order_id, "Loop fallback check", update)
                        rejected = True
                        break
                continue
                
            try:
                data = json.loads(msg["data"])
            except Exception as e:
                print(f"[worker-store] ⚠️ Failed to parse Pub/Sub message data: {e}")
                continue
                
            status = data.get("status", "")
            print(f"[worker-store] 📥 Received Pub/Sub message: status={status}")
            if is_filled_status(status):
                fill_price = float(data.get("average_price") or
                                   data.get("fill_price", 0)) or None
                print(f"[worker-store] ✅ Pub/Sub match: Order filled @ ₹{fill_price}")
                break
            if is_rejected_status(status):
                _note_rejection(order_id, "Pub/Sub match", data)
                rejected = True
                break
    finally:
        try:
            pubsub.unsubscribe(notify_channel)
            pubsub.close()
            print(f"[worker-store] 🔕 Unsubscribed from {notify_channel}")
        except Exception as e:
            print(f"[worker-store] ⚠️ Error closing pubsub: {e}")

    if fill_price is None and not rejected:
        print(f"[worker-store] ⚠️ wait_for_fill_sync timed out after {timeout}s for order_id={order_id}")
    return fill_price
