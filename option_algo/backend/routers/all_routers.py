# backend/routers/all_routers.py
# ================================================================
# All routers consolidated:
#   /api/users  — profile, config, upstox token
#   /api/bot    — start, stop, status, debug
#   /api/trades — history, summary
#   /api/admin  — user management, stats
#   /ws         — WebSocket live feed
# ================================================================

import json
import re
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from pydantic import BaseModel, Field, field_validator
from typing import Optional, List
from backend.db.database import get_db
from backend.db.models import (
    User,
    BotConfig,
    TradeStatus,
    ExecutionMode,
    DailyAutoConsent,
    PendingTrade,
    PendingTradeStatus,
)
from backend.services.auth_service import get_current_user, encrypt_token, decrypt_token
from backend.services.audit_log import log_event
from backend.config import get_settings

router = APIRouter(prefix="/api/users", tags=["users"])

_MOBILE_RE = re.compile(r"^[6-9]\d{9}$")  # 10-digit Indian mobile number


class ProfileOut(BaseModel):
    id: int
    email: str
    full_name: str
    role: str
    is_active: bool
    has_upstox_token: bool
    mobile_number: Optional[str] = None


class ProfileUpdateIn(BaseModel):
    full_name: Optional[str] = Field(default=None, min_length=1)
    mobile_number: Optional[str] = None

    @field_validator("full_name", "mobile_number", mode="before")
    @classmethod
    def _strip(cls, v):
        return v.strip() if isinstance(v, str) else v

    @field_validator("mobile_number")
    @classmethod
    def _validate_mobile(cls, v):
        if v is not None and not _MOBILE_RE.match(v):
            raise ValueError("Enter a valid 10-digit mobile number")
        return v


class BotConfigIn(BaseModel):
    underlying_symbol: Optional[str] = None
    underlying_token: Optional[str] = None
    itm_depth: Optional[int] = None
    strategy: Optional[str] = None
    order_qty: Optional[int] = None
    trail_mode: Optional[str] = None
    target_rr: Optional[float] = None
    sl_pct: Optional[float] = None
    max_trades_per_day: Optional[int] = None
    max_loss_per_day: Optional[float] = None
    trade_start_time: Optional[str] = None
    trade_end_time: Optional[str] = None
    paper_mode: Optional[bool] = None
    execution_mode: Optional[str] = None
    telegram_bot_token: Optional[str] = None
    telegram_chat_id: Optional[str] = None
    telegram_on_entry: Optional[bool] = None
    telegram_on_exit: Optional[bool] = None
    telegram_on_trail: Optional[bool] = None
    telegram_on_summary: Optional[bool] = None
    extra_symbols: Optional[str] = None
    extra_tokens: Optional[str] = None
    # Admin-only — silently ignored if sent by a non-admin user
    custom_lot_sizes: Optional[str] = None
    # Per-symbol additional symbol configs (JSON string array).
    # Each entry: {"symbol":"BANKNIFTY","enabled":true,"trade_mode":"SEMI_AUTO","lots":2}
    # Each symbol stores its own independent lot count.
    extra_symbol_config: Optional[str] = None

    @field_validator("execution_mode")
    @classmethod
    def _validate_execution_mode(cls, v):
        if v is None:
            return v
        value = str(v).strip().upper()
        if value not in {"PAPER", "SEMI_AUTO", "AUTO"}:
            raise ValueError("execution_mode must be PAPER, SEMI_AUTO, or AUTO")
        return value


@router.get("/me", response_model=ProfileOut)
async def me(user: User = Depends(get_current_user)):
    return ProfileOut(
        id=user.id,
        email=user.email,
        full_name=user.full_name,
        role=user.role.value,
        is_active=user.is_active,
        has_upstox_token=bool(user.upstox_token_enc),
        mobile_number=user.mobile_number,
    )


@router.patch("/profile", response_model=ProfileOut)
async def update_profile(
    body: ProfileUpdateIn,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Lets a user update their own full_name / mobile_number (billing
    UI "Account Details" — see frontend/templates/billing.html).
    body's fields are already format-validated by ProfileUpdateIn;
    a malformed mobile number never reaches here (422 first).
    """
    if body.mobile_number is not None:
        res = await db.execute(
            select(User).where(
                User.mobile_number == body.mobile_number, User.id != user.id
            )
        )
        if res.scalar_one_or_none():
            raise HTTPException(
                400, "Mobile number already registered to another account"
            )
        user.mobile_number = body.mobile_number

    if body.full_name is not None:
        user.full_name = body.full_name

    db.add(user)
    await db.commit()
    await db.refresh(user)
    return ProfileOut(
        id=user.id,
        email=user.email,
        full_name=user.full_name,
        role=user.role.value,
        is_active=user.is_active,
        has_upstox_token=bool(user.upstox_token_enc),
        mobile_number=user.mobile_number,
    )


@router.post("/upstox-token")
async def set_upstox_token(
    body: dict,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Manual override — paste a token generated some other way (e.g.
    scripts/get_upstox_token.py). The OAuth flow below
    (/upstox/login-url + /upstox/callback) is the recommended path;
    this stays as a fallback. Manually-set tokens get the same
    "valid until next 3:30 AM IST" expiry assumption.
    """
    token = body.get("token", "").strip()
    if not token:
        raise HTTPException(400, "token required")
    from backend.services.upstox_oauth import next_token_expiry

    user.upstox_token_enc = encrypt_token(token)
    user.upstox_token_expires_at = next_token_expiry()
    db.add(user)
    await db.commit()
    return {"ok": True}


# ================================================================
# UPSTOX OAUTH  — one-time API key/secret setup, daily token refresh
# ================================================================


class UpstoxCredentialsIn(BaseModel):
    api_key: str
    api_secret: str


@router.post("/upstox/credentials")
async def save_upstox_credentials(
    body: UpstoxCredentialsIn,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    One-time setup: store this user's Upstox API Key + API Secret
    (from https://account.upstox.com/developer/apps), encrypted at
    rest. The Redirect URI shown in the UI must be added to that
    Upstox app's config — it's the same URL for every user.
    """
    api_key = body.api_key.strip()
    api_secret = body.api_secret.strip()
    if not api_key or not api_secret:
        raise HTTPException(400, "API Key and API Secret are required")
    user.upstox_api_key_enc = encrypt_token(api_key)
    user.upstox_api_secret_enc = encrypt_token(api_secret)
    db.add(user)
    await db.commit()
    return {"ok": True}


@router.get("/upstox/login-url")
async def get_upstox_login_url(user: User = Depends(get_current_user)):
    """
    Returns the Upstox authorization-dialog URL for this user. The
    frontend redirects the browser here (full page navigation —
    Upstox's login page cannot be iframed). After the user logs in
    (handling their own password/TOTP), Upstox redirects to
    /api/users/upstox/callback, which completes the token exchange.
    """
    from backend.services.upstox_oauth import build_login_url

    if not user.upstox_api_key_enc:
        raise HTTPException(
            400, "Upstox API Key/Secret not configured — " "save your credentials first"
        )
    api_key = decrypt_token(user.upstox_api_key_enc)
    return {"url": build_login_url(user.id, api_key)}


@router.get("/upstox/status")
async def upstox_status(user: User = Depends(get_current_user)):
    """Used by the settings page to show connection/token state."""
    from backend.services.upstox_oauth import is_token_valid

    return {
        "has_credentials": bool(user.upstox_api_key_enc and user.upstox_api_secret_enc),
        "has_token": bool(user.upstox_token_enc),
        "token_valid": is_token_valid(user.upstox_token_expires_at),
        "expires_at": (
            user.upstox_token_expires_at.isoformat() + "Z"
            if user.upstox_token_expires_at
            else None
        ),
        "redirect_uri": get_settings().UPSTOX_REDIRECT_URI,
    }


@router.get("/upstox/callback")
async def upstox_callback(
    code: Optional[str] = None,
    state: Optional[str] = None,
    error: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
):
    """
    Upstox redirects here after the user logs in (registered as this
    app's Redirect URI in every user's Upstox app config). NOT
    authenticated via the normal Bearer token — the user is identified
    via the signed `state` param issued by /upstox/login-url.

    On success/failure, redirects the browser back to /settings with
    a query param the frontend reads to show a message.
    """
    from fastapi.responses import RedirectResponse
    from backend.services.upstox_oauth import (
        decode_state,
        exchange_code_for_token,
        next_token_expiry,
    )
    import httpx as _httpx

    def _redirect(status: str, msg: str = ""):
        url = f"/settings?upstox={status}"
        if msg:
            from urllib.parse import quote

            url += f"&msg={quote(msg)}"
        return RedirectResponse(url=url, status_code=302)

    if error:
        return _redirect("error", f"Upstox login error: {error}")
    if not code or not state:
        return _redirect("error", "Missing code/state from Upstox redirect")

    user_id = decode_state(state)
    if user_id is None:
        return _redirect("error", "Login link expired — please click Connect again")

    res = await db.execute(select(User).where(User.id == user_id))
    user = res.scalar_one_or_none()
    if not user or not user.upstox_api_key_enc or not user.upstox_api_secret_enc:
        return _redirect("error", "Upstox API Key/Secret not configured")

    api_key = decrypt_token(user.upstox_api_key_enc)
    api_secret = decrypt_token(user.upstox_api_secret_enc)

    try:
        token_resp = await exchange_code_for_token(code, api_key, api_secret)
    except _httpx.HTTPStatusError as e:
        detail = e.response.text[:200] if e.response is not None else str(e)
        return _redirect("error", f"Token exchange failed: {detail}")
    except Exception as e:
        return _redirect("error", f"Token exchange failed: {e}")

    access_token = token_resp.get("access_token")
    if not access_token:
        return _redirect("error", "Upstox did not return an access token")

    user.upstox_token_enc = encrypt_token(access_token)
    user.upstox_token_expires_at = next_token_expiry()
    db.add(user)
    await db.commit()

    return _redirect("connected")


@router.get("/config")
async def get_config(
    user: User = Depends(get_current_user), db: AsyncSession = Depends(get_db)
):
    from backend.db.models import UserRole

    res = await db.execute(select(BotConfig).where(BotConfig.user_id == user.id))
    cfg = res.scalar_one_or_none()
    if not cfg:
        raise HTTPException(404, "Config not found")
    result = {
        k: getattr(cfg, k)
        for k in [
            "underlying_symbol",
            "underlying_token",
            "itm_depth",
            "strategy",
            "order_qty",
            "trail_mode",
            "target_rr",
            "sl_pct",
            "max_trades_per_day",
            "max_loss_per_day",
            "trade_start_time",
            "trade_end_time",
            "status",
            "last_started",
            "last_stopped",
            "error_msg",
            "paper_mode",
            "execution_mode",
            "telegram_bot_token",
            "telegram_chat_id",
            "telegram_on_entry",
            "telegram_on_exit",
            "telegram_on_trail",
            "telegram_on_summary",
            "extra_symbols",
            "extra_tokens",
        ]
    }
    if result.get("execution_mode") is not None:
        result["execution_mode"] = (
            result["execution_mode"].value
            if hasattr(result["execution_mode"], "value")
            else str(result["execution_mode"])
        )
    result["custom_lot_sizes"] = (
        getattr(cfg, "custom_lot_sizes", None) if user.role == UserRole.admin else None
    )
    result["extra_symbol_config"] = getattr(cfg, "extra_symbol_config", None)
    return result


@router.get("/available-symbols")
async def get_available_symbols(
    user: User = Depends(get_current_user),
):
    """
    Returns all available trading symbols with their auto-detected
    streamer tokens, history keys, strike steps, and lot sizes.
    
    Combines the admin-managed StreamerSymbolToken table with the
    built-in KNOWN_INDEX_KEYS and NSE_LOT_SIZES tables.
    
    Used by the settings page's Additional Symbols card UI to populate
    the Trading Symbol dropdown and auto-fill token/lot-size/quantity.
    """
    from backend.services.admin_config_cache import get_all_streamer_tokens
    from backend.engine.instruments import KNOWN_INDEX_KEYS, KNOWN_STEPS
    from backend.engine.engine_v6 import NSE_LOT_SIZES
    
    # Start with hardcoded known indices
    db_tokens = get_all_streamer_tokens()
    
    # Collect all symbols (merge KNOWN_INDEX_KEYS + DB tokens)
    all_symbols_set = set()
    for sym in KNOWN_INDEX_KEYS:
        all_symbols_set.add(sym.upper())
    for sym in db_tokens:
        all_symbols_set.add(sym.upper())
    
    result = []
    for sym in sorted(all_symbols_set):
        sym_upper = sym.upper()
        
        # Streamer token: DB override → KNOWN_INDEX_KEYS → ""
        db_row = db_tokens.get(sym_upper, {})
        streamer_token = (
            db_row.get("streamer_token") or
            KNOWN_INDEX_KEYS.get(sym_upper, "")
        )
        
        # History key: DB override → KNOWN_INDEX_KEYS → streamer_token
        history_key = (
            db_row.get("history_key") or
            KNOWN_INDEX_KEYS.get(sym_upper, streamer_token)
        )
        
        # Strike step: DB override → KNOWN_STEPS → 50
        strike_step = (
            db_row.get("strike_step") or
            KNOWN_STEPS.get(sym_upper, 50)
        )
        
        # Lot size: DB custom lot sizes not available here, use NSE_LOT_SIZES
        lot_size = NSE_LOT_SIZES.get(sym_upper, 1)
        
        result.append({
            "symbol": sym_upper,
            "streamer_token": streamer_token,
            "history_key": history_key,
            "strike_step": strike_step,
            "lot_size": lot_size,
        })
    
    return result


@router.post("/test-telegram")
async def test_telegram(
    user: User = Depends(get_current_user), db: AsyncSession = Depends(get_db)
):
    res = await db.execute(select(BotConfig).where(BotConfig.user_id == user.id))
    cfg = res.scalar_one_or_none()
    if not cfg:
        raise HTTPException(404, "Config not found")
    if not cfg.telegram_bot_token or not cfg.telegram_chat_id:
        raise HTTPException(400, "Telegram bot token and chat ID must be saved first")
    from backend.services.telegram_alerts import test_telegram as tg_test

    ok = tg_test(cfg.telegram_bot_token, cfg.telegram_chat_id)
    if ok:
        return {"ok": True, "message": "✅ Test message sent to Telegram!"}
    raise HTTPException(500, "Failed to send Telegram message.")


@router.patch("/config")
async def update_config(
    body: BotConfigIn,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    from backend.db.models import UserRole

    res = await db.execute(select(BotConfig).where(BotConfig.user_id == user.id))
    cfg = res.scalar_one_or_none()
    if not cfg:
        raise HTTPException(404, "Config not found")
    fields = body.dict(exclude_none=True)
    if user.role != UserRole.admin:
        fields.pop("custom_lot_sizes", None)

    if "paper_mode" in fields and "execution_mode" not in fields:
        fields["execution_mode"] = (
            ExecutionMode.PAPER.value
            if fields["paper_mode"]
            else ExecutionMode.AUTO.value
        )

    if user.role != UserRole.admin:
        from backend.db.models import SubscriptionStatus
        from backend.services.subscription_service import (
            get_current_subscription,
            validate_plan_config,
        )

        eff_mode = fields.get("execution_mode") or (
            cfg.execution_mode.value
            if getattr(cfg, "execution_mode", None) is not None
            else ExecutionMode.PAPER.value
        )

        if eff_mode != ExecutionMode.PAPER.value:
            sub = await get_current_subscription(db, user.id)
            if sub is not None and sub.status == SubscriptionStatus.trial:
                raise HTTPException(
                    402,
                    "Free trial only allows Paper Trading — Semi Auto and Fully Automatic "
                    "require a paid plan. Please subscribe to continue.",
                )
            if sub is None:
                raise HTTPException(
                    402,
                    "Semi Auto and Fully Automatic trading require an active subscription plan. "
                    "Please subscribe to continue.",
                )
            if sub.status != SubscriptionStatus.active or sub.end_date < datetime.utcnow():
                raise HTTPException(
                    402,
                    "Your subscription is not active — please renew to continue trading.",
                )

            # Validate the requested main symbol + lots and any additional
            # symbols against the plan's allowed symbols / lot limits.
            main_symbol = fields.get("underlying_symbol") or cfg.underlying_symbol or "NIFTY"
            main_lots = int(fields.get("order_qty") or cfg.order_qty or 1)

            additional = []
            try:
                extra_config = json.loads(fields.get("extra_symbol_config") or cfg.extra_symbol_config or "[]")
            except Exception:
                extra_config = []
            if isinstance(extra_config, list):
                for entry in extra_config:
                    if isinstance(entry, dict) and entry.get("symbol"):
                        additional.append((
                            str(entry["symbol"]).upper(),
                            max(1, int(entry.get("lots", 1) or 1)),
                        ))
            extras_csv = fields.get("extra_symbols") or cfg.extra_symbols
            for sym in (extras_csv or "").split(","):
                if sym.strip():
                    additional.append((sym.strip().upper(), main_lots))

            ok, reason = await validate_plan_config(
                db, sub, main_symbol, main_lots, additional
            )
            if not ok:
                raise HTTPException(403, reason)

    for field, value in fields.items():
        setattr(cfg, field, value)

    if getattr(cfg, "execution_mode", None) is not None:
        cfg.paper_mode = cfg.execution_mode == ExecutionMode.PAPER.value

    db.add(cfg)
    await db.commit()
    return {"ok": True}


class DailyAutoConsentIn(BaseModel):
    accepted: bool
    risk_version: Optional[str] = None
    risk_text_snapshot: Optional[str] = None
    ip_address: Optional[str] = None
    device_information: Optional[str] = None
    browser_information: Optional[str] = None
    user_agent: Optional[str] = None


@router.get("/daily-auto-consent")
async def get_daily_auto_consent(
    user: User = Depends(get_current_user), db: AsyncSession = Depends(get_db)
):
    res = await db.execute(
        select(DailyAutoConsent)
        .where(DailyAutoConsent.user_id == user.id)
        .order_by(DailyAutoConsent.created_at.desc())
        .limit(1)
    )
    consent = res.scalar_one_or_none()
    if not consent:
        return {"accepted": False}
    valid = (
        consent.accepted
        and consent.valid_until
        and consent.valid_until > datetime.utcnow()
    )
    return {
        "accepted": valid,
        "accepted_at": consent.accepted_at,
        "valid_until": consent.valid_until,
        "risk_version": consent.risk_version,
        "risk_text_snapshot": consent.risk_text_snapshot,
    }


@router.post("/daily-auto-consent")
async def set_daily_auto_consent(
    body: DailyAutoConsentIn,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    if not body.accepted:
        raise HTTPException(
            400, "Consent must be accepted to enable fully automatic trading."
        )
    now = datetime.utcnow()
    valid_until = now.replace(hour=23, minute=59, second=59, microsecond=0)
    risk_version = body.risk_version or "v1.0"
    risk_text = body.risk_text_snapshot or "User accepted fully automatic daily trading consent."

    # Compute integrity hash: SHA-256(user_id + timestamp + risk_version + risk_text)
    from backend.services.execution_layer import compute_consent_audit_hash
    audit_hash = compute_consent_audit_hash(
        user_id=user.id,
        timestamp=now.isoformat(),
        risk_version=risk_version,
        risk_text=risk_text,
    )

    consent = DailyAutoConsent(
        user_id=user.id,
        accepted=True,
        accepted_at=now,
        valid_until=valid_until,
        risk_version=risk_version,
        risk_text_snapshot=risk_text,
        ip_address=body.ip_address,
        device_information=body.device_information,
        browser_information=body.browser_information,
        user_agent=body.user_agent,
        audit_hash=audit_hash,
    )
    db.add(consent)
    await db.commit()
    await log_event(
        db,
        user.id,
        "daily_auto_consent_accepted",
        "User accepted daily fully automatic trading consent.",
        metadata={
            "risk_version": consent.risk_version,
            "valid_until": consent.valid_until.isoformat() + "Z",
        },
    )
    return {"ok": True, "accepted": True, "valid_until": valid_until}


@router.get("/pending-trades")
async def list_pending_trades(
    user: User = Depends(get_current_user), db: AsyncSession = Depends(get_db)
):
    res = await db.execute(
        select(PendingTrade)
        .where(PendingTrade.user_id == user.id)
        .order_by(PendingTrade.created_at.desc())
        .limit(50)
    )
    rows = res.scalars().all()
    return [
        {
            "id": row.id,
            "signal_id": row.signal_id,
            "symbol": row.symbol,
            "opt_type": row.opt_type,
            "strategy": row.strategy,
            "quantity": row.quantity,
            "confidence": row.confidence,
            "status": row.status.value,
            "expires_at": row.expires_at.isoformat() if row.expires_at else None,
            "approved_at": row.approved_at.isoformat() if row.approved_at else None,
            "created_at": row.created_at.isoformat(),
        }
        for row in rows
    ]


@router.post("/pending-trades/{trade_id}/reject")
async def reject_pending_trade(
    trade_id: int,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    res = await db.execute(
        select(PendingTrade).where(
            PendingTrade.id == trade_id, PendingTrade.user_id == user.id
        )
    )
    trade = res.scalar_one_or_none()
    if not trade:
        raise HTTPException(404, "Pending trade not found")
    trade.status = PendingTradeStatus.REJECTED
    db.add(trade)
    await db.commit()
    return {"ok": True, "status": trade.status.value}


@router.post("/pending-trades/{trade_id}/approve")
async def approve_pending_trade(
    trade_id: int,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    from backend.services.command_queue import send_command

    res = await db.execute(
        select(PendingTrade).where(
            PendingTrade.id == trade_id, PendingTrade.user_id == user.id
        )
    )
    trade = res.scalar_one_or_none()
    if not trade:
        raise HTTPException(404, "Pending trade not found")
    if trade.status != PendingTradeStatus.WAITING:
        raise HTTPException(400, f"Pending trade is not waiting: {trade.status.value}")
    result = await send_command(
        "approve_pending_trade", user.id, {"trade_id": trade_id}
    )
    if not result.get("ok"):
        raise HTTPException(400, result.get("error", "Failed to approve pending trade"))
    return {
        "ok": True,
        "status": result.get("status", "approved"),
        "message": result.get("message"),
    }


# ================================================================
# BOT ROUTER
#
# This API/web process does NOT hold any SymbolEngine/BotThread
# instances directly — those live in the separate worker process
# (worker.py). All actions go through the Redis command queue
# (backend.services.command_queue), and all status reads come from
# the Redis state store (backend.services.state_store), which the
# worker keeps up to date. See worker.py module docstring for the
# full architecture.
# ================================================================

from fastapi import APIRouter as _R
from backend.services.rate_limit import rate_limit
from backend.services.command_queue import send_command
from backend.services import state_store
from backend.db.models import BotStatus
from datetime import datetime, timedelta

bot_router = _R(prefix="/api/bot", tags=["bot"])


@bot_router.post("/start")
async def start_bot(
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    _rl: None = Depends(rate_limit("bot_start", 5)),
):
    from backend.engine.history_loader import is_market_open

    if not is_market_open():
        raise HTTPException(
            400,
            "Market is closed — bot can only be started on NSE "
            "trading days between 9:15 AM and 3:30 PM IST",
        )

    if not user.upstox_token_enc:
        raise HTTPException(400, "Upstox access token not set")

    cur = await state_store.get_bot_status(user.id)
    if cur.get("running"):
        raise HTTPException(400, "Bot already running")

    res = await db.execute(select(BotConfig).where(BotConfig.user_id == user.id))
    cfg = res.scalar_one_or_none()
    if not cfg:
        raise HTTPException(404, "Bot config not found")

    # Instant feedback before round-tripping to the worker — the
    # authoritative check is resolve_start_inputs() (bot_config_builder.py),
    # which the worker calls right before actually starting the engine.
    # Admins are exempt — same precedent as the email-verified bypass
    # in resolve_start_inputs (platform operator, not a paying customer).
    from backend.db.models import UserRole

    if user.role != UserRole.admin:
        from backend.services.subscription_service import check_trading_permission

        requested_symbols = [cfg.underlying_symbol or "NIFTY"]
        requested_symbols += [
            s.strip() for s in (cfg.extra_symbols or "").split(",") if s.strip()
        ]
        exec_mode = (
            (
                cfg.execution_mode.value
                if hasattr(cfg.execution_mode, "value")
                else str(cfg.execution_mode)
            )
            if getattr(cfg, "execution_mode", None)
            else None
        )
        is_paper = (
            (exec_mode == ExecutionMode.PAPER.value)
            if exec_mode is not None
            else (cfg.paper_mode if cfg.paper_mode is not None else True)
        )
        allowed, reason, _ = await check_trading_permission(
            db, user.id, requested_symbols, cfg.order_qty or 1, is_paper
        )
        if not allowed:
            raise HTTPException(400, reason)

    # Send "start" to the worker — it re-fetches config + decrypts the
    # token itself (the token never travels through Redis).
    result = await send_command("start", user.id, timeout=10.0)

    if result.get("queued"):
        # Worker didn't ack within timeout — still update DB optimistically
        cfg.status = BotStatus.running
        cfg.last_started = datetime.utcnow()
        cfg.error_msg = None
        db.add(cfg)
        await db.commit()
        return {"ok": True, "status": "starting"}

    if not result.get("ok"):
        raise HTTPException(400, result.get("error", "Failed to start bot"))

    cfg.status = BotStatus.running
    cfg.last_started = datetime.utcnow()
    cfg.error_msg = None
    db.add(cfg)
    await db.commit()

    return {"ok": True, "status": result.get("status", "running")}


@bot_router.post("/stop")
async def stop_bot(
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    _rl: None = Depends(rate_limit("bot_stop", 3)),
):
    await send_command("stop", user.id, timeout=5.0)

    res = await db.execute(select(BotConfig).where(BotConfig.user_id == user.id))
    cfg = res.scalar_one_or_none()
    if cfg:
        cfg.status = BotStatus.stopped
        cfg.last_stopped = datetime.utcnow()
        db.add(cfg)
        await db.commit()
    return {"ok": True, "status": "stopped"}


@bot_router.get("/status")
async def bot_status(user: User = Depends(get_current_user)):
    status = await state_store.get_bot_status(user.id)
    return {"running": status.get("running", False)}


@bot_router.get("/debug")
async def bot_debug(user: User = Depends(get_current_user)):
    """Returns bot running status + last error — for troubleshooting."""
    status = await state_store.get_bot_status(user.id)
    return {"running": status.get("running", False), "error": status.get("error")}


# ================================================================
# TRADES ROUTER
# ================================================================

from fastapi import APIRouter as _R2

trades_router = _R2(prefix="/api/trades", tags=["trades"])


def _ist_today_utc_bounds():
    """
    Returns (start_utc, end_utc) covering the current trading day in
    IST as naive UTC datetimes — matches how entry_ts/exit_ts are
    stored (datetime.utcnow(), no tzinfo). Used to filter "today's"
    trades correctly regardless of the server's local timezone.
    """
    from zoneinfo import ZoneInfo

    IST = ZoneInfo("Asia/Kolkata")
    now_ist = datetime.utcnow().replace(tzinfo=ZoneInfo("UTC")).astimezone(IST)
    start_ist = now_ist.replace(hour=0, minute=0, second=0, microsecond=0)
    end_ist = start_ist + timedelta(days=1)
    start_utc = start_ist.astimezone(ZoneInfo("UTC")).replace(tzinfo=None)
    end_utc = end_ist.astimezone(ZoneInfo("UTC")).replace(tzinfo=None)
    return start_utc, end_utc


@trades_router.get("/")
async def get_trades(
    limit: int = 25,
    offset: int = 0,
    status: str = None,
    strategy: str = None,
    mode: str = None,
    today: bool = False,
    from_date: str = None,
    to_date: str = None,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    from backend.db.models import Trade
    from sqlalchemy import desc

    q = select(Trade).where(Trade.user_id == user.id)
    if status:
        try:
            q = q.where(Trade.status == TradeStatus(status))
        except ValueError:
            pass
    if strategy:
        q = q.where(Trade.strategy == strategy)
    if mode in ("paper", "live"):
        q = q.where(Trade.mode == mode)
    if today:
        start_utc, end_utc = _ist_today_utc_bounds()
        q = q.where(Trade.entry_ts >= start_utc, Trade.entry_ts < end_utc)
    if from_date:
        try:
            fd = datetime.strptime(from_date, "%Y-%m-%d")
            q = q.where(Trade.entry_ts >= fd)
        except ValueError:
            pass
    if to_date:
        try:
            td = datetime.strptime(to_date, "%Y-%m-%d")
            from datetime import timedelta
            td_end = td + timedelta(days=1)
            q = q.where(Trade.entry_ts < td_end)
        except ValueError:
            pass
    q = q.order_by(desc(Trade.entry_ts)).limit(limit).offset(offset)
    res = await db.execute(q)
    return [
        {
            "id": t.id,
            "trading_symbol": t.trading_symbol,
            "opt_type": t.opt_type,
            "strike": t.strike,
            "qty": t.qty,
            "entry_price": t.entry_price,
            "exit_price": t.exit_price,
            "sl_trigger": t.sl_trigger,
            "target": t.target,
            "pnl": t.pnl,
            "status": t.status.value,
            "strategy": t.strategy,
            "mode": t.mode,
            "entry_ts": (
                (t.entry_ts.strftime("%Y-%m-%dT%H:%M:%SZ")) if t.entry_ts else None
            ),
            "exit_ts": (
                (t.exit_ts.strftime("%Y-%m-%dT%H:%M:%SZ")) if t.exit_ts else None
            ),
        }
        for t in res.scalars().all()
    ]


@trades_router.get("/summary")
async def trade_summary(
    today: bool = False,
    from_date: str = None,
    to_date: str = None,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Returns aggregate trade stats. By default covers ALL trades —
    pass ?today=true to restrict to the current IST trading day
    (used by the dashboard's "Today" card).
    Pass ?from_date=YYYY-MM-DD&to_date=YYYY-MM-DD for date range
    filtering (used by the Trade History page).
    """
    from backend.db.models import Trade
    from sqlalchemy import func, Integer as SAInt, cast
    from datetime import timedelta

    q = select(
        func.count(Trade.id).label("total"),
        func.sum(Trade.pnl).label("total_pnl"),
        func.sum(cast((Trade.pnl > 0), SAInt)).label("winners"),
    ).where(Trade.user_id == user.id)
    if today:
        start_utc, end_utc = _ist_today_utc_bounds()
        q = q.where(Trade.entry_ts >= start_utc, Trade.entry_ts < end_utc)
    if from_date:
        try:
            fd = datetime.strptime(from_date, "%Y-%m-%d")
            q = q.where(Trade.entry_ts >= fd)
        except ValueError:
            pass
    if to_date:
        try:
            td = datetime.strptime(to_date, "%Y-%m-%d")
            td_end = td + timedelta(days=1)
            q = q.where(Trade.entry_ts < td_end)
        except ValueError:
            pass
    res = await db.execute(q)
    row = res.one()
    total = row.total or 0
    winners = row.winners or 0
    return {
        "total": total,
        "total_pnl": round(float(row.total_pnl or 0), 2),
        "winners": winners,
        "win_rate": round(winners / total * 100, 1) if total > 0 else 0.0,
    }


# ================================================================
# ADMIN ROUTER
# ================================================================

from fastapi import APIRouter as _R3
from backend.services.auth_service import get_admin_user

admin_router = _R3(prefix="/api/admin", tags=["admin"])


@admin_router.get("/users")
async def all_users(admin=Depends(get_admin_user), db: AsyncSession = Depends(get_db)):
    res = await db.execute(select(User).order_by(User.created_at.desc()))
    bots = await state_store.get_all_bot_statuses()
    return [
        {
            "id": u.id,
            "email": u.email,
            "full_name": u.full_name,
            "role": u.role.value,
            "is_active": u.is_active,
            "bot_running": bots.get(u.id, {}).get("running", False),
            "has_token": bool(u.upstox_token_enc),
            "created_at": str(u.created_at),
        }
        for u in res.scalars().all()
    ]


@admin_router.post("/users/{user_id}/stop-bot")
async def admin_stop_bot(
    user_id: int, admin=Depends(get_admin_user), db: AsyncSession = Depends(get_db)
):
    await send_command("stop", user_id, timeout=5.0)
    res = await db.execute(select(BotConfig).where(BotConfig.user_id == user_id))
    cfg = res.scalar_one_or_none()
    if cfg:
        cfg.status = BotStatus.stopped
        db.add(cfg)
        await db.commit()
    return {"ok": True}


@admin_router.post("/users/{user_id}/toggle-active")
async def admin_toggle(
    user_id: int, admin=Depends(get_admin_user), db: AsyncSession = Depends(get_db)
):
    res = await db.execute(select(User).where(User.id == user_id))
    user = res.scalar_one_or_none()
    if not user:
        raise HTTPException(404, "User not found")
    user.is_active = not user.is_active
    db.add(user)
    await db.commit()
    return {"ok": True, "is_active": user.is_active}


@admin_router.get("/stats")
async def admin_stats(
    admin=Depends(get_admin_user), db: AsyncSession = Depends(get_db)
):
    from backend.db.models import Trade
    from sqlalchemy import func

    users_res = await db.execute(select(func.count(User.id)))
    trades_res = await db.execute(select(func.count(Trade.id), func.sum(Trade.pnl)))
    t_row = trades_res.one()
    return {
        "total_users": users_res.scalar(),
        "bots_running": await state_store.count_running_bots(),
        "total_trades": t_row[0] or 0,
        "total_pnl": round(float(t_row[1] or 0), 2),
    }


# ================================================================
# ADMIN — EXCHANGE HOLIDAYS & STREAMER SYMBOL TOKENS
#
# Admin-only CRUD. Reads are consumed automatically by the engine/
# worker via backend.services.admin_config_cache — see
# _is_nse_holiday() (history_loader.py), _is_market_hours()
# (engine_v6.py), build_config_dict() (bot_config_builder.py) and
# resolve_history_key()/detect_strike_step() (instruments.py).
# ================================================================

from datetime import datetime as _datetime
from backend.db.models import ExchangeHoliday, StreamerSymbolToken
from backend.services import admin_config_cache


class HolidayIn(BaseModel):
    holiday_date: str  # "YYYY-MM-DD"
    description: Optional[str] = None
    exchange: Optional[str] = "NSE"


class StreamerTokenIn(BaseModel):
    symbol: str
    streamer_token: str
    history_key: Optional[str] = None
    strike_step: Optional[int] = None
    exchange: Optional[str] = "NSE"
    is_active: Optional[bool] = True


def _holiday_out(h: ExchangeHoliday) -> dict:
    return {
        "id": h.id,
        "exchange": h.exchange,
        "holiday_date": h.holiday_date,
        "description": h.description,
    }


def _token_out(t: StreamerSymbolToken) -> dict:
    return {
        "id": t.id,
        "symbol": t.symbol,
        "exchange": t.exchange,
        "streamer_token": t.streamer_token,
        "history_key": t.history_key,
        "strike_step": t.strike_step,
        "is_active": t.is_active,
    }


@admin_router.get("/holidays")
async def list_holidays(
    admin=Depends(get_admin_user), db: AsyncSession = Depends(get_db)
):
    res = await db.execute(
        select(ExchangeHoliday).order_by(ExchangeHoliday.holiday_date)
    )
    return [_holiday_out(h) for h in res.scalars().all()]


@admin_router.post("/holidays")
async def create_holiday(
    body: HolidayIn, admin=Depends(get_admin_user), db: AsyncSession = Depends(get_db)
):
    try:
        _datetime.strptime(body.holiday_date, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(400, "holiday_date must be in YYYY-MM-DD format")
    h = ExchangeHoliday(
        exchange=body.exchange or "NSE",
        holiday_date=body.holiday_date,
        description=body.description,
    )
    db.add(h)
    try:
        await db.commit()
    except Exception:
        await db.rollback()
        raise HTTPException(409, "Holiday already exists for that exchange/date")
    await db.refresh(h)
    admin_config_cache.refresh(force=True)
    return _holiday_out(h)


@admin_router.put("/holidays/{holiday_id}")
async def update_holiday(
    holiday_id: int,
    body: HolidayIn,
    admin=Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    res = await db.execute(
        select(ExchangeHoliday).where(ExchangeHoliday.id == holiday_id)
    )
    h = res.scalar_one_or_none()
    if not h:
        raise HTTPException(404, "Holiday not found")
    try:
        _datetime.strptime(body.holiday_date, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(400, "holiday_date must be in YYYY-MM-DD format")
    h.exchange = body.exchange or "NSE"
    h.holiday_date = body.holiday_date
    h.description = body.description
    db.add(h)
    try:
        await db.commit()
    except Exception:
        await db.rollback()
        raise HTTPException(409, "Holiday already exists for that exchange/date")
    admin_config_cache.refresh(force=True)
    return _holiday_out(h)


@admin_router.delete("/holidays/{holiday_id}")
async def delete_holiday(
    holiday_id: int, admin=Depends(get_admin_user), db: AsyncSession = Depends(get_db)
):
    res = await db.execute(
        select(ExchangeHoliday).where(ExchangeHoliday.id == holiday_id)
    )
    h = res.scalar_one_or_none()
    if not h:
        raise HTTPException(404, "Holiday not found")
    await db.delete(h)
    await db.commit()
    admin_config_cache.refresh(force=True)
    return {"ok": True}


@admin_router.get("/streamer-tokens")
async def list_streamer_tokens(
    admin=Depends(get_admin_user), db: AsyncSession = Depends(get_db)
):
    res = await db.execute(
        select(StreamerSymbolToken).order_by(StreamerSymbolToken.symbol)
    )
    return [_token_out(t) for t in res.scalars().all()]


@admin_router.post("/streamer-tokens")
async def create_streamer_token(
    body: StreamerTokenIn,
    admin=Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    t = StreamerSymbolToken(
        symbol=body.symbol.upper(),
        exchange=body.exchange or "NSE",
        streamer_token=body.streamer_token,
        history_key=body.history_key,
        strike_step=body.strike_step,
        is_active=body.is_active if body.is_active is not None else True,
    )
    db.add(t)
    try:
        await db.commit()
    except Exception:
        await db.rollback()
        raise HTTPException(409, "A streamer token for that symbol already exists")
    await db.refresh(t)
    admin_config_cache.refresh(force=True)
    return _token_out(t)


@admin_router.put("/streamer-tokens/{token_id}")
async def update_streamer_token(
    token_id: int,
    body: StreamerTokenIn,
    admin=Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    res = await db.execute(
        select(StreamerSymbolToken).where(StreamerSymbolToken.id == token_id)
    )
    t = res.scalar_one_or_none()
    if not t:
        raise HTTPException(404, "Streamer token not found")
    t.symbol = body.symbol.upper()
    t.exchange = body.exchange or "NSE"
    t.streamer_token = body.streamer_token
    t.history_key = body.history_key
    t.strike_step = body.strike_step
    t.is_active = body.is_active if body.is_active is not None else True
    db.add(t)
    try:
        await db.commit()
    except Exception:
        await db.rollback()
        raise HTTPException(409, "A streamer token for that symbol already exists")
    admin_config_cache.refresh(force=True)
    return _token_out(t)


@admin_router.delete("/streamer-tokens/{token_id}")
async def delete_streamer_token(
    token_id: int, admin=Depends(get_admin_user), db: AsyncSession = Depends(get_db)
):
    res = await db.execute(
        select(StreamerSymbolToken).where(StreamerSymbolToken.id == token_id)
    )
    t = res.scalar_one_or_none()
    if not t:
        raise HTTPException(404, "Streamer token not found")
    await db.delete(t)
    await db.commit()
    admin_config_cache.refresh(force=True)
    return {"ok": True}


@router.post("/execution-mode")
async def set_execution_mode(
    body: dict,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Set the user's execution mode to PAPER, SEMI_AUTO, or AUTO.
    Also updates BotConfig.execution_mode so the engine picks it up
    on next start.
    """
    mode_raw = body.get("execution_mode", "").strip().upper()
    if mode_raw not in {"PAPER", "SEMI_AUTO", "AUTO"}:
        raise HTTPException(400, "execution_mode must be PAPER, SEMI_AUTO, or AUTO")

    old_mode = None
    res = await db.execute(select(BotConfig).where(BotConfig.user_id == user.id))
    cfg = res.scalar_one_or_none()
    if cfg:
        old_mode = (
            cfg.execution_mode.value
            if hasattr(cfg.execution_mode, "value")
            else str(cfg.execution_mode)
        )
        cfg.execution_mode = ExecutionMode(mode_raw)
        cfg.paper_mode = mode_raw == ExecutionMode.PAPER.value
        db.add(cfg)

    # Also set on the user model for per-user default
    user_id_row = await db.execute(select(User).where(User.id == user.id))
    user_row = user_id_row.scalar_one_or_none()
    if user_row:
        user_row.execution_mode = ExecutionMode(mode_raw)
        db.add(user_row)

    await db.commit()

    await log_event(
        db,
        user.id,
        "execution_mode_changed",
        f"Execution mode changed: {old_mode or 'N/A'} → {mode_raw}",
        metadata={"old_mode": old_mode, "new_mode": mode_raw},
    )

    return {"ok": True, "execution_mode": mode_raw}


@router.get("/consent-history")
async def get_consent_history(
    user: User = Depends(get_current_user), db: AsyncSession = Depends(get_db)
):
    """Returns the user's consent history."""
    res = await db.execute(
        select(DailyAutoConsent)
        .where(DailyAutoConsent.user_id == user.id)
        .order_by(DailyAutoConsent.created_at.desc())
        .limit(100)
    )
    return [
        {
            "id": r.id,
            "accepted": r.accepted,
            "accepted_at": r.accepted_at.isoformat() if r.accepted_at else None,
            "valid_until": r.valid_until.isoformat() if r.valid_until else None,
            "risk_version": r.risk_version,
            "risk_text_snapshot": r.risk_text_snapshot,
            "created_at": r.created_at.isoformat(),
        }
        for r in res.scalars().all()
    ]


# ================================================================
# ADMIN — COMPLIANCE / AUDIT PORTAL
# ================================================================


@admin_router.get("/compliance/consent-history")
async def admin_consent_history(
    user_id: Optional[int] = None,
    limit: int = 100,
    admin=Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    """View consent history for a user or all users."""
    q = select(DailyAutoConsent).order_by(DailyAutoConsent.created_at.desc())
    if user_id:
        q = q.where(DailyAutoConsent.user_id == user_id)
    q = q.limit(limit)
    res = await db.execute(q)
    return [
        {
            "id": r.id,
            "user_id": r.user_id,
            "accepted": r.accepted,
            "accepted_at": r.accepted_at.isoformat() if r.accepted_at else None,
            "valid_until": r.valid_until.isoformat() if r.valid_until else None,
            "risk_version": r.risk_version,
            "audit_hash": r.audit_hash,
            "created_at": r.created_at.isoformat(),
        }
        for r in res.scalars().all()
    ]


@admin_router.get("/compliance/audit-logs")
async def admin_audit_logs(
    user_id: Optional[int] = None,
    event_type: Optional[str] = None,
    limit: int = 200,
    admin=Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    """View audit logs with optional filters."""
    from backend.db.models import AuditLog

    q = select(AuditLog).order_by(AuditLog.created_at.desc())
    if user_id:
        q = q.where(AuditLog.user_id == user_id)
    if event_type:
        q = q.where(AuditLog.event_type == event_type)
    q = q.limit(limit)
    res = await db.execute(q)
    return [
        {
            "id": r.id,
            "user_id": r.user_id,
            "event_type": r.event_type,
            "description": r.description,
            "log_metadata": r.log_metadata,
            "created_at": r.created_at.isoformat(),
        }
        for r in res.scalars().all()
    ]


@admin_router.get("/compliance/audit-log-event-types")
async def admin_audit_log_event_types(
    admin=Depends(get_admin_user), db: AsyncSession = Depends(get_db)
):
    """Get distinct event types from audit logs."""
    from backend.db.models import AuditLog
    from sqlalchemy import distinct

    res = await db.execute(select(distinct(AuditLog.event_type)))
    return [row[0] for row in res.all()]


# ================================================================
# WEBSOCKET ROUTER
#
# Each connection subscribes (via broadcaster.manager) to this user's
# Redis pub/sub channel — events published by the worker process
# (ENTRY/EXIT/SL_TRAIL/BOT_STATUS) are relayed here in real time,
# regardless of which process/instance is running the bot.
# ================================================================

from fastapi import APIRouter as _R4, WebSocket, WebSocketDisconnect
from backend.services.broadcaster import manager as ws_manager

ws_router = _R4(tags=["websocket"])


@ws_router.websocket("/ws/{user_id}")
async def websocket_endpoint(websocket: WebSocket, user_id: int, token: str = None):
    from backend.services.auth_service import decode_token

    try:
        payload = decode_token(token or "")
        if str(payload.get("sub")) != str(user_id):
            await websocket.close(code=4001)
            return
        if payload.get("type") != "access":
            await websocket.close(code=4001)
            return
    except Exception:
        await websocket.close(code=4001)
        return

    await ws_manager.connect(user_id, websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        await ws_manager.disconnect(user_id, websocket)


# ================================================================
# OPTION CHAIN ROUTER
#
# Reads the latest OC snapshot from Redis (pushed by the worker
# process's SymbolEngine._push_oc_snapshot every ~30s). No direct
# engine access needed — works regardless of which process the
# worker is running in.
# ================================================================

from fastapi import APIRouter as _R5

oc_router = _R5(prefix="/api/oc", tags=["option-chain"])


@oc_router.get("/analysis")
async def oc_analysis(user: User = Depends(get_current_user)):
    """
    Returns latest OC analysis + last chain DataFrame rows.
    Used by oc_dashboard.html frontend.
    """
    snap = await state_store.get_oc_snapshot(user.id)
    return {"analysis": snap.get("analysis"), "chain_df": snap.get("chain_df")}


# ================================================================
# PUSH NOTIFICATIONS  (Web Push / VAPID)
# ================================================================

from fastapi import APIRouter as _R6
from backend.db.models import PushSubscription

push_router = _R6(prefix="/api/push", tags=["push"])


class PushSubscriptionIn(BaseModel):
    endpoint: str
    keys: dict  # {"p256dh": "...", "auth": "..."}


@push_router.get("/vapid-public-key")
async def vapid_public_key():
    """
    Public — the VAPID public key is safe to expose (it's sent to
    every browser that subscribes). Frontend uses this with
    PushManager.subscribe({applicationServerKey: ...}).
    Returns enabled=False if the server hasn't configured VAPID keys
    (see scripts/generate_vapid_keys.py) — frontend should hide the
    "Enable Push" button in that case.
    """
    s = get_settings()
    return {"enabled": s.push_enabled, "public_key": s.VAPID_PUBLIC_KEY}


@push_router.post("/subscribe")
async def push_subscribe(
    body: PushSubscriptionIn,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Saves (or updates) a browser's push subscription. endpoint is
    unique — re-subscribing the same browser (e.g. after clearing
    site data) upserts rather than duplicating.
    """
    p256dh = body.keys.get("p256dh", "")
    auth = body.keys.get("auth", "")
    if not body.endpoint or not p256dh or not auth:
        raise HTTPException(400, "Invalid subscription payload")

    res = await db.execute(
        select(PushSubscription).where(PushSubscription.endpoint == body.endpoint)
    )
    existing = res.scalar_one_or_none()
    if existing:
        existing.user_id = user.id
        existing.p256dh = p256dh
        existing.auth = auth
        db.add(existing)
    else:
        db.add(
            PushSubscription(
                user_id=user.id, endpoint=body.endpoint, p256dh=p256dh, auth=auth
            )
        )
    await db.commit()
    return {"ok": True}


@push_router.post("/unsubscribe")
async def push_unsubscribe(
    body: dict,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    endpoint = body.get("endpoint", "")
    if not endpoint:
        raise HTTPException(400, "endpoint required")
    res = await db.execute(
        select(PushSubscription).where(
            PushSubscription.endpoint == endpoint, PushSubscription.user_id == user.id
        )
    )
    sub = res.scalar_one_or_none()
    if sub:
        await db.delete(sub)
        await db.commit()
    return {"ok": True}


@push_router.get("/status")
async def push_status(
    user: User = Depends(get_current_user), db: AsyncSession = Depends(get_db)
):
    """Returns whether this user has any active push subscriptions."""
    res = await db.execute(
        select(PushSubscription).where(PushSubscription.user_id == user.id)
    )
    subs = res.scalars().all()
    return {"subscribed": len(subs) > 0, "device_count": len(subs)}


@push_router.post("/test")
async def push_test(
    user: User = Depends(get_current_user),
    _rl: None = Depends(rate_limit("push_test", 10)),
):
    """Sends a test push notification to all of this user's devices."""
    from backend.services.push_notifications import send_push_sync

    s = get_settings()
    if not s.push_enabled:
        raise HTTPException(
            400,
            "Push notifications not configured on the server "
            "(VAPID keys missing) — see scripts/generate_vapid_keys.py",
        )
    send_push_sync(
        user.id,
        "AlgoBot",
        "🔔 Test notification — push alerts are working!",
        tag="test",
    )
    return {"ok": True}
