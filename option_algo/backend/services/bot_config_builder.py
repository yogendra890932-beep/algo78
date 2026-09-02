# backend/services/bot_config_builder.py
# ================================================================
# Builds the config dict passed to TradingEngine, and resolves the
# decrypted Upstox access token, from DB rows.
#
# Used by worker.py when it receives a "start" command — the worker
# fetches User + BotConfig itself (rather than receiving the
# decrypted access token over Redis), keeping the access token off
# the wire between web and worker processes.
# ================================================================

from datetime import datetime
from typing import Optional
from sqlalchemy import select

from backend.db.database import AsyncSessionLocal
from backend.db.models import User, BotConfig, ExecutionMode, DailyAutoConsent
from backend.services.auth_service import decrypt_token


async def load_user_and_config(user_id: int):
    """Returns (User, BotConfig) or (None, None) if either is missing."""
    async with AsyncSessionLocal() as db:
        ures = await db.execute(select(User).where(User.id == user_id))
        user = ures.scalar_one_or_none()
        if not user:
            return None, None
        cres = await db.execute(select(BotConfig).where(BotConfig.user_id == user_id))
        cfg = cres.scalar_one_or_none()
        return user, cfg


def _parse_lot_sizes(raw: Optional[str]) -> dict:
    """
    Parses the custom_lot_sizes JSON string into a dict.
    Returns {} on any error so missing/invalid config is silently ignored
    (the engine falls back to NSE_LOT_SIZES built-in table).
    """
    if not raw:
        return {}
    try:
        import json

        data = json.loads(raw)
        return (
            {k.upper(): int(v) for k, v in data.items()}
            if isinstance(data, dict)
            else {}
        )
    except Exception:
        return {}


def _parse_extra_symbol_config(raw: Optional[str]) -> list:
    """
    Parses the extra_symbol_config JSON string into a list of dicts.
    Each entry: {"symbol":"BANKNIFTY","enabled":true,"trade_mode":"SEMI_AUTO","lots":2}
    Returns [] on any error so missing/invalid config is silently ignored
    (trading continues with main symbol only).
    """
    if not raw:
        return []
    try:
        import json
        data = json.loads(raw)
        if isinstance(data, list):
            validated = []
            for entry in data:
                if isinstance(entry, dict) and entry.get("symbol"):
                    validated.append({
                        "symbol": str(entry["symbol"]).strip().upper(),
                        "enabled": bool(entry.get("enabled", True)),
                        "trade_mode": str(entry.get("trade_mode", "SEMI_AUTO")).upper(),
                        "lots": max(1, int(entry.get("lots", 1))),
                    })
            return validated
        return []
    except Exception:
        return []


def _resolve_underlying_token(symbol: str, stored_token: Optional[str]) -> str:
    """
    Streamer token is auto-selected from the admin-managed
    StreamerSymbolToken table for `symbol` (see backend.services.
    admin_config_cache) — no longer a user-editable setting. Falls
    back to a previously stored per-user value (legacy configs saved
    before this was admin-managed), then a hardcoded default.
    """
    from backend.services.admin_config_cache import get_streamer_token

    row = get_streamer_token(symbol)
    if row and row.get("streamer_token"):
        return row["streamer_token"]
    return stored_token or "NSE_INDEX|NIFTY 50"


def _normalize_execution_mode(cfg: BotConfig) -> str:
    """Return the execution mode string for the worker config."""
    if getattr(cfg, "execution_mode", None):
        return (
            cfg.execution_mode.value
            if hasattr(cfg.execution_mode, "value")
            else str(cfg.execution_mode)
        )
    if cfg.paper_mode is None or cfg.paper_mode:
        return ExecutionMode.PAPER.value
    return ExecutionMode.AUTO.value


def build_config_dict(cfg: BotConfig) -> dict:
    """Same shape as the previous inline dict in all_routers.start_bot()."""
    underlying_symbol = cfg.underlying_symbol or "NIFTY"
    return {
        "underlying_symbol": underlying_symbol,
        "underlying_token": _resolve_underlying_token(
            underlying_symbol, cfg.underlying_token
        ),
        "itm_depth": cfg.itm_depth or 1,
        "strategy": cfg.strategy or "both",
        "order_qty": cfg.order_qty or 25,
        "product": getattr(cfg, "product", "I") or "I",
        "trail_mode": cfg.trail_mode or "atr",
        "target_rr": cfg.target_rr or 2.5,
        "sl_pct": cfg.sl_pct or 0.003,
        "target_near_pct": 0.003,
        "max_trades_per_day": cfg.max_trades_per_day or 5,
        "max_loss_per_day": cfg.max_loss_per_day or 5000.0,
        "trade_start_time": cfg.trade_start_time or "09:20",
        "trade_end_time": cfg.trade_end_time or "15:00",
        "reentry_cooldown_sec": 120,
        "execution_mode": _normalize_execution_mode(cfg),
        # Paper trading — derive from canonical execution_mode to keep runtime behavior correct.
        "paper_mode": _normalize_execution_mode(cfg) == ExecutionMode.PAPER.value,
        # Telegram
        "telegram_bot_token": getattr(cfg, "telegram_bot_token", None) or "",
        "telegram_chat_id": getattr(cfg, "telegram_chat_id", None) or "",
        "telegram_on_entry": getattr(cfg, "telegram_on_entry", True),
        "telegram_on_exit": getattr(cfg, "telegram_on_exit", True),
        "telegram_on_trail": getattr(cfg, "telegram_on_trail", False),
        "telegram_on_summary": getattr(cfg, "telegram_on_summary", True),
        # Multi-symbol
        "extra_symbols": getattr(cfg, "extra_symbols", None) or "",
        "extra_tokens": getattr(cfg, "extra_tokens", None) or "",
        # Per-symbol lot-size overrides (JSON string → dict).
        # Overrides the built-in NSE_LOT_SIZES table in engine_v6.py.
        # Example: '{"NIFTY":75,"BANKNIFTY":30}'
        "custom_lot_sizes": _parse_lot_sizes(getattr(cfg, "custom_lot_sizes", None)),
        # Per-symbol independent lot configuration for additional symbols.
        # Each entry: {"symbol":"BANKNIFTY","enabled":true,"trade_mode":"SEMI_AUTO","lots":2}
        # The main symbol always uses "order_qty" for its lots.
        "extra_symbol_configs": _parse_extra_symbol_config(
            getattr(cfg, "extra_symbol_config", None)
        ),
    }


async def resolve_start_inputs(
    user_id: int,
) -> tuple[Optional[dict], Optional[str], Optional[str]]:
    """
    Returns (config_dict, access_token, error).
    error is a human-readable string if start should be rejected
    (no token / no config / inactive user / expired token).
    """
    from backend.services.upstox_oauth import is_token_valid
    from backend.engine.history_loader import is_market_open

    if not is_market_open():
        return (
            None,
            None,
            (
                "Market is closed — bot can only be started on NSE "
                "trading days between 9:15 AM and 3:30 PM IST"
            ),
        )

    user, cfg = await load_user_and_config(user_id)
    if not user:
        return None, None, "User not found"
    # Admins are always pre-verified (set by init_db.py). Skip the
    # email check for them to avoid locking out the admin on deployments
    # where the email_verified backfill hasn't run yet.
    from backend.db.models import UserRole

    if user.role != UserRole.admin and not user.email_verified:
        return (
            None,
            None,
            (
                "Email not verified — check your inbox for the "
                "verification link, or click 'Resend verification email' in Settings"
            ),
        )
    if not user.is_active:
        return None, None, "Account disabled by administrator"
    if not user.upstox_token_enc:
        return None, None, "Upstox access token not set — connect Upstox in Settings"
    if not is_token_valid(user.upstox_token_expires_at):
        return (
            None,
            None,
            (
                "Upstox access token has expired (valid until ~3:30 AM IST) "
                "— click 'Refresh Token' in Settings"
            ),
        )
    if not cfg:
        return None, None, "Bot config not found"

    exec_mode = _normalize_execution_mode(cfg)
    if exec_mode == ExecutionMode.AUTO.value:
        async with AsyncSessionLocal() as consent_db:
            res = await consent_db.execute(
                select(DailyAutoConsent)
                .where(DailyAutoConsent.user_id == user.id)
                .order_by(DailyAutoConsent.created_at.desc())
                .limit(1)
            )
            consent = res.scalar_one_or_none()
        if (
            not consent
            or not consent.accepted
            or not consent.valid_until
            or consent.valid_until <= datetime.utcnow()
        ):
            return (
                None,
                None,
                (
                    "Fully automatic trading requires a valid daily consent record. "
                    "Please accept the daily consent in Settings before starting."
                ),
            )

    # ── Subscription / trial enforcement ──────────────────────────
    # See backend.services.subscription_service.check_trading_permission —
    # this is the boundary that keeps subscription logic out of
    # backend.engine.engine_v6 entirely. Runs on a fresh session since
    # load_user_and_config() already closed its own.
    # Admins are exempt (same precedent as the email-verified check
    # above) — they're the platform operator, not a paying customer,
    # and already have full override tools in the admin billing panel.
    force_paper = False
    if user.role != UserRole.admin:
        from backend.services.subscription_service import check_trading_permission

        requested_symbols = [cfg.underlying_symbol or "NIFTY"]
        requested_symbols += [
            s.strip() for s in (cfg.extra_symbols or "").split(",") if s.strip()
        ]
        is_paper = cfg.paper_mode if cfg.paper_mode is not None else True
        async with AsyncSessionLocal() as sub_db:
            allowed, reason, force_paper = await check_trading_permission(
                sub_db, user_id, requested_symbols, cfg.order_qty or 1, is_paper
            )
        if not allowed:
            return None, None, reason

    try:
        access_token = decrypt_token(user.upstox_token_enc)
        print(f"[bot_config_builder] Decrypted token for user {user_id}: {access_token[:20]}...{access_token[-10:] if len(access_token) > 30 else '***'}")
    except Exception as e:
        print(f"[bot_config_builder] Decrypt FAILED for user {user_id}: {e}")
        return None, None, f"Failed to decrypt Upstox token: {e}"

    config = build_config_dict(cfg)
    if force_paper:
        # Trial users: paper trading only, regardless of their saved
        # BotConfig.paper_mode — enforced once here since paper_mode
        # is read once at engine construction and never re-toggled.
        config["paper_mode"] = True
    return config, access_token, None
