# backend/services/terminal_service.py
# ================================================================
# Advanced Trading Terminal — read-only data service.
#
# PURE PRESENTATION LAYER. This module never executes trades, never
# runs strategies and never duplicates engine logic. It only reads
# the SAME Redis keys the shared worker process writes, so the
# terminal displays live engine output with ZERO impact on the
# existing platform (execution, strategy, risk, market data).
#
# The ITM strike selection mirrors the rule already implemented in
# SharedOptionPremiumBuilder (ITM_DEPTH=1 → underlying above ATM →
# ITM CE, below ATM → ITM PE). No new selection logic is introduced.
# ================================================================

import json
from typing import Optional

from sqlalchemy import select

from backend.services.redis_client import get_redis
from backend.services import state_store
from backend.shared.redis_infra import (
    shared_premium_candles_1m,
    shared_premium_current_1m,
    shared_premium_state,
    shared_indicators,
    shared_indicators_1m,
    shared_indicators_5m,
    shared_market_structure_1m,
    shared_market_structure_5m,
    shared_tick_buffer,
    shared_candles_1m,
    shared_candles_5m,
)
from backend.engine.instruments import KNOWN_STEPS
from backend.shared.shared_cache import get_lot_size

ITM_DEPTH = 1  # mirrors SharedOptionPremiumBuilder.ITM_DEPTH


def _f(v, default=None):
    try:
        if v is None:
            return default
        return float(v)
    except (TypeError, ValueError):
        return default


def _i(v, default=None):
    try:
        if v is None:
            return default
        return int(v)
    except (TypeError, ValueError):
        return default


def _d(v, default=None):
    """Best-effort decimal parse that tolerates str/None/empty."""
    try:
        if v is None:
            return default
        if isinstance(v, (int, float)):
            return round(float(v), 4)
        s = str(v).strip()
        if not s:
            return default
        return round(float(s), 4)
    except (TypeError, ValueError):
        return default


async def get_premium_state(symbol: str) -> Optional[dict]:
    """Return the option the shared premium builder selected for `symbol`."""
    try:
        raw = await get_redis().hgetall(shared_premium_state(symbol))
    except Exception:
        return None
    if not raw:
        return None
    out = {}
    for k, v in raw.items():
        kk = k.decode() if isinstance(k, bytes) else k
        vv = v.decode() if isinstance(v, bytes) else v
        out[kk] = _d(vv, vv) if kk not in ("trading_symbol", "opt_type", "expiry_str") else vv
    return out


async def get_premium_candles(symbol: str) -> list:
    """1m premium candles for the selected option (JSON list)."""
    try:
        raw = await get_redis().get(shared_premium_candles_1m(symbol))
    except Exception:
        return []
    if not raw:
        return []
    try:
        return json.loads(raw)
    except Exception:
        return []


async def get_premium_current(symbol: str) -> Optional[dict]:
    """Developing 1m premium candle (open/high/low/close/volume/minute)."""
    try:
        raw = await get_redis().hgetall(shared_premium_current_1m(symbol))
    except Exception:
        return None
    if not raw:
        return None
    out = {}
    for k, v in raw.items():
        kk = k.decode() if isinstance(k, bytes) else k
        vv = v.decode() if isinstance(v, bytes) else v
        out[kk] = _d(vv, vv) if kk != "minute" else vv
    return out or None


async def _read_redis_hash(key: str) -> dict:
    """Decode a Redis hash into a plain dict (strings kept, numerics parsed)."""
    try:
        raw = await get_redis().hgetall(key)
    except Exception:
        return {}
    if not raw:
        return {}
    out = {}
    for k, v in raw.items():
        kk = k.decode() if isinstance(k, bytes) else k
        vv = v.decode() if isinstance(v, bytes) else v
        out[kk] = _d(vv, vv)
    return out


async def _read_redis_json(key: str):
    try:
        raw = await get_redis().get(key)
    except Exception:
        return None
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return None


async def get_underlying_snapshot(symbol: str) -> dict:
    """Underlying index last price + day change from the live tick buffer."""
    token = None
    try:
        from backend.services.admin_config_cache import get_streamer_token
        row = get_streamer_token(symbol)
        token = row.get("streamer_token") if row else None
    except Exception:
        token = None

    ltp = prev = None
    if token:
        try:
            raw = await get_redis().hget(shared_tick_buffer(symbol), str(token))
        except Exception:
            raw = None
        if raw:
            try:
                tick = json.loads(raw)
                ltp = _f(tick.get("ltp"))
            except Exception:
                pass

    # Fallback to last candle close
    candles = await _read_redis_json(shared_candles_1m(symbol))
    if candles:
        if ltp is None:
            ltp = _f(candles[-1].get("close"))
        if prev is None and len(candles) > 1:
            prev = _f(candles[-2].get("close"))

    change = (ltp - prev) if (ltp is not None and prev) else None
    change_pct = (change / prev * 100.0) if (change is not None and prev) else None
    return {
        "symbol": symbol,
        "token": token,
        "ltp": round(ltp, 2) if ltp is not None else None,
        "prev_close": round(prev, 2) if prev is not None else None,
        "change": round(change, 2) if change is not None else None,
        "change_pct": round(change_pct, 2) if change_pct is not None else None,
    }


async def compute_itm(symbol: str, underlying_ltp: Optional[float]) -> Optional[dict]:
    """
    Auto strike selection: round the underlying to the nearest strike step
    → ATM, then ITM CE = ATM - step, ITM PE = ATM + step (ITM_DEPTH=1).
    Mirrors get_itm_instrument() behaviour used by the premium builder.
    """
    if underlying_ltp is None:
        return None
    step = KNOWN_STEPS.get(symbol.upper())
    if step is None:
        try:
            from backend.services.admin_config_cache import get_streamer_token
            row = get_streamer_token(symbol)
            step = row.get("strike_step") if row else 50
        except Exception:
            step = 50
    atm = round(underlying_ltp / step) * step
    return {
        "strike_step": step,
        "underlying": round(underlying_ltp, 2),
        "atm": atm,
        "itm_ce": atm - ITM_DEPTH * step,
        "itm_pe": atm + ITM_DEPTH * step,
    }


async def get_user_positions(user_id: int) -> list:
    try:
        snap = await state_store.get_positions(user_id)
        return snap.get("positions", [])
    except Exception:
        return []


async def get_pending_trades(user_id: int, symbol: Optional[str] = None) -> list:
    """Latest semi-auto pending trade awaiting approval (WAITING, not yet
    expired). Only the most recent one is surfaced to the terminal."""
    try:
        from backend.db.database import AsyncSessionLocal
        from backend.db.models import PendingTrade, PendingTradeStatus
        from sqlalchemy import or_, select
        from datetime import datetime

        now = datetime.utcnow()
        async with AsyncSessionLocal() as db:
            q = (
                select(PendingTrade)
                .where(
                    PendingTrade.user_id == user_id,
                    PendingTrade.status == PendingTradeStatus.WAITING,
                    or_(
                        PendingTrade.expires_at.is_(None),
                        PendingTrade.expires_at > now,
                    ),
                )
                .order_by(PendingTrade.created_at.desc())
                .limit(1)
            )
            if symbol:
                sym = str(symbol).upper()
                q = q.where(
                    or_(
                        PendingTrade.symbol == sym,
                        PendingTrade.symbol.like(sym + " %"),
                    )
                )
            rows = (await db.execute(q)).scalars().all()
            return [
                {
                    "id": row.id,
                    "symbol": row.symbol,
                    "opt_type": row.opt_type,
                    "strategy": row.strategy,
                    "entry_price": _d(row.entry_price),
                    "stop_loss": _d(row.stop_loss),
                    "quantity": row.quantity,
                    "confidence": _d(row.confidence),
                    "status": row.status.value,
                    "expires_at": row.expires_at.isoformat() if row.expires_at else None,
                    "created_at": row.created_at.isoformat(),
                }
                for row in rows
            ]
    except Exception:
        return []


async def build_symbol_snapshot(user_id: int, symbol: str) -> dict:
    """Full read-only snapshot for one symbol used by the terminal chart + panels."""
    symbol = (symbol or "").upper()
    candles = await _read_redis_json(shared_candles_1m(symbol)) or []
    candles5m = await _read_redis_json(shared_candles_5m(symbol)) or []

    underlying = await get_underlying_snapshot(symbol)
    state = await get_premium_state(symbol)
    current = await get_premium_current(symbol)
    premium_candles = await get_premium_candles(symbol)
    itm = await compute_itm(symbol, underlying.get("ltp"))

    ind_1m = await _read_redis_hash(shared_indicators_1m(symbol))
    ind_5m = await _read_redis_hash(shared_indicators_5m(symbol))
    base_ind = await _read_redis_hash(shared_indicators(symbol))

    struct_1m = await _read_redis_json(shared_market_structure_1m(symbol))
    struct_5m = await _read_redis_json(shared_market_structure_5m(symbol))

    positions = await get_user_positions(user_id)
    pending_trades = await get_pending_trades(user_id, symbol)

    return {
        "symbol": symbol,
        "lot_size": get_lot_size(symbol),
        "underlying": underlying,
        "itm": itm,
        "premium_state": state,
        "premium_current": current,
        "premium_candles": premium_candles,
        "candles": candles,
        "candles_5m": candles5m,
        "indicators_1m": ind_1m,
        "indicators_5m": ind_5m,
        "indicators": base_ind,
        "structure_1m": struct_1m,
        "structure_5m": struct_5m,
        "positions": positions,
        "pending_trades": pending_trades,
    }


async def get_available_symbols() -> list:
    """Symbols the user can watch — known indices + admin-configured tokens."""
    names = set()
    names.update(k.upper() for k in KNOWN_STEPS.keys())
    try:
        from backend.services.admin_config_cache import get_all_streamer_tokens
        names.update(k.upper() for k in get_all_streamer_tokens().keys())
    except Exception:
        pass
    ordered = sorted(names, key=lambda s: (s != "NIFTY", s))
    out = []
    for s in ordered:
        out.append({
            "symbol": s,
            "lot_size": get_lot_size(s),
        })
    return out


async def build_bootstrap(user, db) -> dict:
    """Initial payload for the terminal page (config, symbols, positions)."""
    from backend.db.models import BotConfig

    result = await db.execute(select(BotConfig).where(BotConfig.user_id == user.id))
    cfg = result.scalar_one_or_none()

    symbols = []
    if cfg is not None:
        main = (cfg.underlying_symbol or "NIFTY").upper()
        symbols.append(main)
        for s in (cfg.extra_symbols or "").split(","):
            s = s.strip().upper()
            if s and s not in symbols:
                symbols.append(s)
    if not symbols:
        symbols = ["NIFTY"]

    config = {}
    if cfg is not None:
        config = {
            "underlying_symbol": cfg.underlying_symbol,
            "extra_symbols": cfg.extra_symbols or "",
            "strategy": cfg.strategy,
            "order_qty": cfg.order_qty,
            "execution_mode": cfg.execution_mode,
            "paper_mode": cfg.paper_mode,
            "sl_pct": cfg.sl_pct,
            "target_rr": cfg.target_rr,
            "trail_mode": cfg.trail_mode,
            "itm_depth": cfg.itm_depth,
            "max_trades_per_day": cfg.max_trades_per_day,
            "max_loss_per_day": cfg.max_loss_per_day,
            "trade_start_time": cfg.trade_start_time,
            "trade_end_time": cfg.trade_end_time,
        }

    bot_status = await state_store.get_bot_status(user.id)

    symbol_info = {}
    for sym in symbols:
        state = await get_premium_state(sym)
        underlying = await get_underlying_snapshot(sym)
        itm = await compute_itm(sym, underlying.get("ltp"))
        symbol_info[sym] = {
            "symbol": sym,
            "lot_size": get_lot_size(sym),
            "underlying": underlying,
            "itm": itm,
            "premium_state": state,
            "active_option": (state or {}).get("trading_symbol"),
        }

    return {
        "user": {
            "id": user.id,
            "email": user.email,
            "name": getattr(user, "name", None) or (user.email or "").split("@")[0],
            "role": getattr(user, "role", None),
        },
        "config": config,
        "bot_running": bool(bot_status),
        "bot_status": bot_status,
        "symbols": symbol_info,
        "available_symbols": await get_available_symbols(),
        "positions": await get_user_positions(user.id),
        "pending_trades": await get_pending_trades(user.id),
        "itm_depth": ITM_DEPTH,
    }
