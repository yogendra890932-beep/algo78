# backend/routers/terminal.py
# ================================================================
# Advanced Trading Terminal API.
#
#   GET  /api/terminal/bootstrap          — page config, symbols, positions
#   GET  /api/terminal/snapshot?symbol=   — full read-only symbol snapshot
#   POST /api/terminal/order              — terminal order (reuses the SAME
#                                           command-queue used by /api/position)
#   WS   /ws/terminal                     — live stream (ticks, premium bars,
#                                           signals, positions)
#
# This is an ADDITIVE module. It only reads existing Redis snapshots
# (the ones the worker writes for the rest of the platform) and pushes
# commands onto the existing Redis command queue. No existing page,
# engine or workflow is modified.
# ================================================================

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, WebSocket, WebSocketDisconnect
from pydantic import BaseModel

from backend.db.database import get_db
from backend.db.models import User
from backend.services.auth_service import get_current_user
from backend.services.command_queue import send_command
from backend.services.redis_client import get_redis
from backend.services import state_store
from backend.services.terminal_service import (
    build_bootstrap,
    build_symbol_snapshot,
)
from backend.services.terminal_ws import handle_terminal_ws
from sqlalchemy.ext.asyncio import AsyncSession

router = APIRouter(tags=["terminal"])

# ── Order validation constants ─────────────────────────────────
TICK_SIZE = 0.05          # NSE/BSE equity-option premium tick size
MODIFY_LOCK_TTL_SEC = 8   # per-level concurrency guard window


# ── Request models ──────────────────────────────────────────────

class TerminalOrderRequest(BaseModel):
    """Order issued from the terminal panel.

    action:  modify_sl | modify_target | squareoff | pause | resume
    """
    action: str
    symbol: Optional[str] = None
    value: Optional[float] = None


# ── Order helpers (defence-in-depth; the worker/engine stays authoritative) ──

def _snap_tick(value: float) -> float:
    """Snap a premium value to the tick size (mirrors front-end snapping)."""
    try:
        return round(round(float(value) / TICK_SIZE) * TICK_SIZE, 2)
    except (TypeError, ValueError):
        raise HTTPException(400, "value must be a number")


async def _find_open_position(user_id: int, symbol: str) -> Optional[dict]:
    """Locate the user's open position for `symbol` in the shared snapshot.

    Never trusts front-end identity — the position object is resolved from
    the same Redis snapshot the whole platform reads.
    """
    symbol = (symbol or "").upper()
    snap = await state_store.get_positions(user_id)
    for p in snap.get("positions", []) or []:
        pos = p.get("position") if isinstance(p, dict) and "position" in p else p
        if not isinstance(pos, dict):
            continue
        if str((pos or {}).get("symbol", "")).upper() == symbol:
            return pos
    return None


async def _acquire_modify_lock(user_id: int, symbol: str, level: str) -> bool:
    """Per-user / per-symbol / per-level concurrency guard.

    SL and Target are separate levels so they can be adjusted back-to-back;
    a second modification of the SAME level is rejected while one is live.
    """
    try:
        r = get_redis()
        key = f"term:lock:{user_id}:{symbol.upper()}:{level}"
        ok = await r.set(key, "1", nx=True, ex=MODIFY_LOCK_TTL_SEC)
        return bool(ok)
    except Exception:
        return True  # fail-open only on Redis unavailability


def _validate_level(action: str, value: float, pos: dict):
    """Side-aware price validation for SL / Target modifications.

    Engine positions are long (BUY) premium; rules are still written for
    both sides so a short position is handled correctly too.
    """
    try:
        entry = float(pos.get("entry_price"))
    except (TypeError, ValueError):
        entry = None
    if entry is None:
        return False, "Position entry price is unavailable"

    side = str(pos.get("side") or "BUY").upper()

    def _f(v):
        try:
            return float(v) if v is not None else None
        except (TypeError, ValueError):
            return None

    tgt = _f(pos.get("target"))
    sl = _f(pos.get("sl_trigger"))

    if action == "modify_sl":
        if side == "SELL":
            if value <= entry:
                return False, f"SELL SL must stay above entry {entry}"
            if tgt is not None and value <= tgt:
                return False, f"SELL SL must stay above target {tgt}"
        else:
            if value >= entry:
                return False, f"SL must stay below entry {entry}"
            if tgt is not None and value >= tgt:
                return False, f"SL must stay below target {tgt}"
    else:  # modify_target
        if side == "SELL":
            if value >= entry:
                return False, f"SELL target must stay below entry {entry}"
            if sl is not None and value >= sl:
                return False, f"SELL target must stay below SL {sl}"
        else:
            if value <= entry:
                return False, f"Target must stay above entry {entry}"
            if sl is not None and value <= sl:
                return False, f"Target must stay above SL {sl}"
    return True, ""


# ── Read endpoints ──────────────────────────────────────────────

@router.get("/api/terminal/bootstrap")
async def terminal_bootstrap(
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Everything the terminal needs on first load."""
    payload = await build_bootstrap(user, db)
    return {"ok": True, **payload}


@router.get("/api/terminal/snapshot")
async def terminal_snapshot(
    symbol: str = "NIFTY",
    user: User = Depends(get_current_user),
):
    """Full read-only snapshot for one symbol (chart + panels)."""
    snap = await build_symbol_snapshot(user.id, symbol)
    return {"ok": True, **snap}


# ── Order endpoints (web → Redis command queue → worker) ─────────

@router.post("/api/terminal/order")
async def terminal_order(
    body: TerminalOrderRequest,
    user: User = Depends(get_current_user),
):
    action = (body.action or "").strip().lower()
    if action not in ("modify_sl", "modify_target", "squareoff", "pause", "resume"):
        raise HTTPException(400, f"Unknown terminal action: {action}")

    payload = {"symbol": body.symbol}
    if action in ("modify_sl", "modify_target"):
        symbol = (body.symbol or "").strip().upper()
        if not symbol:
            raise HTTPException(400, f"{action} requires a symbol")
        if body.value is None:
            raise HTTPException(400, f"{action} requires a value")
        value = _snap_tick(body.value)
        level = "sl" if action == "modify_sl" else "target"

        # Per-level concurrency guard — never two modifications of the same
        # level in flight for the same user/symbol.
        if not await _acquire_modify_lock(user.id, symbol, level):
            raise HTTPException(429, "Another SL/Target modification is already in progress")

        pos = await _find_open_position(user.id, symbol)
        if pos is None:
            raise HTTPException(404, f"No open position found for {symbol}")

        ok, reason = _validate_level(action, value, pos)
        if not ok:
            raise HTTPException(400, reason)

        payload = {"symbol": symbol}
        key = "new_sl" if action == "modify_sl" else "new_target"
        payload[key] = value

    result = await send_command(action, user.id, payload)
    if result.get("queued"):
        return {"ok": True, "queued": True, "action": action}
    if not result.get("ok"):
        raise HTTPException(400, result.get("error", "Command failed"))
    return {"ok": True, "action": action, **result}


# ── Live stream ─────────────────────────────────────────────────

@router.websocket("/terminal/ws")
async def terminal_ws(websocket: WebSocket, user_id: int, token: str = ""):
    """Live terminal feed. Query params: /terminal/ws?user_id=&token="""
    try:
        await handle_terminal_ws(websocket, user_id, token)
    except WebSocketDisconnect:
        pass
    except Exception:
        try:
            await websocket.close(code=1011)
        except Exception:
            pass
