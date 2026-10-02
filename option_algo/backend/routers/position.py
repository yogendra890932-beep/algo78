# backend/routers/position.py
# ================================================================
# Position Management API
#   GET  /api/position/         — get all active positions
#   POST /api/position/sl       — modify stop-loss
#   POST /api/position/target   — modify target
#   POST /api/position/squareoff — square off position
#   POST /api/position/pause    — pause new entries
#   POST /api/position/resume   — resume trading
#
# GET reads a Redis snapshot pushed periodically by the worker
# process's SymbolEngine (state_store.set_positions_sync).
#
# POST actions push a command onto the Redis queue
# (command_queue.send_command) — the worker process picks it up,
# calls the relevant SymbolEngine method, and posts a result back.
# This works regardless of which process the worker runs in.
# ================================================================

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import Optional

from backend.services.auth_service import get_current_user
from backend.services.command_queue import send_command
from backend.services import state_store
from backend.db.models import User

router = APIRouter(prefix="/api/position", tags=["position"])


class SLRequest(BaseModel):
    new_sl: float
    symbol: Optional[str] = None   # None = apply to all symbols


class TargetRequest(BaseModel):
    new_target: float
    symbol: Optional[str] = None


class SquareOffRequest(BaseModel):
    symbol: Optional[str] = None   # None = squareoff all
    lots: Optional[int] = None     # None/>=held = full close; else partial reduce


@router.get("/")
async def get_positions(user: User = Depends(get_current_user)):
    """Returns all active positions across all symbols for this user."""
    snap = await state_store.get_positions(user.id)
    return {"positions": snap.get("positions", []), "count": snap.get("count", 0)}


@router.post("/sl")
async def modify_sl(body: SLRequest, user: User = Depends(get_current_user)):
    """Modify stop-loss for open position(s) — dispatched to worker."""
    result = await send_command("modify_sl", user.id,
                                 {"new_sl": body.new_sl, "symbol": body.symbol})
    if result.get("queued"):
        return {"ok": True, "queued": True}
    if not result.get("ok"):
        raise HTTPException(400, result.get("error", "Failed to modify SL"))
    return result


@router.post("/target")
async def modify_target(body: TargetRequest, user: User = Depends(get_current_user)):
    """
    Modify profit target for open position(s) — dispatched to worker.
    Worker also recalculates near_target automatically.
    """
    result = await send_command("modify_target", user.id,
                                 {"new_target": body.new_target, "symbol": body.symbol})
    if result.get("queued"):
        return {"ok": True, "queued": True}
    if not result.get("ok"):
        raise HTTPException(400, result.get("error", "Failed to modify target"))
    return result


@router.post("/squareoff")
async def squareoff(body: SquareOffRequest, user: User = Depends(get_current_user)):
    """
    Close open position(s) — dispatched to worker.
    Worker cancels SL then market-sells, in order.
    If `lots` is given and is fewer than the held lots, only that many
    lots are sold (partial reduce); the remainder stays open.
    """
    payload = {"symbol": body.symbol}
    if body.lots is not None:
        try:
            payload["lots"] = int(body.lots)
        except (TypeError, ValueError):
            raise HTTPException(400, "lots must be an integer")
        if payload["lots"] <= 0:
            raise HTTPException(400, "lots must be greater than 0")
    result = await send_command("squareoff", user.id, payload)
    if result.get("queued"):
        return {"ok": True, "queued": True}
    return result


@router.post("/pause")
async def pause_trading(user: User = Depends(get_current_user)):
    """Pause new trade entries (existing position stays open)."""
    result = await send_command("pause", user.id)
    if result.get("queued"):
        return {"ok": True, "queued": True}
    if not result.get("ok"):
        raise HTTPException(400, result.get("error", "Bot not running"))
    return result


@router.post("/resume")
async def resume_trading(user: User = Depends(get_current_user)):
    """Resume new trade entries."""
    result = await send_command("resume", user.id)
    if result.get("queued"):
        return {"ok": True, "queued": True}
    if not result.get("ok"):
        raise HTTPException(400, result.get("error", "Bot not running"))
    return result
