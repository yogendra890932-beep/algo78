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
from backend.services.terminal_service import (
    build_bootstrap,
    build_symbol_snapshot,
)
from backend.services.terminal_ws import handle_terminal_ws
from sqlalchemy.ext.asyncio import AsyncSession

router = APIRouter(tags=["terminal"])


# ── Request models ──────────────────────────────────────────────

class TerminalOrderRequest(BaseModel):
    """Order issued from the terminal panel.

    action:  modify_sl | modify_target | squareoff | pause | resume
    """
    action: str
    symbol: Optional[str] = None
    value: Optional[float] = None


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
        if body.value is None:
            raise HTTPException(400, f"{action} requires a value")
        key = "new_sl" if action == "modify_sl" else "new_target"
        payload[key] = body.value

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
