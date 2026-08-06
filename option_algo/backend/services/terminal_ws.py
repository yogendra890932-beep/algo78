# backend/services/terminal_ws.py
# ================================================================
# Advanced Trading Terminal — live WebSocket stream handler.
#
# The terminal consumes the SAME channels the rest of the platform
# already publishes (user events via the event bus, premium-option
# closes + market ticks via Redis Pub/Sub) and polls the SAME Redis
# snapshot keys the existing position APIs read. It never executes
# engine code itself.
#
# Client messages:
#   {"action": "subscribe",  "symbol": "NIFTY"}   → start relaying symbol data
#   {"action": "unsubscribe","symbol": "NIFTY"}   → stop relaying symbol data
#   {"action": "snapshot",   "symbol": "NIFTY"}   → fresh full snapshot
#   {"action": "positions"}                        → fresh positions snapshot
#
# Server messages:
#   hello / snapshot / state / premium_bar / current / tick /
#   event / positions / pong
# ================================================================

import asyncio
import json
import time

from backend.services import state_store
from backend.services.event_bus import subscribe as subscribe_user_events
from backend.services.redis_client import get_redis
from backend.shared.redis_infra import (
    shared_premium_close_channel,
    shared_tick_channel,
)
from backend.services.terminal_service import (
    build_symbol_snapshot,
    get_premium_current,
    get_premium_state,
)

TICK_THROTTLE_SEC = 0.35
STATE_POLL_SEC = 0.5
PING_INTERVAL_SEC = 20


async def handle_terminal_ws(websocket, user_id: int, token: str):
    """Authorise and run the terminal stream for one browser connection."""
    from backend.services.auth_service import decode_token

    try:
        payload = decode_token(token or "")
        if payload is None or str(payload.get("sub")) != str(user_id):
            await websocket.close(code=4001)
            return
    except Exception:
        await websocket.close(code=4001)
        return

    await websocket.accept()

    stop = asyncio.Event()
    send_lock = asyncio.Lock()
    relay_tasks: dict = {}

    async def send(obj: dict):
        async with send_lock:
            try:
                await websocket.send_json(obj)
            except Exception:
                pass

    async def relay_user_events():
        """User-scoped execution events (SIGNAL/ENTRY/EXIT/SL_TRAIL/...)."""
        try:
            async for msg in subscribe_user_events(user_id):
                if stop.is_set():
                    break
                await send({"type": "event", **msg})
                if str(msg.get("event", "")).upper() in (
                        "ENTRY", "EXIT", "SL_TRAIL", "SIGNAL",
                        "PENDING_TRADE", "BOT_STATUS", "DIRECTION_FLIP"):
                    snap = await state_store.get_positions(user_id)
                    await send({"type": "positions",
                                "positions": snap.get("positions", []),
                                "count": snap.get("count", 0),
                                "updated_at": snap.get("updated_at")})
        except asyncio.CancelledError:
            pass
        except Exception:
            pass

    async def relay_symbol(symbol: str):
        """Relay ticks, premium bar closes and premium-state changes for one symbol."""
        r = get_redis()
        pubsub = None
        try:
            pubsub = r.pubsub()
            await pubsub.subscribe(shared_premium_close_channel(symbol),
                                   shared_tick_channel(symbol))
        except Exception as e:
            print(f"[terminal_ws] subscribe failed for {symbol}: {e}")
            return

        last_state = None
        last_tick_ts = 0.0
        try:
            while not stop.is_set():
                try:
                    msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=STATE_POLL_SEC)
                except Exception:
                    break
                if msg is None:
                    # poll for premium option rolls (state changes)
                    state = await get_premium_state(symbol)
                    if state != last_state:
                        last_state = state
                        await send({"type": "state", "symbol": symbol, "state": state})
                        snap = await build_symbol_snapshot(user_id, symbol)
                        await send({"type": "snapshot", "symbol": symbol, **snap})
                    continue

                try:
                    data = json.loads(msg["data"])
                except Exception:
                    continue
                chan = msg["channel"]
                chan = chan.decode() if isinstance(chan, bytes) else chan

                if chan == shared_premium_close_channel(symbol):
                    candle = data.get("candle")
                    await send({"type": "premium_bar", "symbol": symbol,
                                "candle": candle, "ts": data.get("ts")})
                    cur = await get_premium_current(symbol)
                    await send({"type": "current", "symbol": symbol, "candle": cur})
                elif chan == shared_tick_channel(symbol):
                    now = time.time()
                    if now - last_tick_ts >= TICK_THROTTLE_SEC:
                        last_tick_ts = now
                        await send({"type": "tick", "symbol": symbol,
                                    "token": data.get("token"),
                                    "ltp": data.get("ltp"),
                                    "ltq": data.get("ltq"),
                                    "ts": data.get("ts")})
                        cur = await get_premium_current(symbol)
                        if cur:
                            await send({"type": "current", "symbol": symbol, "candle": cur})
        except asyncio.CancelledError:
            pass
        except Exception:
            pass
        finally:
            try:
                await pubsub.close()
            except Exception:
                pass

    async def reader():
        while not stop.is_set():
            try:
                raw = await asyncio.wait_for(websocket.receive_text(), timeout=30)
            except asyncio.TimeoutError:
                continue
            except Exception:
                break
            try:
                msg = json.loads(raw)
            except Exception:
                continue

            action = msg.get("action")
            sym = (msg.get("symbol") or "").upper()

            if action == "subscribe":
                if sym and sym not in relay_tasks:
                    relay_tasks[sym] = asyncio.create_task(relay_symbol(sym))
                snap = await build_symbol_snapshot(user_id, sym or "NIFTY")
                await send({"type": "snapshot", "symbol": sym or "NIFTY", **snap})
            elif action == "unsubscribe":
                task = relay_tasks.pop(sym, None)
                if task is not None:
                    task.cancel()
            elif action == "snapshot":
                snap = await build_symbol_snapshot(user_id, sym or "NIFTY")
                await send({"type": "snapshot", "symbol": sym or "NIFTY", **snap})
            elif action == "positions":
                snap = await state_store.get_positions(user_id)
                await send({"type": "positions",
                            "positions": snap.get("positions", []),
                            "count": snap.get("count", 0),
                            "updated_at": snap.get("updated_at")})
            elif action == "ping":
                await send({"type": "pong"})

    async def heartbeat():
        while not stop.is_set():
            await asyncio.sleep(PING_INTERVAL_SEC)
            await send({"_ping": True})

    tasks = [
        asyncio.create_task(relay_user_events()),
        asyncio.create_task(reader()),
        asyncio.create_task(heartbeat()),
    ]

    try:
        await send({"type": "hello", "user_id": user_id,
                    "server_time": time.strftime("%Y-%m-%dT%H:%M:%S")})
        snap = await state_store.get_positions(user_id)
        await send({"type": "positions",
                    "positions": snap.get("positions", []),
                    "count": snap.get("count", 0),
                    "updated_at": snap.get("updated_at")})
        await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    except asyncio.CancelledError:
        pass
    except Exception:
        pass
    finally:
        stop.set()
        for task in list(relay_tasks.values()):
            task.cancel()
        for task in tasks:
            task.cancel()
        for task in list(relay_tasks.values()) + tasks:
            try:
                await task
            except BaseException:
                pass
