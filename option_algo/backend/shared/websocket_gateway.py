# backend/shared/websocket_gateway.py
# ================================================================
# WebSocket Gateway — Enhanced client connection layer.
#
# Wraps the existing broadcaster.ConnectionManager to add:
#   - Delta-only updates (compare vs last-known state, send only changes)
#   - Direct shared-data subscriptions (OC, positions) bypassing event channel
#   - Per-connection rate limiting
#   - Connection lifecycle tracking & metrics
#   - Heartbeat / ping-pong for dead-connection detection
#
# Architecture:
#   browser -> /ws/{user_id}
#     -> WSGateway (this module) -> accepts, auths
#       -> ConnectionManager (broadcaster.py) -> subscribes events:{user_id}
#       -> Optional direct shared-data subscriptions (OC, positions)
#       -> Delta tracker compares payloads, filters unchanged fields
#       -> Sends to browser
#
# This does NOT replace broadcaster.py — it sits between the browser
# and the broadcaster, adding the delta layer.
# ================================================================

import asyncio
import json
import time
from typing import Optional, Any

from backend.services.event_bus import subscribe as subscribe_events
from backend.services.state_store import (
    get_positions as get_positions_async,
    get_oc_snapshot as get_oc_snapshot_async,
    get_bot_status as get_bot_status_async,
)
from backend.services.redis_client import get_redis as get_async_redis
from backend.shared.redis_infra import (
    shared_oc_analysis,
    shared_oc_chain_df,
    shared_candles_1m,
    shared_candles_5m,
    shared_indicators,
    shared_indicators_1m,
    shared_indicators_5m,
    shared_market_structure_1m,
    shared_market_structure_5m,
)
from backend.services.broadcaster import manager as base_manager


def _now() -> str:
    from datetime import datetime
    return datetime.utcnow().isoformat()


def _dict_diff(prev: dict, curr: dict) -> dict:
    """Return only the keys that changed (or new keys)."""
    if prev is None:
        return curr
    changed = {}
    for k, v in curr.items():
        if k not in prev or prev[k] != v:
            changed[k] = v
    return changed


def _nested_dict_diff(prev: dict, curr: dict) -> dict:
    """Deep diff: return only changed leaf keys."""
    if prev is None:
        return curr
    changed = {}
    all_keys = set(prev.keys()) | set(curr.keys())
    for k in all_keys:
        pv = prev.get(k)
        cv = curr.get(k)
        if pv == cv:
            continue
        if isinstance(pv, dict) and isinstance(cv, dict):
            nested = _nested_dict_diff(pv, cv)
            if nested:
                changed[k] = nested
        else:
            changed[k] = cv
    return changed


class DeltaTracker:
    """Per-user state diff tracker — remembers last payload sent to avoid redundant sends."""

    def __init__(self):
        self._states: dict[str, Any] = {}   # key -> last_sent_value

    def diff(self, key: str, current: Any) -> Optional[Any]:
        """Return delta from last known state, or None if unchanged."""
        prev = self._states.get(key)
        if prev == current:
            return None
        result = current
        if isinstance(prev, dict) and isinstance(current, dict):
            result = _nested_dict_diff(prev, current)
            if not result:
                return None
        self._states[key] = current
        return result

    def reset(self, key: str = None):
        if key:
            self._states.pop(key, None)
        else:
            self._states.clear()


class RateLimiter:
    """Per-connection rate limiter — enforces max messages per second."""

    def __init__(self, max_per_sec: float = 30.0):
        self.max_per_sec = max_per_sec
        self._allowances: dict[str, float] = {}
        self._last_check: dict[str, float] = {}

    def allow(self, conn_id: str) -> bool:
        now = time.time()
        prev = self._last_check.get(conn_id)
        if prev is None:
            self._last_check[conn_id] = now
            self._allowances[conn_id] = self.max_per_sec - 1.0
            return True
        elapsed = now - prev
        self._last_check[conn_id] = now
        allowance = self._allowances.get(conn_id, self.max_per_sec) + elapsed * self.max_per_sec
        if allowance > self.max_per_sec:
            allowance = self.max_per_sec
        if allowance < 1.0:
            self._allowances[conn_id] = allowance
            return False
        self._allowances[conn_id] = allowance - 1.0
        return True

    def purge(self, conn_id: str):
        self._allowances.pop(conn_id, None)
        self._last_check.pop(conn_id, None)


class ConnectionTracker:
    """Track active connections per user for metrics."""

    def __init__(self):
        self._conns_per_user: dict[int, set[str]] = {}
        self._total_messages: dict[str, int] = {}

    def add(self, user_id: int, conn_id: str):
        self._conns_per_user[user_id].add(conn_id)

    def remove(self, user_id: int, conn_id: str):
        conns = self._conns_per_user.get(user_id, set())
        conns.discard(conn_id)
        if not conns:
            self._conns_per_user.pop(user_id, None)
        self._total_messages.pop(conn_id, None)

    def record_message(self, conn_id: str):
        self._total_messages[conn_id] += 1

    @property
    def active_connections(self) -> int:
        return sum(len(c) for c in self._conns_per_user.values())

    @property
    def active_users(self) -> int:
        return len(self._conns_per_user)

    def stats(self) -> dict:
        return {
            "active_connections": self.active_connections,
            "active_users": self.active_users,
            "total_messages": sum(self._total_messages.values()),
        }


class WSGateway:
    """
    Enhanced WebSocket gateway for client connections.

    Usage in a WebSocket endpoint:
        gateway = WSGateway()
        await gateway.handle(websocket, user_id, token)
    """

    HEARTBEAT_SEC = 25   # ping interval
    PONG_TIMEOUT = 10    # max wait for pong before disconnect

    def __init__(self):
        self._tracker = ConnectionTracker()
        self._limiter = RateLimiter(max_per_sec=50.0)
        self._delta = DeltaTracker()
        self._conn_counter = 0

        self.user_data_subscriptions: dict[int, asyncio.Task] = {}

    def _next_conn_id(self) -> str:
        self._conn_counter += 1
        return f"{int(time.time() * 1000)}-{self._conn_counter:x}"

    async def handle(self, websocket, user_id: int, token: str):
        """Main entry point for a WebSocket connection."""
        from fastapi import WebSocket
        from backend.services.auth_service import decode_token

        try:
            payload = decode_token(token or "")
        except Exception:
            await websocket.close(code=4001, reason="Invalid token")
            return

        if str(payload.get("sub")) != str(user_id):
            await websocket.close(code=4001, reason="Token user mismatch")
            return

        if payload.get("type") != "access":
            await websocket.close(code=4001, reason="Access token required")
            return

        await websocket.accept()
        conn_id = self._next_conn_id()
        self._tracker.add(user_id, conn_id)

        stop_event = asyncio.Event()
        tasks = []

        try:
            tasks.append(asyncio.create_task(
                self._event_relay(user_id, websocket, conn_id, stop_event),
                name=f"evt-{user_id}",
            ))
            tasks.append(asyncio.create_task(
                self._heartbeat(websocket, conn_id, stop_event),
                name=f"hb-{user_id}",
            ))
            tasks.append(asyncio.create_task(
                self._client_reader(websocket, user_id, conn_id, stop_event),
                name=f"cli-{user_id}",
            ))

            done, pending = await asyncio.wait(
                tasks,
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in done:
                exc = task.exception()
                if exc and not isinstance(exc, asyncio.CancelledError):
                    raise exc
        except Exception:
            pass
        finally:
            stop_event.set()
            for task in tasks:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

            user_prefix = str(user_id)
            relay_keys = [k for k in list(self.user_data_subscriptions.keys())
                          if k.endswith(f":{user_id}")]
            for key in relay_keys:
                task = self.user_data_subscriptions.pop(key, None)
                if task:
                    task.cancel()

            delta_keys = [k for k in list(self._delta._states.keys())
                          if f":{user_id}" in k]
            for k in delta_keys:
                self._delta._states.pop(k, None)

            self._tracker.remove(user_id, conn_id)
            self._limiter.purge(conn_id)

    async def _event_relay(self, user_id: int, websocket, conn_id: str,
                           stop_event: asyncio.Event):
        """Subscribe to per-user event channel and relay with delta filtering."""
        use_delta = True  # enables delta-only messages
        try:
            async for raw_msg in subscribe_events(user_id):
                if stop_event.is_set():
                    break
                if not self._limiter.allow(conn_id):
                    continue
                self._tracker.record_message(conn_id)

                event_type = raw_msg.get("event", "")

                if use_delta:
                    delta_key = f"evt:{user_id}:last"
                    filtered = self._delta.diff(delta_key, raw_msg)
                    if filtered is not None and filtered:
                        raw_msg["_delta"] = True
                    elif filtered is not None and not filtered:
                        continue
                else:
                    raw_msg.pop("_delta", None)

                try:
                    await websocket.send_json(raw_msg)
                except Exception:
                    break
        except asyncio.CancelledError:
            pass

    async def _heartbeat(self, websocket, conn_id: str, stop_event: asyncio.Event):
        """Periodic ping/pong to detect dead connections."""
        try:
            while not stop_event.is_set():
                await asyncio.sleep(self.HEARTBEAT_SEC)
                try:
                    await websocket.send_json({"_ping": True})
                except Exception:
                    break
        except asyncio.CancelledError:
            pass

    async def _client_reader(self, websocket, user_id: int, conn_id: str,
                             stop_event: asyncio.Event):
        """Read messages from client — subscription requests, pong responses."""
        try:
            while not stop_event.is_set():
                try:
                    raw = await asyncio.wait_for(
                        websocket.receive_text(), timeout=30.0
                    )
                except asyncio.TimeoutError:
                    continue
                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    continue

                pong = msg.get("_pong")
                if pong:
                    continue

                action = msg.get("action", "")
                if action == "subscribe":
                    await self._handle_client_subscribe(user_id, msg)
                elif action == "unsubscribe":
                    await self._handle_client_unsubscribe(user_id, msg)
                elif action == "get_snapshot":
                    await self._handle_snapshot_request(websocket, user_id, msg)
        except asyncio.CancelledError:
            pass
        except Exception:
            pass

    async def _handle_client_subscribe(self, user_id: int, msg: dict):
        """Client wants live updates on a shared data channel."""
        channel = msg.get("channel", "")
        if not channel:
            return
        symbol = msg.get("symbol", "")
        expiry = msg.get("expiry", "")
        if channel == "oc" and symbol and expiry:
            self._start_shared_relay(user_id, "oc", symbol, expiry)
        elif channel == "positions":
            pass  # positions come through the event relay already
        elif channel == "indicators" and symbol:
            self._start_shared_relay(user_id, "indicators", symbol)

    async def _handle_client_unsubscribe(self, user_id: int, msg: dict):
        channel = msg.get("channel", "")
        key = f"{channel}:{user_id}"
        task = self.user_data_subscriptions.pop(key, None)
        if task:
            task.cancel()

    async def _handle_snapshot_request(self, websocket, user_id: int, msg: dict):
        """Client requests a one-time snapshot of data."""
        channel = msg.get("channel", "")
        symbol = msg.get("symbol", "")
        expiry = msg.get("expiry", "")

        snapshot = None
        if channel == "positions":
            snapshot = await get_positions_async(user_id)
        elif channel == "bot_status":
            snapshot = await get_bot_status_async(user_id)
        elif channel == "oc" and symbol and expiry:
            snapshot = await self._get_shared_oc(symbol, expiry)
        elif channel == "candles" and symbol:
            snapshot = await self._get_shared_candles(symbol)
        elif channel == "indicators" and symbol:
            snapshot = await self._get_shared_indicators(symbol)
        elif channel == "structure" and symbol:
            snapshot = await self._get_shared_structure(symbol)

        if snapshot:
            try:
                await websocket.send_json({
                    "_snapshot": channel,
                    "data": snapshot,
                })
            except Exception as e:
                print(f"[WS:{user_id}] Connection error: {e}")

    async def _get_shared_oc(self, symbol: str, expiry: str) -> dict:
        r = get_async_redis()
        analysis_raw = await r.get(shared_oc_analysis(symbol, expiry))
        chain_raw = await r.get(shared_oc_chain_df(symbol, expiry))
        return {
            "analysis": json.loads(analysis_raw) if analysis_raw else None,
            "chain_df": json.loads(chain_raw) if chain_raw else None,
        }

    async def _get_shared_candles(self, symbol: str) -> dict:
        r = get_async_redis()
        c1m = await r.get(shared_candles_1m(symbol))
        c5m = await r.get(shared_candles_5m(symbol))
        return {
            "candles_1m": json.loads(c1m) if c1m else None,
            "candles_5m": json.loads(c5m) if c5m else None,
        }

    async def _get_shared_indicators(self, symbol: str) -> dict:
        r = get_async_redis()
        ind = await r.hgetall(shared_indicators(symbol))
        ind_1m = await r.get(shared_indicators_1m(symbol))
        ind_5m = await r.get(shared_indicators_5m(symbol))
        return {
            "live": {k.decode() if isinstance(k, bytes) else k:
                     v.decode() if isinstance(v, bytes) else v
                     for k, v in ind.items()} if ind else {},
            "1m_history": json.loads(ind_1m) if ind_1m else None,
            "5m_history": json.loads(ind_5m) if ind_5m else None,
        }

    async def _get_shared_structure(self, symbol: str) -> dict:
        r = get_async_redis()
        s1m = await r.get(shared_market_structure_1m(symbol))
        s5m = await r.get(shared_market_structure_5m(symbol))
        return {
            "1m": json.loads(s1m) if s1m else None,
            "5m": json.loads(s5m) if s5m else None,
        }

    def _start_shared_relay(self, user_id: int, channel: str, symbol: str,
                            expiry: str = ""):
        """Start a background task to periodically poll shared data and relay to user."""
        key = f"{channel}:{user_id}"
        if key in self.user_data_subscriptions:
            return

        async def _relay_loop():
            import asyncio as _asyncio
            r = get_async_redis()
            interval = 2.0

            while True:
                try:
                    data = None
                    if channel == "oc":
                        analysis = await r.get(shared_oc_analysis(symbol, expiry))
                        chain = await r.get(shared_oc_chain_df(symbol, expiry))
                        data = {
                            "_shared": channel,
                            "symbol": symbol,
                            "analysis": json.loads(analysis) if analysis else None,
                            "chain_df": json.loads(chain) if chain else None,
                        }
                    elif channel == "indicators":
                        ind = await r.hgetall(shared_indicators(symbol))
                        data = {
                            "_shared": channel,
                            "symbol": symbol,
                            "indicators": {
                                k.decode() if isinstance(k, bytes) else k:
                                v.decode() if isinstance(v, bytes) else v
                                for k, v in ind.items()
                            },
                        }
                    elif channel == "candles":
                        c1m = await r.get(shared_candles_1m(symbol))
                        c5m = await r.get(shared_candles_5m(symbol))
                        data = {
                            "_shared": channel,
                            "symbol": symbol,
                            "candles_1m": json.loads(c1m) if c1m else None,
                            "candles_5m": json.loads(c5m) if c5m else None,
                        }
                    elif channel == "structure":
                        s1m = await r.get(shared_market_structure_1m(symbol))
                        s5m = await r.get(shared_market_structure_5m(symbol))
                        data = {
                            "_shared": channel,
                            "symbol": symbol,
                            "structure_1m": json.loads(s1m) if s1m else None,
                            "structure_5m": json.loads(s5m) if s5m else None,
                        }

                    if data:
                        delta_key = f"shared:{channel}:{symbol}:{user_id}"
                        filtered = self._delta.diff(delta_key, data)
                        if filtered is not None:
                            await base_manager.send_to_user(user_id, filtered)

                except asyncio.CancelledError:
                    break
                except Exception:
                    pass
                await _asyncio.sleep(interval)

        task = asyncio.create_task(_relay_loop(), name=f"relay-{channel}-{user_id}")
        self.user_data_subscriptions[key] = task

    @property
    def stats(self) -> dict:
        return self._tracker.stats()


# Singleton
gateway = WSGateway()
