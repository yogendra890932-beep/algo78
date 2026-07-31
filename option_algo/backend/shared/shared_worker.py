# backend/shared/shared_worker.py
# ================================================================
# Shared Worker Orchestrator — Replaces per-user BotThread with
# shared market processing + per-user execution managers.
#
# This is the central component that ties together all shared
# services and user execution managers. The worker process
# (worker.py) instantiates ONE SharedWorkerOrchestrator, which:
#
#   1. Manages lifecycle of all shared services per symbol
#      (market data, candle builder, indicators, structure,
#       option chain, strategy engine)
#   2. Accepts per-user start/stop commands from the Redis
#      command queue
#   3. Creates/destroys UserExecutionManager per user
#   4. Routes signals from shared strategy engine to
#      per-user execution managers
#   5. Publishes status/position/event updates via Redis
#
# Architecture:
#
#   Command Queue (Redis)
#        |
#   SharedWorkerOrchestrator._command_loop()
#        |
#        ├─> User A → UserExecutionManager(user=1)
#        ├─> User B → UserExecutionManager(user=2)
#        └─> User C → UserExecutionManager(user=3)
#
#   Shared Services (one per symbol):
#        MarketDataService → CandleBuilder → Indicators
#                                         → Structure → StrategyEngine
#                                         → OptionChain
#                |
#        Redis Pub/Sub shared:signal:{symbol}
#                |
#        Each UserExecutionManager subscribes independently
#        and decides execution mode (paper/semi-auto/auto)
#
# ================================================================

import asyncio
import json
import threading
import time
import traceback
from datetime import datetime
from typing import Optional

from backend.shared.market_data_service import SharedMarketDataService
from backend.shared.candle_builder import SharedCandleBuilder
from backend.shared.indicator_engine import SharedIndicatorEngine
from backend.shared.market_structure_engine import (
    SharedMarketStructureEngine,
    SharedUnderlyingMarketStructureEngine,
)
from backend.shared.option_chain_service import SharedOptionChainService
from backend.shared.strategy_engine import SharedStrategyEngine
from backend.shared.symbol_manager import (
    add_subscriber, remove_subscriber, get_user_symbols, get_active_symbols,
    get_subscriber_count,
)
from backend.shared.user_execution_manager import (
    UserExecutionManager, UserExecutionRegistry, user_registry,
)
from backend.shared.redis_infra import worker_heartbeat, worker_health_key
from backend.db.models import BotStatus
from backend.services.redis_client import get_redis_sync
from backend.services.command_queue import pop_command_sync, post_result_sync
from backend.services.event_bus import publish_event_sync
from backend.services.state_store import set_bot_status_sync, clear_bot_status_sync


_now = lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S")


class SharedWorkerOrchestrator:
    """
    Central orchestrator for the shared workers architecture.

    Replaces BotManager / BotThread — instead of creating a full
    TradingEngine per user, it creates lightweight UserExecutionManager
    instances that share the market processing pipeline.
    """

    HEARTBEAT_SEC = 8
    RECONNECT_BACKOFF_INITIAL = 1
    RECONNECT_BACKOFF_MAX = 30

    def __init__(self):
        self._stop_event = threading.Event()
        self._r = get_redis_sync()
        self._worker_id = f"worker-{int(time.time())}"

        self._user_registry = user_registry

        self.on_status_change = None
        self.on_trade = None
        self._main_loop: Optional[asyncio.AbstractEventLoop] = None

        self._services_initialized: set[str] = set()
        self._services_lock = threading.Lock()
        self._command_thread: Optional[threading.Thread] = None
        self._heartbeat_thread: Optional[threading.Thread] = None
        self._cleanup_threads: list[threading.Thread] = []

    def set_main_loop(self, loop: asyncio.AbstractEventLoop):
        """Set the asyncio event loop for dispatching async callbacks."""
        self._main_loop = loop

    def set_callbacks(self, on_status_change=None, on_trade=None):
        self.on_status_change = on_status_change
        self.on_trade = on_trade

    def start(self):
        """Start the orchestrator — command loop + heartbeat."""
        self._register_worker()

        self._command_thread = threading.Thread(
            target=self._command_loop, daemon=True,
            name="shared-command-consumer",
        )
        self._command_thread.start()

        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop, daemon=True,
            name="shared-heartbeat",
        )
        self._heartbeat_thread.start()

        # Restore any persisted running state
        self._restore_running_users()

        print(f"{_now()} [shared-orch] Orchestrator started (worker={self._worker_id})")

    def stop(self):
        """Graceful shutdown — stop all users and shared services."""
        self._stop_event.set()
        print(f"{_now()} [shared-orch] Shutting down...")

        self._user_registry.stop_all()

        for t in self._cleanup_threads:
            if t.is_alive():
                t.join(timeout=2.0)
        self._cleanup_threads.clear()

        self._cleanup_shared_services()

        self._unregister_worker()

        for uid in list(self._user_registry._managers.keys()):
            clear_bot_status_sync(uid)

        print(f"{_now()} [shared-orch] Shutdown complete")

    def is_running(self) -> bool:
        return not self._stop_event.is_set()

    # ================================================================
    # USER LIFECYCLE
    # ================================================================

    def start_user(self, user_id: int, config: dict, access_token: str) -> bool:
        """Start execution for a user — activate shared services + create manager."""
        if self._user_registry.is_running(user_id):
            return False

        symbol = config.get("underlying_symbol", "NIFTY").upper()

        self._ensure_symbol_services(symbol, access_token)

        extra = config.get("extra_symbols", "")
        if extra:
            for s in extra.split(","):
                s = s.strip().upper()
                if s:
                    self._ensure_symbol_services(s, access_token)

        def _on_status(status, error=None):
            if self.on_status_change:
                self._post_async(
                    self.on_status_change(user_id, BotStatus(status), error)
                )

        def _on_trade_cb(trade_data):
            if self.on_trade:
                self._post_async(self.on_trade(user_id, trade_data))

        mgr = self._user_registry.start_user(
            user_id, config, access_token,
            on_status_change=_on_status,
            on_trade=_on_trade_cb,
        )

        ref_count = get_subscriber_count(symbol)
        print(f"{_now()} [SharedWorker] RefCount {symbol} = {ref_count} "
              f"(user {user_id} started)")
        return True

    def stop_user(self, user_id: int) -> bool:
        """Stop execution for a user — cleanup shared services if no subscribers left."""
        if not self._user_registry.is_running(user_id):
            return False

        # Get user's symbols BEFORE stopping (stop clears them)
        user_syms = set()
        mgr = self._user_registry._managers.get(user_id)
        if mgr:
            user_syms = set(mgr.symbols)

        self._user_registry.stop_user(user_id)

        # Check each symbol — cleanup shared services if no subscribers left
        for sym in user_syms:
            count = get_subscriber_count(sym)
            print(f"{_now()} [SharedWorker] RefCount {sym} = {count} "
                  f"(user {user_id} stopped)")
            if count <= 0:
                self._schedule_symbol_cleanup(sym)

        print(f"{_now()} [shared-orch] User {user_id} stopped")
        return True

    def _schedule_symbol_cleanup(self, symbol: str):
        """Schedule cleanup of shared services for a symbol after a grace period."""
        symbol = symbol.upper()

        self._cleanup_threads = [t for t in self._cleanup_threads if t.is_alive()]

        def _do_cleanup():
            time.sleep(60)
            with self._services_lock:
                if symbol in self._services_initialized:
                    count = get_subscriber_count(symbol)
                    if count <= 0:
                        self._stop_services_for(symbol)
                        self._services_initialized.discard(symbol)
                        remove_subscriber(-1, symbol)
                        print(f"{_now()} [SharedWorker] Cleaned up shared services for {symbol}")

        thread = threading.Thread(
            target=_do_cleanup, daemon=True,
            name=f"cleanup-svc-{symbol}",
        )
        self._cleanup_threads.append(thread)
        thread.start()

    def _stop_services_for(self, symbol: str):
        """Stop all shared services for a specific symbol (acquires class locks)."""
        services = [
            (SharedStrategyEngine, "StrategyEngine", SharedStrategyEngine._instances_lock),
            (SharedMarketStructureEngine, "Structure(1m)", SharedMarketStructureEngine._instances_lock),
            (SharedUnderlyingMarketStructureEngine, "Structure(5m)", SharedUnderlyingMarketStructureEngine._instances_lock),
            (SharedIndicatorEngine, "Indicators", SharedIndicatorEngine._instances_lock),
            (SharedCandleBuilder, "CandleBuilder", SharedCandleBuilder._instances_lock),
            (SharedMarketDataService, "MarketData", SharedMarketDataService._instances_lock),
        ]
        for svc_cls, name, lock in services:
            with lock:
                inst = svc_cls._instances.pop(symbol, None)
            if inst:
                try:
                    inst.stop()
                    print(f"{_now()} [SharedWorker] Stopped {name} for {symbol}")
                except Exception as e:
                    print(f"{_now()} [SharedWorker] Error stopping {name}: {e}")

    def status_all(self) -> dict[int, str]:
        """Return {user_id: status_str} for all running users."""
        return {
            uid: "running"
            for uid in self._user_registry.running_users()
        }

    def get_engines_for_user(self, user_id: int):
        """Return a list of mock 'engine' objects for command handlers like modify_sl.

        In the shared architecture, modify_sl/squareoff/etc. need access to
        position/order data. This returns a simplified wrapper around the
        UserExecutionManager's position state.
        """
        mgr = self._user_registry._managers.get(user_id)
        if not mgr:
            return []

        engines = []
        with mgr._positions_lock:
            for sym, pos in mgr._positions.items():
                wrapper = _MockEngine(
                    symbol=sym,
                    position=pos,
                    paper_mode=pos.get("paper_mode", True),
                    manager=mgr,
                )
                engines.append(wrapper)
        return engines

    def _post_async(self, coro):
        """Schedule a coroutine on the main asyncio event loop."""
        if coro and self._main_loop and self._main_loop.is_running():
            future = asyncio.run_coroutine_threadsafe(coro, self._main_loop)

            def _on_done(fut):
                try:
                    fut.result()
                except Exception:
                    traceback.print_exc()

            future.add_done_callback(_on_done)

    # ================================================================
    # SHARED SERVICE MANAGEMENT
    # ================================================================

    def _ensure_symbol_services(self, symbol: str, access_token: str):
        """Idempotent — ensures all shared services are active for a symbol."""
        with self._services_lock:
            if symbol in self._services_initialized:
                return
            self._services_initialized.add(symbol)

        add_subscriber(-1, symbol)  # system subscriber

        def _init():
            try:
                SharedMarketDataService.get_or_create(symbol, access_token)
                print(f"{_now()} [shared-orch] MarketData: {symbol}")

                SharedCandleBuilder.get_or_create(symbol, access_token)
                print(f"{_now()} [shared-orch] CandleBuilder: {symbol}")

                SharedIndicatorEngine.get_or_create(symbol)
                print(f"{_now()} [shared-orch] Indicators: {symbol}")

                SharedMarketStructureEngine.get_or_create(symbol)
                print(f"{_now()} [shared-orch] Structure(1m): {symbol}")

                SharedUnderlyingMarketStructureEngine.get_or_create(symbol)
                print(f"{_now()} [shared-orch] Structure(5m): {symbol}")

                SharedStrategyEngine.get_or_create(symbol)
                print(f"{_now()} [shared-orch] StrategyEngine: {symbol}")

            except Exception as e:
                print(f"{_now()} [shared-orch] Failed to init services for {symbol}: {e}")

        thread = threading.Thread(target=_init, daemon=True,
                                  name=f"init-svc-{symbol}")
        thread.start()

    def _start_option_chain(self, symbol: str, underlying_key: str,
                             expiry: str, access_token: str):
        """Start option chain analysis for a symbol/expiry pair."""
        try:
            SharedOptionChainService.get_or_create(
                symbol, underlying_key, expiry, access_token,
            )
            print(f"{_now()} [shared-orch] OptionChain: {symbol}/{expiry}")
        except Exception as e:
            print(f"{_now()} [shared-orch] OC init failed for {symbol}/{expiry}: {e}")

    def _cleanup_shared_services(self):
        """Stop all shared services (during full shutdown)."""
        for symbol in list(self._services_initialized):
            remove_subscriber(-1, symbol)

        services = [
            (SharedMarketDataService, SharedMarketDataService._instances_lock),
            (SharedCandleBuilder, SharedCandleBuilder._instances_lock),
            (SharedIndicatorEngine, SharedIndicatorEngine._instances_lock),
            (SharedMarketStructureEngine, SharedMarketStructureEngine._instances_lock),
            (SharedUnderlyingMarketStructureEngine, SharedUnderlyingMarketStructureEngine._instances_lock),
            (SharedStrategyEngine, SharedStrategyEngine._instances_lock),
        ]
        for svc_cls, lock in services:
            with lock:
                for instance in list(svc_cls._instances.values()):
                    try:
                        instance.stop()
                    except Exception:
                        pass
                svc_cls._instances.clear()

    # ================================================================
    # COMMAND LOOP
    # ================================================================

    def _command_loop(self):
        """Consume commands from Redis queue — identical API to old command loop."""
        print(f"{_now()} [shared-orch] Command consumer started")

        while not self._stop_event.is_set():
            try:
                cmd = pop_command_sync(timeout=5)
            except Exception as e:
                print(f"{_now()} [shared-orch] pop error: {e}")
                continue

            if cmd is None:
                continue

            cmd_id = cmd.get("id")
            action = cmd.get("action")
            user_id = cmd.get("user_id")
            payload = cmd.get("payload") or {}

            try:
                result = self._dispatch_command(action, user_id, payload)
            except Exception:
                tb = traceback.format_exc()
                print(f"{_now()} [shared-orch] '{action}' for {user_id} failed:\n{tb}")
                result = {"ok": False, "error": "Internal error"}

            if cmd_id:
                try:
                    post_result_sync(cmd_id, result)
                except Exception as e:
                    print(f"{_now()} [shared-orch] post_result error: {e}")

        print(f"{_now()} [shared-orch] Command consumer stopped")

    def _dispatch_command(self, action: str, user_id: int, payload: dict) -> dict:
        """Route command to appropriate handler."""
        if action == "start":
            return self._handle_start(user_id)
        elif action == "stop":
            return self._handle_stop(user_id)
        elif action == "modify_sl":
            return self._handle_modify_sl(user_id, payload)
        elif action == "modify_target":
            return self._handle_modify_target(user_id, payload)
        elif action == "squareoff":
            return self._handle_squareoff(user_id, payload)
        elif action == "pause":
            return self._handle_pause_resume(user_id, True)
        elif action == "resume":
            return self._handle_pause_resume(user_id, False)
        elif action == "update_token":
            return self._handle_update_token(user_id, payload)
        else:
            return {"ok": False, "error": f"Unknown action: {action}"}

    def _handle_start(self, user_id: int) -> dict:
        from backend.services.bot_config_builder import resolve_start_inputs

        if not self._main_loop or not self._main_loop.is_running():
            print(f"{_now()} [shared-orch:start] Main loop not running — start deferred")
            return {"ok": False, "error": "Worker not ready"}

        fut = asyncio.run_coroutine_threadsafe(
            resolve_start_inputs(user_id), self._main_loop
        )
        try:
            config, access_token, error = fut.result(timeout=15)
        except Exception as e:
            return {"ok": False, "error": f"start failed: {e}"}

        print(f"{_now()} [shared-orch:start] user={user_id} token_preview={access_token[:20] + '...' + access_token[-10:] if access_token and len(access_token) > 30 else '***'}")
        if error:
            return {"ok": False, "error": error}

        if self._user_registry.is_running(user_id):
            return {"ok": False, "error": "Bot already running"}

        started = self.start_user(user_id, config, access_token)
        if not started:
            return {"ok": False, "error": "Bot already running"}

        set_bot_status_sync(user_id, "running")
        return {"ok": True, "status": "running"}

    def _handle_stop(self, user_id: int) -> dict:
        self.stop_user(user_id)
        set_bot_status_sync(user_id, "stopped")
        return {"ok": True, "status": "stopping"}

    def _handle_modify_sl(self, user_id: int, payload: dict) -> dict:
        new_sl = payload.get("new_sl")
        symbol = payload.get("symbol")
        engines = self.get_engines_for_user(user_id)
        if not engines:
            return {"ok": False, "error": "Bot not running"}
        changed, errors = [], []
        for eng in engines:
            if symbol and eng.symbol != symbol:
                continue
            pos = getattr(eng, "position", None)
            if not pos:
                continue
            entry = pos.get("entry_price", 0)
            if new_sl >= entry:
                errors.append(f"{eng.symbol}: SL must be below entry Rs{entry}")
                continue
            try:
                eng.modify_sl(new_sl)
                changed.append(eng.symbol)
            except Exception as e:
                errors.append(f"{eng.symbol}: {e}")
        if not changed and not errors:
            return {"ok": False, "error": "No open positions found"}
        return {"ok": True, "changed": changed, "errors": errors}

    def _handle_modify_target(self, user_id: int, payload: dict) -> dict:
        new_target = payload.get("new_target")
        symbol = payload.get("symbol")
        engines = self.get_engines_for_user(user_id)
        if not engines:
            return {"ok": False, "error": "Bot not running"}
        changed, errors = [], []
        for eng in engines:
            if symbol and eng.symbol != symbol:
                continue
            pos = getattr(eng, "position", None)
            if not pos:
                continue
            try:
                eng.modify_target(new_target)
                changed.append({
                    "symbol": eng.symbol,
                    "new_target": new_target,
                })
            except Exception as e:
                errors.append(f"{eng.symbol}: {e}")
        if not changed and not errors:
            return {"ok": False, "error": "No open positions found"}
        return {"ok": True, "changed": changed, "errors": errors}

    def _handle_squareoff(self, user_id: int, payload: dict) -> dict:
        symbol = payload.get("symbol")
        engines = self.get_engines_for_user(user_id)
        if not engines:
            return {"ok": False, "error": "Bot not running"}
        closed, errors = [], []
        for eng in engines:
            if symbol and eng.symbol != symbol:
                continue
            pos = getattr(eng, "position", None)
            if not pos:
                continue
            try:
                eng.squareoff()
                closed.append(eng.symbol)
            except Exception as e:
                errors.append(f"{eng.symbol}: {e}")
        if not closed and not errors:
            return {"ok": True, "message": "No open positions to close"}
        return {"ok": True, "closed": closed, "errors": errors}

    def _handle_pause_resume(self, user_id: int, paused: bool) -> dict:
        mgr = self._user_registry._managers.get(user_id)
        if not mgr:
            return {"ok": False, "error": "Bot not running"}
        if paused:
            mgr.pause()
        else:
            mgr.resume()
        key = "paused" if paused else "resumed"
        return {"ok": True, key: list(mgr.symbols)}

    def _handle_update_token(self, user_id: int, payload: dict) -> dict:
        new_token = payload.get("access_token", "")
        if not new_token:
            return {"ok": False, "error": "Access token required"}
        mgr = self._user_registry._managers.get(user_id)
        if not mgr:
            return {"ok": False, "error": "Bot not running"}
        if mgr.update_token(new_token):
            mgr.resume()
            return {"ok": True, "status": "running", "action": "token_updated_and_resumed"}
        else:
            return {"ok": False, "error": "Invalid access token"}

    # ================================================================
    # HEARTBEAT & RECOVERY
    # ================================================================

    def _heartbeat_loop(self):
        """Periodically refresh worker heartbeat and running user statuses."""
        cycle = 0
        while not self._stop_event.is_set():
            try:
                self._register_worker()
                for uid in self._user_registry.running_users():
                    set_bot_status_sync(uid, "running")
            except Exception as e:
                print(f"{_now()} [shared-orch] heartbeat error: {e}")

            cycle += 1
            if cycle % 6 == 0:
                try:
                    from backend.services.command_queue import (
                        check_and_warn_queue, trim_queue,
                    )
                    check_and_warn_queue()
                    trim_queue()
                except Exception as e:
                    print(f"{_now()} [shared-orch] queue monitor error: {e}")

            self._stop_event.wait(self.HEARTBEAT_SEC)

    def _register_worker(self):
        hb_key = worker_heartbeat("orchestrator", self._worker_id)
        self._r.set(hb_key, _now(), ex=30)
        self._r.sadd(worker_health_key(), self._worker_id)

    def _unregister_worker(self):
        self._r.srem(worker_health_key(), self._worker_id)
        hb_key = worker_heartbeat("orchestrator", self._worker_id)
        self._r.delete(hb_key)

    def _restore_running_users(self):
        """Restore active symbol subscriptions from Redis after restart.

        After a worker reboot, shared services must be re-initialized.
        While we do not auto-restart user bots (that requires fresh
        access tokens and explicit user action), we do restore the
        active symbol set so that market data, candles, and indicators
        resume immediately for any symbols that had subscribers.
        """
        try:
            from backend.shared.symbol_manager import recover_active_symbols
            active = recover_active_symbols()
            if active:
                print(
                    f"{_now()} [shared-orch] Recovered {len(active)} "
                    f"active symbols: {sorted(active)}"
                )
            else:
                print(f"{_now()} [shared-orch] No active symbols to restore")
        except Exception as e:
            print(f"{_now()} [shared-orch] State recovery failed: {e}")


class _MockEngine:
    """Minimal engine wrapper for modify_sl/target/squareoff commands.

    In the shared architecture, there is no SymbolEngine per user.
    This provides a compatible interface for command handlers that
    need to modify SL/target/squareoff on a user's open position.
    """

    def __init__(self, symbol: str, position: dict, paper_mode: bool, manager: UserExecutionManager):
        self.symbol = symbol
        self.position = position
        self.paper_mode = paper_mode
        self._mgr = manager
        self.sl_order_id = position.get("sl_order_id")
        self._tg_token = manager._tg_token
        self._tg_chat = manager._tg_chat
        self.cfg = manager.config
        self.on_trade = manager.on_trade
        self.opt_type = position.get("opt_type", "")
        self.strike = position.get("strike")

    def _modify_sl_from_telegram(self, new_sl: float):
        """Modify stop loss from Telegram command."""
        self.modify_sl(new_sl)

    def modify_sl(self, new_sl: float):
        """Modify the stop loss for an open position."""
        with self._mgr._positions_lock:
            pos = self._mgr._positions.get(self.symbol)
            if not pos:
                return
            pos["sl_trigger"] = new_sl

        if self.sl_order_id and not self.paper_mode:
            pass  # Real order mod would go here

        self._mgr._push_position_snapshot(self.symbol)

        if self._mgr.on_trade:
            self._mgr.on_trade({
                "event": "SL_TRAIL",
                "user_id": self._mgr.user_id,
                "mode": "paper" if self.paper_mode else "live",
                "symbol": self.symbol,
                "trading_symbol": pos.get("trading_symbol", self.symbol),
                "new_sl": new_sl,
                "ltp": pos.get("entry_price", 0),
                "opt_type": pos.get("opt_type", ""),
                "strike": pos.get("strike"),
            })

    def _modify_target_from_telegram(self, new_target: float):
        self.modify_target(new_target)

    def modify_target(self, new_target: float):
        """Modify the target for an open position."""
        with self._mgr._positions_lock:
            pos = self._mgr._positions.get(self.symbol)
            if not pos:
                return
            pos["target"] = new_target
            near_pct = self._mgr.config.get("target_near_pct", 0.003)
            pos["near_target"] = round(new_target * (1 - near_pct), 2)

        self._mgr._push_position_snapshot(self.symbol)

    def _squareoff_from_telegram(self):
        self.squareoff()

    def squareoff(self):
        """Square off an open position."""
        with self._mgr._positions_lock:
            pos = self._mgr._positions.pop(self.symbol, None)
            if not pos:
                return

        if self._mgr.on_trade:
            entry = pos.get("entry_price", 0)
            exit_price = pos.get("entry_price", 0)
            pnl = 0
            self._mgr.on_trade({
                "event": "EXIT",
                "user_id": self._mgr.user_id,
                "mode": "paper" if self.paper_mode else "live",
                "symbol": self.symbol,
                "trading_symbol": pos.get("trading_symbol", self.symbol),
                "entry_price": entry,
                "exit_price": exit_price,
                "sl_trigger": pos.get("sl_trigger", 0),
                "target": pos.get("target", 0),
                "qty": pos.get("qty", 0),
                "pnl": pnl,
                "status": "MANUAL_SQUAREOFF",
                "strategy": pos.get("strategy", ""),
                "opt_type": pos.get("opt_type", ""),
                "strike": pos.get("strike"),
                "expiry": pos.get("expiry", ""),
                "instrument_key": pos.get("instrument_key", ""),
                "entry_ts": pos.get("entry_ts", datetime.utcnow()),
            })

        self._mgr._push_position_snapshot(self.symbol)


# ================================================================
# SYNC HELPERS
# ================================================================
