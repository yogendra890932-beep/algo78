# backend/services/bot_manager.py
# ================================================================
# Manages trading engine instances per user.
#
# Two operating modes:
#   1. LEGACY  (USE_SHARED_WORKER=False)
#      Creates one BotThread per user, each with its own full
#      TradingEngine (WebSocket, candles, indicators, strategies).
#
#   2. SHARED  (USE_SHARED_WORKER=True)
#      Delegates to SharedWorkerOrchestrator, which runs one
#      shared pipeline per symbol and one lightweight
#      UserExecutionManager per user.
#
# Used exclusively by the WORKER process (worker.py) — the web
# process (backend.main) never imports this module directly.
#
# Public API is identical in both modes:
#   is_running(user_id)    → bool
#   start(user_id, ...)    → bool
#   stop(user_id)
#   stop_all(join_timeout)
#   status_all()           → {user_id: status_str}
#   running_count()        → int
#   get_error(user_id)     → Optional[str]
# ================================================================

import threading
import asyncio
import traceback
import logging
from datetime import datetime
from typing import Optional

from backend.db.models import BotStatus

logger = logging.getLogger("bot_manager")


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _log(level: str, msg: str):
    if level == "info":
        logger.info(msg)
    elif level == "warn":
        logger.warning(msg)
    else:
        logger.info(msg)
    print(f"{_now()} [BotManager] {msg}")


# ── Worker process event loop — captured at startup ──────────────
# Bot threads need this to post async callbacks (_on_status_change,
# _on_trade) back onto worker.py's asyncio loop for DB writes /
# Redis publishes. Set once in worker.py's main() via set_main_loop(loop).
_main_loop: Optional[asyncio.AbstractEventLoop] = None


def set_main_loop(loop: asyncio.AbstractEventLoop):
    global _main_loop
    _main_loop = loop


def _post_to_main(coro):
    """Safely schedule a coroutine on the main FastAPI event loop."""
    if _main_loop and _main_loop.is_running():
        asyncio.run_coroutine_threadsafe(coro, _main_loop)


# ================================================================
# BotThread — Legacy per-user engine (USE_SHARED_WORKER=False)
# ================================================================

class BotThread(threading.Thread):
    def __init__(self, user_id: int, config: dict, access_token: str,
                 on_status_change, on_trade):
        super().__init__(daemon=True, name=f"bot-user-{user_id}")
        self.user_id          = user_id
        self.config           = config
        self.access_token     = access_token
        self.on_status_change = on_status_change
        self.on_trade         = on_trade
        self._stop_event      = threading.Event()
        self.error_msg: Optional[str] = None
        self.engine           = None

    def request_stop(self):
        self._stop_event.set()

    def update_token(self, new_token: str) -> bool:
        """Hot-swap the token on the live engine (no restart)."""
        if not new_token:
            return False
        self.access_token = new_token
        if self.engine is not None:
            try:
                self.engine.access_token = new_token
            except Exception:
                pass
        return True

    def run(self):
        try:
            from backend.engine.engine_v6 import TradingEngine
            engine = TradingEngine(
                user_id      = self.user_id,
                config       = self.config,
                access_token = self.access_token,
                stop_event   = self._stop_event,
                on_trade     = self._sync_on_trade,
            )
            self.engine = engine
            engine.run_sync()
        except Exception as e:
            self.error_msg = traceback.format_exc()
            _log("error", f"Bot user {self.user_id} CRASH: {self.error_msg}")
            _post_to_main(
                self.on_status_change(self.user_id, BotStatus.error, self.error_msg)
            )

    def _sync_on_trade(self, trade_data: dict):
        """Called synchronously by engine → post to FastAPI main loop."""
        _post_to_main(self.on_trade(self.user_id, trade_data))


# ================================================================
# BotManager — Unified interface (legacy + shared modes)
# ================================================================

class BotManager:
    def __init__(self):
        self._bots: dict[int, BotThread] = {}
        self._lock = threading.Lock()
        self._shared_mode = False
        self._orchestrator = None
        self._on_status_change = None
        self._on_trade = None

    # ── Shared mode activation ───────────────────────────────────

    def init_shared(self, orchestrator, main_loop=None,
                    on_status_change=None, on_trade=None):
        """
        Activate shared-worker mode. Call ONCE at worker startup.

        Args:
            orchestrator: SharedWorkerOrchestrator instance
            main_loop:    asyncio event loop for dispatching async callbacks
            on_status_change: async callback(user_id, status, error)
            on_trade:         async callback(user_id, trade_data)
        """
        self._orchestrator = orchestrator
        self._shared_mode = True
        self._on_status_change = on_status_change
        self._on_trade = on_trade
        if main_loop:
            orchestrator.set_main_loop(main_loop)
        orchestrator.set_callbacks(on_status_change, on_trade)
        _log("info", "Shared-worker mode activated")

    @property
    def is_shared_mode(self) -> bool:
        return self._shared_mode

    @property
    def orchestrator(self):
        """Access the orchestrator for advanced operations."""
        return self._orchestrator

    # ── Public API (identical in both modes) ─────────────────────

    def is_running(self, user_id: int) -> bool:
        if self._shared_mode:
            return self._orchestrator._user_registry.is_running(user_id)
        with self._lock:
            t = self._bots.get(user_id)
            return t is not None and t.is_alive()

    def get_error(self, user_id: int) -> Optional[str]:
        if self._shared_mode:
            return None   # orchestrator surfaces errors via callbacks
        with self._lock:
            t = self._bots.get(user_id)
            return t.error_msg if t else None

    def start(self, user_id: int, config: dict, access_token: str,
              on_status_change, on_trade) -> bool:
        if self._shared_mode:
            if self._orchestrator._user_registry.is_running(user_id):
                return False
            symbol = config.get("underlying_symbol", "NIFTY").upper()
            _log("info",
                 f"[SharedWorker] Registered user {user_id}"
                 f" on {symbol}")
            started = self._orchestrator.start_user(
                user_id, config, access_token,
            )
            return started

        with self._lock:
            existing = self._bots.get(user_id)
            if existing and existing.is_alive():
                return False
            if existing and not existing.is_alive():
                self._bots.pop(user_id, None)

            t = BotThread(
                user_id          = user_id,
                config           = config,
                access_token     = access_token,
                on_status_change = on_status_change,
                on_trade         = on_trade,
            )
            self._bots[user_id] = t
            t.start()
            return True

    def stop(self, user_id: int):
        if self._shared_mode:
            _log("info", f"[SharedWorker] Unregistering user {user_id}")
            self._orchestrator.stop_user(user_id)
            return

        with self._lock:
            t = self._bots.get(user_id)
            if t:
                t.request_stop()

    def update_token(self, user_id: int, access_token: str) -> bool:
        """Hot-swap a running bot's broker token (no restart), both modes."""
        if self._shared_mode:
            res = self._orchestrator._handle_update_token(
                user_id, {"access_token": access_token})
            return bool(res.get("ok"))
        with self._lock:
            t = self._bots.get(user_id)
        if not t:
            return False
        return t.update_token(access_token)

    def stop_all(self, join_timeout: float = 10.0):
        if self._shared_mode:
            _log("info", "Stopping all execution managers...")
            self._orchestrator._user_registry.stop_all()
            _log("info", "Stopping strategy engines...")
            self._orchestrator._cleanup_shared_services()
            return

        with self._lock:
            threads = list(self._bots.values())
        for t in threads:
            t.request_stop()
        for t in threads:
            if t.is_alive():
                t.join(timeout=join_timeout)
                if t.is_alive():
                    _log("warn",
                         f"bot-user-{t.user_id} "
                         f"did not stop within {join_timeout}s")

    def status_all(self) -> dict[int, str]:
        if self._shared_mode:
            return self._orchestrator.status_all()
        with self._lock:
            return {
                uid: ("running" if t.is_alive() else "stopped")
                for uid, t in self._bots.items()
            }

    def running_count(self) -> int:
        if self._shared_mode:
            return len(self._orchestrator._user_registry.running_users())
        with self._lock:
            return sum(1 for t in self._bots.values() if t.is_alive())

    # ── Extended API for shared mode (used by worker.py handlers) ─

    def get_engines_for_user(self, user_id: int) -> list:
        """Return engine-like objects for modify_sl/squareoff commands."""
        if self._shared_mode:
            return self._orchestrator.get_engines_for_user(user_id)

        try:
            from backend.services.telegram_bot import get_engines
            return get_engines(user_id)
        except Exception:
            return []

    def get_user_manager(self, user_id: int):
        """Access the UserExecutionManager for a running user (shared mode only)."""
        if self._shared_mode:
            return self._orchestrator._user_registry._managers.get(user_id)
        return None


# ================================================================
# Singleton
# ================================================================

bot_manager = BotManager()
