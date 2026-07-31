# backend/shared/user_execution_manager.py
# ================================================================
# User Execution Manager — Per-user isolated execution.
#
# Each user has ONE ExecutionManager instance that:
#   - Subscribes to shared signal channels for the user's symbols
#   - Validates risk limits (per-user, account-wide)
#   - Calculates lot sizes (per-user config)
#   - Routes to Paper / Semi-Auto / Auto based on user mode
#   - Manages open positions, SL, trailing
#   - Publishes position updates, trade events
#   - Handles Telegram notifications
#
# The user NEVER touches shared market processing.
# ================================================================

import json
import threading
import time
from datetime import datetime
from typing import Optional, Callable

from backend.shared.redis_infra import (
    shared_signal_channel,
    user_execution_state,
    user_position_snapshot,
    user_risk_snapshot,
)
from backend.shared.shared_cache import get_lot_size
from backend.shared.symbol_manager import (
    add_subscriber, remove_subscriber, get_user_symbols,
)
from backend.services.redis_client import get_redis_sync
from backend.services.event_bus import publish_event_sync
from backend.services.state_store import set_bot_status_sync, set_positions_sync


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


class UserExecutionManager:
    """
    Per-user execution context. Handles everything user-specific:
    risk, orders, positions, notifications, mode routing.
    """

    def __init__(self, user_id: int, config: dict, access_token: str,
                 on_status_change: Callable = None,
                 on_trade: Callable = None):
        self.user_id = user_id
        self.config = config
        self.access_token = access_token
        self.on_status_change = on_status_change
        self.on_trade = on_trade

        self.paper_mode = config.get("paper_mode", True)
        self.symbol = config.get("underlying_symbol", "NIFTY")
        self.symbols: set[str] = {self.symbol.upper()}

        # Parse extra symbols
        extra = config.get("extra_symbols", "")
        if extra:
            for s in extra.split(","):
                s = s.strip().upper()
                if s:
                    self.symbols.add(s)

        self._stop_event = threading.Event()
        self._paused = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._r = get_redis_sync()

        # Position state per symbol
        self._positions: dict[str, dict] = {}
        self._positions_lock = threading.Lock()

        # Risk tracking
        self._trades_today: int = 0
        self._net_pnl_today: float = 0.0
        self._last_date: Optional[str] = None
        self._risk_lock = threading.Lock()

        # Telegram config
        self._tg_token = config.get("telegram_bot_token") or ""
        self._tg_chat = config.get("telegram_chat_id") or ""

    def start(self):
        """Start the execution manager — subscribe to signal channels."""
        if self._thread and self._thread.is_alive():
            return

        # Register user symbols with subscription manager
        for sym in self.symbols:
            add_subscriber(self.user_id, sym)

        self._thread = threading.Thread(
            target=self._loop, daemon=True,
            name=f"exec-user-{self.user_id}",
        )
        self._thread.start()
        print(f"{_now()} [exec:u{self.user_id}] Started for symbols: {self.symbols}")

    def stop(self):
        """Stop execution and cleanup."""
        self._stop_event.set()
        self._paused.set()  # unblock if paused

        # Remove subscriptions
        for sym in self.symbols:
            remove_subscriber(self.user_id, sym)

        print(f"{_now()} [exec:u{self.user_id}] Stopped")

    def pause(self, reason: str = "token_expired"):
        """Pause execution without stopping shared services."""
        if self._paused.is_set():
            return
        self._paused.set()
        set_bot_status_sync(self.user_id, "token_expired", reason)
        publish_event_sync(self.user_id, {
            "event": "token_expired",
            "user_id": self.user_id,
            "reason": reason,
        })
        print(f"{_now()} [exec:u{self.user_id}] Paused: {reason}")

    def update_token(self, new_token: str) -> bool:
        """Validate and update broker access token."""
        if not new_token or not isinstance(new_token, str) or len(new_token) < 10:
            print(f"{_now()} [exec:u{self.user_id}] Token rejected: invalid format")
            return False
        self.access_token = new_token
        set_bot_status_sync(self.user_id, "running")
        publish_event_sync(self.user_id, {
            "event": "token_updated",
            "user_id": self.user_id,
        })
        print(f"{_now()} [exec:u{self.user_id}] Token updated")
        return True

    def resume(self):
        """Resume execution after pause."""
        if not self._paused.is_set():
            return
        self._paused.clear()
        set_bot_status_sync(self.user_id, "running")
        publish_event_sync(self.user_id, {
            "event": "execution_resumed",
            "user_id": self.user_id,
        })
        print(f"{_now()} [exec:u{self.user_id}] Resumed")

    @property
    def is_paused(self) -> bool:
        return self._paused.is_set()

    def _loop(self):
        """Subscribe to signal channels for user's symbols."""
        from backend.shared.pubsub_utils import resilient_pubsub_consumer
        channels = [shared_signal_channel(sym) for sym in self.symbols]
        resilient_pubsub_consumer(
            tag=f"exec:u{self.user_id}",
            channels=channels,
            handler=self._process_signal,
            stop_event=self._stop_event,
        )

    def _process_signal(self, signal: dict):
        """Process a trading signal from the shared strategy engine."""
        if self._paused.is_set():
            return
        sig_symbol = signal.get("symbol", "")
        if sig_symbol.upper() not in self.symbols:
            return

        sig_strategy = signal.get("strategy", "")

        # Check if this matches user's configured strategy filter
        cfg_strategy = self.config.get("strategy", "all")
        strategy_name = sig_strategy.split("_")[0] if "_" in sig_strategy else sig_strategy or ""

        strategy_map = {
            "pullback": "pullback",
            "trend_follow": "trend",
            "breakout_1m": "breakout",
            "vwap_bounce": "vwap",
            "ema_cross": "ema_cross",
            "vcgb": "vcgb",
            "unified_ce": "all",
            "unified_pe": "all",
        }

        allowed_types = strategy_map.get(sig_strategy, "")
        if cfg_strategy not in ("all", "both") and allowed_types not in (cfg_strategy, "all"):
            print(f"{_now()} [exec:u{self.user_id}] Skip {sig_strategy} — "
                  f"filter={cfg_strategy}")
            return

        # Risk check
        if not self._risk_ok():
            print(f"{_now()} [exec:u{self.user_id}] Skip {sig_strategy} — risk limit hit")
            return

        # Trading hours check
        if not self._trading_hours_ok():
            print(f"{_now()} [exec:u{self.user_id}] Skip {sig_strategy} — outside trading hours")
            return

        # Already in a position for this symbol?
        with self._positions_lock:
            if self._positions:
                print(f"{_now()} [exec:u{self.user_id}] Skip {sig_strategy} — "
                      f"position already open ({list(self._positions)})")
                return  # One position at a time

        # Calculate quantity based on user config
        num_lots = max(1, int(self.config.get("order_qty", 1)))
        custom_ls = self.config.get("custom_lot_sizes") or {}
        if isinstance(custom_ls, str):
            try:
                custom_ls = json.loads(custom_ls)
            except Exception:
                custom_ls = {}

        lot_size = get_lot_size(sig_symbol, custom_ls)
        qty = num_lots * lot_size

        print(f"{_now()} [exec:u{self.user_id}] Signal {sig_strategy} {sig_symbol} "
              f"{signal.get('opt_type','')} @ {signal.get('entry_price')} "
              f"SL {signal.get('stop_loss')} qty={qty} ({num_lots} lots x {lot_size})")

        # Build the signal with user-specific quantities
        enriched_signal = {
            **signal,
            "user_id": self.user_id,
            "quantity": qty,
            "lot_size": lot_size,
            "num_lots": num_lots,
            "mode": "paper" if self.paper_mode else "live",
            "config": self.config,
        }

        # Publish to user's event channel (for dashboard WebSocket)
        publish_event_sync(self.user_id, {
            "event": "SIGNAL",
            "signal": enriched_signal,
        })

        # Route to execution mode
        self._route_execution(enriched_signal)

    def _route_execution(self, signal: dict):
        """Route signal to Paper / Semi-Auto / Auto execution."""
        execution_mode = self.config.get("execution_mode", "PAPER")
        print(f"{_now()} [exec:u{self.user_id}] Route signal → mode={execution_mode}")

        if execution_mode == "PAPER" or self.paper_mode:
            self._execute_paper(signal)
        elif execution_mode == "SEMI_AUTO":
            self._execute_semi_auto(signal)
        elif execution_mode == "AUTO":
            self._execute_auto(signal)

    def _execute_paper(self, signal: dict):
        """Execute paper trade."""
        from backend.services.paper_trading import get_paper_book
        paper = get_paper_book(self.user_id)

        entry_price = signal.get("entry_price", 0)
        stop_loss = signal.get("stop_loss", 0)
        qty = signal.get("quantity", 0)
        strategy = signal.get("strategy", "")
        symbol = signal.get("symbol", "")

        entry_id = paper.place_market_order(
            "BUY", qty, entry_price,
            signal.get("instrument_key", ""),
            f"paper:{self.user_id}"
        )

        sl_id = paper.place_sl_order(
            "SELL", qty, stop_loss,
            signal.get("instrument_key", ""),
            f"paper:{self.user_id}"
        )

        with self._positions_lock:
            self._positions[symbol] = {
                "entry_price": entry_price,
                "qty": qty,
                "sl_trigger": stop_loss,
                "strategy": strategy,
                "entry_ts": datetime.utcnow(),
                "paper_mode": True,
                "symbol": symbol,
                "entry_order_id": entry_id.get("order_id") if entry_id else None,
                "sl_order_id": sl_id.get("order_id") if sl_id else None,
            }

        self._record_entry()

        print(f"{_now()} [exec:u{self.user_id}] PAPER ENTRY {symbol} {strategy} "
              f"@ {entry_price} SL {stop_loss} qty={qty}")

        # Notify
        if self.on_trade:
            self.on_trade({
                "event": "ENTRY",
                "user_id": self.user_id,
                "mode": "paper",
                "symbol": symbol,
                "entry_price": entry_price,
                "stop_loss": stop_loss,
                "qty": qty,
                "strategy": strategy,
            })

        self._push_position_snapshot(symbol)

    def _execute_semi_auto(self, signal: dict):
        """Create a pending trade record for user approval."""
        try:
            import asyncio
            from backend.services.execution_layer import (
                TradeSignal, SemiAutoExecutor, execution_router,
            )

            trade_signal = TradeSignal(
                symbol=signal.get("symbol", ""),
                opt_type=signal.get("opt_type", "CE"),
                direction="BUY",
                entry_price=signal.get("entry_price", 0),
                stop_loss=signal.get("stop_loss", 0),
                quantity=signal.get("quantity", 0),
                strategy_name=signal.get("strategy", ""),
            )

            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            result = loop.run_until_complete(
                execution_router.execute(self.user_id, trade_signal)
            )
            loop.close()

            if self.on_trade and result.status.value == "PENDING_APPROVAL":
                print(f"{_now()} [exec:u{self.user_id}] SEMI_AUTO pending "
                      f"trade#{result.pending_trade_id} {signal.get('symbol')} "
                      f"{signal.get('strategy')} @ {signal.get('entry_price')}")
                self.on_trade({
                    "event": "PENDING_TRADE",
                    "user_id": self.user_id,
                    "mode": "semi_auto",
                    "symbol": signal.get("symbol"),
                    "trading_symbol": signal.get("trading_symbol") or signal.get("symbol"),
                    "opt_type": signal.get("opt_type"),
                    "entry_price": signal.get("entry_price"),
                    "stop_loss": signal.get("stop_loss"),
                    "quantity": signal.get("quantity"),
                    "pending_trade_id": result.pending_trade_id,
                    "strategy": signal.get("strategy"),
                })
        except Exception as e:
            print(f"{_now()} [exec:u{self.user_id}] Semi-auto err: {e}")

    def _execute_auto(self, signal: dict):
        """Execute live trade automatically."""
        print(f"{_now()} [exec:u{self.user_id}] Auto execution not yet implemented in shared mode")

    # ================================================================
    # RISK MANAGEMENT
    # ================================================================

    def _record_entry(self):
        with self._risk_lock:
            self._maybe_reset_risk()
            self._trades_today += 1

    def _record_pnl(self, pnl: float):
        with self._risk_lock:
            self._maybe_reset_risk()
            self._net_pnl_today += pnl

    def _risk_ok(self) -> bool:
        max_trades = self.config.get("max_trades_per_day", 5)
        max_loss = self.config.get("max_loss_per_day", 5000.0)
        with self._risk_lock:
            self._maybe_reset_risk()
            if self._trades_today >= max_trades:
                return False
            if self._net_pnl_today <= -abs(max_loss):
                return False
            return True

    def _maybe_reset_risk(self):
        today = datetime.now().strftime("%Y-%m-%d")
        if self._last_date != today:
            self._last_date = today
            self._trades_today = 0
            self._net_pnl_today = 0.0

    def _trading_hours_ok(self) -> bool:
        try:
            now = datetime.now()
            s = datetime.strptime(self.config.get("trade_start_time", "09:20"), "%H:%M")
            e = datetime.strptime(self.config.get("trade_end_time", "15:00"), "%H:%M")
            s = s.replace(year=now.year, month=now.month, day=now.day)
            e = e.replace(year=now.year, month=now.month, day=now.day)
            return s <= now <= e
        except Exception:
            return True

    def _push_position_snapshot(self, symbol: str):
        """Push position snapshot to Redis for dashboard access."""
        with self._positions_lock:
            positions = [
                {
                    "symbol": sym,
                    "mode": "paper" if self.paper_mode else "live",
                    "position": pos,
                }
                for sym, pos in self._positions.items()
            ]

        set_positions_sync(self.user_id, positions)


# ================================================================
# EXECUTION MANAGER REGISTRY
# ================================================================

class UserExecutionRegistry:
    """Tracks active UserExecutionManager instances."""

    def __init__(self):
        self._managers: dict[int, UserExecutionManager] = {}
        self._lock = threading.Lock()

    def start_user(self, user_id: int, config: dict, access_token: str,
                   on_status_change: Callable = None,
                   on_trade: Callable = None) -> UserExecutionManager:
        with self._lock:
            if user_id in self._managers:
                self._managers[user_id].stop()
            mgr = UserExecutionManager(
                user_id, config, access_token,
                on_status_change, on_trade,
            )
            self._managers[user_id] = mgr
            mgr.start()
            return mgr

    def stop_user(self, user_id: int):
        with self._lock:
            mgr = self._managers.pop(user_id, None)
            if mgr:
                mgr.stop()

    def stop_all(self):
        with self._lock:
            for mgr in list(self._managers.values()):
                mgr.stop()
            self._managers.clear()

    def is_running(self, user_id: int) -> bool:
        with self._lock:
            return user_id in self._managers

    def running_users(self) -> list[int]:
        with self._lock:
            return list(self._managers.keys())


user_registry = UserExecutionRegistry()
