# backend/services/execution_layer.py
# ================================================================
# Execution Layer — Multi-Mode Execution System
#
# This is the SINGLE authority controlling how every trade signal is
# executed. Trading strategies NEVER know which execution mode is
# active — they only generate TradeSignal objects.
#
# Three execution modes:
#   1. PAPER      — simulated trades via PaperOrderBook
#   2. SEMI_AUTO  — creates PendingTrade, requires user approval
#   3. AUTO       — live orders (with daily risk disclosure check)
#
# Architecture:
#   TradeSignal
#       ↓
#   ExecutionRouter.get_executor(user_id)
#       ↓
#   PaperExecutor / SemiAutoExecutor / AutoExecutor
#       ↓
#   SymbolEngine._place_order()
#
# Design Principles:
#   - Modular & independent
#   - No trading strategy modifications needed
#   - Backward compatible (default SEMI_AUTO for new users)
#   - Audit-friendly (every event logged)
#   - Extensible for future modes (OTP, e-sign, etc.)
# ================================================================

import json
import hashlib
import threading
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Optional, Callable, Any
from uuid import uuid4

from backend.db.database import AsyncSessionLocal
from backend.db.models import (
    ExecutionMode,
    PendingTrade,
    PendingTradeStatus,
    DailyAutoConsent,
    AuditLog,
    BotConfig,
)
from backend.services.paper_trading import get_paper_book
from backend.services.audit_log import log_event


# ================================================================
# TradeSignal — Standardised signal object
# ================================================================

@dataclass
class TradeSignal:
    """Standardised signal object consumed by the Execution Layer."""
    symbol: str
    opt_type: str  # "CE" or "PE"
    direction: str  # "BUY" (always BUY for options)
    entry_price: float
    stop_loss: float
    quantity: int
    confidence: Optional[float] = None
    strategy_name: Optional[str] = None
    strategy: Optional[str] = None
    timestamp: Optional[str] = None
    instrument_key: Optional[str] = None
    trading_symbol: Optional[str] = None
    strike: Optional[float] = None
    regime: Optional[str] = None
    reason: Optional[str] = None
    signal_id: str = field(default_factory=lambda: uuid4().hex[:16])

    def __post_init__(self):
        if self.strategy and not self.strategy_name:
            self.strategy_name = self.strategy
        if self.strategy_name and not self.strategy:
            self.strategy = self.strategy_name
        if not self.timestamp:
            self.timestamp = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()


# ================================================================
# Execution Result
# ================================================================

class ExecutionStatus(Enum):
    EXECUTED = "EXECUTED"
    PENDING_APPROVAL = "PENDING_APPROVAL"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"
    FAILED = "FAILED"
    BLOCKED = "BLOCKED"  # No consent for AUTO mode


@dataclass
class ExecutionResult:
    status: ExecutionStatus
    message: str
    signal_id: Optional[str] = None
    trade_id: Optional[str] = None
    pending_trade_id: Optional[int] = None
    details: Optional[dict] = None

    def to_dict(self) -> dict:
        return {
            "status": self.status.value,
            "message": self.message,
            "signal_id": self.signal_id,
            "trade_id": self.trade_id,
            "pending_trade_id": self.pending_trade_id,
            "details": self.details,
        }


# ================================================================
# Approved-trade placement (shared by worker.py + shared_worker.py)
# ================================================================

def place_order_from_engines(user_id: int, engines: list, signal: TradeSignal,
                             trade_id: Optional[int] = None) -> Optional[str]:
    """
    Place an approved trade on the first engine matching the signal's
    symbol. Works with both legacy TradingEngine instances and the
    shared-mode _MockEngine wrappers (same public interface:
    _place_order / _get_fill_price / position / on_trade / _risk).

    Returns the entry order id on success, or None on failure.
    """
    from backend.shared.shared_cache import get_lot_size

    for eng in engines:
        if getattr(eng, "symbol", None) != signal.symbol:
            continue

        # Shared-mode wrappers expose _seed_ltp; seed LTP from the signal
        # only when no live tick has arrived yet — never overwrite the
        # current LTP with the stale signal price, so approved semi-auto
        # trades fill at the price current at approval time.
        seed = getattr(eng, "_seed_ltp", None)
        if seed and signal.entry_price:
            seed(signal.entry_price)

        num_lots = getattr(eng, "symbol_lots",
                           max(1, int(eng.cfg.get("order_qty", 1))))
        custom_ls = eng.cfg.get("custom_lot_sizes") or {}
        lot_size = get_lot_size(eng.symbol, custom_ls)
        qty = num_lots * lot_size
        if getattr(eng, "_regime", None) and eng._regime.regime == "VOLATILE":
            qty = max(lot_size, (num_lots // 2) * lot_size)

        # Only shared-mode wrappers accept an explicit instrument_key.
        kw = {}
        if signal.instrument_key and getattr(eng, "_seed_ltp", None) is not None:
            kw["instrument_key"] = signal.instrument_key

        eid = eng._place_order("BUY", qty, **kw)
        if not eid:
            return None
        fill = eng._get_fill_price(eid)
        if not fill:
            return None

        # Approved semi-auto trades fill at the CURRENT market price,
        # which may differ from the signal-time entry — recompute the SL
        # from the actual fill (sl_pct of the fill) so risk is anchored
        # to the real entry, not the stale signal price.
        sl_pct = float(eng.cfg.get("sl_pct", 0.003) or 0.003)
        stop_loss = round(fill * (1 - sl_pct), 2) if fill else signal.stop_loss

        sl_id = eng._place_order("SELL", qty, order_type="SL-M",
                                 trigger=stop_loss, **kw)
        if not sl_id:
            eng._place_order("SELL", qty)
            return None

        rr = eng.cfg.get("target_rr", 1.3)
        risk = abs(fill - stop_loss)
        target = round(fill + risk * rr, 2)
        near_pct = eng.cfg.get("target_near_pct", 0.003)
        eng.position = {
            "entry_price": fill, "qty": qty,
            "entry_order_id": eid, "sl_order_id": sl_id,
            "sl_trigger": stop_loss, "target": target,
            "near_target": round(target * (1 - near_pct), 2),
            "strategy": signal.strategy_name or signal.strategy or "",
            "entry_ts": datetime.now(timezone.utc).replace(tzinfo=None),
            "instrument_key": signal.instrument_key or eng.instrument_key,
            "trading_symbol": signal.trading_symbol or eng.trading_symbol,
            "opt_type": signal.opt_type or eng.opt_type,
            "strike": signal.strike or eng.strike,
            "paper_mode": eng.paper_mode, "symbol": signal.symbol,
            "regime": getattr(eng, "_regime", None).regime
                      if getattr(eng, "_regime", None) else "",
        }
        eng.sl_order_id = sl_id
        eng.trailing_sl = stop_loss
        eng._sl_mod_ts = time.time()
        eng._risk.record_entry()
        eng._last_entry_price = fill

        # Push the position snapshot to Redis immediately so the
        # dashboard/terminal shows the fresh position (Entry/SL/Target
        # lines) as soon as the approval events below are relayed —
        # otherwise the WS position reader returns a stale (empty)
        # snapshot until the next 2s monitor poll.
        try:
            if getattr(eng, "_seed_ltp", None) is not None:
                mgr = getattr(eng, "_mgr", None)
                if mgr and hasattr(mgr, "_push_position_snapshot"):
                    mgr._push_position_snapshot(eng.symbol)
            else:
                eng._push_position_snapshot()
        except Exception:
            pass

        mode = "paper" if eng.paper_mode else "live"
        if eng._tg_token and eng._tg_chat and eng.cfg.get("telegram_on_entry", True):
            from backend.services import telegram_alerts as tg
            tg.alert_entry(eng._tg_token, eng._tg_chat,
                           signal.trading_symbol or signal.symbol,
                           signal.opt_type or "", fill, stop_loss,
                           target, qty, signal.strategy_name or "", mode)
        if eng.on_trade:
            eng.on_trade({
                "event": "ENTRY", "user_id": user_id, "mode": mode,
                **eng.position,
            })
            eng.on_trade({
                "event": "SIGNAL_APPROVED", "user_id": user_id,
                "mode": mode, "symbol": signal.symbol,
                "trading_symbol": signal.trading_symbol,
                "opt_type": signal.opt_type,
                "strike": signal.strike,
                "entry_price": fill,
                "pending_trade_id": trade_id,
            })
        return eid

    print(f"[execution_layer] No engine available for approved trade "
          f"#{trade_id} on {signal.symbol}")
    return None


# ================================================================
# Daily Auto Consent Validator
# ================================================================

RISK_DISCLOSURE_VERSION = "v1.0"

RISK_DISCLOSURE_TEXT = (
    "RISK DISCLOSURE AND AUTHORIZATION FOR FULLY AUTOMATIC TRADING\n\n"
    "1. This system places live trades automatically on your behalf.\n"
    "2. Real money from your linked broker account will be used for trading.\n"
    "3. Trading in options and derivatives involves substantial financial risk.\n"
    "4. Losses may be partial or total, including the possibility of losing your entire investment.\n"
    "5. Profit is never guaranteed. Past performance does not guarantee future results.\n"
    "6. Internet failures, exchange failures, broker failures, and software failures may occur, "
    "which could result in delayed execution or unexecuted orders.\n"
    "7. You remain fully responsible for monitoring your account and maintaining sufficient margin "
    "at all times.\n"
    "8. By continuing, you authorize this platform to execute live trades automatically "
    "for the current trading session.\n\n"
    "By accepting this disclosure, you confirm that you understand and accept these risks."
)


def compute_consent_audit_hash(
    user_id: int,
    timestamp: str,
    risk_version: str,
    risk_text: str,
) -> str:
    """
    Generates an integrity hash for consent records.
    SHA-256(user_id + timestamp + risk_version + risk_text)
    Any future modification invalidates the hash.
    """
    raw = f"{user_id}|{timestamp}|{risk_version}|{risk_text}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


async def check_daily_auto_consent(user_id: int) -> bool:
    """
    Checks if the user has a valid daily consent for AUTO mode.
    Returns True if valid consent exists, False otherwise.
    """
    from sqlalchemy import select as _select

    async with AsyncSessionLocal() as db:
        res = await db.execute(
            _select(DailyAutoConsent)
            .where(DailyAutoConsent.user_id == user_id)
            .order_by(DailyAutoConsent.created_at.desc())
            .limit(1)
        )
        consent = res.scalar_one_or_none()
        if not consent:
            return False
        if not consent.accepted:
            return False
        if not consent.valid_until or consent.valid_until <= datetime.now(timezone.utc).replace(tzinfo=None):
            return False
        return True


# ================================================================
# EXECUTORS
# ================================================================

class PaperExecutor:
    """
    Executes trades through the paper trading engine.
    Never places live orders.
    Maintains virtual positions, P&L, and trade history.
    """

    def __init__(self, user_id: int, signal: TradeSignal):
        self.user_id = user_id
        self.signal = signal
        self._paper = get_paper_book(user_id)

    async def execute(self) -> ExecutionResult:
        """
        Execute the signal via paper trading.
        Returns immediately with simulated fill.
        """
        try:
            ltp = self.signal.entry_price
            order = self._paper.place_market_order(
                side="BUY",
                qty=self.signal.quantity,
                ltp=ltp,
                instrument_key=self.signal.instrument_key or "",
                tag=f"paper:{self.user_id}:{self.signal.signal_id}",
            )
            return ExecutionResult(
                status=ExecutionStatus.EXECUTED,
                message=f"Paper trade executed at ₹{order.get('fill_price', ltp)}",
                signal_id=self.signal.signal_id,
                trade_id=order.get("order_id"),
                details={"mode": "paper", "fill_price": order.get("fill_price", ltp)},
            )
        except Exception as e:
            return ExecutionResult(
                status=ExecutionStatus.FAILED,
                message=f"Paper execution failed: {e}",
                signal_id=self.signal.signal_id,
            )

    def execute_sync(self) -> ExecutionResult:
        """Sync version of execute() - paper execution has no async ops."""
        try:
            ltp = self.signal.entry_price
            order = self._paper.place_market_order(
                side="BUY",
                qty=self.signal.quantity,
                ltp=ltp,
                instrument_key=self.signal.instrument_key or "",
                tag=f"paper:{self.user_id}:{self.signal.signal_id}",
            )
            return ExecutionResult(
                status=ExecutionStatus.EXECUTED,
                message=f"Paper trade executed at ₹{order.get('fill_price', ltp)}",
                signal_id=self.signal.signal_id,
                trade_id=order.get("order_id"),
                details={"mode": "paper", "fill_price": order.get("fill_price", ltp)},
            )
        except Exception as e:
            return ExecutionResult(
                status=ExecutionStatus.FAILED,
                message=f"Paper execution failed: {e}",
                signal_id=self.signal.signal_id,
            )



class SemiAutoExecutor:
    """
    Creates a PendingTrade record that requires user approval before
    execution. No live trade without explicit approval.
    Supports configurable timeout (default 25 seconds).
    """

    PENDING_TIMEOUT_SEC = 25

    def __init__(self, user_id: int, signal: TradeSignal):
        self.user_id = user_id
        self.signal = signal

    async def execute(self) -> ExecutionResult:
        """
        Create a PendingTrade in WAITING status and notify the user.
        The trade will be executed when the user approves via:
        - Dashboard
        - Telegram
        - Mobile App (future)
        """
        try:
            expires_at = datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(seconds=self.PENDING_TIMEOUT_SEC)

            pending_trade = PendingTrade(
                user_id=self.user_id,
                signal_id=self.signal.signal_id,
                symbol=self.signal.symbol,
                opt_type=self.signal.opt_type,
                strategy=self.signal.strategy_name or self.signal.strategy or "",
                entry_price=self.signal.entry_price,
                stop_loss=self.signal.stop_loss,
                quantity=self.signal.quantity,
                confidence=self.signal.confidence,
                status=PendingTradeStatus.WAITING,
                expires_at=expires_at,
                signal_payload=json.dumps(asdict(self.signal), default=str),
            )

            async with AsyncSessionLocal() as db:
                db.add(pending_trade)
                await db.commit()
                await db.refresh(pending_trade)
                trade_id = pending_trade.id

                # Audit log
                await log_event(
                    db,
                    self.user_id,
                    "SIGNAL_GENERATED",
                    f"Pending trade created: {self.signal.symbol} "
                    f"{self.signal.opt_type} @ ₹{self.signal.entry_price}",
                    metadata={
                        "signal_id": self.signal.signal_id,
                        "pending_trade_id": trade_id,
                        "strategy": self.signal.strategy_name,
                        "expires_at": expires_at.isoformat(),
                    },
                )

            return ExecutionResult(
                status=ExecutionStatus.PENDING_APPROVAL,
                message=f"Pending trade #{trade_id} created — waiting for approval",
                signal_id=self.signal.signal_id,
                pending_trade_id=trade_id,
                details={
                    "expires_at": expires_at.isoformat(),
                    "timeout_sec": self.PENDING_TIMEOUT_SEC,
                },
            )
        except Exception as e:
            return ExecutionResult(
                status=ExecutionStatus.FAILED,
                message=f"Semi-auto execution failed: {e}",
                signal_id=self.signal.signal_id,
            )


class AutoExecutor:
    """
    Executes live trades automatically.
    Requires valid daily risk disclosure and authorization before
    the first trade of the trading session.
    """

    def __init__(
        self,
        user_id: int,
        signal: TradeSignal,
        place_order_fn: Callable,
    ):
        self.user_id = user_id
        self.signal = signal
        self._place_order = place_order_fn

    async def execute(self) -> ExecutionResult:
        """
        Check daily consent, then place live order.
        """
        # Check daily consent
        consent_valid = await check_daily_auto_consent(self.user_id)
        if not consent_valid:
            return ExecutionResult(
                status=ExecutionStatus.BLOCKED,
                message="Daily risk disclosure not accepted — "
                "please accept in Settings before starting auto trading",
                signal_id=self.signal.signal_id,
            )

        try:
            # Place live order via the provided callable
            order_id = self._place_order(self.signal)

            if order_id:
                return ExecutionResult(
                    status=ExecutionStatus.EXECUTED,
                    message=f"Live order placed: {order_id}",
                    signal_id=self.signal.signal_id,
                    trade_id=order_id,
                    details={"mode": "live", "order_id": order_id},
                )
            else:
                return ExecutionResult(
                    status=ExecutionStatus.FAILED,
                    message="Live order placement failed",
                    signal_id=self.signal.signal_id,
                )
        except Exception as e:
            return ExecutionResult(
                status=ExecutionStatus.FAILED,
                message=f"Auto execution failed: {e}",
                signal_id=self.signal.signal_id,
            )

    def execute_sync(self) -> ExecutionResult:
        """Sync version of execute() - uses sync consent check."""
        from backend.db.database import get_sync_session
        from sqlalchemy import select as _select

        consent_valid = False
        with get_sync_session() as db:
            res = db.execute(
                _select(DailyAutoConsent)
                .where(DailyAutoConsent.user_id == self.user_id)
                .order_by(DailyAutoConsent.created_at.desc())
                .limit(1)
            )
            consent = res.scalar_one_or_none()
            if consent and consent.accepted:
                if consent.valid_until and consent.valid_until > datetime.now(timezone.utc).replace(tzinfo=None):
                    consent_valid = True

        if not consent_valid:
            return ExecutionResult(
                status=ExecutionStatus.BLOCKED,
                message="Daily risk disclosure not accepted — "
                "please accept in Settings before starting auto trading",
                signal_id=self.signal.signal_id,
            )

        try:
            order_id = self._place_order(self.signal)
            if order_id:
                return ExecutionResult(
                    status=ExecutionStatus.EXECUTED,
                    message=f"Live order placed: {order_id}",
                    signal_id=self.signal.signal_id,
                    trade_id=order_id,
                    details={"mode": "live", "order_id": order_id},
                )
            else:
                return ExecutionResult(
                    status=ExecutionStatus.FAILED,
                    message="Live order placement failed",
                    signal_id=self.signal.signal_id,
                )
        except Exception as e:
            return ExecutionResult(
                status=ExecutionStatus.FAILED,
                message=f"Auto execution failed: {e}",
                signal_id=self.signal.signal_id,
            )


# ================================================================
# EXECUTION ROUTER
# ================================================================

class ExecutionRouter:
    """
    Routes trade signals to the correct executor based on the user's
    saved execution mode. This is the only component that decides
    how a trade is executed.
    """

    def __init__(self):
        self._mode_cache: dict[int, tuple[str, float]] = {}
        self._cache_ttl = 30.0  # seconds
        self._lock = threading.Lock()

    async def get_execution_mode(self, user_id: int) -> ExecutionMode:
        """
        Fetch the user's execution mode from the database,
        with a short TTL cache to avoid DB hits on every signal.
        """
        from sqlalchemy import select as _select

        now = time.time()
        with self._lock:
            cached = self._mode_cache.get(user_id)
            if cached and (now - cached[1]) < self._cache_ttl:
                return ExecutionMode(cached[0])

        async with AsyncSessionLocal() as db:
            # Check User-level execution_mode first
            res = await db.execute(
                _select(BotConfig.execution_mode).where(BotConfig.user_id == user_id)
            )
            mode = res.scalar_one_or_none()
            if mode is None:
                mode = ExecutionMode.SEMI_AUTO  # default for all users

            with self._lock:
                self._mode_cache[user_id] = (mode.value, now)
            return mode

    async def invalidate_cache(self, user_id: int):
        """Force next lookup to hit the database."""
        with self._lock:
            self._mode_cache.pop(user_id, None)

    async def execute(
        self,
        user_id: int,
        signal: TradeSignal,
        place_order_fn: Optional[Callable] = None,
    ) -> ExecutionResult:
        """
        Route the signal to the appropriate executor.
        This is the main entry point for the Execution Layer.

        Args:
            user_id: The user ID
            signal: The TradeSignal to execute
            place_order_fn: Callable for live order placement
                           (provided by SymbolEngine)

        Returns:
            ExecutionResult with status and details
        """
        mode = await self.get_execution_mode(user_id)

        if mode == ExecutionMode.PAPER:
            executor = PaperExecutor(user_id, signal)
            return await executor.execute()

        elif mode == ExecutionMode.SEMI_AUTO:
            executor = SemiAutoExecutor(user_id, signal)
            return await executor.execute()

        elif mode == ExecutionMode.AUTO:
            if place_order_fn is None:
                return ExecutionResult(
                    status=ExecutionStatus.FAILED,
                    message="Auto mode requires a place_order_fn",
                    signal_id=signal.signal_id,
                )
            executor = AutoExecutor(user_id, signal, place_order_fn)
            return await executor.execute()

        else:
            return ExecutionResult(
                status=ExecutionStatus.FAILED,
                message=f"Unknown execution mode: {mode}",
                signal_id=signal.signal_id,
            )

    def execute_sync(
        self,
        user_id: int,
        signal: TradeSignal,
        place_order_fn: Optional[Callable] = None,
    ) -> ExecutionResult:
        """
        Sync version of execute() for worker-thread contexts.
        Avoids cross-event-loop asyncpg operations.
        """
        from sqlalchemy import select as _select

        # Fetch execution mode from DB via sync session
        now = time.time()
        with self._lock:
            cached = self._mode_cache.get(user_id)
            if cached and (now - cached[1]) < self._cache_ttl:
                mode = ExecutionMode(cached[0])
            else:
                mode = None

        if mode is None:
            from backend.db.database import get_sync_session
            with get_sync_session() as db:
                res = db.execute(
                    _select(BotConfig.execution_mode).where(BotConfig.user_id == user_id)
                )
                raw = res.scalar_one_or_none()
                mode = raw if raw is not None else ExecutionMode.SEMI_AUTO
            with self._lock:
                self._mode_cache[user_id] = (mode.value, now)

        if mode == ExecutionMode.PAPER:
            executor = PaperExecutor(user_id, signal)
            return executor.execute_sync()

        elif mode == ExecutionMode.SEMI_AUTO:
            return PendingTradeManager.create_pending_trade_sync(user_id, signal)

        elif mode == ExecutionMode.AUTO:
            if place_order_fn is None:
                return ExecutionResult(
                    status=ExecutionStatus.FAILED,
                    message="Auto mode requires a place_order_fn",
                    signal_id=signal.signal_id,
                )
            executor = AutoExecutor(user_id, signal, place_order_fn)
            return executor.execute_sync()

        else:
            return ExecutionResult(
                status=ExecutionStatus.FAILED,
                message=f"Unknown execution mode: {mode}",
                signal_id=signal.signal_id,
            )


# ================================================================
# Pending Trade Manager (approve/reject/expire)
# ================================================================

class PendingTradeManager:
    """
    Handles approval, rejection, and expiration of pending trades.
    Used by the worker process handlers and API endpoints.
    """

    @staticmethod
    async def approve(
        trade_id: int,
        user_id: int,
        place_order_fn: Callable,
    ) -> ExecutionResult:
        """
        Approve a pending trade and execute it.
        Validates that the trade is still WAITING and not expired.
        """
        from sqlalchemy import select as _select

        async with AsyncSessionLocal() as db:
            res = await db.execute(
                _select(PendingTrade).where(
                    PendingTrade.id == trade_id,
                    PendingTrade.user_id == user_id,
                )
            )
            trade = res.scalar_one_or_none()
            if not trade:
                return ExecutionResult(
                    status=ExecutionStatus.FAILED,
                    message="Pending trade not found",
                )

            if trade.status != PendingTradeStatus.WAITING:
                return ExecutionResult(
                    status=ExecutionStatus.FAILED,
                    message=f"Trade is not waiting: {trade.status.value}",
                )

            # Check expiration
            if trade.expires_at and trade.expires_at <= datetime.now(timezone.utc).replace(tzinfo=None):
                trade.status = PendingTradeStatus.EXPIRED
                db.add(trade)
                await db.commit()
                await log_event(
                    db, user_id, "SIGNAL_EXPIRED",
                    f"Pending trade #{trade_id} expired",
                    metadata={"signal_id": trade.signal_id},
                )
                return ExecutionResult(
                    status=ExecutionStatus.EXPIRED,
                    message="Pending trade has expired",
                )

            # Execute the trade
            try:
                # Reconstruct signal from stored payload
                signal_data = json.loads(trade.signal_payload) if trade.signal_payload else {}
                signal_data.setdefault("direction", "BUY")
                signal = TradeSignal(**signal_data)

                # Mark as approved
                trade.status = PendingTradeStatus.APPROVED
                trade.approved_at = datetime.now(timezone.utc).replace(tzinfo=None)
                db.add(trade)

                # Audit
                await log_event(
                    db, user_id, "SIGNAL_APPROVED",
                    f"Trade #{trade_id} approved: {trade.symbol} "
                    f"{trade.opt_type} @ ₹{trade.entry_price}",
                    metadata={
                        "signal_id": trade.signal_id,
                        "pending_trade_id": trade_id,
                    },
                )
                await db.commit()

                # Place the order using the engine's order placement
                # This happens AFTER commit so the approval is recorded
                order_id = place_order_fn(signal)

                if order_id:
                    return ExecutionResult(
                        status=ExecutionStatus.EXECUTED,
                        message=f"Trade #{trade_id} approved and executed: {order_id}",
                        signal_id=trade.signal_id,
                        trade_id=order_id,
                        pending_trade_id=trade_id,
                    )
                else:
                    return ExecutionResult(
                        status=ExecutionStatus.FAILED,
                        message=f"Trade #{trade_id} approved but order placement failed",
                        signal_id=trade.signal_id,
                        pending_trade_id=trade_id,
                    )
            except Exception as e:
                await log_event(
                    db, user_id, "SIGNAL_APPROVED_FAILED",
                    f"Trade #{trade_id} approval failed: {e}",
                    metadata={"signal_id": trade.signal_id, "error": str(e)},
                )
                return ExecutionResult(
                    status=ExecutionStatus.FAILED,
                    message=f"Trade execution failed: {e}",
                    signal_id=trade.signal_id,
                    pending_trade_id=trade_id,
                )

    @staticmethod
    async def reject(trade_id: int, user_id: int) -> ExecutionResult:
        """Reject a pending trade."""
        from sqlalchemy import select as _select

        async with AsyncSessionLocal() as db:
            res = await db.execute(
                _select(PendingTrade).where(
                    PendingTrade.id == trade_id,
                    PendingTrade.user_id == user_id,
                )
            )
            trade = res.scalar_one_or_none()
            if not trade:
                return ExecutionResult(
                    status=ExecutionStatus.FAILED,
                    message="Pending trade not found",
                )

            if trade.status != PendingTradeStatus.WAITING:
                return ExecutionResult(
                    status=ExecutionStatus.FAILED,
                    message=f"Trade is not waiting: {trade.status.value}",
                )

            trade.status = PendingTradeStatus.REJECTED
            db.add(trade)
            await log_event(
                db, user_id, "SIGNAL_REJECTED",
                f"Trade #{trade_id} rejected by user: {trade.symbol} "
                f"{trade.opt_type} @ ₹{trade.entry_price}",
                metadata={"signal_id": trade.signal_id},
            )
            await db.commit()

            return ExecutionResult(
                status=ExecutionStatus.REJECTED,
                message=f"Trade #{trade_id} rejected",
                signal_id=trade.signal_id,
                pending_trade_id=trade_id,
            )

    @staticmethod
    async def expire_stale_trades():
        """
        Background task: mark any WAITING trades past their expiry
        as EXPIRED. Called periodically by the worker.
        """
        from sqlalchemy import select as _select, update as _update

        async with AsyncSessionLocal() as db:
            now = datetime.now(timezone.utc).replace(tzinfo=None)
            res = await db.execute(
                _select(PendingTrade).where(
                    PendingTrade.status == PendingTradeStatus.WAITING,
                    PendingTrade.expires_at <= now,
                )
            )
            expired = res.scalars().all()
            for trade in expired:
                trade.status = PendingTradeStatus.EXPIRED
                db.add(trade)
                await log_event(
                    db, trade.user_id, "SIGNAL_EXPIRED",
                    f"Pending trade #{trade.id} expired (timeout)",
                    metadata={
                        "signal_id": trade.signal_id,
                        "expires_at": trade.expires_at.isoformat() if trade.expires_at else None,
                    },
                )
            if expired:
                await db.commit()
                print(f"[execution_layer] Expired {len(expired)} stale pending trade(s)")


    @staticmethod
    def create_pending_trade_sync(user_id: int, signal: 'TradeSignal') -> 'ExecutionResult':
        """
        Synchronously create a pending trade for semi-auto mode.
        Used when called from sync context (e.g., engine in worker thread).
        """
        from backend.db.database import get_sync_session
        from sqlalchemy import select as _select
        from backend.services.audit_log import log_event
        
        with get_sync_session() as db:
            expires_at = datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(seconds=30)
            
            payload = {
                "strategy_name": signal.strategy_name,
                "symbol": signal.symbol,
                "opt_type": signal.opt_type,
                "direction": getattr(signal, "direction", "BUY"),
                "strike": signal.strike,
                "entry_price": signal.entry_price,
                "stop_loss": signal.stop_loss,
                "quantity": signal.quantity,
                "reason": getattr(signal, "reason", None),
                "confidence": signal.confidence,
                "instrument_key": signal.instrument_key,
                "trading_symbol": signal.trading_symbol,
                "regime": getattr(signal, "regime", None),
            }
            
            from backend.db.models import PendingTrade, PendingTradeStatus
            trade = PendingTrade(
                user_id=user_id,
                signal_id=signal.signal_id,
                symbol=signal.trading_symbol or signal.symbol,
                opt_type=signal.opt_type,
                entry_price=signal.entry_price,
                stop_loss=signal.stop_loss,
                quantity=signal.quantity,
                strategy=signal.strategy_name,
                status=PendingTradeStatus.WAITING,
                signal_payload=json.dumps(payload),
                expires_at=expires_at,
            )
            db.add(trade)
            db.commit()
            
            return ExecutionResult(
                status=ExecutionStatus.PENDING_APPROVAL,
                message="Awaiting user approval",
                signal_id=signal.signal_id,
                pending_trade_id=trade.id,
                details={
                    "expires_at": expires_at.isoformat(),
                },
            )

    @staticmethod
    def approve_sync(
        trade_id: int,
        user_id: int,
        place_order_fn: Callable,
    ) -> 'ExecutionResult':
        """
        Synchronously approve a pending trade and execute it.
        Used when called from sync context (e.g., worker thread).
        """
        from backend.db.database import get_sync_session
        from sqlalchemy import select as _select

        with get_sync_session() as db:
            res = db.execute(
                _select(PendingTrade).where(
                    PendingTrade.id == trade_id,
                    PendingTrade.user_id == user_id,
                )
            )
            trade = res.scalar_one_or_none()
            if not trade:
                return ExecutionResult(
                    status=ExecutionStatus.FAILED,
                    message="Pending trade not found",
                )

            if trade.status != PendingTradeStatus.WAITING:
                return ExecutionResult(
                    status=ExecutionStatus.FAILED,
                    message=f"Trade is not waiting: {trade.status.value}",
                )

            # Check expiration
            if trade.expires_at and trade.expires_at <= datetime.now(timezone.utc).replace(tzinfo=None):
                trade.status = PendingTradeStatus.EXPIRED
                db.add(trade)
                db.commit()
                return ExecutionResult(
                    status=ExecutionStatus.EXPIRED,
                    message="Pending trade has expired",
                )

            # Execute the trade
            try:
                # Reconstruct signal from stored payload
                signal_data = json.loads(trade.signal_payload) if trade.signal_payload else {}
                signal_data.setdefault("direction", "BUY")
                signal = TradeSignal(**signal_data)

                # Mark as approved
                trade.status = PendingTradeStatus.APPROVED
                trade.approved_at = datetime.now(timezone.utc).replace(tzinfo=None)
                db.add(trade)
                db.commit()

                # Place the order using the engine's order placement
                # This happens AFTER commit so the approval is recorded
                order_id = place_order_fn(signal)

                if order_id:
                    return ExecutionResult(
                        status=ExecutionStatus.EXECUTED,
                        message=f"Trade #{trade_id} approved and executed: {order_id}",
                        signal_id=trade.signal_id,
                        trade_id=order_id,
                        pending_trade_id=trade_id,
                    )
                else:
                    return ExecutionResult(
                        status=ExecutionStatus.FAILED,
                        message=f"Trade #{trade_id} approved but order placement failed",
                        signal_id=trade.signal_id,
                        pending_trade_id=trade_id,
                    )
            except Exception as e:
                return ExecutionResult(
                    status=ExecutionStatus.FAILED,
                    message=f"Trade execution failed: {e}",
                    signal_id=trade.signal_id,
                    pending_trade_id=trade_id,
                )

    @staticmethod
    def reject_sync(trade_id: int, user_id: int) -> 'ExecutionResult':
        """Synchronously reject a pending trade."""
        from backend.db.database import get_sync_session
        from sqlalchemy import select as _select

        with get_sync_session() as db:
            res = db.execute(
                _select(PendingTrade).where(
                    PendingTrade.id == trade_id,
                    PendingTrade.user_id == user_id,
                )
            )
            trade = res.scalar_one_or_none()
            if not trade:
                return ExecutionResult(
                    status=ExecutionStatus.FAILED,
                    message="Pending trade not found",
                )

            if trade.status != PendingTradeStatus.WAITING:
                return ExecutionResult(
                    status=ExecutionStatus.FAILED,
                    message=f"Trade is not waiting: {trade.status.value}",
                )

            trade.status = PendingTradeStatus.REJECTED
            db.add(trade)
            db.commit()

            return ExecutionResult(
                status=ExecutionStatus.REJECTED,
                message=f"Trade #{trade_id} rejected",
                signal_id=trade.signal_id,
                pending_trade_id=trade_id,
            )


# ================================================================
# Global Singleton
# ================================================================

execution_router = ExecutionRouter()
pending_trade_manager = PendingTradeManager()

