# backend/shared/market_structure_engine.py
# ================================================================
# Shared Market Structure Engine — ONE per symbol.
#
# Wraps the existing MarketStructureEngine (1m premium) and
# UnderlyingMarketStructureEngine (5m underlying) so they execute
# ONCE per symbol rather than once per user per symbol.
#
# State is stored in Redis for recovery. Results are served via
# Redis so any process/user can read them.
# ================================================================

import json
import threading
import time
from datetime import datetime
from typing import Optional

from backend.shared.candle_builder import SharedCandleBuilder
from backend.shared.option_premium_service import SharedOptionPremiumBuilder
from backend.shared.redis_infra import (
    shared_candle_close_channel,
    shared_premium_close_channel,
    shared_market_structure_1m,
    shared_market_structure_5m,
    shared_market_structure_state,
)
from backend.shared.shared_cache import STRUCTURE_TTL_SEC
from backend.services.redis_client import get_redis_sync


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


class SharedMarketStructureEngine:
    """
    Maintains ONE MarketStructureEngine and ONE
    UnderlyingMarketStructureEngine per symbol.

    Processes candle-close events from the shared candle builder.
    """

    def __init__(self, symbol: str):
        self.symbol = symbol.upper()
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._r = get_redis_sync()

        self._premium_engine = None    # MarketStructureEngine
        self._underlying_engine = None # UnderlyingMarketStructureEngine
        self._init_engines()

        self._lock = threading.Lock()

        # Tracking to prevent duplicate analysis
        self._premium_analyzed_min: Optional[str] = None
        self._last_underlying_min: Optional[str] = None
        self._last_5m_boundary: Optional[str] = None

    def _init_engines(self):
        """Lazily initialize structure engines; try to restore from Redis."""
        try:
            from backend.engine.market_structure import MarketStructureEngine
            self._premium_engine = MarketStructureEngine(max_history=1000)
        except Exception as e:
            print(f"{_now()} [structure:{self.symbol}] Premium init err: {e}")

        try:
            from backend.engine.underlying_market_structure import (
                UnderlyingMarketStructureEngine,
            )
            self._underlying_engine = UnderlyingMarketStructureEngine()
        except Exception as e:
            print(f"{_now()} [structure:{self.symbol}] Under init err: {e}")

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(
            target=self._loop, daemon=True,
            name=f"structure-{self.symbol}",
        )
        self._thread.start()
        print(f"{_now()} [structure:{self.symbol}] Started")

    def stop(self):
        self._stop_event.set()
        self._save_state()
        print(f"{_now()} [structure:{self.symbol}] Stopped")

    def _save_state(self):
        """Save engine state to Redis for recovery."""
        # Save premium structure state
        if self._premium_engine is not None:
            state = {
                "candles": self._premium_engine.candles[-200:],
                "global_index": self._premium_engine.global_index,
                "trend_direction": self._premium_engine.trend_direction.value
                if hasattr(self._premium_engine.trend_direction, "value")
                else str(self._premium_engine.trend_direction),
            }
            self._r.set(shared_market_structure_state(self.symbol, "1m"),
                        json.dumps(state, default=str),
                        ex=STRUCTURE_TTL_SEC)

        # Save underlying structure state
        if self._underlying_engine is not None:
            state = {
                "candles": self._underlying_engine.candles[-200:]
                if hasattr(self._underlying_engine, "candles") else [],
                "global_index": getattr(self._underlying_engine, "global_index", -1),
            }
            self._r.set(shared_market_structure_state(self.symbol, "5m"),
                        json.dumps(state, default=str),
                        ex=STRUCTURE_TTL_SEC)

    def _loop(self):
        """Subscribe to candle close events and run market structure analysis.

        Premium analysis runs on option-premium bar closes
        (shared_premium_close_channel); underlying analysis runs on
        underlying candle closes (shared_candle_close_channel).
        """
        from backend.shared.pubsub_utils import resilient_pubsub_consumer
        resilient_pubsub_consumer(
            tag=f"structure:{self.symbol}",
            channels=[shared_candle_close_channel(self.symbol),
                      shared_premium_close_channel(self.symbol)],
            handler=self._on_candle_close,
            stop_event=self._stop_event,
        )

    def _on_candle_close(self, event: dict):
        """Process candle close and update structure analysis."""
        # Option-premium close (marked with kind="premium") drives the
        # premium market structure analysis on the OPTION chart. The
        # underlying index closes drive only the underlying analysis —
        # index candles must never be treated as premium data.
        if event.get("kind") == "premium":
            self._run_premium_analysis(event)
            return

        interval = event.get("interval", "1m")
        if interval == "5m":
            self._run_underlying_analysis(event, is_confirmed=True)
        else:
            # 1m underlying close — developing underlying analysis
            self._run_underlying_analysis(event, is_confirmed=False)

    def _run_premium_analysis(self, event: dict):
        """Run premium (1-minute) market structure analysis.

        Feeds the OPTION-PREMIUM candle series into the premium engine
        (mirrors legacy engine_v6._run_premium_analysis, which analyses
        opt_df). Underlying index candles are never used here.
        """
        if self._premium_engine is None:
            return

        now_min = event.get("ts", datetime.now().strftime("%Y-%m-%d %H:%M"))
        if self._premium_analyzed_min == now_min:
            return

        df = SharedOptionPremiumBuilder.get_1m_df(self.symbol)
        if df.empty or len(df) < 5:
            return

        last = df.iloc[-1]
        candle = {
            "open": float(last["open"]),
            "high": float(last["high"]),
            "low": float(last["low"]),
            "close": float(last["close"]),
            "volume": float(last.get("volume", 0)),
            "time": last.name if hasattr(last, "name") else last["time"],
        }

        with self._lock:
            try:
                result = self._premium_engine.update(candle, full_analysis=True)
                self._premium_analyzed_min = now_min

                if result is not None:
                    ser = self._serialize_structure_result(result)
                    self._store_result("1m", ser)
                    print(f"{_now()} [structure:{self.symbol}] Premium 1m "
                          f"trend={ser['trend']['direction']}/{ser['trend']['strength']} "
                          f"phase={ser['phase']} conf={ser['confidence_score']} "
                          f"pb={ser['pullback']['type']} rec={ser['recovery']['status']}")
            except Exception as e:
                print(f"{_now()} [structure:{self.symbol}] Premium err: {e}")

    def _run_underlying_analysis(self, event: dict, is_confirmed: bool = False):
        """Run underlying (5-minute) market structure analysis."""
        if self._underlying_engine is None:
            return

        now_min = event.get("ts", datetime.now().strftime("%Y-%m-%d %H:%M"))
        if not is_confirmed and self._last_underlying_min == now_min:
            return

        df = SharedCandleBuilder.get_5m_df_from_redis(self.symbol)
        if df.empty:
            return

        last = df.iloc[-1]
        candle = {
            "open": float(last["open"]),
            "high": float(last["high"]),
            "low": float(last["low"]),
            "close": float(last["close"]),
            "volume": float(last.get("volume", 0)),
            "time": last.name if hasattr(last, "name") else last["time"],
        }

        with self._lock:
            try:
                if is_confirmed:
                    result = self._underlying_engine.update(candle, full_analysis=True)
                    self._last_5m_boundary = now_min
                else:
                    self._underlying_engine.update(candle, full_analysis=False)
                    trend = self._underlying_engine._get_trend_metrics()
                    phase_result = self._underlying_engine._determine_market_phase()
                    confidence = self._underlying_engine._compute_confidence_score(
                        phase_result.phase)
                    result = self._underlying_engine._build_result(trend, phase_result, confidence)

                self._last_underlying_min = now_min

                if result is not None:
                    ser = self._serialize_underlying_result(result)
                    self._store_result("5m", ser)
                    print(f"{_now()} [structure:{self.symbol}] Underlying 5m "
                          f"trend={ser['trend']} bias={ser['market_bias']} "
                          f"phase={ser['market_phase']} conf={ser['confidence']}")
            except Exception as e:
                print(f"{_now()} [structure:{self.symbol}] Under err: {e}")

    def _serialize_structure_result(self, result) -> dict:
        """Serialize a premium StructureResult to a JSON-safe dict."""
        from backend.engine.market_structure import TrendDirection as PTrend
        from backend.engine.market_structure import PullbackType as PPullback
        from backend.engine.market_structure import RecoveryStatus as RecStatus

        return {
            "trend": {
                "direction": result.trend.direction.value,
                "strength": result.trend.strength,
                "quality": result.trend.quality,
                "age": result.trend.age,
                "momentum": result.trend.momentum,
                "exhaustion": result.trend.exhaustion,
                "continuation_probability": result.trend.continuation_probability,
            },
            "pullback": {
                "type": result.pullback.type.value,
                "quality": result.pullback.quality,
                "strength": result.pullback.strength,
                "duration": result.pullback.duration,
                "depth_pct": result.pullback.depth_pct,
            },
            "recovery": {
                "status": result.recovery.status.value,
                "quality": result.recovery.quality,
                "strength": result.recovery.strength,
                "confidence": result.recovery.confidence,
            },
            "phase": result.phase.value if hasattr(result.phase, "value") else str(result.phase),
            "confidence_score": result.confidence_score,
            "protected_high": result.protected_high.price if result.protected_high else None,
            "protected_low": result.protected_low.price if result.protected_low else None,
            "support_levels": [
                {"price": s.price, "strength": s.strength, "touches": s.touches}
                for s in (result.support_levels or [])
            ],
            "resistance_levels": [
                {"price": r.price, "strength": r.strength, "touches": r.touches}
                for r in (result.resistance_levels or [])
            ],
            "recent_breaks": [
                {"type": b.type.value, "direction": b.direction.value,
                 "level": b.level, "confirmed": b.confirmed}
                for b in (result.recent_breaks or [])
            ] if result.recent_breaks else [],
        }

    def _serialize_underlying_result(self, result) -> dict:
        """Serialize an UnderlyingStructureResult to JSON-safe dict."""
        return {
            "trend": result.trend.value if hasattr(result.trend, "value") else str(result.trend),
            "market_bias": result.market_bias.value
            if hasattr(result.market_bias, "value") else str(result.market_bias),
            "market_phase": result.market_phase.value
            if hasattr(result.market_phase, "value") else str(result.market_phase),
            "confidence": result.confidence,
            "active_pattern": {
                "pattern_type": result.active_pattern.pattern_type.value,
                "direction": result.active_pattern.direction.value,
                "confidence": result.active_pattern.confidence,
            } if result.active_pattern else None,
            "pattern_state": result.pattern_state.value
            if hasattr(result.pattern_state, "value") else str(result.pattern_state)
            if result.pattern_state else None,
            "liquidity_event": {
                "liquidity_type": result.liquidity_event.liquidity_type.value,
                "confidence": result.liquidity_event.confidence,
            } if result.liquidity_event else None,
        }

    def _store_result(self, engine_type: str, result: dict):
        """Store structure analysis result in Redis."""
        key = (shared_market_structure_1m(self.symbol) if engine_type == "1m"
               else shared_market_structure_5m(self.symbol))
        self._r.set(key, json.dumps(result, default=str), ex=STRUCTURE_TTL_SEC)

    # ================================================================
    # STATIC READERS
    # ================================================================

    @staticmethod
    def get_premium_structure(symbol: str) -> Optional[dict]:
        """Get latest premium market structure result."""
        raw = get_redis_sync().get(shared_market_structure_1m(symbol.upper()))
        if raw:
            return json.loads(raw)
        return None

    @staticmethod
    def get_underlying_structure(symbol: str) -> Optional[dict]:
        """Get latest underlying market structure result."""
        raw = get_redis_sync().get(shared_market_structure_5m(symbol.upper()))
        if raw:
            return json.loads(raw)
        return None

    # ================================================================
    # INSTANCE MANAGEMENT
    # ================================================================

    _instances: dict[str, "SharedMarketStructureEngine"] = {}
    _instances_lock = threading.Lock()

    @classmethod
    def get_or_create(cls, symbol: str) -> "SharedMarketStructureEngine":
        sym = symbol.upper()
        with cls._instances_lock:
            if sym not in cls._instances:
                engine = cls(sym)
                cls._instances[sym] = engine
                engine.start()
            return cls._instances[sym]

    @classmethod
    def stop_symbol(cls, symbol: str):
        sym = symbol.upper()
        with cls._instances_lock:
            engine = cls._instances.pop(sym, None)
            if engine:
                engine.stop()

    @classmethod
    def stop_all(cls):
        with cls._instances_lock:
            for engine in list(cls._instances.values()):
                engine.stop()
            cls._instances.clear()


class SharedUnderlyingMarketStructureEngine(SharedMarketStructureEngine):
    """
    Dedicated engine for 5-minute UNDERLYING market structure analysis.

    Shares the same internals as SharedMarketStructureEngine but has its
    own instance registry so it can be independently created, stopped,
    and counted. Only processes 5m candle close events (underlying).
    """

    _instances: dict[str, "SharedUnderlyingMarketStructureEngine"] = {}
    _instances_lock = threading.Lock()

    def _on_candle_close(self, event: dict):
        """Underlying structure only — ignore option-premium closes.

        This engine analyses the 5m UNDERLYING chart exclusively; the
        option-premium closes are handled by SharedMarketStructureEngine.
        """
        if event.get("kind") == "premium":
            return
        interval = event.get("interval", "1m")
        if interval == "5m":
            self._run_underlying_analysis(event, is_confirmed=True)
        else:
            self._run_underlying_analysis(event, is_confirmed=False)

    @staticmethod
    def get_structure(symbol: str):
        """Read underlying structure from Redis."""
        return SharedMarketStructureEngine.get_underlying_structure(symbol)

    @classmethod
    def get_or_create(cls, symbol: str) -> "SharedUnderlyingMarketStructureEngine":
        sym = symbol.upper()
        with cls._instances_lock:
            if sym not in cls._instances:
                engine = cls(sym)
                cls._instances[sym] = engine
                engine.start()
            return cls._instances[sym]

    @classmethod
    def stop_symbol(cls, symbol: str):
        sym = symbol.upper()
        with cls._instances_lock:
            engine = cls._instances.pop(sym, None)
            if engine:
                engine.stop()

    @classmethod
    def stop_all(cls):
        with cls._instances_lock:
            for engine in list(cls._instances.values()):
                engine.stop()
            cls._instances.clear()
