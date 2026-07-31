# backend/shared/strategy_engine.py
# ================================================================
# Shared Strategy Engine — ONE per symbol.
#
# Evaluates ALL strategies once per symbol per bar close.
# Generates TradeSignal objects and publishes via Redis Pub/Sub.
#
# Users subscribe to their symbols' signal channel and independently
# decide whether to execute (paper/semi-auto/auto).
# ================================================================

import json
import threading
import time
from datetime import datetime
from typing import Optional

import pandas as pd

from backend.shared.redis_infra import (
    shared_signal_channel,
    shared_signal_stream,
    shared_candle_close_channel,
)
from backend.shared.candle_builder import SharedCandleBuilder
from backend.shared.indicator_engine import SharedIndicatorEngine
from backend.shared.market_structure_engine import SharedMarketStructureEngine
from backend.shared.option_chain_service import SharedOptionChainService
from backend.services.redis_client import get_redis_sync
from backend.shared.shared_cache import get_lot_size, is_market_open
from backend.services.state_store import set_bot_status_sync


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# ================================================================
# REGIME ANALYZER (copied from engine_v6 — kept identical)
# ================================================================

ADX_TREND_MIN = 20
ADX_RANGE_MAX = 18
ATR_PCT_MIN   = 0.002
VWAP_BAND_PCT = 0.004


class MarketRegimeAnalyzer:
    """Identical to engine_v6.MarketRegimeAnalyzer."""

    def __init__(self):
        self.regime: str = "NO_TRADE"
        self.adx: float = 0.0
        self.atr_pct: float = 0.0
        self._lock = threading.Lock()

    def analyse(self, df: pd.DataFrame) -> str:
        if df.empty or len(df) < 20:
            return "NO_TRADE"
        close = df["close"]
        high = df["high"]
        low = df["low"]
        tr = pd.concat([
            high - low,
            (high - close.shift()).abs(),
            (low - close.shift()).abs(),
        ], axis=1).max(axis=1)
        atr14 = float(tr.rolling(14).mean().iloc[-1])
        ltp = float(close.iloc[-1])
        atr_pct = atr14 / ltp if ltp > 0 else 0
        adx = self._calc_adx(df, 14)
        ef = float(close.ewm(span=9, adjust=False).mean().iloc[-1])
        em = float(close.ewm(span=15, adjust=False).mean().iloc[-1])
        el = float(close.ewm(span=21, adjust=False).mean().iloc[-1])
        recent_range = (high.iloc[-8:] - low.iloc[-8:]).mean()
        avg_range = (high.iloc[-20:] - low.iloc[-20:]).mean()
        range_ratio = recent_range / avg_range if avg_range > 0 else 1.0
        if adx >= ADX_TREND_MIN and ef > em > el:
            regime = "TRENDING_UP"
        elif adx >= ADX_TREND_MIN and ef < em < el:
            regime = "TRENDING_DOWN"
        elif adx >= ADX_TREND_MIN:
            regime = "TRENDING_UP" if ef > em else "TRENDING_DOWN"
        elif atr_pct > ATR_PCT_MIN * 2.5 and adx < 25:
            regime = "VOLATILE"
        elif adx < ADX_RANGE_MAX and range_ratio < 0.7:
            regime = "RANGING"
        elif atr_pct < ATR_PCT_MIN:
            regime = "NO_TRADE"
        else:
            regime = "NO_TRADE"
        with self._lock:
            self.regime = regime
            self.adx = round(adx, 2)
            self.atr_pct = round(atr_pct * 100, 3)
        return regime

    def get_allowed_strategies(self, opt_type: str) -> list:
        with self._lock:
            regime = self.regime
        full = ["trend_follow", "pullback", "breakout", "vwap_bounce", "ema_cross", "vcgb"]
        if regime == "NO_TRADE":
            return ["pullback"]
        if regime == "RANGING":
            return ["pullback", "breakout", "vcgb"]
        if regime == "VOLATILE":
            return ["pullback"]
        return full

    @staticmethod
    def _calc_adx(df: pd.DataFrame, period: int = 14) -> float:
        try:
            if len(df) < period + 1:
                return 0.0
            high = df["high"]; low = df["low"]; close = df["close"]
            tr = pd.concat([
                high - low,
                (high - close.shift()).abs(),
                (low - close.shift()).abs(),
            ], axis=1).max(axis=1)
            dm_p = ((high - high.shift()) > (low.shift() - low)).astype(float) * \
                   (high - high.shift()).clip(lower=0)
            dm_m = ((low.shift() - low) > (high - high.shift())).astype(float) * \
                   (low.shift() - low).clip(lower=0)
            atr14 = tr.ewm(alpha=1/period, adjust=False).mean()
            di_p = 100 * dm_p.ewm(alpha=1/period, adjust=False).mean() / atr14
            di_m = 100 * dm_m.ewm(alpha=1/period, adjust=False).mean() / atr14
            dx = 100 * (di_p - di_m).abs() / (di_p + di_m).replace(0, 1)
            return float(dx.ewm(alpha=1/period, adjust=False).mean().iloc[-1])
        except Exception:
            return 0.0


# ================================================================
# VWAP CALCULATOR
# ================================================================

def _calc_vwap(df: pd.DataFrame) -> pd.Series:
    try:
        df = df.copy()
        df["typical"] = (df["high"] + df["low"] + df["close"]) / 3
        df["tpv"] = df["typical"] * df["volume"]
        df["date"] = pd.to_datetime(df["time"]).dt.date
        df["cum_tpv"] = df.groupby("date")["tpv"].cumsum()
        df["cum_vol"] = df.groupby("date")["volume"].cumsum()
        return df["cum_tpv"] / df["cum_vol"].replace(0, 1)
    except Exception:
        return pd.Series(dtype=float)


# ================================================================
# SHARED STRATEGY ENGINE
# ================================================================

class SharedStrategyEngine:
    """
    Evaluates all strategies ONCE per symbol per bar close.
    Generates signal dicts and publishes via Redis Pub/Sub.

    The strategies are identical to engine_v6 — only the input data
    source changes (shared cache instead of per-user DataFrames).
    """

    def __init__(self, symbol: str, expiry: str = ""):
        self.symbol = symbol.upper()
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._r = get_redis_sync()
        self._lock = threading.Lock()

        self._prev_direction: Optional[str] = None
        self._regime = MarketRegimeAnalyzer()
        self._last_regime_log: float = 0.0

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(
            target=self._loop, daemon=True,
            name=f"strategy-{self.symbol}",
        )
        self._thread.start()
        print(f"{_now()} [strategy:{self.symbol}] Started")

    def stop(self):
        self._stop_event.set()
        print(f"{_now()} [strategy:{self.symbol}] Stopped")

    def _loop(self):
        """Subscribe to candle close events and evaluate strategies."""
        from backend.shared.pubsub_utils import resilient_pubsub_consumer

        def _on_event(event: dict):
            if event.get("interval") == "1m":
                self._on_1m_close(event)

        resilient_pubsub_consumer(
            tag=f"strategy:{self.symbol}",
            channels=[shared_candle_close_channel(self.symbol)],
            handler=_on_event,
            stop_event=self._stop_event,
        )

    def _on_1m_close(self, event: dict):
        """Evaluate all strategies on 1m candle close."""
        with self._lock:
            self._evaluate_all()

    def _evaluate_all(self):
        """Run all 7 strategies and publish signals."""
        # Get shared data
        df_1m = SharedCandleBuilder.get_1m_df_from_redis(self.symbol)
        if df_1m.empty or len(df_1m) < 25:
            return

        indicators = SharedIndicatorEngine.get_indicators(self.symbol)
        if not indicators:
            return

        # Determine direction from underlying
        direction = self._get_direction(df_1m)
        if direction is None:
            return

        # Regime analysis
        self._regime.analyse(df_1m)
        if time.time() - self._last_regime_log > 300:
            print(f"{_now()} [strategy:{self.symbol}] Regime={self._regime.regime}")
            self._last_regime_log = time.time()
        allowed = self._regime.get_allowed_strategies("CE")

        # Evaluate all strategies
        signals = []

        if "pullback" in allowed:
            s = self._eval_pullback(df_1m, indicators, direction)
            if s: signals.append(s)

        if "trend_follow" in allowed:
            s = self._eval_trend_follow(df_1m, indicators, direction)
            if s: signals.append(s)

        if "breakout" in allowed:
            s = self._eval_breakout(df_1m, indicators, direction)
            if s: signals.append(s)

        if "vwap_bounce" in allowed:
            s = self._eval_vwap_bounce(df_1m, indicators, direction)
            if s: signals.append(s)

        if "ema_cross" in allowed:
            s = self._eval_ema_cross(df_1m, indicators, direction)
            if s: signals.append(s)

        if "vcgb" in allowed:
            s = self._eval_vcgb(df_1m, indicators, direction)
            if s: signals.append(s)

        # Evaluate unified structure-based strategy
        s = self._eval_unified_strategy(df_1m, direction)
        if s: signals.append(s)

        # Publish signals
        self._prev_direction = direction

        ltp = float(df_1m["close"].iloc[-1])
        print(f"{_now()} [strategy:{self.symbol}] Evaluate 1m "
              f"dir={direction} regime={self._regime.regime} "
              f"bars={len(df_1m)} ltp={ltp} signals={len(signals)}")

        for signal in signals:
            self._publish_signal(signal)

    def _get_direction(self, df: pd.DataFrame) -> Optional[str]:
        """Determine BULL/BEAR from underlying EMA cross."""
        if df.empty or len(df) < 15:
            return None
        close = df["close"]
        ef = float(close.ewm(span=9, adjust=False).mean().iloc[-1])
        es = float(close.ewm(span=15, adjust=False).mean().iloc[-1])
        return "BULL" if ef > es else "BEAR"

    def _publish_signal(self, signal: dict):
        """Publish a signal to Redis Pub/Sub and Stream with retry."""
        signal_id = signal.get("id", "")

        print(f"{_now()} [strategy:{self.symbol}] SIGNAL {signal.get('strategy','')} "
              f"{signal.get('opt_type','')} @ {signal.get('entry_price')} "
              f"SL {signal.get('stop_loss')} regime={signal.get('regime','')}")

        if hasattr(self, "_last_published_id") and signal_id and signal_id == getattr(self, "_last_published_id", ""):
            return

        r = self._r
        data = json.dumps(signal, default=str)
        channel = shared_signal_channel(self.symbol)
        stream = shared_signal_stream(self.symbol)

        for attempt in range(4):
            try:
                r.publish(channel, data)
                if signal_id:
                    self._last_published_id = signal_id
                if attempt > 0:
                    print(f"{_now()} [strategy:{self.symbol}] "
                          f"Signal published after {attempt} retries")
                else:
                    print(f"{_now()} [strategy:{self.symbol}] Signal published OK")
                break
            except Exception as e:
                if attempt < 3:
                    delay = 0.2 * (2 ** attempt)
                    print(f"{_now()} [strategy:{self.symbol}] "
                          f"Signal publish retry {attempt + 1}/3 in {delay:.1f}s: {e}")
                    time.sleep(delay)
                else:
                    print(f"{_now()} [strategy:{self.symbol}] "
                          f"SIGNAL PUBLISH FAILED after 4 attempts: {e}")
                    set_bot_status_sync(0, "error",
                                        f"Signal publish failure for {self.symbol}")

        try:
            r.xadd(stream, {"signal": data}, maxlen=1000)
        except Exception:
            pass  # Redis Streams not supported (Redis < 5.0)

    # ================================================================
    # STRATEGY IMPLEMENTATIONS (identical logic to engine_v6)
    # ================================================================

    def _get_opt_type(self, direction: str) -> str:
        return "CE" if direction == "BULL" else "PE"

    def _eval_pullback(self, df: pd.DataFrame, ind: dict, direction: str) -> Optional[dict]:
        ind_df = df.copy()

        if not (ind.get("ema_9") and ind.get("ema_15") and ind.get("ema_21")):
            return None
        if not (ind["ema_9"] > ind["ema_15"] > ind["ema_21"] and ind["last_close"] > ind["ema_21"]):
            return None

        # EMA touch
        tol = 0.015
        rec = ind_df.iloc[-8:]
        evas = [ind["ema_9"], ind["ema_15"], ind["ema_21"]]
        touched = (
            any(any(e * (1 - tol) <= float(r["low"]) <= e * (1 + tol) for e in evas)
                for _, r in rec.iterrows()) or
            any(ind["ema_21"] * 0.985 <= float(r["low"]) <= ind["ema_9"] * 1.015
                for _, r in rec.iterrows()))
        if not touched:
            return None

        # Recovery candle
        last_bar = ind_df.iloc[-1]
        rng = float(last_bar["high"]) - float(last_bar["low"])
        body_r = abs(float(last_bar["close"]) - float(last_bar["open"])) / rng if rng > 0 else 0
        if body_r < 0.25:
            return None
        if float(last_bar["close"]) <= float(last_bar["open"]):
            return None

        entry_ref = ind["last_close"]
        lows = ind.get("recent_lows", [entry_ref])
        sl = round(max(min(lows) - 0.05 if lows else entry_ref - 0.05,
                       entry_ref * 0.997), 2)

        return {
            "symbol": self.symbol,
            "opt_type": self._get_opt_type(direction),
            "direction": "BUY",
            "entry_price": entry_ref,
            "stop_loss": sl,
            "strategy": "pullback_1m",
            "regime": self._regime.regime,
            "timestamp": datetime.utcnow().isoformat(),
        }

    def _eval_trend_follow(self, df: pd.DataFrame, ind: dict, direction: str) -> Optional[dict]:
        if not (ind.get("ema_9") and ind.get("ema_15") and ind.get("ema_21")):
            return None
        if not (ind["ema_9"] > ind["ema_15"] > ind["ema_21"]):
            return None
        if ind.get("ema_9_slope", 0) <= 0:
            return None

        pct = (ind["last_close"] - ind["ema_9"]) / ind["ema_9"] if ind["ema_9"] > 0 else 0
        if pct < 0.001:
            return None
        if ind.get("mom_3", 0) <= 0:
            return None
        if not ind.get("is_bullish", False):
            return None

        cur_ltp = ind["last_close"]
        sl = round(max(ind["ema_15"] * 0.997, cur_ltp * 0.996), 2)

        return {
            "symbol": self.symbol,
            "opt_type": self._get_opt_type(direction),
            "direction": "BUY",
            "entry_price": cur_ltp,
            "stop_loss": sl,
            "strategy": "trend_follow",
            "regime": self._regime.regime,
            "timestamp": datetime.utcnow().isoformat(),
        }

    def _eval_breakout(self, df: pd.DataFrame, ind: dict, direction: str) -> Optional[dict]:
        if len(df) < 12:
            return None
        window = df.iloc[-10:]
        s_level = float(window["high"].iloc[:-1].max())
        lc = float(window["close"].iloc[-1])
        pc = float(window["close"].iloc[-2])
        if not (lc > s_level and pc <= s_level):
            return None
        if not (ind.get("is_bullish") and ind.get("ema_9", 0) > ind.get("ema_15", 0) > ind.get("ema_21", 0)):
            return None

        entry_ref = ind["last_close"]
        sl = round(max(s_level * 0.997, entry_ref * 0.997), 2)

        return {
            "symbol": self.symbol,
            "opt_type": self._get_opt_type(direction),
            "direction": "BUY",
            "entry_price": entry_ref,
            "stop_loss": sl,
            "strategy": "breakout_1m",
            "regime": self._regime.regime,
            "timestamp": datetime.utcnow().isoformat(),
        }

    def _eval_vwap_bounce(self, df: pd.DataFrame, ind: dict, direction: str) -> Optional[dict]:
        vwap = ind.get("vwap", 0)
        lc = ind["last_close"]
        if vwap <= 0:
            return None
        if abs(lc - vwap) / vwap > VWAP_BAND_PCT:
            return None
        if lc < vwap * 0.997:
            return None
        if not (ind.get("ema_9", 0) > ind.get("ema_15", 0)):
            return None

        last_bar = df.iloc[-1]
        rng = float(last_bar["high"]) - float(last_bar["low"])
        body_r = abs(float(last_bar["close"]) - float(last_bar["open"])) / rng if rng > 0 else 0
        if body_r < 0.35:
            return None
        if float(last_bar["close"]) <= float(last_bar["open"]):
            return None

        cur_ltp = lc
        sl = round(max(vwap * 0.997, cur_ltp * 0.997), 2)

        return {
            "symbol": self.symbol,
            "opt_type": self._get_opt_type(direction),
            "direction": "BUY",
            "entry_price": cur_ltp,
            "stop_loss": sl,
            "strategy": "vwap_bounce",
            "regime": self._regime.regime,
            "timestamp": datetime.utcnow().isoformat(),
        }

    def _eval_ema_cross(self, df: pd.DataFrame, ind: dict, direction: str) -> Optional[dict]:
        if len(df) < 20:
            return None
        close = df["close"]
        ema9 = close.ewm(span=9, adjust=False).mean()
        ema15 = close.ewm(span=15, adjust=False).mean()
        ef_now, ef_prev = float(ema9.iloc[-1]), float(ema9.iloc[-2])
        em_now, em_prev = float(ema15.iloc[-1]), float(ema15.iloc[-2])
        if not (ef_now > em_now and ef_prev <= em_prev):
            return None

        el_val = float(close.ewm(span=21, adjust=False).mean().iloc[-1])
        if ef_now < el_val:
            return None

        cur_ltp = ind["last_close"]
        lows = ind.get("recent_lows", [cur_ltp])
        sl = round(max(min(lows) - 0.05 if lows else cur_ltp * 0.997,
                       cur_ltp * 0.997), 2)

        return {
            "symbol": self.symbol,
            "opt_type": self._get_opt_type(direction),
            "direction": "BUY",
            "entry_price": cur_ltp,
            "stop_loss": sl,
            "strategy": "ema_cross",
            "regime": self._regime.regime,
            "timestamp": datetime.utcnow().isoformat(),
        }

    def _eval_vcgb(self, df: pd.DataFrame, ind: dict, direction: str) -> Optional[dict]:
        if len(df) < 22:
            return None
        close = df["close"]
        high = df["high"]
        low = df["low"]
        volume = df["volume"]

        bb_mid = close.rolling(20).mean()
        bb_std = close.rolling(20).std()
        bb_upper = bb_mid + 2.0 * bb_std
        bb_lower = bb_mid - 2.0 * bb_std

        kc_mid = close.ewm(span=20, adjust=False).mean()
        tr = pd.concat([
            high - low, (high - close.shift()).abs(), (low - close.shift()).abs()
        ], axis=1).max(axis=1)
        atr20 = tr.rolling(20).mean()
        kc_upper = kc_mid + 1.5 * atr20
        kc_lower = kc_mid - 1.5 * atr20

        squeeze = (bb_upper < kc_upper) & (bb_lower > kc_lower)
        if not squeeze.iloc[-5:].any():
            return None

        adx_val = MarketRegimeAnalyzer._calc_adx(df, 14)
        if adx_val <= 20:
            return None

        vol_ma = volume.rolling(10).mean()
        if float(volume.iloc[-1]) <= float(vol_ma.iloc[-1]) * 1.5:
            return None

        kc_up = float(kc_upper.iloc[-1])
        kc_lo = float(kc_lower.iloc[-1])
        cur_ltp = ind["last_close"]

        if cur_ltp <= kc_up:
            return None
        sl = round(max(kc_lo * 0.997, cur_ltp * 0.997), 2)

        return {
            "symbol": self.symbol,
            "opt_type": self._get_opt_type(direction),
            "direction": "BUY",
            "entry_price": cur_ltp,
            "stop_loss": sl,
            "strategy": "vcgb",
            "regime": self._regime.regime,
            "timestamp": datetime.utcnow().isoformat(),
        }

    def _eval_unified_strategy(self, df: pd.DataFrame, direction: str) -> Optional[dict]:
        opt_type = self._get_opt_type(direction)
        is_ce = direction == "BULL"

        premium_structure = SharedMarketStructureEngine.get_premium_structure(self.symbol)
        underlying_structure = SharedMarketStructureEngine.get_underlying_structure(self.symbol)

        if not premium_structure or not underlying_structure:
            return None

        pr_trend = premium_structure.get("trend", {}).get("direction", "")
        expected_trend = "BULLISH" if is_ce else "BEARISH"
        if pr_trend != expected_trend:
            return None

        pr_conf = premium_structure.get("confidence_score", 0)
        if pr_conf < 50:
            return None

        pb_type = premium_structure.get("pullback", {}).get("type", "")
        if pb_type not in ("HEALTHY", "DEEP", "NESTED"):
            return None

        rec_status = premium_structure.get("recovery", {}).get("status", "")
        if rec_status != "CONFIRMED":
            return None

        under_trend = underlying_structure.get("trend", "")
        expected_under = "UPTREND" if is_ce else "DOWNTREND"
        if under_trend != expected_under:
            return None

        under_bias = underlying_structure.get("market_bias", "")
        if under_bias != ("BULLISH" if is_ce else "BEARISH"):
            return None

        under_conf = underlying_structure.get("confidence", 0)
        if under_conf < 40:
            return None

        under_phase = underlying_structure.get("market_phase", "")
        if under_phase not in ("STRONG_TREND", "WEAK_TREND"):
            return None

        inds = SharedIndicatorEngine.get_indicators(self.symbol)
        if not inds:
            return None

        if not (inds.get("ema_9", 0) > inds.get("ema_15", 0) > inds.get("ema_21", 0)):
            return None

        cur_ltp = inds["last_close"]
        lows = inds.get("recent_lows", [cur_ltp])
        sl = round(max(min(lows) - 0.05 if lows else cur_ltp * 0.997,
                       cur_ltp * 0.997), 2)

        return {
            "symbol": self.symbol,
            "opt_type": opt_type,
            "direction": "BUY",
            "entry_price": cur_ltp,
            "stop_loss": sl,
            "strategy": f"unified_{opt_type.lower()}",
            "regime": self._regime.regime,
            "timestamp": datetime.utcnow().isoformat(),
            "premium_confidence": pr_conf,
            "underlying_confidence": under_conf,
        }

    # ================================================================
    # INSTANCE MANAGEMENT
    # ================================================================

    _instances: dict[str, "SharedStrategyEngine"] = {}
    _instances_lock = threading.Lock()

    @classmethod
    def get_or_create(cls, symbol: str) -> "SharedStrategyEngine":
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
