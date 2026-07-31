# backend/shared/indicator_engine.py
# ================================================================
# Shared Indicator Engine — ONE per symbol.
#
# Calculates indicators ONCE per symbol and stores results in Redis.
# Users read indicator values — never calculate them independently.
#
# Indicators:
#   EMA (9, 15, 21)
#   VWAP
#   RSI (7)
#   ATR (14)
#   Bollinger Bands (20, 2)
#   Keltner Channel (20, 1.5)
#   MACD
#   SuperTrend
#   Volume Profile
#
# Updates on each candle close (listens to candle_close_channel).
# ================================================================

import json
import threading
import time
from datetime import datetime
from typing import Optional

import pandas as pd
import numpy as np

from backend.shared.redis_infra import (
    shared_indicators,
    shared_indicators_1m,
    shared_indicators_5m,
    shared_vwap,
    shared_candle_close_channel,
    INDICATOR_TTL_SEC,
)
from backend.shared.candle_builder import SharedCandleBuilder
from backend.services.redis_client import get_redis_sync


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


class SharedIndicatorEngine:
    """
    Calculates and caches indicators for ONE symbol.
    Only ONE instance exists per active symbol.
    """

    def __init__(self, symbol: str):
        self.symbol = symbol.upper()
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._r = get_redis_sync()
        self._update_lock = threading.Lock()

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(
            target=self._loop, daemon=True,
            name=f"ind-{self.symbol}",
        )
        self._thread.start()
        print(f"{_now()} [ind:{self.symbol}] Started")

    def stop(self):
        self._stop_event.set()
        print(f"{_now()} [ind:{self.symbol}] Stopped")

    def _loop(self):
        """Subscribe to candle close events and recalculate indicators."""
        from backend.shared.pubsub_utils import resilient_pubsub_consumer
        resilient_pubsub_consumer(
            tag=f"ind:{self.symbol}",
            channels=[shared_candle_close_channel(self.symbol)],
            handler=self._on_candle_close,
            stop_event=self._stop_event,
        )

    def _on_candle_close(self, event: dict):
        """Recalculate indicators on candle close."""
        interval = event.get("interval", "1m")

        # Get candle data from shared cache
        if interval == "1m":
            df = SharedCandleBuilder.get_1m_df_from_redis(self.symbol)
        else:
            df = SharedCandleBuilder.get_5m_df_from_redis(self.symbol)

        if df.empty or len(df) < 3:
            return

        indicators = self._calculate_all(df, interval)
        with self._update_lock:
            self._store(indicators, interval)

        ema9  = indicators.get("ema_9", "-")
        rsi   = indicators.get("rsi_7", "-")
        atr   = indicators.get("atr_14", "-")
        vwap  = indicators.get("vwap", "-")
        print(f"{_now()} [ind:{self.symbol}] {interval} recompute "
              f"bars={indicators.get('bars')} last_close={indicators.get('last_close')} "
              f"ema9={ema9} rsi={rsi} atr={atr} vwap={vwap}")

    def _calculate_all(self, df: pd.DataFrame, interval: str) -> dict:
        """Calculate all indicators from a DataFrame."""
        if df.empty:
            return {}

        close = df["close"]
        high = df["high"]
        low = df["low"]
        volume = df["volume"] if "volume" in df.columns else pd.Series([0] * len(df))

        lc = float(close.iloc[-1])

        result = {
            "last_close": lc,
            "bars": len(df),
        }

        # EMA 9/15/21
        for span in [9, 15, 21]:
            if len(close) >= span:
                val = float(close.ewm(span=span, adjust=False).mean().iloc[-1])
                result[f"ema_{span}"] = val

        # EMA slopes
        if len(close) >= 13:
            ema9_s = close.ewm(span=9, adjust=False).mean()
            if len(ema9_s) >= 4:
                result["ema_9_slope"] = float(ema9_s.iloc[-1] - ema9_s.iloc[-4])

        # VWAP — compute directly on DataFrame columns, no copy
        try:
            typical = (high + low + close) / 3
            tpv = typical * volume
            dates = pd.to_datetime(df["time"]).dt.date
            df_vwap = pd.DataFrame({"date": dates, "tpv": tpv, "vol": volume})
            cum_tpv = df_vwap.groupby("date")["tpv"].cumsum()
            cum_vol = df_vwap.groupby("date")["vol"].cumsum()
            vwap_val = float(cum_tpv.iloc[-1] / cum_vol.iloc[-1]) if cum_vol.iloc[-1] > 0 else 0
            result["vwap"] = vwap_val
            self._r.set(shared_vwap(self.symbol), str(result["vwap"]), ex=INDICATOR_TTL_SEC)
        except Exception:
            pass

        # RSI (7)
        if len(close) >= 8:
            try:
                delta = close.diff().dropna()
                up = delta.clip(lower=0)
                down = -delta.clip(upper=0)
                ma_up = up.rolling(window=7).mean()
                ma_dn = down.rolling(window=7).mean()
                if ma_dn.iloc[-1] > 0:
                    rs = ma_up.iloc[-1] / ma_dn.iloc[-1]
                    result["rsi_7"] = float(100 - (100 / (1 + rs)))
            except Exception:
                pass

        # ATR (14)
        if len(df) >= 15:
            tr = pd.concat([
                high - low,
                (high - close.shift()).abs(),
                (low - close.shift()).abs(),
            ], axis=1).max(axis=1)
            atr14 = float(tr.rolling(14).mean().iloc[-1])
            result["atr_14"] = atr14
            result["atr_pct"] = round(atr14 / lc * 100, 3) if lc > 0 else 0

        # Bollinger Bands (20, 2)
        if len(close) >= 20:
            bb_mid = close.rolling(20).mean()
            bb_std = close.rolling(20).std()
            result["bb_upper"] = float(bb_mid.iloc[-1] + 2 * bb_std.iloc[-1])
            result["bb_mid"] = float(bb_mid.iloc[-1])
            result["bb_lower"] = float(bb_mid.iloc[-1] - 2 * bb_std.iloc[-1])

        # Keltner Channel (20, 1.5) — reuses ATR from above
        if len(df) >= 20:
            kc_mid = close.ewm(span=20, adjust=False).mean()
            kc_atr = tr.rolling(20).mean()
            result["kc_upper"] = float(kc_mid.iloc[-1] + 1.5 * kc_atr.iloc[-1])
            result["kc_mid"] = float(kc_mid.iloc[-1])
            result["kc_lower"] = float(kc_mid.iloc[-1] - 1.5 * kc_atr.iloc[-1])

        # Momentum (3-bar)
        if len(close) >= 4:
            result["mom_3"] = float(close.iloc[-1] - close.iloc[-4])

        # Candle-specific
        last = df.iloc[-1]
        rng = float(last["high"]) - float(last["low"])
        result["body_ratio"] = abs(float(last["close"]) - float(last["open"])) / rng if rng > 0 else 0.0
        result["is_bullish"] = float(last["close"]) > float(last["open"])
        result["wick_ratio"] = (float(last["high"]) - float(last["close"])) / rng if rng > 0 else 1.0
        result["recent_lows"] = [float(x) for x in low.iloc[-5:].tolist()]
        result["recent_highs"] = [float(x) for x in high.iloc[-5:].tolist()]

        return result

    def _store(self, indicators: dict, interval: str):
        """Store indicators in Redis."""
        r = self._r
        # Store as hash for fast field access
        pipe = r.pipeline()
        for k, v in indicators.items():
            pipe.hset(shared_indicators(self.symbol), k, str(v))
        pipe.execute()
        r.expire(shared_indicators(self.symbol), INDICATOR_TTL_SEC)

        # Also store full JSON
        key = shared_indicators_1m(self.symbol) if interval == "1m" else shared_indicators_5m(self.symbol)
        r.set(key, json.dumps(indicators, default=str), ex=INDICATOR_TTL_SEC)

    # ================================================================
    # STATIC READERS
    # ================================================================

    @staticmethod
    def get_indicators(symbol: str) -> Optional[dict]:
        """Get latest indicator values for a symbol."""
        raw = get_redis_sync().hgetall(shared_indicators(symbol.upper()))
        if not raw:
            return None
        result = {}
        for k, v in raw.items():
            try:
                if k in ("recent_lows", "recent_highs"):
                    result[k] = [float(x) for x in json.loads(v)]
                elif k in ("is_bullish",):
                    result[k] = v.lower() == "true"
                else:
                    result[k] = float(v)
            except (ValueError, TypeError):
                result[k] = v
        return result

    @staticmethod
    def get_indicator(symbol: str, name: str) -> Optional[float]:
        """Get a single indicator value."""
        raw = get_redis_sync().hget(shared_indicators(symbol.upper()), name)
        if raw is None:
            return None
        try:
            return float(raw)
        except (ValueError, TypeError):
            return None

    # ================================================================
    # INSTANCE MANAGEMENT
    # ================================================================

    _instances: dict[str, "SharedIndicatorEngine"] = {}
    _instances_lock = threading.Lock()

    @classmethod
    def get_or_create(cls, symbol: str) -> "SharedIndicatorEngine":
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
