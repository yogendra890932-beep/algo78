# backend/shared/candle_builder.py
# ================================================================
# Shared Candle Builder — ONE per symbol.
#
# Replaces the per-user _candle_loop and per-user candle DataFrames.
#
# Pipeline:
#   Live Tick (from shared Market Data Service)
#       ↓
#   1-Minute Candle (accumulated in Redis Hash, closed bar appended)
#       ↓
#   5-Minute Candle (aggregated from 1m bars)
#
# Stores:
#   - 1m candles in Redis (JSON-serialized list of OHLCV dicts)
#   - 5m candles in Redis (aggregated)
#   - Current developing candle state in Redis Hash
#   - Historical previous-day data in Redis
#
# Publishers:
#   - candle_close channel on every new closed bar
# ================================================================

import json
import threading
from datetime import datetime, date, timedelta
from typing import Optional

import pandas as pd

from backend.shared.redis_infra import (
    shared_candles_1m,
    shared_candles_5m,
    shared_candle_current_1m,
    shared_candle_current_5m,
    shared_candle_close_channel,
    shared_historical_1m,
    shared_historical_5m,
    shared_tick_channel,
    CANDLE_TTL_SEC,
    HISTORICAL_TTL_SEC,
)
from backend.shared.shared_cache import (
    is_market_open, last_trading_day, get_streamer_token,
)
from backend.shared.dist_locks import acquire_lock_wait, release_lock
from backend.services.redis_client import get_redis_sync

MAX_1M_BARS = 750  # ~12.5 hours at 1-min
MAX_5M_BARS = 150  # ~12.5 hours at 5-min

# Minimum 1m bars the shared strategy engine needs before it will
# evaluate (strategy_engine._evaluate_all returns when len(df) < 25).
# Below this, the candle builder fetches warm candles from Upstox so
# strategies can trade from minute 1 (like legacy engine_v6).
WARM_MIN_BARS = 25


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


class SharedCandleBuilder:
    """
    Builds and maintains candles for ONE symbol.
    Only ONE instance exists per active symbol.

    Listens to tick_channel from SharedMarketDataService.
    """

    def __init__(self, symbol: str, access_token: str):
        self.symbol = symbol.upper()
        self.access_token = access_token
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._r = get_redis_sync()

        self._cur_1m_min: Optional[str] = None
        self._cur_1m: dict = {}

        self._cur_5m_min: Optional[str] = None
        self._cur_5m: dict = {}

        self._1m_bars: list[dict] = []
        self._5m_bars: list[dict] = []
        self._lock = threading.Lock()
        self._tick_counter = 0

        # The ONLY tick token that may build the underlying index candle
        # series. Option-premium ticks are published to the same Redis
        # tick channel (see SharedMarketDataService) and MUST be ignored
        # here, otherwise the index candles get polluted with option
        # premiums (mirrors legacy engine_v6, which feeds _process_ul_tick
        # only from self.underlying_token).
        self._underlying_token = get_streamer_token(self.symbol)

    def start(self):
        """Start the candle builder in a background thread."""
        if self._thread and self._thread.is_alive():
            return

        # Load existing data from Redis (recovery)
        self._load_from_redis()

        # Preload historical + intraday candles from Upstox so the
        # strategy engine has enough bars to trade immediately.
        self._warm_up()

        self._thread = threading.Thread(
            target=self._loop, daemon=True,
            name=f"candle-{self.symbol}",
        )
        self._thread.start()
        print(f"{_now()} [candle:{self.symbol}] Started (1m={len(self._1m_bars)}, 5m={len(self._5m_bars)})")

    def _warm_up(self):
        """
        Preload warm candles (previous days + today's intraday) from
        Upstox into Redis, mirroring legacy engine_v6's warm-load so
        strategies/indicators have 25+ bars from minute 1.

        No-op when enough bars already exist (e.g. after a restart
        with data still cached in Redis) or for unknown symbols.
        """
        if len(self._1m_bars) >= WARM_MIN_BARS:
            return

        try:
            from backend.engine.history_loader import load_warm_candles
            from backend.shared.shared_cache import KNOWN_HISTORY_KEYS

            history_key = KNOWN_HISTORY_KEYS.get(self.symbol)
            if not history_key:
                print(f"{_now()} [candle:{self.symbol}] Warm-up skipped — "
                      f"no known history key for symbol")
                return

            print(f"{_now()} [candle:{self.symbol}] Warm-up: fetching "
                  f"{history_key} candles...")
            df = load_warm_candles(history_key, self.access_token)
            if df.empty:
                print(f"{_now()} [candle:{self.symbol}] Warm-up: no data")
                return

            warm = df[["time", "open", "high", "low", "close", "volume"]].copy()
            warm["time"] = pd.to_datetime(warm["time"])

            with self._lock:
                merged = pd.concat(
                    [warm, pd.DataFrame(self._1m_bars)], ignore_index=True
                )
                merged["time"] = pd.to_datetime(merged["time"])
                merged = merged.sort_values("time").drop_duplicates(
                    subset=["time"], keep="last").reset_index(drop=True)
                merged = merged.dropna(subset=["open", "high", "low", "close"])

                self._1m_bars = merged.to_dict("records")
                self._5m_bars = self._build_5m_from_1m(self._1m_bars)

            self._save_to_redis()
            print(f"{_now()} [candle:{self.symbol}] Warm-up done: "
                  f"1m={len(self._1m_bars)} 5m={len(self._5m_bars)} bars")
        except Exception as e:
            print(f"{_now()} [candle:{self.symbol}] Warm-up failed: {e}")

    @staticmethod
    def _build_5m_from_1m(bars: list) -> list:
        """Aggregate 1m bars into 5m bars (time = 5m bucket end,
        matching SharedCandleBuilder._close_5m_bar semantics)."""
        if not bars:
            return []
        df = pd.DataFrame(bars)
        df["time"] = pd.to_datetime(df["time"])
        df = df.sort_values("time")
        df["bucket_end"] = df["time"].dt.floor("5min") + pd.Timedelta(minutes=5)
        out = []
        for _, grp in df.groupby("bucket_end"):
            out.append({
                "time": grp["bucket_end"].iloc[0],
                "open": float(grp["open"].iloc[0]),
                "high": float(grp["high"].max()),
                "low": float(grp["low"].min()),
                "close": float(grp["close"].iloc[-1]),
                "volume": float(grp["volume"].sum()),
            })
        return out

    def stop(self):
        """Stop the candle builder."""
        self._stop_event.set()
        # Persist final state to Redis
        self._save_to_redis()
        print(f"{_now()} [candle:{self.symbol}] Stopped")

    def _load_from_redis(self):
        """Recover candle state from Redis after restart."""
        r = self._r
        raw_1m = r.get(shared_candles_1m(self.symbol))
        if raw_1m:
            self._1m_bars = json.loads(raw_1m)

        raw_5m = r.get(shared_candles_5m(self.symbol))
        if raw_5m:
            self._5m_bars = json.loads(raw_5m)

        # Recover current developing candles
        cur_1m = r.hgetall(shared_candle_current_1m(self.symbol))
        if cur_1m:
            self._cur_1m = {
                k: float(v) if k in ("open", "high", "low", "close", "volume") else v
                for k, v in cur_1m.items()
            }
            self._cur_1m_min = cur_1m.get("minute")

        cur_5m = r.hgetall(shared_candle_current_5m(self.symbol))
        if cur_5m:
            self._cur_5m = {
                k: float(v) if k in ("open", "high", "low", "close", "volume") else v
                for k, v in cur_5m.items()
            }
            self._cur_5m_min = cur_5m.get("minute")

    def _save_to_redis(self):
        """Persist candle state to Redis."""
        r = self._r
        with self._lock:
            r.set(shared_candles_1m(self.symbol),
                  json.dumps(self._1m_bars[-MAX_1M_BARS:], default=str),
                  ex=CANDLE_TTL_SEC)
            r.set(shared_candles_5m(self.symbol),
                  json.dumps(self._5m_bars[-MAX_5M_BARS:], default=str),
                  ex=CANDLE_TTL_SEC)

            if self._cur_1m:
                hkey = shared_candle_current_1m(self.symbol)
                self._r.delete(hkey)
                pipe = self._r.pipeline()
                pipe.hset(hkey, "open", str(self._cur_1m.get("open", 0)))
                pipe.hset(hkey, "high", str(self._cur_1m.get("high", 0)))
                pipe.hset(hkey, "low", str(self._cur_1m.get("low", 0)))
                pipe.hset(hkey, "close", str(self._cur_1m.get("close", 0)))
                pipe.hset(hkey, "volume", str(self._cur_1m.get("volume", 0)))
                pipe.hset(hkey, "minute", self._cur_1m_min or "")
                pipe.execute()

            if self._cur_5m:
                hkey = shared_candle_current_5m(self.symbol)
                self._r.delete(hkey)
                pipe = self._r.pipeline()
                pipe.hset(hkey, "open", str(self._cur_5m.get("open", 0)))
                pipe.hset(hkey, "high", str(self._cur_5m.get("high", 0)))
                pipe.hset(hkey, "low", str(self._cur_5m.get("low", 0)))
                pipe.hset(hkey, "close", str(self._cur_5m.get("close", 0)))
                pipe.hset(hkey, "volume", str(self._cur_5m.get("volume", 0)))
                pipe.hset(hkey, "minute", self._cur_5m_min or "")
                pipe.execute()

    def _loop(self):
        """Main candle building loop — subscribes to tick channel."""
        from backend.shared.pubsub_utils import resilient_pubsub_consumer
        channel = shared_tick_channel(self.symbol)

        def _on_tick(data: dict):
            self._process_tick(data)
            self._tick_counter += 1
            if self._tick_counter >= 60:
                try:
                    self._save_to_redis()
                except Exception as e:
                    print(f"{_now()} [candle:{self.symbol}] save err: {e}")
                self._tick_counter = 0

        resilient_pubsub_consumer(
            tag=f"candle:{self.symbol}",
            channels=[channel],
            handler=_on_tick,
            stop_event=self._stop_event,
        )

    def _process_tick(self, tick: dict):
        """Process a single tick and update candles."""
        ltp = tick.get("ltp", 0)
        ltq = tick.get("ltq", 0)
        token = tick.get("token", "")
        ts = tick.get("ts", "")

        if ltp <= 0:
            return

        # Only the underlying index token feeds the underlying candle
        # series. Option-premium ticks for the selected CE/PE arrive on
        # the same channel and must never be mixed into the index data.
        if token and token != self._underlying_token:
            return

        now = datetime.now()
        now_1m = now.strftime("%Y-%m-%d %H:%M")

        # ── 1-Minute Candle ──────────────────────────────────────
        with self._lock:
            if self._cur_1m_min != now_1m:
                # Close previous 1m bar
                if self._cur_1m.get("open") is not None:
                    closed = self._close_1m_bar()
                    if closed:
                        # Publish candle close event
                        r = self._r
                        r.publish(shared_candle_close_channel(self.symbol),
                                  json.dumps({"symbol": self.symbol, "interval": "1m",
                                              "candle": closed, "ts": now_1m}, default=str))

                        # Check if 5m boundary
                        minute_part = int(now_1m.split(":")[1])
                        if minute_part % 5 == 0:
                            self._close_5m_bar(now_1m)

                # Start new 1m candle
                self._cur_1m_min = now_1m
                self._cur_1m = {
                    "open": ltp, "high": ltp, "low": ltp,
                    "close": ltp, "volume": ltq, "time": ts,
                }
            else:
                # Update developing candle
                self._cur_1m["close"] = ltp
                self._cur_1m["high"] = max(self._cur_1m["high"], ltp)
                self._cur_1m["low"] = min(self._cur_1m["low"], ltp)
                self._cur_1m["volume"] = self._cur_1m.get("volume", 0) + ltq

            # Update current 1m in Redis
            hkey = shared_candle_current_1m(self.symbol)
            self._r.delete(hkey)
            pipe = self._r.pipeline()
            pipe.hset(hkey, "open", str(self._cur_1m["open"]))
            pipe.hset(hkey, "high", str(self._cur_1m["high"]))
            pipe.hset(hkey, "low", str(self._cur_1m["low"]))
            pipe.hset(hkey, "close", str(self._cur_1m["close"]))
            pipe.hset(hkey, "volume", str(self._cur_1m["volume"]))
            pipe.hset(hkey, "minute", self._cur_1m_min)
            pipe.execute()

    def _close_1m_bar(self) -> Optional[dict]:
        """Close the current 1-minute bar and append to the list."""
        candle = dict(self._cur_1m)
        candle["time"] = pd.Timestamp(self._cur_1m_min)
        self._1m_bars.append(candle)

        # Limit size
        if len(self._1m_bars) > MAX_1M_BARS:
            self._1m_bars = self._1m_bars[-MAX_1M_BARS:]

        print(f"{_now()} [candle:{self.symbol}] 1m close {self._cur_1m_min} "
              f"O={candle['open']} H={candle['high']} L={candle['low']} "
              f"C={candle['close']} V={candle['volume']}")

        return candle

    def _close_5m_bar(self, minute_str: str):
        """Aggregate and close a 5-minute bar from 1m bars."""
        now = pd.Timestamp(minute_str)
        start = now - pd.Timedelta(minutes=5)

        # Find 1m bars in the last 5 minutes
        bars = [
            b for b in self._1m_bars[-6:]
            if isinstance(b.get("time"), pd.Timestamp)
            and b["time"] > start
            and b["time"] <= now
        ]

        if not bars:
            return

        candle_5m = {
            "time": now,
            "open": bars[0]["open"],
            "high": max(b["high"] for b in bars),
            "low": min(b["low"] for b in bars),
            "close": bars[-1]["close"],
            "volume": sum(b.get("volume", 0) for b in bars),
        }

        self._5m_bars.append(candle_5m)
        if len(self._5m_bars) > MAX_5M_BARS:
            self._5m_bars = self._5m_bars[-MAX_5M_BARS:]

        print(f"{_now()} [candle:{self.symbol}] 5m close {minute_str} "
              f"O={candle_5m['open']} H={candle_5m['high']} L={candle_5m['low']} "
              f"C={candle_5m['close']} V={candle_5m['volume']}")

        # Save 5m to Redis immediately
        self._r.set(shared_candles_5m(self.symbol),
                    json.dumps(self._5m_bars, default=str),
                    ex=CANDLE_TTL_SEC)

        # Publish 5m close event
        self._r.publish(shared_candle_close_channel(self.symbol),
                        json.dumps({"symbol": self.symbol, "interval": "5m",
                                    "candle": candle_5m, "ts": minute_str}, default=str))

    # ================================================================
    # DATA READERS — for downstream consumers
    # ================================================================

    def get_1m_df(self) -> pd.DataFrame:
        """Get 1-minute candles as DataFrame (from in-memory list)."""
        with self._lock:
            bars = list(self._1m_bars)
        if not bars:
            return pd.DataFrame()
        df = pd.DataFrame(bars)
        if "time" in df.columns and not pd.api.types.is_datetime64_any_dtype(df["time"]):
            df["time"] = pd.to_datetime(df["time"])
        for col in ["open", "high", "low", "close", "volume"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        return df.sort_values("time").reset_index(drop=True)

    def get_5m_df(self) -> pd.DataFrame:
        """Get 5-minute candles as DataFrame."""
        with self._lock:
            bars = list(self._5m_bars)
        if not bars:
            return pd.DataFrame()
        df = pd.DataFrame(bars)
        if "time" in df.columns and not pd.api.types.is_datetime64_any_dtype(df["time"]):
            df["time"] = pd.to_datetime(df["time"])
        for col in ["open", "high", "low", "close", "volume"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        return df.sort_values("time").reset_index(drop=True)

    def get_current_1m(self) -> dict:
        """Get the current developing 1-minute candle."""
        with self._lock:
            return dict(self._cur_1m)

    def get_current_5m(self) -> dict:
        """Get the current developing 5-minute candle."""
        with self._lock:
            return dict(self._cur_5m)

    # ================================================================
    # REDIS READERS (for cross-process access)
    # ================================================================

    @staticmethod
    def get_1m_df_from_redis(symbol: str) -> pd.DataFrame:
        """Get 1-minute candles from Redis (accessible from any process)."""
        raw = get_redis_sync().get(shared_candles_1m(symbol.upper()))
        if not raw:
            return pd.DataFrame()
        bars = json.loads(raw)
        if not bars:
            return pd.DataFrame()
        df = pd.DataFrame(bars)
        if "time" in df.columns:
            df["time"] = pd.to_datetime(df["time"])
        for col in ["open", "high", "low", "close", "volume"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        return df.sort_values("time").reset_index(drop=True)

    @staticmethod
    def get_5m_df_from_redis(symbol: str) -> pd.DataFrame:
        """Get 5-minute candles from Redis."""
        raw = get_redis_sync().get(shared_candles_5m(symbol.upper()))
        if not raw:
            return pd.DataFrame()
        bars = json.loads(raw)
        if not bars:
            return pd.DataFrame()
        df = pd.DataFrame(bars)
        if "time" in df.columns:
            df["time"] = pd.to_datetime(df["time"])
        for col in ["open", "high", "low", "close", "volume"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        return df.sort_values("time").reset_index(drop=True)

    # ================================================================
    # PER-SYMBOL INSTANCE MANAGEMENT
    # ================================================================

    _instances: dict[str, "SharedCandleBuilder"] = {}
    _instances_lock = threading.Lock()

    @classmethod
    def get_or_create(cls, symbol: str, access_token: str) -> "SharedCandleBuilder":
        sym = symbol.upper()
        with cls._instances_lock:
            if sym not in cls._instances:
                builder = cls(sym, access_token)
                cls._instances[sym] = builder
                builder.start()
            return cls._instances[sym]

    @classmethod
    def stop_symbol(cls, symbol: str):
        sym = symbol.upper()
        with cls._instances_lock:
            builder = cls._instances.pop(sym, None)
            if builder:
                builder.stop()

    @classmethod
    def stop_all(cls):
        with cls._instances_lock:
            for builder in list(cls._instances.values()):
                builder.stop()
            cls._instances.clear()
