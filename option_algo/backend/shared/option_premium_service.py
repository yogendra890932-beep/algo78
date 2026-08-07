# backend/shared/option_premium_service.py
# ================================================================
# Shared Option Premium Builder — ONE per symbol.
#
# Mirrors legacy engine_v6's premium-option flow in the shared
# architecture:
#
#   - Selects the active CE/PE ITM option instrument via
#     get_itm_instrument(opt_type, underlying_ltp, symbol, itm_depth,
#     strike_step), where opt_type follows the underlying EMA9/15
#     direction (BULL → CE, BEAR → PE).
#   - Rolls the option whenever the underlying direction flips.
#   - Subscribes the option's streamer token through the shared
#     MarketDataService (option ticks flow into the symbol's tick
#     channel keyed by token).
#   - Builds a 1-minute premium candle series (like opt_df in legacy)
#     and publishes a premium-close event on every closed bar.
#
# Stores:
#   - 1m premium candles in Redis (JSON-serialized list of OHLCV)
#   - Current developing premium candle in Redis Hash
#   - Selected-option state (instrument_key/opt_type/strike/expiry)
#
# Publishers:
#   - shared:premium_close:{symbol} on every new closed premium bar
# ================================================================

import json
import threading
from datetime import datetime
from typing import Optional

import pandas as pd

from backend.shared.redis_infra import (
    shared_premium_candles_1m,
    shared_premium_current_1m,
    shared_premium_close_channel,
    shared_premium_state,
    shared_candle_close_channel,
    shared_tick_channel,
    CANDLE_TTL_SEC,
)
from backend.services.redis_client import get_redis_sync
from backend.shared.candle_builder import SharedCandleBuilder

MAX_1M_BARS = 750
WARM_MIN_BARS = 25
ITM_DEPTH = 1
ITM_RESELECT_EXTRA_STEPS = 2


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


class SharedOptionPremiumBuilder:
    """
    ONE per symbol — selects and tracks the active option premium
    (CE/PE ITM instrument) and builds its 1-minute candle series,
    mirroring legacy engine_v6's opt_df / instrument_key flow.
    """

    _instances: dict[str, "SharedOptionPremiumBuilder"] = {}
    _instances_lock = threading.Lock()

    def __init__(self, symbol: str, access_token: str):
        self.symbol = symbol.upper()
        self.access_token = access_token
        self._r = get_redis_sync()
        self._lock = threading.RLock()
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

        self._instrument_key: Optional[str] = None
        self._trading_symbol: Optional[str] = None
        self._opt_type: Optional[str] = None
        self._strike: Optional[float] = None
        self._expiry_str: str = ""
        self._strike_step: int = 0

        self._bars: list[dict] = []
        self._cur_min: Optional[str] = None
        self._cur: dict = {}
        self._tick_counter = 0

    # ================================================================
    # LIFECYCLE
    # ================================================================

    def start(self):
        """Start the premium builder in a background thread."""
        if self._thread and self._thread.is_alive():
            return

        self._load_from_redis()

        # On restart the selected option is recovered from Redis, but its
        # token was never (re)subscribed to the freshly started global
        # streamer — without this, no option-premium ticks ever arrive and
        # the premium 1m candle is never built live.
        if self._instrument_key:
            try:
                from backend.shared.market_data_service import (
                    SharedMarketDataService,
                )
                md = SharedMarketDataService.get_or_create(
                    self.symbol, self.access_token)
                md.subscribe_option(self._instrument_key)
                print(f"{_now()} [premium:{self.symbol}] Re-subscribed "
                      f"{self._instrument_key}")
            except Exception as e:
                print(f"{_now()} [premium:{self.symbol}] re-subscribe err: {e}")

        try:
            from backend.engine.instruments import detect_strike_step
            self._strike_step = detect_strike_step(self.symbol) or 0
        except Exception as e:
            print(f"{_now()} [premium:{self.symbol}] strike step err: {e}")

        # First-time selection (direction from underlying candles)
        self._maybe_roll_option()

        # Warm-load premium candles for the selected option
        self._warm_up()

        self._thread = threading.Thread(
            target=self._loop, daemon=True,
            name=f"premium-{self.symbol}",
        )
        self._thread.start()
        print(f"{_now()} [premium:{self.symbol}] Started "
              f"opt={self._trading_symbol} "
              f"key={self._instrument_key} bars={len(self._bars)}")

    def stop(self):
        """Stop the premium builder."""
        self._stop_event.set()
        self._save_to_redis()
        print(f"{_now()} [premium:{self.symbol}] Stopped")

    def _loop(self):
        """Subscribe to ticks (premium candles) + underlying candle closes (roll)."""
        from backend.shared.pubsub_utils import resilient_pubsub_consumer

        def _handler(data: dict):
            if "token" in data:
                self._on_tick(data)
            elif data.get("interval") == "1m":
                self._on_underlying_close(data)

        resilient_pubsub_consumer(
            tag=f"premium:{self.symbol}",
            channels=[shared_tick_channel(self.symbol),
                      shared_candle_close_channel(self.symbol)],
            handler=_handler,
            stop_event=self._stop_event,
        )

    # ================================================================
    # OPTION SELECTION / ROLLING
    # ================================================================

    def _maybe_roll_option(self) -> bool:
        """If the underlying direction flipped, switch to the new ITM option.

        Idempotent — no-op when the current option already matches the
        direction-derived opt_type.
        """
        df = SharedCandleBuilder.get_1m_df_from_redis(self.symbol)
        if df.empty or len(df) < 15:
            return False

        close = df["close"]
        ef = float(close.ewm(span=9, adjust=False).mean().iloc[-1])
        es = float(close.ewm(span=15, adjust=False).mean().iloc[-1])
        direction = "BULL" if ef > es else "BEAR"
        opt_type = "CE" if direction == "BULL" else "PE"

        if self._opt_type == opt_type and self._instrument_key:
            return False

        ltp = float(df["close"].iloc[-1])
        try:
            from backend.engine.instruments import get_itm_instrument
            info = get_itm_instrument(
                opt_type, ltp, self.symbol, ITM_DEPTH, self._strike_step)
        except Exception as e:
            print(f"{_now()} [premium:{self.symbol}] ITM select err: {e}")
            return False

        self._switch_option(info)
        return True

    def _check_itm_depth(self) -> bool:
        """Re-select the 1 ITM strike when the underlying drifts >= 2 steps.

        Mirrors legacy engine_v6._check_itm_depth so the premium option
        stays at the 1 ITM strike relative to the underlying as it moves
        — even without a direction flip. Skipped while ANY user holds an
        open position on this symbol so a live/paper trade never rolls
        under its feet.
        """
        if not self._instrument_key or not self._strike or not self._strike_step:
            return False
        if not self._opt_type:
            return False

        try:
            from backend.shared.user_execution_manager import user_registry
            if user_registry.has_open_position(self.symbol):
                return False
        except Exception as e:
            print(f"{_now()} [premium:{self.symbol}] itm guard err: {e}")
            return False

        df = SharedCandleBuilder.get_1m_df_from_redis(self.symbol)
        if df.empty:
            return False
        underlying_ltp = float(df["close"].iloc[-1])

        step = self._strike_step
        atm = round(underlying_ltp / step) * step
        if self._opt_type == "CE":
            extra_steps = ((atm - ITM_DEPTH * step) - self._strike) / step
        else:
            extra_steps = (self._strike - (atm + ITM_DEPTH * step)) / step
        if extra_steps < ITM_RESELECT_EXTRA_STEPS:
            return False

        print(f"{_now()} [premium:{self.symbol}] Strike {self._strike} "
              f"{extra_steps:.0f} steps deep — reselecting 1 ITM")
        try:
            from backend.engine.instruments import get_itm_instrument
            info = get_itm_instrument(self._opt_type, underlying_ltp,
                                      self.symbol, ITM_DEPTH, step)
        except Exception as e:
            print(f"{_now()} [premium:{self.symbol}] reselect err: {e}")
            return False
        if info.get("instrument_key") == self._instrument_key:
            return False
        self._switch_option(info)
        return True

    def _switch_option(self, info: dict):
        """Subscribe the new option token and reset the premium candle series."""
        from backend.shared.market_data_service import SharedMarketDataService
        md = SharedMarketDataService.get_or_create(self.symbol, self.access_token)

        new_key = info["instrument_key"]
        with self._lock:
            if self._instrument_key and self._instrument_key != new_key:
                md.unsubscribe_option(self._instrument_key)
            self._instrument_key = new_key
            self._trading_symbol = info.get("trading_symbol")
            self._opt_type = str(info.get("type", "")).upper()
            self._strike = info.get("strike")
            self._expiry_str = info.get("expiry_str", "")
            self._bars = []
            self._cur_min = None
            self._cur = {}

        md.subscribe_option(new_key)
        self._save_state()

        # Drop stale premium candles from the previous option so the
        # strategy engine never evaluates old-option bars during the roll
        self._r.delete(shared_premium_candles_1m(self.symbol))
        self._r.delete(shared_premium_current_1m(self.symbol))

        print(f"{_now()} [premium:{self.symbol}] Active option: "
              f"{self._trading_symbol} ({self._opt_type} {self._strike})")

    # ================================================================
    # WARM-UP (mirrors SharedCandleBuilder._warm_up)
    # ================================================================

    def _warm_up(self):
        """Preload historical + intraday premium candles from Upstox."""
        with self._lock:
            key = self._instrument_key
            have = len(self._bars)
        if not key or have >= WARM_MIN_BARS:
            return

        try:
            from backend.engine.history_loader import load_warm_candles
            print(f"{_now()} [premium:{self.symbol}] Warm-up: fetching "
                  f"{key} candles...")
            df = load_warm_candles(key, self.access_token)
            if df.empty:
                print(f"{_now()} [premium:{self.symbol}] Warm-up: no data")
                return

            warm = df[["time", "open", "high", "low", "close", "volume"]].copy()
            warm["time"] = pd.to_datetime(warm["time"])

            with self._lock:
                merged = pd.concat([warm, pd.DataFrame(self._bars)], ignore_index=True)
                merged["time"] = pd.to_datetime(merged["time"])
                merged = merged.sort_values("time").drop_duplicates(
                    subset=["time"], keep="last").reset_index(drop=True)
                merged = merged.dropna(subset=["open", "high", "low", "close"])
                self._bars = merged.to_dict("records")

            self._save_to_redis()
            print(f"{_now()} [premium:{self.symbol}] Warm-up done: "
                  f"{len(self._bars)} bars")
        except Exception as e:
            print(f"{_now()} [premium:{self.symbol}] Warm-up failed: {e}")

    # ================================================================
    # EVENT HANDLERS
    # ================================================================

    def _on_underlying_close(self, event: dict):
        """On each underlying 1m close, roll option if direction flipped and
        keep the selected option at the 1 ITM strike (like legacy)."""
        rolled = self._maybe_roll_option()
        reselected = self._check_itm_depth()
        if rolled or reselected:
            self._warm_up()

    def _on_tick(self, tick: dict):
        """Build the premium 1m candle from the selected option's ticks."""
        if tick.get("token") != self._instrument_key:
            return

        ltp = tick.get("ltp", 0)
        ltq = tick.get("ltq", 0) or 0
        ts = tick.get("ts", "")
        if ltp <= 0:
            return

        now_1m = datetime.now().strftime("%Y-%m-%d %H:%M")

        with self._lock:
            if self._cur_min != now_1m:
                # Close previous premium bar
                if self._cur.get("open") is not None:
                    closed = self._close_bar()
                    if closed:
                        r = self._r
                        r.publish(shared_premium_close_channel(self.symbol),
                                  json.dumps({"symbol": self.symbol, "interval": "1m",
                                              "candle": closed, "ts": now_1m,
                                              "kind": "premium"}, default=str))
                self._cur_min = now_1m
                self._cur = {
                    "open": ltp, "high": ltp, "low": ltp,
                    "close": ltp, "volume": ltq, "time": ts,
                }
            else:
                self._cur["close"] = ltp
                self._cur["high"] = max(self._cur["high"], ltp)
                self._cur["low"] = min(self._cur["low"], ltp)
                self._cur["volume"] = self._cur.get("volume", 0) + ltq

            self._tick_counter += 1
            if self._tick_counter >= 60:
                self._save_to_redis()
                self._tick_counter = 0
            else:
                self._save_current_candle()

    def _close_bar(self) -> Optional[dict]:
        """Close the current premium bar and append to the list."""
        candle = dict(self._cur)
        candle["time"] = pd.Timestamp(self._cur_min)
        # Enforce strict ascending time order — same reasoning as the
        # underlying candle builder (Lightweight Charts rejects unsorted data).
        if self._bars:
            last = self._bars[-1]
            try:
                last_time = pd.Timestamp(last["time"]) if last.get("time") is not None else None
            except (TypeError, ValueError):
                last_time = None
            if last_time is not None and candle["time"] <= last_time:
                if candle["time"] == last_time:
                    self._bars[-1] = candle
                return candle
        self._bars.append(candle)

        if len(self._bars) > MAX_1M_BARS:
            self._bars = self._bars[-MAX_1M_BARS:]

        print(f"{_now()} [premium:{self.symbol}] 1m close {self._cur_min} "
              f"O={candle['open']} H={candle['high']} L={candle['low']} "
              f"C={candle['close']} V={candle['volume']}")

        return candle

    # ================================================================
    # REDIS PERSISTENCE
    # ================================================================

    @staticmethod
    def _normalize_bars(bars):
        """Sort bars ascending by time and drop duplicate-time bars (keep last)."""
        seen = {}
        for b in bars:
            if not isinstance(b, dict) or b.get("time") is None:
                continue
            try:
                t = pd.Timestamp(b["time"])
            except (TypeError, ValueError):
                continue
            b["time"] = t
            seen[t] = b
        return [seen[t] for t in sorted(seen)]

    def _load_from_redis(self):
        """Recover premium state from Redis after restart."""
        r = self._r

        raw = r.get(shared_premium_candles_1m(self.symbol))
        if raw:
            self._bars = self._normalize_bars(json.loads(raw))

        state = r.hgetall(shared_premium_state(self.symbol))
        if state:
            self._instrument_key = state.get("instrument_key") or None
            self._trading_symbol = state.get("trading_symbol") or None
            self._opt_type = state.get("opt_type") or None
            self._expiry_str = state.get("expiry_str") or ""
            try:
                self._strike = float(state["strike"]) if state.get("strike") else None
            except (TypeError, ValueError):
                self._strike = None

        cur = r.hgetall(shared_premium_current_1m(self.symbol))
        if cur:
            self._cur = {
                k: float(v) if k in ("open", "high", "low", "close", "volume") else v
                for k, v in cur.items()
            }
            self._cur_min = cur.get("minute")

    def _save_state(self):
        """Persist the selected-option state to Redis."""
        r = self._r
        key = shared_premium_state(self.symbol)
        r.delete(key)
        pipe = r.pipeline()
        pipe.hset(key, "instrument_key", str(self._instrument_key or ""))
        pipe.hset(key, "trading_symbol", str(self._trading_symbol or ""))
        pipe.hset(key, "opt_type", str(self._opt_type or ""))
        pipe.hset(key, "strike", str(self._strike or ""))
        pipe.hset(key, "expiry_str", str(self._expiry_str))
        pipe.execute()

    def _save_current_candle(self):
        """Persist the developing premium candle to Redis."""
        if not self._cur:
            return
        key = shared_premium_current_1m(self.symbol)
        self._r.delete(key)
        pipe = self._r.pipeline()
        for field in ("open", "high", "low", "close", "volume"):
            pipe.hset(key, field, str(self._cur.get(field, 0)))
        pipe.hset(key, "minute", self._cur_min or "")
        pipe.execute()

    def _save_to_redis(self):
        """Persist candle list + developing candle to Redis."""
        r = self._r
        with self._lock:
            r.set(shared_premium_candles_1m(self.symbol),
                  json.dumps(self._bars[-MAX_1M_BARS:], default=str),
                  ex=CANDLE_TTL_SEC)
        self._save_current_candle()

    # ================================================================
    # REDIS READERS (cross-process)
    # ================================================================

    @staticmethod
    def get_1m_df(symbol: str) -> pd.DataFrame:
        """Get option-premium 1-minute candles from Redis."""
        raw = get_redis_sync().get(shared_premium_candles_1m(symbol.upper()))
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
    def get_state(symbol: str) -> Optional[dict]:
        """Get the currently selected option state from Redis."""
        raw = get_redis_sync().hgetall(shared_premium_state(symbol.upper()))
        if not raw:
            return None
        state = {k: v for k, v in raw.items()}
        try:
            state["strike"] = float(state["strike"]) if state.get("strike") else None
        except (TypeError, ValueError):
            state["strike"] = None
        return state

    # ================================================================
    # INSTANCE MANAGEMENT
    # ================================================================

    @classmethod
    def get_or_create(cls, symbol: str, access_token: str) -> "SharedOptionPremiumBuilder":
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
