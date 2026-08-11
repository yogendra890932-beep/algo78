# backend/shared/shared_cache.py
# ================================================================
# Shared In-Memory & Redis Cache — Frequently-Used Reference Data.
#
# Caches data that is the same for ALL users and changes rarely:
#   - Instrument master (DataFrame, loaded once from disk)
#   - Streamer tokens per symbol
#   - Lot sizes per symbol
#   - Expiry dates
#   - Trading holidays
#   - Exchange metadata
#   - Trading session times
#
# Uses Redis for cross-process sharing + in-process memory for speed.
# ================================================================

import json
import time
import threading
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo
from typing import Optional

import pandas as pd

from backend.services.redis_client import get_redis_sync

# NSE market-hours decisions must be evaluated in IST regardless of the
# server's local timezone (a UTC VPS would otherwise shift every check
# by 5.5 hours and wrongly report the market as closed).
IST = ZoneInfo("Asia/Kolkata")


def now_ist() -> datetime:
    """Current date/time in IST (Asia/Kolkata)."""
    return datetime.now(IST)


def today_ist() -> date:
    """Current date in IST."""
    return now_ist().date()


# ================================================================
# IN-MEMORY CACHE (process-local, fast)
# ================================================================

_cache: dict = {}
_cache_lock = threading.Lock()


def _r():
    return get_redis_sync()


# ================================================================
# LOT SIZES
# ================================================================

LOT_SIZE_CACHE_TTL = 3600  # 1 hour

# Built-in defaults (sync with engine_v6.NSE_LOT_SIZES)
DEFAULT_LOT_SIZES: dict[str, int] = {
    "NIFTY": 75, "BANKNIFTY": 15, "FINNIFTY": 40, "MIDCPNIFTY": 75,
    "NIFTYNXT50": 25, "SENSEX": 10, "BANKEX": 15,
    "RELIANCE": 250, "TCS": 150, "INFY": 300, "HDFCBANK": 550,
    "ICICIBANK": 700, "SBIN": 1500, "AXISBANK": 625, "BAJFINANCE": 125,
    "WIPRO": 1500, "TATASTEEL": 5500, "TATAMOTORS": 2850,
    "ADANIPORTS": 1250, "MARUTI": 100, "SUNPHARMA": 350, "KOTAKBANK": 400,
}


def get_lot_size(symbol: str, custom: Optional[dict] = None) -> int:
    """Get lot size for a symbol, with optional user overrides."""
    import re
    clean = re.sub(r'\d{2}[A-Z]{3}.*$', '', symbol.upper().strip())

    if custom:
        hit = custom.get(clean) or custom.get(symbol.upper())
        if hit:
            return int(hit)

    # Check Redis cache
    cache_key = f"sys:cache:lot_size:{clean}"
    r = _r()
    cached = r.get(cache_key)
    if cached:
        return int(cached)

    # Fall back to defaults
    size = DEFAULT_LOT_SIZES.get(clean) or DEFAULT_LOT_SIZES.get(symbol.upper())
    if size:
        r.set(cache_key, str(size), ex=LOT_SIZE_CACHE_TTL)
        return size

    print(f"[shared_cache] Unknown symbol '{symbol}', defaulting lot size to 1")
    return 1


# ================================================================
# STREAMER TOKENS
# ================================================================

KNOWN_INDEX_TOKENS = {
    "NIFTY":      "NSE_INDEX|Nifty 50",
    "BANKNIFTY":  "NSE_INDEX|Nifty Bank",
    "FINNIFTY":   "NSE_INDEX|Nifty Fin Service",
    "MIDCPNIFTY": "NSE_INDEX|NIFTY MID SELECT",
    "SENSEX":     "BSE_INDEX|SENSEX",
    "BANKEX":     "BSE_INDEX|BANKEX",
}

KNOWN_HISTORY_KEYS = {
    "NIFTY":      "NSE_INDEX|Nifty 50",
    "BANKNIFTY":  "NSE_INDEX|Nifty Bank",
    "FINNIFTY":   "NSE_INDEX|Nifty Fin Service",
    "MIDCPNIFTY": "NSE_INDEX|Nifty MidCap Select",
    "SENSEX":     "BSE_INDEX|SENSEX",
    "BANKEX":     "BSE_INDEX|BANKEX",
}

KNOWN_STRIKE_STEPS = {
    "NIFTY": 50, "BANKNIFTY": 100, "FINNIFTY": 50,
    "MIDCPNIFTY": 25, "SENSEX": 100,
}


def get_streamer_token(symbol: str) -> str:
    """Get Upstox streamer token for a symbol.

    Resolved from the instruments master (CSV) FIRST — the streamer needs
    the master's instrument_key form (e.g. "NSE_INDEX|Nifty 50"); legacy
    numeric tokens and admin-configured history-style keys are stale and
    must not be used for streaming. Falls back to admin config, then the
    known-index map.
    """
    sym = symbol.upper()
    r = _r()
    cache_key = f"sys:cache:streamer_token:{sym}"
    cached = r.get(cache_key)
    if cached:
        return cached

    token = None
    try:
        from backend.engine.instruments import resolve_streamer_token
        token = resolve_streamer_token(sym)
    except Exception as e:
        print(f"⚠️  streamer token resolve err for {sym}: {e}")

    if not token:
        # Admin config (may hold a history-style key — only used as fallback)
        try:
            from backend.services.admin_config_cache import get_streamer_token as admin_token
            db_row = admin_token(sym)
            if db_row and db_row.get("streamer_token"):
                token = db_row["streamer_token"]
        except Exception as e:
            print(f"⚠️  streamer token admin lookup err for {sym}: {e}")

    if not token:
        token = KNOWN_INDEX_TOKENS.get(sym, f"NSE_INDEX|{sym}")

    r.set(cache_key, token, ex=86400)
    return token


def get_history_key(symbol: str) -> str:
    """Get Upstox History API key for a symbol."""
    sym = symbol.upper()
    from backend.services.admin_config_cache import get_streamer_token as admin_token
    db_row = admin_token(sym)
    if db_row and db_row.get("history_key"):
        return db_row["history_key"]
    return KNOWN_HISTORY_KEYS.get(sym, f"NSE_INDEX|{sym}")


def get_strike_step(symbol: str) -> int:
    """Get strike step for a symbol."""
    sym = symbol.upper()
    from backend.services.admin_config_cache import get_streamer_token as admin_token
    db_row = admin_token(sym)
    if db_row and db_row.get("strike_step"):
        return db_row["strike_step"]
    return KNOWN_STRIKE_STEPS.get(sym, 50)


# ================================================================
# TRADING HOLIDAYS
# ================================================================

HOLIDAYS_2025 = {
    "2025-01-26", "2025-02-26", "2025-03-14", "2025-03-31",
    "2025-04-10", "2025-04-14", "2025-04-18", "2025-05-01",
    "2025-08-15", "2025-08-27", "2025-10-02", "2025-10-21",
    "2025-10-22", "2025-11-05", "2025-12-25",
}

HOLIDAYS_2026 = {
    "2026-01-26", "2026-03-20", "2026-04-03", "2026-04-14",
    "2026-04-30", "2026-05-01", "2026-08-15", "2026-10-02",
    "2026-11-14", "2026-12-25",
}

ALL_HOLIDAYS = HOLIDAYS_2025 | HOLIDAYS_2026


def is_nse_holiday(d: date = None) -> bool:
    """Check if a date is an NSE holiday or weekend."""
    d = d or today_ist()
    if d.weekday() >= 5:
        return True
    from backend.services.admin_config_cache import is_holiday
    return is_holiday(d) or d.strftime("%Y-%m-%d") in ALL_HOLIDAYS


def last_trading_day(from_date: date = None) -> date:
    """Get the most recent trading day before from_date."""
    d = (from_date or today_ist()) - timedelta(days=1)
    for _ in range(10):
        if not is_nse_holiday(d):
            return d
        d -= timedelta(days=1)
    return d


def is_market_open(now: datetime = None) -> bool:
    """Check if market is currently open (9:15 AM - 3:30 PM IST)."""
    if now is None:
        now = now_ist()
    elif now.tzinfo is None:
        now = now.replace(tzinfo=IST)
    if is_nse_holiday(now.date()):
        return False
    market_open = now.replace(hour=9, minute=15, second=0, microsecond=0)
    market_close = now.replace(hour=15, minute=30, second=0, microsecond=0)
    return market_open <= now <= market_close


# ================================================================
# COMPUTATION CACHE — Read-only access to shared computed data
# ================================================================
# Provides fast, thread-safe, read-only access to:
#   - Candles (1m, 5m) via SharedCandleBuilder
#   - Indicators via SharedIndicatorEngine
#   - Market structure via SharedMarketStructureEngine
#   - Option chain via SharedOptionChainService
#   - Signals via recent Redis signal stream
#
# Users NEVER recalculate — they read from this cache.
# ================================================================

CANDLE_DF_TTL_SEC = 5        # Re-read from Redis every 5s max
INDICATOR_TTL_SEC = 3         # Re-read from Redis every 3s max
STRUCTURE_TTL_SEC = 5         # Re-read from Redis every 5s max
OC_TTL_SEC = 10               # Re-read from Redis every 10s max
CACHE_CLEANUP_INTERVAL = 60   # Clean stale entries every 60s


class ComputationCache:
    """
    Thread-safe, instance-per-process cache for shared computed data.
    Each worker or web process creates its own instance.

    Usage:
        cache = ComputationCache()
        df = cache.get_candles_1m("NIFTY")
        inds = cache.get_indicators("NIFTY")
        struct = cache.get_structure("NIFTY")
        oc = cache.get_option_chain("NIFTY", "28DEC2023")
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._candles_1m: dict[str, tuple[float, "pd.DataFrame"]] = {}
        self._candles_5m: dict[str, tuple[float, "pd.DataFrame"]] = {}
        self._indicators: dict[str, tuple[float, dict]] = {}
        self._structure: dict[str, tuple[float, dict]] = {}
        self._oc: dict[str, tuple[float, dict]] = {}
        self._last_cleanup = time.time()

    def _is_stale(self, ts: float, ttl: float) -> bool:
        return (time.time() - ts) > ttl

    def _cleanup_if_due(self):
        now = time.time()
        if now - self._last_cleanup < CACHE_CLEANUP_INTERVAL:
            return
        self._last_cleanup = now
        with self._lock:
            self._candles_1m = {
                k: v for k, v in self._candles_1m.items()
                if not self._is_stale(v[0], CANDLE_DF_TTL_SEC * 6)
            }
            self._candles_5m = {
                k: v for k, v in self._candles_5m.items()
                if not self._is_stale(v[0], CANDLE_DF_TTL_SEC * 6)
            }
            self._indicators = {
                k: v for k, v in self._indicators.items()
                if not self._is_stale(v[0], INDICATOR_TTL_SEC * 6)
            }
            self._structure = {
                k: v for k, v in self._structure.items()
                if not self._is_stale(v[0], STRUCTURE_TTL_SEC * 6)
            }
            self._oc = {
                k: v for k, v in self._oc.items()
                if not self._is_stale(v[0], OC_TTL_SEC * 6)
            }

    # ── Candles ──────────────────────────────────────────────────

    def get_candles_1m(self, symbol: str) -> "pd.DataFrame":
        """Get 1-minute candles (cached, read-only)."""
        from backend.shared.candle_builder import SharedCandleBuilder
        sym = symbol.upper()
        self._cleanup_if_due()
        with self._lock:
            entry = self._candles_1m.get(sym)
            if entry and not self._is_stale(entry[0], CANDLE_DF_TTL_SEC):
                return entry[1]
        df = SharedCandleBuilder.get_1m_df_from_redis(sym)
        with self._lock:
            self._candles_1m[sym] = (time.time(), df)
        return df

    def get_candles_5m(self, symbol: str) -> "pd.DataFrame":
        """Get 5-minute candles (cached, read-only)."""
        from backend.shared.candle_builder import SharedCandleBuilder
        sym = symbol.upper()
        self._cleanup_if_due()
        with self._lock:
            entry = self._candles_5m.get(sym)
            if entry and not self._is_stale(entry[0], CANDLE_DF_TTL_SEC):
                return entry[1]
        df = SharedCandleBuilder.get_5m_df_from_redis(sym)
        with self._lock:
            self._candles_5m[sym] = (time.time(), df)
        return df

    # ── Indicators ───────────────────────────────────────────────

    def get_indicators(self, symbol: str) -> dict:
        """Get live indicator values (cached, read-only)."""
        from backend.shared.indicator_engine import SharedIndicatorEngine
        sym = symbol.upper()
        self._cleanup_if_due()
        with self._lock:
            entry = self._indicators.get(sym)
            if entry and not self._is_stale(entry[0], INDICATOR_TTL_SEC):
                return dict(entry[1])
        inds = SharedIndicatorEngine.get_indicators(sym)
        if inds is None:
            inds = {}
        with self._lock:
            self._indicators[sym] = (time.time(), inds)
        return dict(inds)

    def get_indicator(self, symbol: str, name: str):
        """Get a single indicator value (cached)."""
        inds = self.get_indicators(symbol)
        return inds.get(name)

    # ── Market Structure ─────────────────────────────────────────

    def get_structure(self, symbol: str) -> dict:
        """Get market structure analysis (cached, read-only)."""
        from backend.shared.market_structure_engine import (
            SharedMarketStructureEngine,
            SharedUnderlyingMarketStructureEngine,
        )
        sym = symbol.upper()
        self._cleanup_if_due()
        with self._lock:
            entry = self._structure.get(sym)
            if entry and not self._is_stale(entry[0], STRUCTURE_TTL_SEC):
                return dict(entry[1])

        result = {
            "1m": SharedMarketStructureEngine.get_structure(sym) or {},
            "5m": SharedUnderlyingMarketStructureEngine.get_structure(sym) or {},
        }
        with self._lock:
            self._structure[sym] = (time.time(), result)
        return dict(result)

    # ── Option Chain ─────────────────────────────────────────────

    def get_option_chain(self, symbol: str, expiry: str) -> dict:
        """Get option chain analysis (cached, read-only)."""
        from backend.shared.option_chain_service import SharedOptionChainService
        sym = symbol.upper()
        key = f"{sym}:{expiry}"
        self._cleanup_if_due()
        with self._lock:
            entry = self._oc.get(key)
            if entry and not self._is_stale(entry[0], OC_TTL_SEC):
                return dict(entry[1])

        data = SharedOptionChainService.get_analysis(sym, expiry) or {}
        with self._lock:
            self._oc[key] = (time.time(), data)
        return dict(data)

    # ── Bulk invalidation ────────────────────────────────────────

    def invalidate_candles(self, symbol: str):
        """Force cache refresh for candles after a candle close."""
        sym = symbol.upper()
        with self._lock:
            self._candles_1m.pop(sym, None)
            self._candles_5m.pop(sym, None)

    def invalidate_indicators(self, symbol: str):
        """Force cache refresh for indicators after recomputation."""
        sym = symbol.upper()
        with self._lock:
            self._indicators.pop(sym, None)

    def invalidate_structure(self, symbol: str):
        """Force cache refresh for market structure."""
        sym = symbol.upper()
        with self._lock:
            self._structure.pop(sym, None)

    def invalidate_option_chain(self, symbol: str, expiry: str):
        """Force cache refresh for option chain."""
        sym = symbol.upper()
        key = f"{sym}:{expiry}"
        with self._lock:
            self._oc.pop(key, None)

    def invalidate_all(self):
        """Clear all cached entries."""
        with self._lock:
            self._candles_1m.clear()
            self._candles_5m.clear()
            self._indicators.clear()
            self._structure.clear()
            self._oc.clear()

    @property
    def stats(self) -> dict:
        """Cache statistics for monitoring."""
        with self._lock:
            return {
                "candles_1m_entries": len(self._candles_1m),
                "candles_5m_entries": len(self._candles_5m),
                "indicator_entries": len(self._indicators),
                "structure_entries": len(self._structure),
                "oc_entries": len(self._oc),
            }


# Process-local singleton
computation_cache = ComputationCache()

