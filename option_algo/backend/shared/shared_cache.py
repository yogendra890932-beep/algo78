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
# Bump this whenever DEFAULT_LOT_SIZES changes so running processes stop
# reading stale values from the previous table (cache keys are versioned).
LOT_SIZE_CACHE_VERSION = 2

# Built-in NSE F&O lot sizes. Source: https://dhan.co/nse-fno-lot-size/
# fetched 2026-10-06. engine_v6 imports this table as NSE_LOT_SIZES so
# there is a single source of truth (do not duplicate elsewhere).
DEFAULT_LOT_SIZES: dict[str, int] = {
    # Nifty / NSE indices
    "NIFTY": 65,
    "BANKNIFTY": 30,
    "FINNIFTY": 60,
    "MIDCPNIFTY": 120,
    "NIFTYNXT50": 25,
    "GOLDSTAR": 1100,
    # BSE indices
    "SENSEX": 20,
    "BANKEX": 30,
    # NSE F&O stocks
    "360ONE": 500, "ABB": 125, "ABCAPITAL": 3100,
    "ADANIENSOL": 675, "ADANIENT": 309, "ADANIGREEN": 600,
    "ADANIPORTS": 475, "ADANIPOWER": 3550, "ALKEM": 125,
    "AMBER": 100, "AMBUJACEM": 1200, "ANANDRATHI": 250,
    "ANGELONE": 2500, "APLAPOLLO": 350, "APOLLOHOSP": 125,
    "ASHOKLEY": 5000, "ASIANPAINT": 250, "ASTRAL": 425,
    "ATHERENERG": 375, "AUBANK": 1000, "AUROPHARMA": 550,
    "AXISBANK": 625, "BAJAJ-AUTO": 75, "BAJAJFINSV": 300,
    "BAJAJHLDNG": 75, "BAJFINANCE": 750, "BANDHANBNK": 3600,
    "BANKBARODA": 2925, "BANKINDIA": 5200, "BDL": 425,
    "BEL": 1425, "BHARATFORG": 500, "BHARTIARTL": 475,
    "BHEL": 2625, "BIOCON": 2500, "BLUESTARCO": 325,
    "BOSCHLTD": 25, "BPCL": 1975, "BRITANNIA": 125,
    "BSE": 200, "CAMS": 825, "CANBK": 6750,
    "CDSL": 475, "CGPOWER": 850, "CHOLAFIN": 625,
    "CIPLA": 425, "COALINDIA": 1350, "COCHINSHIP": 400,
    "COFORGE": 475, "COLPAL": 275, "CONCOR": 1250,
    "CROMPTON": 2150, "CUMMINSIND": 200, "DABUR": 1250,
    "DELHIVERY": 2075, "DIVISLAB": 100, "DIXON": 50,
    "DLF": 950, "DMART": 150, "DRREDDY": 625,
    "EICHERMOT": 100, "ENRIN": 175, "ETERNAL": 2425,
    "FEDERALBNK": 2500, "FORCEMOT": 25, "FORTIS": 775,
    "GAIL": 3550, "GLENMARK": 375, "GMRAIRPORT": 6975,
    "GODFRYPHLP": 275, "GODREJCP": 500, "GODREJPROP": 325,
    "GRASIM": 250, "GVT&D": 125, "HAL": 150,
    "HAVELLS": 500, "HCLTECH": 400, "HDFCAMC": 300,
    "HDFCBANK": 650, "HDFCLIFE": 1100, "HEROMOTOCO": 150,
    "HINDALCO": 700, "HINDPETRO": 2025, "HINDUNILVR": 300,
    "HINDZINC": 1225, "HYUNDAI": 275, "ICICIBANK": 700,
    "ICICIGI": 325, "ICICIPRULI": 925, "IDEA": 71475,
    "IDFCFIRSTB": 9275, "IEX": 4350, "INDHOTEL": 1000,
    "INDIANB": 1000, "INDIGO": 150, "INDUSINDBK": 700,
    "INDUSTOWER": 1700, "INFY": 400, "INOXWIND": 6400,
    "IOC": 4875, "IREDA": 4525, "IRFC": 5425,
    "ITC": 1725, "JINDALSTEL": 625, "JIOFIN": 2350,
    "JSWENERGY": 1075, "JSWSTEEL": 675, "JUBLFOOD": 1250,
    "KALYANKJIL": 1350, "KAYNES": 150, "KEI": 175,
    "KFINTECH": 575, "KOTAKBANK": 2000, "KPITTECH": 775,
    "LAURUSLABS": 850, "LICHSGFIN": 1000, "LICI": 1400,
    "LODHA": 625, "LT": 175, "LTF": 2250,
    "LTM": 150, "LUPIN": 425, "M&M": 200,
    "MAHABANK": 6500, "MANAPPURAM": 3000, "MANKIND": 250,
    "MARICO": 1200, "MARUTI": 50, "MAXHEALTH": 525,
    "MAZDOCK": 225, "MCX": 225, "MFSL": 400,
    "MOTHERSON": 6150, "MOTILALOFS": 775, "MPHASIS": 275,
    "MUTHOOTFIN": 275, "NAM-INDIA": 625, "NATIONALUM": 1875,
    "NAUKRI": 550, "NBCC": 6500, "NESTLEIND": 500,
    "NHPC": 6950, "NMDC": 6750, "NTPC": 1500,
    "NYKAA": 3125, "OBEROIRLTY": 350, "OFSS": 100,
    "OIL": 1400, "ONGC": 2250, "PAGEIND": 20,
    "PATANJALI": 1075, "PAYTM": 725, "PERSISTENT": 125,
    "PETRONET": 1900, "PFC": 1300, "PGEL": 950,
    "PHOENIXLTD": 350, "PIDILITIND": 500, "PIIND": 175,
    "PNB": 8000, "PNBHOUSING": 650, "POLICYBZR": 350,
    "POLYCAB": 125, "POWERGRID": 1900, "POWERINDIA": 25,
    "PREMIERENE": 650, "PRESTIGE": 450, "RADICO": 150,
    "RBLBANK": 3175, "RECLTD": 1575, "RELIANCE": 500,
    "RVNL": 1925, "SAGILITY": 12000, "SAIL": 4700,
    "SBICARD": 800, "SBILIFE": 375, "SBIN": 750,
    "SHREECEM": 25, "SHRIRAMFIN": 825, "SIEMENS": 175,
    "SOLARINDS": 50, "SONACOMS": 1225, "SRF": 200,
    "SUNPHARMA": 350, "SUPREMEIND": 175, "SUZLON": 12700,
    "SWIGGY": 1825, "TATACONSUM": 550, "TATAELXSI": 125,
    "TATAPOWER": 1450, "TATASTEEL": 2750, "TCS": 225,
    "TECHM": 600, "TIINDIA": 200, "TITAN": 175,
    "TMPV": 1600, "TORNTPHARM": 125, "TRENT": 225,
    "TVSMOTOR": 175, "UJJIVANSFB": 8000, "ULTRACEMCO": 50,
    "UNIONBANK": 4425, "UNITDSPR": 400, "UNOMINDA": 550,
    "UPL": 1355, "VBL": 1275, "VEDL": 1150,
    "VMM": 4850, "VOLTAS": 375, "WAAREEENER": 175,
    "WIPRO": 3000, "YESBANK": 31100, "ZYDUSLIFE": 900,
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
    cache_key = f"sys:cache:lot_size:v{LOT_SIZE_CACHE_VERSION}:{clean}"
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

