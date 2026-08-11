# backend/engine/history_loader.py
# ================================================================
# Historical + Intraday Candle Loader
#
# Problem solved:
#   1. After 9:20 AM, indicators need warm-up data (EMAs need 21+ bars)
#      Solution: load last trading day's 1m candles via HistoryV3Api
#      get_historical_candle_data1(), then append today's intraday bars
#
#   2. Upstox changes instrument_key after expiry
#      Old key (e.g. NSE_FO|41722) becomes invalid next week
#      Solution: save candle data per key to local SQLite cache for 7 days
#      On startup, load cached data for old key + new key and merge
#
# API used:
#   get_historical_candle_data1(instrument_key, unit, interval, to_date, from_date)
#   e.g.: get_historical_candle_data1("NSE_INDEX|Nifty 50", "minutes", "1", "2025-01-02", "2025-01-01")
#
# Result: strategies have 21+ warm bars from minute 1 of trading
# ================================================================

import os
import sqlite3
import threading
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
import pandas as pd
import upstox_client
from upstox_client.rest import ApiException

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

# ── Cache DB path ─────────────────────────────────────────────────
CACHE_DIR = "candle_cache"
CACHE_DB   = os.path.join(CACHE_DIR, "candle_history.db")
CACHE_DAYS = 7   # Keep candle data for 7 days (covers expiry key changes)

_db_lock = threading.Lock()


def _ensure_db():
    """Create cache directory and SQLite table if not exists."""
    os.makedirs(CACHE_DIR, exist_ok=True)
    with sqlite3.connect(CACHE_DB) as con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS candles (
                instrument_key TEXT NOT NULL,
                ts             TEXT NOT NULL,
                open           REAL,
                high           REAL,
                low            REAL,
                close          REAL,
                volume         REAL,
                PRIMARY KEY (instrument_key, ts)
            )
        """)
        con.execute("""
            CREATE INDEX IF NOT EXISTS idx_key_ts
            ON candles (instrument_key, ts)
        """)
        con.commit()


def _save_candles_to_cache(instrument_key: str, df: pd.DataFrame):
    """Persist candles to local SQLite cache."""
    if df.empty:
        return
    _ensure_db()
    rows = [
        (
            instrument_key,
            str(row["time"]),
            float(row["open"]),
            float(row["high"]),
            float(row["low"]),
            float(row["close"]),
            float(row.get("volume", 0)),
        )
        for _, row in df.iterrows()
    ]
    with _db_lock:
        with sqlite3.connect(CACHE_DB) as con:
            con.executemany("""
                INSERT OR REPLACE INTO candles
                (instrument_key, ts, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, rows)
            con.commit()


def _load_candles_from_cache(instrument_key: str,
                              from_date: date,
                              to_date: date) -> pd.DataFrame:
    """Load cached candles for an instrument_key within a date range."""
    _ensure_db()
    from_str = from_date.strftime("%Y-%m-%d")
    to_str   = (to_date + timedelta(days=1)).strftime("%Y-%m-%d")
    with _db_lock:
        with sqlite3.connect(CACHE_DB) as con:
            rows = con.execute("""
                SELECT ts, open, high, low, close, volume
                FROM candles
                WHERE instrument_key = ?
                  AND ts >= ?
                  AND ts <  ?
                ORDER BY ts
            """, (instrument_key, from_str, to_str)).fetchall()

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=["time","open","high","low","close","volume"])
    df["time"] = pd.to_datetime(df["time"])
    return df


def _purge_old_cache():
    """Remove cache entries older than CACHE_DAYS to save disk space."""
    _ensure_db()
    cutoff = (datetime.now() - timedelta(days=CACHE_DAYS)).strftime("%Y-%m-%d")
    with _db_lock:
        with sqlite3.connect(CACHE_DB) as con:
            con.execute("DELETE FROM candles WHERE ts < ?", (cutoff,))
            con.commit()



# ── Indian NSE Market Holidays (update yearly) ───────────────────
# Source: NSE website — https://www.nseindia.com/products-services/equity-market-trading-holidays
NSE_HOLIDAYS_2025 = {
    "2025-01-26",  # Republic Day
    "2025-02-26",  # Mahashivratri
    "2025-03-14",  # Holi
    "2025-03-31",  # Id-Ul-Fitr (Ramzan Eid)
    "2025-04-10",  # Shri Ram Navami
    "2025-04-14",  # Dr. Baba Saheb Ambedkar Jayanti
    "2025-04-18",  # Good Friday
    "2025-05-01",  # Maharashtra Day
    "2025-08-15",  # Independence Day
    "2025-08-27",  # Ganesh Chaturthi
    "2025-10-02",  # Mahatma Gandhi Jayanti
    "2025-10-02",  # Dussehra
    "2025-10-21",  # Diwali Laxmi Pujan
    "2025-10-22",  # Diwali Balipratipada
    "2025-11-05",  # Prakash Gurpurb Sri Guru Nanak Dev Ji
    "2025-12-25",  # Christmas
}

NSE_HOLIDAYS_2026 = {
    "2026-01-26",  # Republic Day
    "2026-03-20",  # Holi
    "2026-04-03",  # Good Friday
    "2026-04-14",  # Dr. Baba Saheb Ambedkar Jayanti
    "2026-04-30",  # Shri Ram Navami (tentative)
    "2026-05-01",  # Maharashtra Day
    "2026-08-15",  # Independence Day
    "2026-10-02",  # Mahatma Gandhi Jayanti
    "2026-11-14",  # Diwali (tentative)
    "2026-12-25",  # Christmas
}

NSE_HOLIDAYS: set = NSE_HOLIDAYS_2025 | NSE_HOLIDAYS_2026


def _is_nse_holiday(d: date) -> bool:
    """
    Returns True if date is a weekend or NSE market holiday.
    Holidays are admin-managed (backend.db.models.ExchangeHoliday) with
    the hardcoded NSE_HOLIDAYS sets above as a fallback — see
    backend.services.admin_config_cache.is_holiday().
    """
    if d.weekday() >= 5:   # Saturday=5, Sunday=6
        return True
    from backend.services.admin_config_cache import is_holiday
    return is_holiday(d)


def _last_trading_day(from_date: date = None) -> date:
    """
    Returns the most recent NSE trading day before `from_date` (default: today IST).
    Skips weekends AND Indian market holidays.
    Looks back up to 10 days to handle long holiday runs.
    """
    d = (from_date or today_ist()) - timedelta(days=1)
    for _ in range(10):
        if not _is_nse_holiday(d):
            return d
        d -= timedelta(days=1)
    # Fallback — should never reach here
    return d


def _is_trading_day(d: date = None) -> bool:
    """Returns True if `d` (default: today IST) is an NSE trading day."""
    return not _is_nse_holiday(d or today_ist())


def is_market_open(now: datetime = None) -> bool:
    """
    True if `now` (default: current IST time) falls within NSE trading
    hours (9:15 AM-3:30 PM) on an NSE trading day. Shared by the
    engine's per-tick guard (engine_v6._is_market_hours()) and the
    bot start API (backend.routers.all_routers, POST /api/bot/start),
    which rejects start requests outside market hours.
    Evaluated in IST (Asia/Kolkata) so results don't depend on the
    server's local timezone.
    """
    if now is None:
        now = now_ist()
    elif now.tzinfo is None:
        now = now.replace(tzinfo=IST)
    if _is_nse_holiday(now.date()):
        return False
    return (now.replace(hour=9,  minute=15, second=0, microsecond=0) <=
            now <=
            now.replace(hour=15, minute=30, second=0, microsecond=0))




def _parse_candle_response(resp) -> pd.DataFrame:
    """Parse HistoryV3Api response into a clean DataFrame."""
    raw = None
    if hasattr(resp, "data") and resp.data is not None:
        raw = getattr(resp.data, "candles", None)
    elif isinstance(resp, dict):
        raw = (resp.get("data") or {}).get("candles")

    if not raw:
        return pd.DataFrame()

    df = pd.DataFrame(raw, columns=["time","open","high","low","close","volume","oi"])
    df["time"] = pd.to_datetime(df["time"], utc=True, errors="coerce")
    df["time"] = df["time"].dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
    df = df.sort_values("time").reset_index(drop=True)
    for c in ["open","high","low","close","volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df.dropna(subset=["open","high","low","close"])


# ================================================================
# PUBLIC API
# ================================================================

def fetch_historical_candles(instrument_key: str,
                              from_date: date,
                              to_date: date,
                              access_token: str) -> pd.DataFrame:
    """
    Fetches historical 1-minute candles using get_historical_candle_data1().
    Caches results locally so repeated calls are instant.

    Args:
        instrument_key: History API key (e.g. "NSE_INDEX|Nifty 50")
        from_date:      Start date (inclusive)
        to_date:        End date   (inclusive)
        access_token:   Upstox access token

    Returns:
        DataFrame with columns: time, open, high, low, close, volume
    """
    # Check cache first
    cached = _load_candles_from_cache(instrument_key, from_date, to_date)
    if not cached.empty:
        print(f"[HistCache] {instrument_key[:30]} {from_date}→{to_date}: "
              f"{len(cached)} bars from cache")
        return cached

    # Fetch from API
    try:
        cfg = upstox_client.Configuration()
        cfg.access_token = access_token
        api  = upstox_client.HistoryV3Api(upstox_client.ApiClient(cfg))

        # API signature: (instrument_key, unit, interval, to_date, from_date)
        # Note: to_date comes BEFORE from_date in the Upstox SDK
        resp = api.get_historical_candle_data1(
            instrument_key,
            "minutes",
            "1",
            to_date.strftime("%Y-%m-%d"),    # to_date first
            from_date.strftime("%Y-%m-%d"),  # from_date second
        )
        df = _parse_candle_response(resp)

        if not df.empty:
            _save_candles_to_cache(instrument_key, df)
            print(f"[HistAPI] {instrument_key[:30]} {from_date}→{to_date}: "
                  f"{len(df)} bars fetched and cached")
        else:
            print(f"[HistAPI] {instrument_key[:30]}: no data returned")

        return df

    except ApiException as e:
        print(f"[HistAPI] ApiException ({instrument_key[:30]}): {getattr(e,'body',str(e))}")
        return pd.DataFrame()
    except Exception as e:
        print(f"[HistAPI] Error ({instrument_key[:30]}): {e}")
        return pd.DataFrame()


def fetch_intraday_candles(instrument_key: str,
                            access_token: str) -> pd.DataFrame:
    """
    Fetches today's intraday 1-minute candles using get_intra_day_candle_data().
    Does NOT cache — always fetches fresh.
    """
    try:
        cfg = upstox_client.Configuration()
        cfg.access_token = access_token
        api  = upstox_client.HistoryV3Api(upstox_client.ApiClient(cfg))
        resp = api.get_intra_day_candle_data(instrument_key, "minutes", "1")
        df   = _parse_candle_response(resp)
        if not df.empty:
            # Also save today's bars to cache as we go
            _save_candles_to_cache(instrument_key, df)
        return df
    except ApiException as e:
        print(f"[IntraAPI] ApiException ({instrument_key[:30]}): {getattr(e,'body',str(e))}")
        return pd.DataFrame()
    except Exception as e:
        print(f"[IntraAPI] Error ({instrument_key[:30]}): {e}")
        return pd.DataFrame()


def load_warm_candles(instrument_key: str,
                      access_token: str,
                      old_instrument_key: str = None,
                      lookback_days: int = 3) -> pd.DataFrame:
    """
    Main entry point called at engine startup and on direction change.

    Builds a warm candle DataFrame by:
      1. Loading up to `lookback_days` of historical data
         (tries cache first, then HistoryV3Api)
      2. If `old_instrument_key` provided, loads its cached data too
         (handles expiry key change — old key data merged with new key data)
      3. Appending today's intraday data
      4. Deduplicating and sorting by time

    Returns a single merged DataFrame ready for indicator computation.
    Guaranteed to have enough bars for EMA(21) warm-up even at 9:20 AM.
    """
    _purge_old_cache()   # housekeeping

    today     = today_ist()
    frames    = []

    # ── Step 1: Historical data (last N trading days) ────────────
    from_date = today - timedelta(days=lookback_days + 3)   # +3 for weekends
    to_date   = _last_trading_day()

    # New instrument key
    hist_new = fetch_historical_candles(
        instrument_key, from_date, to_date, access_token
    )
    if not hist_new.empty:
        frames.append(hist_new)

    # ── Step 2: Old instrument key (expiry rollover) ──────────────
    if old_instrument_key and old_instrument_key != instrument_key:
        print(f"[WarmLoad] Loading old key data: {old_instrument_key[:30]}")
        # Old key: only load from cache (API won't have it if expired)
        cached_old = _load_candles_from_cache(old_instrument_key, from_date, to_date)
        if not cached_old.empty:
            frames.append(cached_old)
            print(f"[WarmLoad] Old key: {len(cached_old)} bars from cache")
        else:
            # Try API too — might still be available
            hist_old = fetch_historical_candles(
                old_instrument_key, from_date, to_date, access_token
            )
            if not hist_old.empty:
                frames.append(hist_old)

    # ── Step 3: Today's intraday data ────────────────────────────
    if _is_market_hours_or_after():
        intra = fetch_intraday_candles(instrument_key, access_token)
        if not intra.empty:
            frames.append(intra)
            print(f"[WarmLoad] Intraday: {len(intra)} bars")

    if not frames:
        print(f"[WarmLoad] No data available for {instrument_key[:30]}")
        return pd.DataFrame()

    # ── Step 4: Merge, deduplicate, sort ─────────────────────────
    merged = pd.concat(frames, ignore_index=True)
    merged["time"] = pd.to_datetime(merged["time"])
    merged = merged.sort_values("time").drop_duplicates(subset=["time"])
    merged = merged.reset_index(drop=True)

    # Keep only last 5 trading days of bars to avoid stale data
    cutoff = pd.Timestamp(datetime.now() - timedelta(days=5))
    merged = merged[merged["time"] >= cutoff]

    print(f"[WarmLoad] Final: {len(merged)} bars | "
          f"First: {merged['time'].iloc[0]} | Last: {merged['time'].iloc[-1]}")
    return merged


def _is_market_hours_or_after() -> bool:
    """True from 9:00 AM IST onwards on weekdays."""
    n = now_ist()
    if n.weekday() >= 5:
        return False
    return n.hour >= 9


def save_instrument_key_history(user_id: int, symbol: str,
                                 instrument_key: str, opt_type: str,
                                 strike: float, expiry: str):
    """
    Saves the current instrument key to a persistent log.
    Called whenever direction changes and a new key is selected.
    Allows us to retrieve old keys for cache lookup after expiry.
    """
    _ensure_db()
    with _db_lock:
        with sqlite3.connect(CACHE_DB) as con:
            con.execute("""
                CREATE TABLE IF NOT EXISTS instrument_key_log (
                    user_id        INTEGER,
                    symbol         TEXT,
                    instrument_key TEXT,
                    opt_type       TEXT,
                    strike         REAL,
                    expiry         TEXT,
                    saved_at       TEXT,
                    PRIMARY KEY (user_id, instrument_key)
                )
            """)
            con.execute("""
                INSERT OR REPLACE INTO instrument_key_log
                (user_id, symbol, instrument_key, opt_type, strike, expiry, saved_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                user_id, symbol, instrument_key,
                opt_type, strike, expiry,
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            ))
            # Purge entries older than 7 days
            cutoff = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
            con.execute(
                "DELETE FROM instrument_key_log WHERE saved_at < ?", (cutoff,)
            )
            con.commit()


def get_previous_instrument_keys(user_id: int, symbol: str,
                                  opt_type: str) -> list[str]:
    """
    Returns up to 3 most recent instrument keys for this user/symbol/type.
    Used to find old keys after expiry rollover.
    """
    _ensure_db()
    try:
        with sqlite3.connect(CACHE_DB) as con:
            con.execute("""
                CREATE TABLE IF NOT EXISTS instrument_key_log (
                    user_id INTEGER, symbol TEXT, instrument_key TEXT,
                    opt_type TEXT, strike REAL, expiry TEXT, saved_at TEXT,
                    PRIMARY KEY (user_id, instrument_key)
                )
            """)
            rows = con.execute("""
                SELECT instrument_key FROM instrument_key_log
                WHERE user_id = ? AND symbol = ? AND opt_type = ?
                ORDER BY saved_at DESC
                LIMIT 3
            """, (user_id, symbol, opt_type)).fetchall()
        return [r[0] for r in rows]
    except Exception:
        return []
