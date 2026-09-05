# backend/engine/instruments.py
# ================================================================
# Instrument Key Fetcher — complete rewrite
#
# Based on user's proven working code with additions:
#   • 7-day cache (Parquet → CSV fallback, no pyarrow needed)
#   • force_refresh=True at startup ensures fresh options chain
#   • get_itm_instrument() for 1-strike ITM selection
#   • detect_strike_step() via GCD of available strikes
#   • resolve_history_key() for HistoryV3Api key resolution
#   • save_key_to_log() / get_previous_keys() for expiry rollover
#   • skip_expired: after 15:30 skips today's expiry contracts
# ================================================================

import os
import gzip
import json
import math
import sqlite3
import threading
from datetime import datetime, timedelta, date
from typing import Optional
import pandas as pd
import requests

# ================================================================
# CONFIG
# ================================================================

INSTRUMENTS_FILE   = "instruments_nse.parquet"
INSTRUMENTS_URL    = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"
# SENSEX / BANKEX derivatives trade on BSE (segment BSE_FO) and their option
# rows are NOT part of Upstox's NSE master, so we merge a second BSE master
# into the combined instrument universe. NSE-only flows are unaffected — the
# BSE rows simply make get_instrument_key_auto() able to resolve SENSEX CE/PE.
BSE_INSTRUMENTS_FILE = "instruments_bse.parquet"
BSE_INSTRUMENTS_URL  = "https://assets.upstox.com/market-quote/instruments/exchange/BSE.json.gz"
CACHE_MAX_AGE_DAYS = 7          # 7 days covers full weekly expiry cycle
CACHE_DIR          = "candle_cache"
KEY_LOG_DB         = os.path.join(CACHE_DIR, "key_log.db")

_key_log_lock = threading.Lock()

# Verified History API keys for major indices
KNOWN_INDEX_KEYS = {
    "NIFTY":       "NSE_INDEX|Nifty 50",
    "BANKNIFTY":   "NSE_INDEX|Nifty Bank",
    "FINNIFTY":    "NSE_INDEX|Nifty Fin Service",
    "MIDCPNIFTY":  "NSE_INDEX|Nifty MidCap Select",
    "SENSEX":      "BSE_INDEX|SENSEX",
    "BANKEX":      "BSE_INDEX|BANKEX",
}

# Known strike steps for major indices (fast path)
KNOWN_STEPS = {
    "NIFTY":      50,
    "BANKNIFTY":  100,
    "FINNIFTY":   50,
    "MIDCPNIFTY": 25,
    "SENSEX":     100,
}


# ================================================================
# INSTRUMENTS LOADER
# ================================================================

def _cache_expired(path: str, max_days: int) -> bool:
    if not os.path.exists(path):
        return True
    mtime = datetime.fromtimestamp(os.path.getmtime(path))
    return (datetime.now() - mtime) > timedelta(days=max_days)


# ================================================================
# IN-MEMORY SINGLETON CACHE
# ================================================================
# The instrument master (~96k rows) doesn't change while the worker
# process is running — it's only refreshed once per day at most
# (CACHE_MAX_AGE_DAYS=7 on disk, but realistically re-downloaded daily
# at worker startup). Without this in-memory cache, EVERY call to
# load_instruments() — which happens on every direction change, every
# ITM strike lookup, every strike-step detection — re-reads the
# Parquet/CSV file from disk and re-parses 96k rows into a fresh
# DataFrame. With multiple symbols and frequent direction flips this
# was the dominant source of repeated "Loaded 96291 instruments from
# cache" log lines and unnecessary CPU/I/O.
#
# Fix: load from disk ONCE per process, then serve the same in-memory
# DataFrame to every caller. Thread-safe via a lock (worker runs each
# symbol's SymbolEngine in its own thread, all sharing one process).
_instruments_cache: Optional[pd.DataFrame] = None
_instruments_cache_lock = threading.Lock()


def preload_instruments() -> pd.DataFrame:
    """
    Call this ONCE at worker startup (see worker.py main()) to load
    the instrument master into memory before any bot threads start.
    Subsequent load_instruments() calls from any symbol/thread reuse
    this same in-memory DataFrame — no repeated disk reads or parsing.
    """
    return load_instruments(force=False, _bypass_memory_cache=True)


def invalidate_instruments_cache():
    """
    Clears the in-memory cache, forcing the next load_instruments()
    call to re-read from disk (or re-download). Call this if you
    explicitly refresh the on-disk instrument file mid-session (rare
    — normally only needed across a multi-day-running process).
    """
    global _instruments_cache
    with _instruments_cache_lock:
        _instruments_cache = None
        print("[instruments] In-memory cache invalidated — next call reloads from disk")


def load_instruments(force: bool = False, _bypass_memory_cache: bool = False) -> pd.DataFrame:
    """
    Load the combined NSE + BSE instrument master (Parquet → CSV fallback).
    NSE carries NIFTY-family derivatives; BSE carries SENSEX/BANKEX options
    (segment BSE_FO) so SENSEX premium charts can resolve their CE/PE rows.
    Cache valid for 7 days on disk — refreshed on startup or when force=True.

    IN-MEMORY CACHING: after the first successful load in this process,
    the DataFrame is kept in memory and returned directly on every
    subsequent call — avoiding repeated disk reads/parsing of the same
    multi-hundred-thousand-row files on every direction change / strike lookup.

    _bypass_memory_cache is used internally by preload_instruments() —
    callers should not need to pass it directly.
    """
    global _instruments_cache

    # Fast path — serve from memory if already loaded and not forcing
    if not force:
        with _instruments_cache_lock:
            if _instruments_cache is not None:
                return _instruments_cache

    nse = _load_single_exchange_master(INSTRUMENTS_FILE, INSTRUMENTS_URL,
                                       "NSE", force)
    if nse is None:
        raise RuntimeError("Unable to load or download instruments.")

    # BSE is optional: if it can't be loaded we degrade to NSE-only rather
    # than breaking every NIFTY-family flow that already works.
    bse = _load_single_exchange_master(BSE_INSTRUMENTS_FILE, BSE_INSTRUMENTS_URL,
                                       "BSE", force)
    if bse is None:
        print("⚠️  BSE master unavailable — continuing with NSE instruments only")
        df = nse
    else:
        df = pd.concat([nse, bse], ignore_index=True, sort=False)
        if "instrument_key" in df.columns:
            df = df.drop_duplicates(subset=["instrument_key"], keep="first")
            df = df.reset_index(drop=True)
        print(f"✅ Combined instrument universe: {len(df)} rows "
              f"(NSE {len(nse)} + BSE {len(bse)})")

    with _instruments_cache_lock:
        _instruments_cache = df
    return df


def _load_single_exchange_master(file: str, url: str, label: str,
                                 force: bool) -> Optional[pd.DataFrame]:
    """
    Load/refresh a single exchange's master (Parquet → CSV fallback), same
    cache semantics as the old all-in-one loader. Returns None only if the
    exchange master cannot be loaded from disk OR downloaded.
    """
    parquet = file
    csv     = parquet.replace(".parquet", ".csv")

    # ── Try cache first ──────────────────────────────────────────
    if not force:
        for path in [parquet, csv]:
            if os.path.exists(path) and not _cache_expired(path, CACHE_MAX_AGE_DAYS):
                try:
                    df = (pd.read_parquet(path) if path.endswith(".parquet")
                          else pd.read_csv(path))
                    print(f"✅ Loaded {len(df)} {label} instruments from cache "
                          f"({os.path.basename(path)}) — caching in memory for this session")
                    return df
                except Exception as e:
                    print(f"⚠️  {label} cache read failed ({path}): {e}")

    # ── Download from Upstox ─────────────────────────────────────
    print(f"🌐 Downloading {label} instruments from Upstox...")
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        decompressed = gzip.decompress(response.content)
        instruments  = json.loads(decompressed.decode("utf-8"))
        df           = pd.DataFrame(instruments)

        # Save cache
        try:
            df.to_parquet(parquet, index=False)
            print(f"✅ Downloaded and cached {len(df)} {label} instruments → {parquet}")
        except Exception:
            df.to_csv(csv, index=False)
            print(f"⚠️  Parquet engine missing → saved as CSV ({csv})")

        return df

    except Exception as e:
        print(f"❌ Error downloading {label} instruments: {e}")
        # Fallback to stale cache
        for path in [parquet, csv]:
            if os.path.exists(path):
                df = (pd.read_parquet(path) if path.endswith(".parquet")
                      else pd.read_csv(path))
                print(f"✅ Loaded from stale {label} fallback: {path}")
                return df
        return None


# ================================================================
# INSTRUMENT KEY AUTO-FETCHER  (user's proven code)
# ================================================================

def get_instrument_key_auto(
    symbol:           str   = None,
    name:             str   = None,
    strike:           float = None,
    opt_type:         str   = None,    # "CE" / "PE"
    instrument_type:  str   = None,    # "EQ" / "FUT" / "CE" / "PE"
    underlying_price: float = None,    # for ATM detection
    skip_expired:     bool  = True,    # skip today's expired contracts
) -> dict:
    """
    Fully automatic instrument fetcher (user's proven code + expiry skip).

    Logic:
    - Returns EQ by default if no instrument_type given
    - Auto-picks nearest FUT for F&O
    - Auto-picks nearest ATM CE/PE if strike not given
    - After 15:30 IST, skips today's expired contracts (skip_expired=True)
    """
    df = load_instruments()

    if not symbol and not name:
        raise ValueError("Must provide at least symbol or name.")

    # ── Symbol filter ─────────────────────────────────────────────
    symbol_cols = [c for c in
                   ["underlying_symbol", "symbol", "tradingsymbol", "trading_symbol"]
                   if c in df.columns]

    if symbol:
        mask = pd.Series(False, index=df.index)
        for col in symbol_cols:
            mask |= df[col].astype(str).str.upper() == symbol.upper()
        df = df[mask]

    if name and "name" in df.columns:
        df = df[df["name"].astype(str).str.upper()
                .str.contains(name.upper(), na=False)]

    if df.empty:
        raise ValueError(f"No instruments found for symbol={symbol} name={name}")

    # ── Auto instrument_type ──────────────────────────────────────
    if not instrument_type:
        if opt_type in ["CE", "PE"]:
            instrument_type = opt_type
        else:
            eq_df = df[df["segment"].astype(str).str.upper() == "NSE_EQ"]
            if not eq_df.empty:
                instrument_type = "EQ"
                df = eq_df
            else:
                fut_df = df[df["segment"].astype(str).str.upper() == "NSE_FO"]
                if fut_df.empty:
                    raise ValueError(f"No EQ or FUT found for symbol={symbol}")
                instrument_type = "FUT"
                df = fut_df

    # ── Filter by instrument_type ─────────────────────────────────
    if instrument_type in ["EQ", "FUT", "CE", "PE"]:
        df = df[df["instrument_type"].astype(str).str.upper()
                == instrument_type.upper()]

    # ── F&O: expiry, strike, ATM logic ───────────────────────────
    if instrument_type in ["FUT", "CE", "PE"]:
        df = df.copy()

        if "expiry" in df.columns and df["expiry"].notna().any():
            df["expiry_dt"]   = pd.to_datetime(df["expiry"], unit="ms", errors="coerce")
            df                = df.dropna(subset=["expiry_dt"])
            df["expiry_date"] = df["expiry_dt"].dt.normalize().dt.date

            # Skip expired contracts
            if skip_expired:
                # "after 3:30 PM IST" cutoff — evaluated in IST regardless
                # of the server's local timezone.
                from backend.engine.history_loader import now_ist
                today         = now_ist().date()
                now           = now_ist().replace(tzinfo=None)
                cutoff_passed = (now.hour > 15 or
                                 (now.hour == 15 and now.minute >= 30))
                min_date      = (today + timedelta(days=1)
                                 if cutoff_passed else today)
                valid = df[df["expiry_date"] >= min_date]
                if not valid.empty:
                    df = valid

            # Sort by nearest expiry first
            df = df.sort_values("expiry_dt")

        # ATM strike detection if strike not provided
        if instrument_type in ["CE", "PE"] and strike is None:
            if underlying_price is None:
                # Try last_price column or use median of strikes
                if "last_price" in df.columns:
                    lp = pd.to_numeric(df["last_price"], errors="coerce").dropna()
                    underlying_price = float(lp.median()) if not lp.empty else None
                if underlying_price is None:
                    underlying_price = float(
                        pd.to_numeric(df["strike_price"], errors="coerce")
                        .dropna().median()
                    )
            df["_strike_diff"] = abs(
                df["strike_price"].astype(float) - float(underlying_price)
            )
            df = df.sort_values(["expiry_dt", "_strike_diff"])

        # Filter by exact strike if provided
        if strike is not None:
            df = df[df["strike_price"].astype(float) == float(strike)]

    if df.empty:
        raise ValueError(
            f"No matching instrument: symbol={symbol} "
            f"type={instrument_type} strike={strike} opt_type={opt_type}"
        )

    selected = df.iloc[0]

    # ── Build result dict ─────────────────────────────────────────
    trading_symbol = (selected.get("trading_symbol")
                      or selected.get("tradingsymbol")
                      or selected.get("symbol"))

    expiry_dt  = selected.get("expiry_dt")
    expiry_date = selected.get("expiry_date")
    expiry_str  = str(expiry_date) if expiry_date else (
                  str(expiry_dt.date()) if expiry_dt is not None else "")

    strike_val = None
    if "strike_price" in selected.index:
        try:
            strike_val = float(selected["strike_price"])
        except Exception:
            pass

    info = {
        "instrument_key": selected["instrument_key"],
        "trading_symbol": trading_symbol,
        "lot_size":       int(selected.get("lot_size", 1)),
        "expiry_dt":      expiry_dt,
        "expiry_str":     expiry_str,
        "type":           selected["instrument_type"],
        "strike":         strike_val,
        "segment":        selected.get("segment"),
    }

    print(f"🎯 Selected: {info['trading_symbol']} → {info['instrument_key']} "
          f"(Type {info['type']}, Lot {info['lot_size']}, Exp {info['expiry_str']})")
    return info


# ================================================================
# ITM INSTRUMENT SELECTOR
# ================================================================

def get_itm_instrument(
    opt_type:         str,
    underlying_price: float,
    underlying_symbol: str,
    itm_depth:        int,
    strike_step:      int,
) -> dict:
    """
    Returns the ITM option instrument for given symbol/type.

    CE ITM: strike = ATM - (itm_depth × step)   ← below price = in the money
    PE ITM: strike = ATM + (itm_depth × step)   ← above price = in the money
    """
    atm    = round(underlying_price / strike_step) * strike_step
    target = (atm - itm_depth * strike_step if opt_type.upper() == "CE"
              else atm + itm_depth * strike_step)

    print(f"🔍 ITM calc: LTP={underlying_price:.0f} ATM={atm} "
          f"{opt_type} target_strike={target}")

    info = get_instrument_key_auto(
        symbol           = underlying_symbol,
        opt_type         = opt_type.upper(),
        strike           = target,
        underlying_price = underlying_price,
        skip_expired     = True,
    )
    info["strike_step"] = strike_step
    return info


# ================================================================
# STRIKE STEP DETECTION
# ================================================================

def detect_strike_step(symbol: str) -> int:
    """
    Auto-detects strike step using GCD of available strikes.
    Fast path for known indices.
    """
    sym = symbol.upper()
    from backend.services.admin_config_cache import get_streamer_token
    db_row = get_streamer_token(sym)
    if db_row and db_row.get("strike_step"):
        print(f"📐 Strike step for {symbol}: {db_row['strike_step']} (admin-configured)")
        return db_row["strike_step"]
    if sym in KNOWN_STEPS:
        print(f"📐 Strike step for {symbol}: {KNOWN_STEPS[sym]} (known index)")
        return KNOWN_STEPS[sym]

    try:
        df = load_instruments()
        sym_cols = [c for c in
                    ["underlying_symbol", "symbol", "tradingsymbol"]
                    if c in df.columns]
        mask = pd.Series(False, index=df.index)
        for c in sym_cols:
            mask |= df[c].astype(str).str.upper() == sym

        fo = df[
            mask &
            (df["segment"].astype(str).str.upper() == "NSE_FO") &
            (df["instrument_type"].astype(str).str.upper() == "CE")
        ].copy()

        if fo.empty:
            print(f"⚠️  No CE options for {symbol}, defaulting step=50")
            return 50

        fo["expiry_dt"] = pd.to_datetime(fo["expiry"], unit="ms", errors="coerce")
        fo = fo.dropna(subset=["expiry_dt"])
        nearest = fo["expiry_dt"].min()
        fo = fo[fo["expiry_dt"] == nearest]

        strikes = sorted(
            pd.to_numeric(fo["strike_price"], errors="coerce")
            .dropna().unique().tolist()
        )
        if len(strikes) < 2:
            return 50

        diffs = [int(strikes[i + 1] - strikes[i])
                 for i in range(len(strikes) - 1)
                 if strikes[i + 1] > strikes[i]]
        if not diffs:
            return 50

        step = diffs[0]
        for d in diffs[1:]:
            step = math.gcd(step, d)
        step = max(step, 1)

        print(f"📐 Strike step for {symbol}: {step} "
              f"(auto-detected from {len(strikes)} strikes)")
        return step

    except Exception as e:
        print(f"⚠️  Strike step detection failed ({e}), defaulting to 50")
        return 50


# ================================================================
# HISTORY KEY RESOLVER
# ================================================================

def resolve_streamer_token(symbol: str) -> Optional[str]:
    """
    Resolve the Upstox streamer instrument key for an index from the
    instruments master (CSV/Parquet).

    Streamer tokens are the master's instrument_key form (e.g.
    "NSE_INDEX|Nifty 50") — the numeric legacy tokens (e.g. NSE_INDEX|13)
    are stale and must not be used for the v3 market-data streamer.
    Matching uses trading_symbol first (NIFTY/BANKNIFTY/FINNIFTY/...),
    then the known index name. Returns None when unresolvable.
    """
    sym = symbol.upper()
    try:
        df = load_instruments()
    except Exception as e:
        print(f"⚠️  Streamer token lookup failed for {symbol}: {e}")
        return None

    seg = df["segment"].astype(str).str.upper()
    idx = df[seg.str.endswith("_INDEX")]
    if idx.empty:
        print(f"⚠️  Streamer token lookup failed for {symbol}: no index rows")
        return None

    # Primary — exact trading_symbol match (NIFTY, BANKNIFTY, FINNIFTY, ...)
    if "trading_symbol" in idx.columns:
        hit = idx[idx["trading_symbol"].astype(str).str.upper() == sym]
        if not hit.empty and "instrument_key" in hit.columns:
            key = str(hit.iloc[0]["instrument_key"])
            if key and key.lower() != "nan":
                print(f"🔑 Streamer token for {symbol}: {key} (from instruments master)")
                return key

    # Secondary — match the known index name (e.g. "NIFTY BANK")
    known = KNOWN_INDEX_KEYS.get(sym, "")
    if known and "name" in idx.columns:
        name_part = known.split("|", 1)[1].strip().upper()
        hit = idx[idx["name"].astype(str).str.upper() == name_part]
        if not hit.empty and "instrument_key" in hit.columns:
            key = str(hit.iloc[0]["instrument_key"])
            if key and key.lower() != "nan":
                print(f"🔑 Streamer token for {symbol}: {key} (from instruments master name)")
                return key

    return None


def resolve_history_key(symbol: str, streamer_token: str) -> str:
    """
    Resolves the correct instrument_key for HistoryV3Api.

    Streamer tokens use numeric IDs (e.g. NSE_INDEX|13).
    History API uses name-based keys (e.g. NSE_INDEX|Nifty 50).
    These are DIFFERENT — mapping is done via KNOWN_INDEX_KEYS first.
    """
    sym = symbol.upper()

    # ── Admin-configured override (backend.db.models.StreamerSymbolToken) ──
    from backend.services.admin_config_cache import get_streamer_token
    db_row = get_streamer_token(sym)
    if db_row and db_row.get("history_key"):
        print(f"🔑 History key for {symbol}: {db_row['history_key']} (admin-configured)")
        return db_row["history_key"]

    # ── Known indices (fastest path) ─────────────────────────────
    if sym in KNOWN_INDEX_KEYS:
        key = KNOWN_INDEX_KEYS[sym]
        print(f"🔑 History key for {symbol}: {key} (known index)")
        return key

    # ── Try FUT underlying_key from instruments ───────────────────
    try:
        df = load_instruments()
        sym_cols = [c for c in
                    ["underlying_symbol", "symbol", "tradingsymbol"]
                    if c in df.columns]
        mask = pd.Series(False, index=df.index)
        for c in sym_cols:
            mask |= df[c].astype(str).str.upper() == sym

        futs = df[mask & (df["instrument_type"].astype(str).str.upper() == "FUT")]
        if not futs.empty and "underlying_key" in futs.columns:
            uk = str(futs.iloc[0]["underlying_key"])
            if uk and uk.lower() != "nan":
                print(f"🔑 History key for {symbol}: {uk} (from FUT underlying_key)")
                return uk
    except Exception as e:
        print(f"⚠️  History key lookup failed for {symbol}: {e}")

    # ── Last resort — use streamer token as-is ────────────────────
    print(f"⚠️  Using streamer token as history key for {symbol}: {streamer_token}")
    return streamer_token


# ================================================================
# INSTRUMENT KEY LOG  (7-day persistence for expiry rollover)
# ================================================================

def _ensure_key_log():
    """Creates key_log table in the candle cache SQLite DB."""
    os.makedirs(CACHE_DIR, exist_ok=True)
    with sqlite3.connect(KEY_LOG_DB) as con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS key_log (
                user_id        INTEGER NOT NULL,
                symbol         TEXT    NOT NULL,
                instrument_key TEXT    NOT NULL,
                opt_type       TEXT,
                strike         REAL,
                expiry_str     TEXT,
                saved_at       TEXT    NOT NULL,
                PRIMARY KEY (user_id, instrument_key)
            )
        """)
        con.commit()


def save_key_to_log(user_id: int, symbol: str,
                    instrument_key: str, opt_type: str,
                    strike: float, expiry_str: str):
    """
    Saves the active instrument key to a local 7-day log.

    Called every time direction changes and a new key is activated.
    On expiry rollover, old entries (from previous week) remain cached
    so we can load their candle data for warm indicator concat.
    """
    _ensure_key_log()
    cutoff = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    with _key_log_lock:
        with sqlite3.connect(KEY_LOG_DB) as con:
            con.execute("""
                INSERT OR REPLACE INTO key_log
                (user_id, symbol, instrument_key, opt_type,
                 strike, expiry_str, saved_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                user_id, symbol, instrument_key,
                opt_type, strike, expiry_str,
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            ))
            # Purge entries older than 7 days
            con.execute(
                "DELETE FROM key_log WHERE saved_at < ?", (cutoff,)
            )
            con.commit()


def get_previous_keys(user_id: int, symbol: str,
                      opt_type: str,
                      exclude_key: str = None) -> list:
    """
    Returns up to 5 most recent instrument keys for this user/symbol/type.
    Used to fetch cached candle data after weekly expiry rollover.
    The current active key is excluded via exclude_key.
    """
    _ensure_key_log()
    try:
        with sqlite3.connect(KEY_LOG_DB) as con:
            rows = con.execute("""
                SELECT instrument_key FROM key_log
                WHERE user_id = ?
                  AND symbol  = ?
                  AND opt_type = ?
                ORDER BY saved_at DESC
                LIMIT 5
            """, (user_id, symbol, opt_type)).fetchall()
        keys = [r[0] for r in rows]
        if exclude_key:
            keys = [k for k in keys if k != exclude_key]
        return keys
    except Exception as e:
        print(f"⚠️  get_previous_keys error: {e}")
        return []
