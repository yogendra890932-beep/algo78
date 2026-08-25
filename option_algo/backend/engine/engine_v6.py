# backend/engine/engine_v6.py
# ================================================================
# AlgoBot — Options Scalping Engine v6
#
# EMAs : 9 / 15 / 21
#
# Market Regime (ADX-based, no option chain):
#   TRENDING_UP    → all 6 strategies (CE only)
#   TRENDING_DOWN  → all 6 strategies (PE only)
#   RANGING        → Breakout + VCGB only
#   VOLATILE       → Pullback only (qty ÷ 2)
#   NO_TRADE       → ATR extremely low
#
# 6 Strategies:
#   1. PULLBACK      EMA9/15/21 touch (1.5% tol) + recovery + trigger break
#   2. TREND_FOLLOW  EMA stack + slope + momentum
#   3. BREAKOUT      10-bar structure break
#   4. VWAP_BOUNCE   Price returns to VWAP (0.4% band)
#   5. EMA_CROSS     EMA9 crosses EMA15 with volume
#   6. VCGB          BB squeeze inside Keltner → gamma burst
#
# Order rules:
#   SL-L only (SL-M banned for NSE/BSE options)
#     order_type="SL", limit = trigger × 0.995 for SELL
#   No target order at broker — cancel SL + market exit near target
#
# Candle freshness:
#   65s candle_loop  — warm prev+today API poll
#   Every new UL 1m bar (streamer) — fresh API fetch + direction update
#   Every new OPT 1m bar — live tick append to opt_df
#
# Direction: instant flip on EMA9 vs EMA15 cross (no debounce)
# ================================================================

import threading
import time
from datetime import datetime
from typing import Optional, Callable
import pandas as pd
import upstox_client
from upstox_client.rest import ApiException

from backend.engine.instruments import (
    load_instruments, resolve_history_key,
    detect_strike_step, get_itm_instrument,
    save_key_to_log, get_previous_keys,
)
from backend.services.paper_trading import get_paper_book
from backend.services import telegram_alerts as tg

# Analysis engine imports — used for incremental structure analysis
# These MUST NOT be modified; engine.py is an orchestration layer only.
from backend.engine import market_structure
from backend.engine import underlying_market_structure

# Execution Layer — routes signals to Paper / Semi Auto / Auto executors.
# Strategies NEVER know which mode is active; they only generate signals.
from backend.services.execution_layer import (
    TradeSignal,
    ExecutionStatus,
    ExecutionRouter,
    execution_router as _global_execution_router,
)

# ================================================================
# NSE/BSE OPTIONS LOT SIZES
# ================================================================
# Standard lot sizes as of the last SEBI circular. Upstox enforces
# these — orders not a multiple of the lot size are rejected.
# These are periodically revised by exchanges (quarterly review).
#
# The engine uses these to translate user-configured "number of lots"
# into the actual order quantity: qty = num_lots × lot_size(symbol).
# A user setting order_qty=1 always means "1 lot", regardless of symbol.
#
# To override (e.g. after an exchange revision), set custom_lot_sizes
# in the bot config: {"NIFTY": 75, "BANKNIFTY": 30}

NSE_LOT_SIZES: dict[str, int] = {
    # Nifty indices
    "NIFTY":        75,
    "BANKNIFTY":    15,
    "FINNIFTY":     40,
    "MIDCPNIFTY":   75,
    "NIFTYNXT50":   25,
    # BSE indices
    "SENSEX":       10,
    "BANKEX":       15,
    # Individual stock futures/options (common ones — extend as needed)
    "RELIANCE":     250,
    "TCS":          150,
    "INFY":         300,
    "HDFCBANK":     550,
    "ICICIBANK":    700,
    "SBIN":         1500,
    "AXISBANK":     625,
    "BAJFINANCE":   125,
    "WIPRO":        1500,
    "TATASTEEL":    5500,
    "TATAMOTORS":   2850,
    "ADANIPORTS":   1250,
    "MARUTI":       100,
    "SUNPHARMA":    350,
    "KOTAKBANK":    400,
}

# Fallback if symbol not in table — warn and use 1 so order
# quantity = exactly what user typed (raw qty, not lots).
_LOT_SIZE_FALLBACK = 1


def get_lot_size(symbol: str, custom: Optional[dict] = None) -> int:
    """
    Returns the lot size for a given underlying symbol.

    Resolution order:
      1. custom_lot_sizes from bot config (user override)
      2. NSE_LOT_SIZES table (built-in defaults)
      3. _LOT_SIZE_FALLBACK (1) with a warning log

    Symbol matching is case-insensitive and strips common suffixes
    like "25JAN" expiry tags so "NIFTY25JAN" resolves to "NIFTY".
    """
    import re as _re
    # Strip expiry suffix: NIFTY25JAN → NIFTY, BANKNIFTY24DEC → BANKNIFTY
    clean = _re.sub(r'\d{2}[A-Z]{3}.*$', '', symbol.upper().strip())

    if custom:
        hit = custom.get(clean) or custom.get(symbol.upper())
        if hit:
            return int(hit)

    size = NSE_LOT_SIZES.get(clean) or NSE_LOT_SIZES.get(symbol.upper())
    if size:
        return size

    print(f"[lot_size] ⚠️ Unknown symbol '{symbol}' (cleaned: '{clean}') — "
          f"defaulting lot size to {_LOT_SIZE_FALLBACK}. "
          f"Add it to NSE_LOT_SIZES or set custom_lot_sizes in bot config.")
    return _LOT_SIZE_FALLBACK

CANDLE_REFRESH_INTERVAL = 65

ADX_TREND_MIN = 20
ADX_RANGE_MAX = 18
ATR_PCT_MIN   = 0.002   # 0.2% — Nifty options have lower ATR%
VWAP_BAND_PCT = 0.004


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _ist_min() -> str:
    """Current IST wall-clock minute key (matches broker candle boundaries)."""
    from backend.engine.history_loader import now_ist
    return now_ist().replace(tzinfo=None).strftime("%Y-%m-%d %H:%M")


def _is_market_hours() -> bool:
    from backend.engine.history_loader import is_market_open
    return is_market_open()


# ================================================================
# MARKET REGIME ANALYZER
# ================================================================

class MarketRegimeAnalyzer:
    """
    Classifies market regime using ADX(14), ATR(14), EMA(9/15/21) alignment
    and candle range spread. No option chain required.

    Priority order (trending always wins over ATR check):
      1. TRENDING_UP/DOWN — ADX >= 20 + EMA aligned
      2. VOLATILE         — ATR spike > 2.5x + ADX < 25
      3. RANGING          — ADX < 18 + tight range
      4. NO_TRADE         — ATR extremely low
    """

    def __init__(self):
        self.regime:  str   = "NO_TRADE"
        self.adx:     float = 0.0
        self.atr_pct: float = 0.0
        self._lock          = threading.Lock()

    def analyse(self, df: pd.DataFrame) -> str:
        if df.empty or len(df) < 20:
            return "NO_TRADE"
        close = df["close"]
        high  = df["high"]
        low   = df["low"]
        # True Range ATR
        tr = pd.concat([
            high - low,
            (high - close.shift()).abs(),
            (low  - close.shift()).abs(),
        ], axis=1).max(axis=1)
        atr14   = float(tr.rolling(14).mean().iloc[-1])
        ltp     = float(close.iloc[-1])
        atr_pct = atr14 / ltp if ltp > 0 else 0
        adx     = self._calc_adx(df, 14)
        ef = float(close.ewm(span=9,  adjust=False).mean().iloc[-1])
        em = float(close.ewm(span=15, adjust=False).mean().iloc[-1])
        el = float(close.ewm(span=21, adjust=False).mean().iloc[-1])
        recent_range = (high.iloc[-8:]  - low.iloc[-8:]).mean()
        avg_range    = (high.iloc[-20:] - low.iloc[-20:]).mean()
        range_ratio  = recent_range / avg_range if avg_range > 0 else 1.0
        # Trending takes priority over ATR
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
            self.regime  = regime
            self.adx     = round(adx, 2)
            self.atr_pct = round(atr_pct * 100, 3)
        return regime

    def get_allowed_strategies(self, opt_type: str) -> list:
        """
        Returns allowed strategy list for the CURRENT leg (opt_type).

        Direction (self.direction / opt_type) is already aligned with the
        underlying EMA9/15/21 cross in _check_direction. Regime is a SEPARATE
        ADX/ATR-based classification of the SAME underlying data.

        BUG FIX: previously TRENDING_UP returned [] for PE and TRENDING_DOWN
        returned [] for CE. But ADX can sit just below 20 while EMA9<EMA15<EMA21
        (a soft downtrend) — regime falls through to RANGING/NO_TRADE while
        direction is correctly BEAR/PE. RANGING only allowed breakout+vcgb,
        NO_TRADE allowed nothing — so PULLBACK/TREND/VWAP/EMA_CROSS never fired
        for PE in these conditions, even though direction was correctly PE.

        Fix: all regimes now return a full or partial strategy set regardless
        of opt_type — the opt_type/direction match is already guaranteed by
        _check_direction, so no extra mismatch filter is needed here.
        """
        with self._lock:
            regime = self.regime

        full = ["trend_follow", "pullback", "breakout",
                "vwap_bounce", "ema_cross", "vcgb"]

        if regime == "NO_TRADE":
            # Even in NO_TRADE, allow pullback — it has its own strict filters
            # (EMA touch + recovery candle) and shouldn't be fully blocked.
            return ["pullback"]
        if regime == "RANGING":
            return ["pullback", "breakout", "vcgb"]
        if regime == "VOLATILE":
            return ["pullback"]
        # TRENDING_UP / TRENDING_DOWN — full strategy set for whichever leg
        # direction has selected (opt_type already matches direction)
        return full

    def summary(self) -> str:
        with self._lock:
            return f"Regime={self.regime} ADX={self.adx} ATR%={self.atr_pct}"

    @staticmethod
    def _calc_adx(df: pd.DataFrame, period: int = 14) -> float:
        try:
            if len(df) < period + 1:
                return 0.0
            high  = df["high"]
            low   = df["low"]
            close = df["close"]
            tr = pd.concat([
                high - low,
                (high - close.shift()).abs(),
                (low  - close.shift()).abs(),
            ], axis=1).max(axis=1)
            dm_p = ((high - high.shift()) > (low.shift() - low)).astype(float) * \
                   (high - high.shift()).clip(lower=0)
            dm_m = ((low.shift() - low) > (high - high.shift())).astype(float) * \
                   (low.shift() - low).clip(lower=0)
            atr14 = tr.ewm(alpha=1/period, adjust=False).mean()
            di_p  = 100 * dm_p.ewm(alpha=1/period, adjust=False).mean() / atr14
            di_m  = 100 * dm_m.ewm(alpha=1/period, adjust=False).mean() / atr14
            dx    = 100 * (di_p - di_m).abs() / (di_p + di_m).replace(0, 1)
            return float(dx.ewm(alpha=1/period, adjust=False).mean().iloc[-1])
        except Exception:
            return 0.0


# ================================================================
# VWAP
# ================================================================

def _calc_vwap(df: pd.DataFrame) -> pd.Series:
    try:
        df = df.copy()
        df["typical"] = (df["high"] + df["low"] + df["close"]) / 3
        df["tpv"]     = df["typical"] * df["volume"]
        df["date"]    = pd.to_datetime(df["time"]).dt.date
        df["cum_tpv"] = df.groupby("date")["tpv"].cumsum()
        df["cum_vol"] = df.groupby("date")["volume"].cumsum()
        return df["cum_tpv"] / df["cum_vol"].replace(0, 1)
    except Exception:
        return pd.Series(dtype=float)


# ================================================================
# RISK TRACKER  (shared across all symbols for one user/account)
# ================================================================

class RiskTracker:
    """
    Account-wide daily risk counters, shared by ALL SymbolEngine
    instances belonging to one user (one per traded symbol).

    BUG FIX: previously trades_today/loss_today lived on each
    SymbolEngine separately — a user trading N symbols effectively
    got N independent risk budgets (e.g. max_loss_per_day=5000 with
    3 symbols could lose up to 15000 before fully halting). This
    class is now instantiated ONCE per TradingEngine (i.e. once per
    user) and shared by reference across every symbol's engine, so
    "Max Loss Per Day" and "Max Trades Per Day" are genuinely account-wide.

    BUG FIX: loss_today now tracks NET daily P&L (wins offset
    losses), matching what the "Max Loss Per Day (₹)" label in
    Settings implies to users. The old behavior summed only losing
    trades — a user up ₹3000 net could still trip a ₹2000 "loss"
    limit from one bad trade, halting an otherwise profitable day.

    BUG FIX: the Telegram "risk limit hit" alert previously fired on
    every single bar-close once the limit was breached (every minute,
    for the rest of the day) — now fires once per breach per day.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self.trades_today: int = 0
        self.net_pnl_today: float = 0.0
        self._last_date: Optional[str] = None
        self._alerted_loss_today  = False
        self._alerted_trades_today = False

    def _maybe_reset(self):
        # Risk counters reset on the IST trading day, not the server-local day.
        from backend.engine.history_loader import today_ist
        today = today_ist().isoformat()
        if self._last_date != today:
            self._last_date = today
            self.trades_today = 0
            self.net_pnl_today = 0.0
            self._alerted_loss_today  = False
            self._alerted_trades_today = False

    def record_entry(self):
        with self._lock:
            self._maybe_reset()
            self.trades_today += 1

    def record_pnl(self, pnl: float):
        with self._lock:
            self._maybe_reset()
            self.net_pnl_today += pnl

    def check(self, max_trades: int, max_loss: float) -> tuple:
        """
        Returns (ok: bool, reason: Optional[str]).
        reason is set only the FIRST time a given limit is breached
        today — caller uses this to send exactly one alert per limit
        per day instead of spamming on every bar close.
        """
        with self._lock:
            self._maybe_reset()

            if self.trades_today >= max_trades:
                first_time = not self._alerted_trades_today
                self._alerted_trades_today = True
                return False, ("max_trades" if first_time else None)

            # Net P&L floor — a negative net_pnl_today beyond max_loss
            # means the account is down more than the configured cap.
            if self.net_pnl_today <= -abs(max_loss):
                first_time = not self._alerted_loss_today
                self._alerted_loss_today = True
                return False, ("max_loss" if first_time else None)

            return True, None

    def snapshot(self) -> dict:
        with self._lock:
            self._maybe_reset()
            return {"trades_today": self.trades_today,
                    "net_pnl_today": round(self.net_pnl_today, 2)}


# ================================================================
# SYMBOL ENGINE
# ================================================================

class SymbolEngine:

    def __init__(self, user_id: int, config: dict, access_token: str,
                 stop_event: threading.Event, on_trade: Callable,
                 symbol: str, underlying_token: str,
                 risk_tracker: Optional["RiskTracker"] = None,
                 symbol_lots: Optional[int] = None):

        self.user_id          = user_id
        self.cfg              = config
        self.access_token     = access_token
        self._stop            = stop_event
        self.on_trade         = on_trade
        self.symbol           = symbol
        self.underlying_token = underlying_token

        # Per-symbol lot count — independent for every symbol.
        # Main symbol uses cfg.order_qty, additional symbols use their own config.
        self.symbol_lots = symbol_lots if symbol_lots is not None else max(1, int(config.get("order_qty", 1)))

        self.paper_mode = config.get("paper_mode", True)
        self._paper     = get_paper_book(user_id) if self.paper_mode else None

        self._tg_token = config.get("telegram_bot_token") or ""
        self._tg_chat  = config.get("telegram_chat_id")   or ""

        # candle stores
        self.ul_df     = pd.DataFrame()
        self.opt_df    = pd.DataFrame()
        self.ul_df_5m  = pd.DataFrame()
        self.opt_df_5m = pd.DataFrame()
        self._df_lock  = threading.Lock()

        # live tick bars
        self._cur_min:       Optional[str] = None
        self._cur_candle:    dict          = {}
        self._ul_cur_min:    Optional[str] = None
        self._ul_cur_candle: dict          = {}

        # API refresh throttle (once per minute)
        self._last_api_refresh_min: Optional[str] = None

        # instrument state
        self.direction:      Optional[str]   = None
        self.instrument_key: Optional[str]   = None
        self.trading_symbol: Optional[str]   = None
        self.opt_type:       Optional[str]   = None
        self.strike:         Optional[float] = None
        self.strike_step:    int             = 50
        self.underlying_ltp: Optional[float] = None
        self._expiry_str:    str             = ""

        # position state
        self.position:    Optional[dict]  = None
        self.trailing_sl: Optional[float] = None
        self.sl_order_id: Optional[str]   = None
        self._sl_mod_ts:  float           = 0.0

        # risk counters — shared account-wide across all symbols for
        # this user via RiskTracker (see class above _SymbolEngine).
        # Falls back to a private RiskTracker if none is passed in
        # (e.g. SymbolEngine used standalone / in tests) so trading
        # hours / risk checks still work, just scoped to this symbol.
        self._risk: RiskTracker = risk_tracker or RiskTracker()

        # re-entry memory
        self._last_exit_ts:       float           = 0.0
        self._last_exit_side:     Optional[str]   = None
        self._last_exit_reason:   Optional[str]   = None
        self._last_entry_price:   Optional[float] = None
        self._last_sl_price:      Optional[float] = None
        self._last_exit_strategy: Optional[str]   = None

        self._paused:  bool = False
        self._dir_lock = threading.Lock()

        self.ul_history_key = resolve_history_key(symbol, underlying_token)

        self._streamer:       object        = None
        self._opt_subscribed: Optional[str] = None

        self.ITM_RESELECT_EXTRA_STEPS = 2
        self._regime           = MarketRegimeAnalyzer()
        self._last_regime_log: float = 0.0

        # Option Chain monitor (optional — started after first instrument selected)
        self._oc: Optional[object] = None   # OptionChainAnalyzer instance

        # Redis position-snapshot push throttling + multi-symbol aggregation.
        # _snapshot_aggregator is set by TradingEngine for multi-symbol bots
        # so all symbols' snapshots are combined into one Redis key.
        self._last_snapshot_push: float = 0.0
        self._snapshot_aggregator = None   # Optional[Callable[[str, dict], None]]
        self.SNAPSHOT_PUSH_INTERVAL_SEC = 2.0

        # ── Analysis Engine Instances (stateful, created once per symbol) ────
        # Premium 1-minute market structure engine.
        self.market_structure_engine: Optional["market_structure.MarketStructureEngine"] = None
        # Underlying 5-minute market structure engine.
        self.underlying_structure_engine: Optional[
            "underlying_market_structure.UnderlyingMarketStructureEngine"] = None

        # ── Cached Analysis Results ──────────────────────────────────────────
        # Latest full StructureResult from premium 1m market structure analysis.
        # Set only once per completed 1-minute premium candle.
        self.premium_structure: Optional["market_structure.StructureResult"] = None

        # Latest UnderlyingStructureResult from 5m underlying analysis.
        # Updated every minute (developing) and confirmed on 5m close.
        self.underlying_structure: Optional[
            "underlying_market_structure.UnderlyingStructureResult"] = None

        # Tracking variables to prevent duplicate analysis.
        self._premium_analyzed_min: Optional[str] = None   # Minute-string already analyzed this minute
        self._last_underlying_min: Optional[str] = None     # Last minute we ran underlying analysis
        self._last_5m_boundary: Optional[str] = None        # Last 5m candle boundary detected

    # ================================================================
    # ANALYSIS ENGINE INITIALIZATION
    # ================================================================

    def _init_analysis_engines(self):
        """
        Lazily initialise the premium and underlying market structure
        engines.  Created once per SymbolEngine instance and reused for
        the lifetime of the symbol -- never recreated.

        The engines process candles incrementally (one at a time) so
        they maintain internal state across calls.  Do NOT call the
        top-level analyze(df) function which creates a fresh engine
        each time -- that would discard all previous state.
        """
        if self.market_structure_engine is None:
            try:
                self.market_structure_engine = market_structure.MarketStructureEngine()
            except Exception as e:
                print(_now(), f"[{self.symbol}] ⚠️ MS engine init: {e}")

        if self.underlying_structure_engine is None:
            try:
                self.underlying_structure_engine = (
                    underlying_market_structure.UnderlyingMarketStructureEngine()
                )
            except Exception as e:
                print(_now(), f"[{self.symbol}] ⚠️ UMS engine init: {e}")

    # ================================================================
    # COMMAND METHODS  (Telegram / UI)
    # ================================================================

    def _modify_sl_from_telegram(self, new_sl: float):
        """SL-L only — SL-M banned for NSE/BSE options."""
        pos = self.position
        if not pos:
            raise ValueError("No open position")
        limit_price = round(new_sl * 0.995, 2)
        print(_now(), f"[{self.symbol}] Telegram request: Modify SL trigger={new_sl} limit={limit_price} for order_id={pos['sl_order_id']}")
        if self.paper_mode and self._paper:
            self._paper.modify_sl_order(pos["sl_order_id"], new_sl)
        else:
            cfg = upstox_client.Configuration()
            cfg.access_token = self.access_token
            api = upstox_client.OrderApiV3(upstox_client.ApiClient(cfg))
            body = upstox_client.ModifyOrderRequest(
                order_id=pos["sl_order_id"],
                price=limit_price,
                trigger_price=new_sl,
                order_type="SL",
                quantity=pos["qty"],
                validity="DAY"
            )
            print(_now(), f"[{self.symbol}] Live Modify SL Request Payload: {body.to_dict() if hasattr(body, 'to_dict') else str(body)}")
            try:
                resp = api.modify_order(body=body)
                print(_now(), f"[{self.symbol}] Live Modify SL Response: {resp.to_dict() if hasattr(resp, 'to_dict') else str(resp)}")
            except Exception as e:
                print(_now(), f"[{self.symbol}] ❌ Live Modify SL Failed: {e}")
                raise e
        pos["sl_trigger"] = new_sl
        self.position     = pos
        self.trailing_sl  = new_sl
        print(_now(), f"[{self.symbol}] SL → trigger={new_sl} limit={limit_price}")

    def _modify_target_from_telegram(self, new_target: float):
        pos = self.position
        if not pos:
            raise ValueError("No open position")
        if new_target <= pos["entry_price"]:
            raise ValueError(f"Target must be above entry ₹{pos['entry_price']}")
        near_pct           = self.cfg.get("target_near_pct", 0.003)
        pos["target"]      = new_target
        pos["near_target"] = round(new_target * (1 - near_pct), 2)
        self.position      = pos
        print(_now(), f"[{self.symbol}] Target → {new_target} Near → {pos['near_target']}")

    def _squareoff_from_telegram(self):
        pos = self.position
        if not pos:
            raise ValueError("No open position")
        sl_id = pos.get("sl_order_id")
        if sl_id:
            if self.paper_mode and self._paper:
                self._paper.cancel_order(sl_id)
            else:
                try:
                    upstox_client.OrderApiV3(self._api_client()).cancel_order(sl_id)
                except Exception as e:
                    print(_now(), f"[{self.symbol}] SL cancel: {e}")
            self._alert_sl_cancel(pos.get("sl_trigger", 0), "MANUAL_SQUAREOFF")
            time.sleep(0.15)
        ltp = self._cur_candle.get("close") or pos["entry_price"]
        exit_id = self._place_order("SELL", pos["qty"])
        if not exit_id:
            self._alert_order_failed(
                f"⚠️ Manual squareoff SELL order REJECTED for {pos['qty']} qty — "
                f"position may still be OPEN. Check broker terminal.")
            return
        self._record_exit(ltp, "MANUAL_SQUAREOFF",
                          round((ltp - pos["entry_price"]) * pos["qty"], 2))

    # ================================================================
    # LIFECYCLE
    # ================================================================

    def run(self):
        print(_now(), f"[{self.symbol} user {self.user_id}] "
              f"Starting [{'PAPER' if self.paper_mode else 'LIVE'}]")
        try:
            load_instruments(force=False)
        except Exception as e:
            print(_now(), f"[{self.symbol}] Instruments: {e}")
        self.strike_step = detect_strike_step(self.symbol)
        if _is_market_hours():
            self._preload_ul_candles()
        threading.Thread(
            target=self._candle_loop, daemon=True,
            name=f"candle-{self.user_id}-{self.symbol}").start()
        if self._tg_token and self._tg_chat:
            try:
                from backend.services.telegram_bot import start_polling
                start_polling(self.user_id, self._tg_token, self._tg_chat, self._stop)
            except Exception as e:
                print(_now(), f"[{self.symbol}] TG: {e}")
            tg.alert_bot_started(self._tg_token, self._tg_chat, self.symbol,
                                  self.cfg.get("strategy", "all"),
                                  "paper" if self.paper_mode else "live")
        self._start_streamer()
        while not self._stop.is_set():
            time.sleep(1)
        print(_now(), f"[{self.symbol} user {self.user_id}] Stopping")
        if self._streamer:
            try: self._streamer.disconnect()
            except: pass
        if self._oc:
            try:
                self._oc.stop()
            except Exception as e:
                print(_now(), f"[{self.symbol}] OC stop error: {e}")
        if self._tg_token and self._tg_chat:
            tg.alert_bot_stopped(self._tg_token, self._tg_chat, self.symbol)
            if self.cfg.get("telegram_on_summary", True):
                self._send_daily_summary()

    # ================================================================
    # CANDLE LOOP  (65s background poll)
    # ================================================================

    def _candle_loop(self):
        while not self._stop.is_set():
            try:
                if _is_market_hours():
                    self._refresh_candles()
                self._check_direction()
            except Exception as e:
                print(_now(), f"[{self.symbol}] candle_loop: {e}")
            time.sleep(CANDLE_REFRESH_INTERVAL)

    def _refresh_candles(self):
        """Pull warm (prev+today) 1m+5m candles from API."""
        ul = self._fetch_warm(self.ul_history_key)
        if not ul.empty:
            with self._df_lock:
                self.ul_df = ul
        self._check_itm_depth()
        if self.instrument_key:
            opt = self._fetch_warm_opt()
            if not opt.empty:
                with self._df_lock:
                    self.opt_df = opt
        self._refresh_5m_candles()

    # ================================================================
    # CANDLE FETCHERS
    # ================================================================

    def _parse_candle_resp(self, resp) -> pd.DataFrame:
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

    def _fetch_candles(self, key: str, interval: str = "1") -> pd.DataFrame:
        try:
            cfg = upstox_client.Configuration()
            cfg.access_token = self.access_token
            api  = upstox_client.HistoryV3Api(upstox_client.ApiClient(cfg))
            resp = api.get_intra_day_candle_data(key, "minutes", interval)
            return self._parse_candle_resp(resp)
        except ApiException as e:
            print(_now(), f"[{self.symbol}] intra {interval}m: "
                  f"{getattr(e,'body',str(e))[:60]}")
            return pd.DataFrame()
        except Exception as e:
            print(_now(), f"[{self.symbol}] intra {interval}m: {e}")
            return pd.DataFrame()

    def _fetch_prev_day(self, key: str, interval: str = "1") -> pd.DataFrame:
        try:
            from backend.engine.history_loader import fetch_historical_candles, _last_trading_day
            prev = _last_trading_day()
            if interval == "1":
                return fetch_historical_candles(key, prev, prev, self.access_token)
            cfg = upstox_client.Configuration()
            cfg.access_token = self.access_token
            api  = upstox_client.HistoryV3Api(upstox_client.ApiClient(cfg))
            resp = api.get_historical_candle_data1(
                key, "minutes", interval,
                prev.strftime("%Y-%m-%d"), prev.strftime("%Y-%m-%d"))
            return self._parse_candle_resp(resp)
        except Exception as e:
            print(_now(), f"[{self.symbol}] prev {interval}m: {e}")
            return pd.DataFrame()

    def _merge_warm(self, prev: pd.DataFrame, today: pd.DataFrame) -> pd.DataFrame:
        frames = [f for f in [prev, today] if not f.empty]
        if not frames:
            return pd.DataFrame()
        df = pd.concat(frames, ignore_index=True)
        df["time"] = pd.to_datetime(df["time"])
        return df.sort_values("time").drop_duplicates(subset=["time"]).reset_index(drop=True)

    def _fetch_warm(self, key: str) -> pd.DataFrame:
        merged = self._merge_warm(
            self._fetch_prev_day(key, "1"),
            self._fetch_candles(key, "1"))
        if not merged.empty:
            print(_now(), f"[{self.symbol}] 1m {key[:18]}: {len(merged)} bars")
        return merged

    def _fetch_warm_opt(self) -> pd.DataFrame:
        if not self.instrument_key:
            return pd.DataFrame()
        try:
            save_key_to_log(self.user_id, self.symbol, self.instrument_key,
                            self.opt_type or "CE", self.strike or 0, self._expiry_str)
        except Exception:
            pass
        current    = self._fetch_warm(self.instrument_key)
        today_bars = len(self._fetch_candles(self.instrument_key, "1"))
        if today_bars >= 30:
            return current
        prev_keys = get_previous_keys(self.user_id, self.symbol,
                                       self.opt_type or "CE",
                                       exclude_key=self.instrument_key)
        if not prev_keys:
            return current
        try:
            from backend.engine.history_loader import (
                _load_candles_from_cache, _last_trading_day, fetch_historical_candles)
            from datetime import timedelta
            prev_day  = _last_trading_day()
            from_date = prev_day - timedelta(days=3)
            old_df    = _load_candles_from_cache(prev_keys[0], from_date, prev_day)
            if old_df.empty:
                old_df = fetch_historical_candles(
                    prev_keys[0], from_date, prev_day, self.access_token)
            if not old_df.empty:
                merged = self._merge_warm(old_df, current)
                print(_now(), f"[{self.symbol}] Rollover: {len(merged)} bars")
                return merged
        except Exception as e:
            print(_now(), f"[{self.symbol}] rollover: {e}")
        return current

    def _refresh_5m_candles(self):
        ul5 = self._merge_warm(self._fetch_prev_day(self.ul_history_key, "5"),
                               self._fetch_candles(self.ul_history_key, "5"))
        if not ul5.empty:
            with self._df_lock:
                self.ul_df_5m = ul5
        if self.instrument_key:
            opt5 = self._merge_warm(self._fetch_prev_day(self.instrument_key, "5"),
                                    self._fetch_candles(self.instrument_key, "5"))
            if not opt5.empty:
                with self._df_lock:
                    self.opt_df_5m = opt5
                print(_now(), f"[{self.symbol}] 5m OPT: {len(opt5)} bars")

    def _preload_ul_candles(self):
        print(_now(), f"[{self.symbol}] Pre-loading warm candles...")
        df = self._fetch_warm(self.ul_history_key)
        if df.empty:
            df = self._fetch_candles(self.ul_history_key, "1")
        if not df.empty:
            with self._df_lock:
                self.ul_df = df
            self.underlying_ltp = float(df["close"].iloc[-1])
            print(_now(), f"[{self.symbol}] Warm: {len(df)} bars | "
                  f"LTP {self.underlying_ltp:.2f}")
            self._check_direction()

    def _load_new_instrument_candles(self):
        """Load 1m+5m for newly selected instrument (background thread)."""
        try:
            opt = self._fetch_warm_opt()
            if not opt.empty:
                with self._df_lock:
                    self.opt_df = opt
            self._refresh_5m_candles()
            with self._df_lock:
                b1, b5 = len(self.opt_df), len(self.opt_df_5m)
            print(_now(), f"[{self.symbol}] Candles: {b1} 1m | {b5} 5m")
        except Exception as e:
            print(_now(), f"[{self.symbol}] load_candles: {e}")

    # ================================================================
    # DIRECTION + REGIME
    # ================================================================

    def _check_direction(self):
        """
        Instant direction flip on EMA9 vs EMA15 cross.
        No debounce — responds immediately to every bar.
        """
        with self._df_lock:
            df = self.ul_df.copy()
        if df.empty or len(df) < 15:
            return

        self._regime.analyse(df)

        if time.time() - self._last_regime_log > 300:
            print(_now(), f"[{self.symbol}] {self._regime.summary()}")
            self._last_regime_log = time.time()

        ef      = float(df["close"].ewm(span=9,  adjust=False).mean().iloc[-1])
        es      = float(df["close"].ewm(span=15, adjust=False).mean().iloc[-1])
        new_dir = "BULL" if ef > es else "BEAR"

        if new_dir == self.direction:
            return

        if not self._dir_lock.acquire(blocking=False):
            return
        try:
            if new_dir == self.direction:
                return
            old_dir        = self.direction
            self.direction = new_dir
            print(_now(), f"[{self.symbol}] Direction: {old_dir} → {new_dir}")
            if self.underlying_ltp is None:
                return
            if self.position:
                self._emergency_exit()
            opt_type = "CE" if new_dir == "BULL" else "PE"
            try:
                info = get_itm_instrument(opt_type, self.underlying_ltp, self.symbol,
                                          self.cfg.get("itm_depth", 1), self.strike_step)
            except Exception as e:
                print(_now(), f"[{self.symbol}] ITM: {e}"); return
            self.instrument_key  = info["instrument_key"]
            self.trading_symbol  = info["trading_symbol"]
            self.opt_type        = opt_type
            self.strike          = info["strike"]
            self._expiry_str     = info.get("expiry_str", "")
            with self._df_lock:
                self.opt_df    = pd.DataFrame()
                self.opt_df_5m = pd.DataFrame()
            self._resubscribe(info["instrument_key"])
            try:
                save_key_to_log(self.user_id, self.symbol, info["instrument_key"],
                                opt_type, info["strike"], info.get("expiry_str", ""))
            except Exception:
                pass

            # Initialise / update Option Chain monitor
            self._init_oc(info.get("expiry_str", ""))

            threading.Thread(
                target=self._load_new_instrument_candles, daemon=True,
                name=f"init-{self.symbol}-{opt_type}").start()
            print(_now(), f"[{self.symbol}] Active: {info['trading_symbol']} "
                  f"({opt_type} {info['strike']})")
            if self._tg_token and self._tg_chat:
                tg.alert_direction_change(self._tg_token, self._tg_chat,
                                          old_dir, new_dir, opt_type,
                                          info["strike"], info["trading_symbol"])
        finally:
            self._dir_lock.release()

    def _init_oc(self, expiry: str):
        """
        Initialise or restart OptionChainAnalyzer when instrument changes.
        Runs in background — does not block direction change.
        """
        if not expiry:
            return
        try:
            from backend.engine.option_chain import OptionChainAnalyzer
            # Stop old monitor if expiry changed
            if self._oc is not None:
                old_expiry = getattr(self._oc, "expiry", "")
                if old_expiry == expiry:
                    return   # same expiry — keep existing monitor running
                try:
                    self._oc.stop_event.set()
                except Exception:
                    pass
            oc_stop = threading.Event()
            self._oc = OptionChainAnalyzer(
                symbol         = self.symbol,
                underlying_key = self.ul_history_key,
                expiry         = expiry,
                access_token   = self.access_token,
                stop_event     = oc_stop,
                on_update      = self._push_oc_snapshot,
            )
            self._oc.start()
            print(_now(), f"[{self.symbol}] OC monitor started (expiry={expiry})")
        except Exception as e:
            print(_now(), f"[{self.symbol}] OC init: {e}")

    def _push_oc_snapshot(self, analysis: dict, chain_df):
        """
        on_update callback from OptionChainAnalyzer — pushes the latest
        OC analysis + a compact chain table to Redis (state_store) so
        the web process's /api/oc/analysis endpoint can serve it without
        touching this engine object directly (worker/web split).
        """
        try:
            from backend.services.state_store import set_oc_snapshot_sync
            chain_records = None
            if chain_df is not None and not chain_df.empty:
                chain_records = chain_df[["strike","ce_oi","ce_volume","ce_ltp",
                                          "pe_oi","pe_volume","pe_ltp"]].to_dict("records")
            set_oc_snapshot_sync(self.user_id, analysis, chain_records)
        except Exception as e:
            print(_now(), f"[{self.symbol}] OC snapshot push: {e}")

    def _push_position_snapshot(self):
        """
        Pushes a snapshot of THIS symbol's position/state to Redis
        (state_store.set_positions_sync) so /api/position/ can serve it
        from the web process. Called periodically (throttled) from tick
        processing — see _process_opt_tick / _process_ul_tick.

        NOTE: each SymbolEngine only knows about its own symbol. For
        multi-symbol bots, TradingEngine aggregates all engines' single-
        symbol snapshots into one list under bot:positions:{user_id}
        (see TradingEngine._push_all_positions).
        """
        try:
            from backend.services.state_store import set_positions_sync
            pos    = self.position
            ltp    = self._cur_candle.get("close")
            pnl    = None
            if pos and ltp:
                pnl = round((float(ltp) - pos["entry_price"]) * pos["qty"], 2)
            risk_snap = self._risk.snapshot()
            snapshot = {
                "symbol":         self.symbol,
                "mode":           "paper" if self.paper_mode else "live",
                "paused":         self._paused,
                "underlying_ltp": self.underlying_ltp,
                "opt_ltp":        ltp,
                "direction":      self.direction,
                "opt_type":       self.opt_type,
                "strike":         self.strike,
                "trading_symbol": self.trading_symbol,
                "position":       pos,
                "unrealized_pnl": pnl,
                # Account-wide (shared across all symbols), not per-symbol —
                # see RiskTracker. trades_today/net_pnl_today will read the
                # SAME values regardless of which symbol's snapshot you look at.
                "trades_today":   risk_snap["trades_today"],
                "net_pnl_today":  risk_snap["net_pnl_today"],
            }
            # Single-symbol fast path — multi-symbol aggregation happens
            # in TradingEngine via _all_snapshots if registered.
            agg = getattr(self, "_snapshot_aggregator", None)
            if agg:
                agg(self.symbol, snapshot)
            else:
                set_positions_sync(self.user_id, [snapshot])
        except Exception as e:
            print(_now(), f"[{self.symbol}] position snapshot push: {e}")

    # ================================================================
    # ITM DEPTH CHECK
    # ================================================================

    def _check_itm_depth(self):
        if self.position: return
        if not self.instrument_key or not self.underlying_ltp: return
        if not self.direction or not self.opt_type or not self.strike: return
        step      = self.strike_step
        itm_depth = self.cfg.get("itm_depth", 1)
        atm       = round(self.underlying_ltp / step) * step
        if self.opt_type == "CE":
            extra_steps = ((atm - itm_depth * step) - self.strike) / step
        else:
            extra_steps = (self.strike - (atm + itm_depth * step)) / step
        if extra_steps < self.ITM_RESELECT_EXTRA_STEPS: return
        print(_now(), f"[{self.symbol}] ⚠️ Strike {self.strike} "
              f"{extra_steps:.0f} steps deep — reselecting")
        try:
            info = get_itm_instrument(self.opt_type, self.underlying_ltp,
                                      self.symbol, itm_depth, step)
        except Exception as e:
            print(_now(), f"[{self.symbol}] reselect: {e}"); return
        if info["instrument_key"] == self.instrument_key: return
        old_strike           = self.strike
        self.instrument_key  = info["instrument_key"]
        self.trading_symbol  = info["trading_symbol"]
        self.strike          = info["strike"]
        self._expiry_str     = info.get("expiry_str", "")
        with self._df_lock:
            self.opt_df    = pd.DataFrame()
            self.opt_df_5m = pd.DataFrame()
        self._resubscribe(info["instrument_key"])
        try:
            save_key_to_log(self.user_id, self.symbol, info["instrument_key"],
                            self.opt_type, info["strike"], self._expiry_str)
        except Exception:
            pass
        threading.Thread(
            target=self._load_new_instrument_candles, daemon=True,
            name=f"reselect-{self.symbol}").start()
        print(_now(), f"[{self.symbol}] ✅ Strike: {old_strike} → {info['strike']}")
        if self._tg_token and self._tg_chat:
            try:
                from backend.services.telegram_alerts import _send_message
                _send_message(self._tg_token, self._tg_chat,
                              f"🔄 Strike [{self.symbol}] "
                              f"{old_strike}→{info['strike']} | {info['trading_symbol']}")
            except Exception:
                pass

    # ================================================================
    # STREAMER
    # ================================================================

    def _start_streamer(self):
        cfg = upstox_client.Configuration()
        cfg.access_token = self.access_token
        streamer = upstox_client.MarketDataStreamerV3(upstox_client.ApiClient(cfg))
        self._streamer = streamer

        def on_open():
            print(_now(), f"[{self.symbol}] Stream open")
            try:
                tokens = [self.underlying_token]
                if self.instrument_key:
                    tokens.append(self.instrument_key)
                    self._opt_subscribed = self.instrument_key
                streamer.subscribe(tokens, "full")
                print(_now(), f"[{self.symbol}] Subscribed: {tokens}")
            except Exception as e:
                print(_now(), f"[{self.symbol}] Subscribe: {e}")

        def on_message(msg):
            try:
                ul = self._extract_ltp(msg, self.underlying_token)
                if ul is not None:
                    self._process_ul_tick(ul)
                if self.instrument_key:
                    opt = self._extract_ltp(msg, self.instrument_key)
                    if opt is not None:
                        ltq = self._extract_ltq(msg, self.instrument_key)
                        self._process_opt_tick(opt, ltq)
            except Exception as e:
                print(_now(), f"[{self.symbol}] msg: {e}")

        def on_error(e):
            print(_now(), f"[{self.symbol}] Stream error: {e}")

        def on_close(code, reason):
            print(_now(), f"[{self.symbol}] Stream closed: {code}")
            if not self._stop.is_set():
                time.sleep(3)
                try: streamer.connect()
                except Exception as e:
                    print(_now(), f"[{self.symbol}] Reconnect: {e}")

        streamer.on("open",    on_open)
        streamer.on("message", on_message)
        streamer.on("error",   on_error)
        streamer.on("close",   on_close)
        streamer.connect()

    def _resubscribe(self, new_key: str):
        if not self._streamer: return
        try:
            if self._opt_subscribed and self._opt_subscribed != new_key:
                self._streamer.unsubscribe([self._opt_subscribed])
            self._streamer.subscribe([new_key], "full")
            self._opt_subscribed = new_key
        except Exception as e:
            print(_now(), f"[{self.symbol}] Resubscribe: {e}")

    @staticmethod
    def _extract_ltp(msg: dict, token: str) -> Optional[float]:
        try:
            if not isinstance(msg, dict): return None
            feed = msg.get("feeds", {}).get(token, {})
            full = feed.get("fullFeed", {})
            if isinstance(full, dict):
                if "marketFF" in full and full["marketFF"].get("ltpc"):
                    return float(full["marketFF"]["ltpc"]["ltp"])
                if "indexFF" in full and full["indexFF"].get("ltpc"):
                    return float(full["indexFF"]["ltpc"]["ltp"])
                if full.get("ltp"):
                    return float(full["ltp"])
        except Exception:
            pass
        return None

    @staticmethod
    def _extract_ltq(msg: dict, token: str) -> float:
        """
        Last Traded Quantity for this tick — Upstox V3 full feed
        marketFF.ltpc.ltq. Used to accumulate per-minute volume for the
        live tick-built candle (since _append_*_closed_bar otherwise
        always writes volume=0, breaking all volume-based filters).
        Returns 0.0 if not present (indices have no ltq).
        """
        try:
            if not isinstance(msg, dict): return 0.0
            feed = msg.get("feeds", {}).get(token, {})
            full = feed.get("fullFeed", {})
            if isinstance(full, dict):
                if "marketFF" in full and full["marketFF"].get("ltpc"):
                    ltq = full["marketFF"]["ltpc"].get("ltq")
                    if ltq is not None:
                        return float(ltq)
        except Exception:
            pass
        return 0.0

    # ================================================================
    # TICK PROCESSING
    # ================================================================

    def _process_ul_tick(self, ltp: float):
        """
        Track UL 1m bar. On new minute:
          - Append closed UL bar to ul_df
          - Trigger API refresh (fresh candles + direction update)
          - Run underlying market structure analysis (developing every
            minute, confirmed on 5-minute candle close)
        Also throttle-pushes the position snapshot so the dashboard
        shows the bot's underlying LTP and direction even before an
        option instrument is selected (before _process_opt_tick fires).
        """
        self.underlying_ltp = ltp
        now_min = _ist_min()
        if self._ul_cur_min != now_min:
            if self._ul_cur_candle.get("open") is not None:
                self._append_ul_closed_bar(self._ul_cur_candle, self._ul_cur_min)

                # ── Underlying analysis: detect 5-minute boundary ──
                # A 5-minute boundary occurs when the minute % 5 == 0
                # e.g. 09:35, 09:40, 09:45, etc.
                closed_min = self._ul_cur_min
                minute_part = int(closed_min.split(":")[1])
                is_5m_boundary = (minute_part % 5 == 0)

                if is_5m_boundary and self._last_5m_boundary != closed_min:
                    self._last_5m_boundary = closed_min
                    # Feed the 5m data to the engine with full analysis
                    # (runs in background thread so tick processing is not blocked)
                    threading.Thread(
                        target=self._run_underlying_analysis,
                        args=(True,), daemon=True,
                        name=f"ul5m-{self.symbol}").start()
                else:
                    # Every minute: run developing analysis (lightweight)
                    self._run_underlying_analysis(is_confirmed=False)

                if self._last_api_refresh_min != self._ul_cur_min:
                    self._last_api_refresh_min = self._ul_cur_min
                    threading.Thread(
                        target=self._refresh_candles_on_new_bar,
                        daemon=True,
                        name=f"api-{self.symbol}").start()
            self._ul_cur_min    = now_min
            self._ul_cur_candle = {"open": ltp, "high": ltp, "low": ltp,
                                   "close": ltp, "volume": 0}
        else:
            self._ul_cur_candle["close"] = ltp
            self._ul_cur_candle["high"]  = max(self._ul_cur_candle["high"], ltp)
            self._ul_cur_candle["low"]   = min(self._ul_cur_candle["low"],  ltp)

        # Push snapshot on UL tick too (throttled) — ensures dashboard
        # shows underlying LTP/direction before option is subscribed.
        import time as _t
        now_ts = _t.time()
        if now_ts - self._last_snapshot_push >= self.SNAPSHOT_PUSH_INTERVAL_SEC:
            self._last_snapshot_push = now_ts
            self._push_position_snapshot()

    def _append_ul_closed_bar(self, candle: dict, minute_str: str):
        """Append closed 1m UL bar to ul_df in-memory."""
        try:
            ts = pd.to_datetime(minute_str, format="%Y-%m-%d %H:%M")
            row = pd.DataFrame([{
                "time":   ts,
                "open":   candle.get("open",  candle["close"]),
                "high":   candle.get("high",  candle["close"]),
                "low":    candle.get("low",   candle["close"]),
                "close":  candle["close"],
                "volume": candle.get("volume", 0),
                "oi":     0,
            }])
            with self._df_lock:
                if self.ul_df.empty: return
                if ts in self.ul_df["time"].values: return
                self.ul_df = pd.concat([self.ul_df, row], ignore_index=True).tail(750)
        except Exception as e:
            print(_now(), f"[{self.symbol}] append_ul: {e}")

    def _refresh_candles_on_new_bar(self):
        """
        Fired every new UL minute bar (background thread).
        Fetches fresh intraday 1m+5m from API for UL and OPT.
        Complements the 65s candle_loop (warm prev+today).
        """
        try:
            if not _is_market_hours():
                return
            # UL 1m
            ul_today = self._fetch_candles(self.ul_history_key, "1")
            if not ul_today.empty:
                with self._df_lock:
                    if not self.ul_df.empty:
                        cutoff = ul_today["time"].min()
                        old    = self.ul_df[self.ul_df["time"] < cutoff]
                        merged = self._merge_warm(old, ul_today)
                        self.ul_df = merged.tail(750) if not merged.empty else ul_today
                    else:
                        self.ul_df = ul_today
            # OPT 1m
            if self.instrument_key:
                opt_today = self._fetch_candles(self.instrument_key, "1")
                if not opt_today.empty:
                    with self._df_lock:
                        if not self.opt_df.empty:
                            cutoff = opt_today["time"].min()
                            old    = self.opt_df[self.opt_df["time"] < cutoff]
                            merged = self._merge_warm(old, opt_today)
                            self.opt_df = merged.tail(750) if not merged.empty else opt_today
                        else:
                            self.opt_df = opt_today
                # OPT 5m
                opt5_today = self._fetch_candles(self.instrument_key, "5")
                if not opt5_today.empty:
                    with self._df_lock:
                        if not self.opt_df_5m.empty:
                            cutoff = opt5_today["time"].min()
                            old    = self.opt_df_5m[self.opt_df_5m["time"] < cutoff]
                            merged = self._merge_warm(old, opt5_today)
                            self.opt_df_5m = merged.tail(500) if not merged.empty else opt5_today
                        else:
                            self.opt_df_5m = opt5_today
            # Instant direction update after fresh data
            self._check_direction()
        except Exception as e:
            print(_now(), f"[{self.symbol}] api_refresh: {e}")

    def _process_opt_tick(self, ltp: float, ltq: float = 0.0):
        """
        Track OPT 1m bar. On new minute:
          - Append closed bar to opt_df (live refresh without API call)
          - Evaluate all strategies
        Volume: accumulate ltq (last-traded-qty) per tick into the
        live candle's "volume" field, so volume-based filters have
        real data even before the next API refresh overwrites the bar.
        """
        now_min = _ist_min()
        if self._cur_min != now_min:
            if self._cur_candle.get("open") is not None:
                self._append_opt_closed_bar(self._cur_candle, self._cur_min)
                self._on_bar_close()
            self._cur_min    = now_min
            self._cur_candle = {"open": ltp, "high": ltp, "low": ltp,
                                "close": ltp, "volume": ltq}
        else:
            self._cur_candle["close"]   = ltp
            self._cur_candle["high"]    = max(self._cur_candle["high"], ltp)
            self._cur_candle["low"]     = min(self._cur_candle["low"],  ltp)
            self._cur_candle["volume"]  = self._cur_candle.get("volume", 0) + ltq
        if self.position:
            self._manage_position(ltp)

        # Throttled push of position/state snapshot to Redis — lets the
        # web process serve /api/position/ without touching this engine
        # object directly (worker/web split).
        now_ts = time.time()
        if now_ts - self._last_snapshot_push >= self.SNAPSHOT_PUSH_INTERVAL_SEC:
            self._last_snapshot_push = now_ts
            self._push_position_snapshot()

    def _append_opt_closed_bar(self, candle: dict, minute_str: str):
        """Append closed 1m OPT bar to opt_df — strategies see it instantly."""
        try:
            ts = pd.to_datetime(minute_str, format="%Y-%m-%d %H:%M")
            row = pd.DataFrame([{
                "time":   ts,
                "open":   candle.get("open",  candle["close"]),
                "high":   candle.get("high",  candle["close"]),
                "low":    candle.get("low",   candle["close"]),
                "close":  candle["close"],
                "volume": candle.get("volume", 0),
                "oi":     0,
            }])
            with self._df_lock:
                if self.opt_df.empty: return
                if ts in self.opt_df["time"].values: return
                self.opt_df = pd.concat([self.opt_df, row], ignore_index=True).tail(750)
        except Exception as e:
            print(_now(), f"[{self.symbol}] append_opt: {e}")

    def _on_bar_close(self):
        if self._paused or not self._risk_ok() or not self._trading_hours_ok():
            return

        # ── Step 1: Premium Market Structure Analysis ─────────────
        # Run exactly once per closed 1-minute premium candle.
        # The result is cached in self.premium_structure and reused by
        # both existing strategies and the new unified strategy.
        self._run_premium_analysis()

        # ── Step 2: Existing strategy evaluation ──────────────────
        # Keep the existing 6 strategies intact — they have been
        # working and are complementary to the new unified strategy.
        # The unified strategy is an ADDITIONAL layer, not a replacement.
        allowed      = self._regime.get_allowed_strategies(self.opt_type or "CE")
        if not allowed:
            return
        cfg_strategy = self.cfg.get("strategy", "all")
        if "trend_follow" in allowed and cfg_strategy in ("trend",    "all", "both"):
            self._eval_trend_follow()
        if "pullback"    in allowed and cfg_strategy in ("pullback",  "all", "both"):
            self._eval_pullback()
        if "breakout"    in allowed and cfg_strategy in ("breakout",  "all", "both"):
            self._eval_breakout()
        if "vwap_bounce" in allowed and cfg_strategy in ("vwap",      "all"):
            self._eval_vwap_bounce()
        if "ema_cross"   in allowed and cfg_strategy in ("ema_cross", "all"):
            self._eval_ema_cross()
        if "vcgb"        in allowed and cfg_strategy in ("vcgb",      "all"):
            self._eval_vcgb()

        # ── Step 3: Unified structure-based strategy ──────────────
        # Only evaluate if no position is open (existing strategies
        # may already have entered).
        if self.position:
            return
        if not self.direction or not self.instrument_key:
            return

        # Map engine direction to unified strategy direction
        if self.direction == "BULL":
            self._eval_unified_strategy("CE")
        elif self.direction == "BEAR":
            self._eval_unified_strategy("PE")

    # ================================================================
    # RISK & HOURS
    # ================================================================

    def _risk_ok(self) -> bool:
        """
        Checks account-wide (all symbols) daily risk limits via the
        shared RiskTracker. Sends exactly ONE alert per limit breach
        per day — across Telegram, the live feed, and voice/push
        (via on_trade) — instead of re-alerting on every bar close.
        """
        max_trades = self.cfg.get("max_trades_per_day", 5)
        max_loss   = self.cfg.get("max_loss_per_day", 5000.0)
        ok, breach = self._risk.check(max_trades, max_loss)

        if breach:   # first time this limit was hit today
            snap = self._risk.snapshot()
            if breach == "max_trades":
                reason = f"Max trades/day reached ({max_trades})"
                value  = snap["trades_today"]
            else:
                reason = f"Max loss/day reached (₹{max_loss})"
                value  = snap["net_pnl_today"]

            if self._tg_token and self._tg_chat:
                tg.alert_risk_limit_hit(self._tg_token, self._tg_chat, reason, value)

            mode = "paper" if self.paper_mode else "live"
            self.on_trade({
                "event": "RISK_LIMIT_HIT", "user_id": self.user_id, "mode": mode,
                "symbol": self.symbol, "reason": reason, "value": value,
            })
            print(_now(), f"[{self.symbol}] 🚫 RISK LIMIT: {reason} "
                  f"(account-wide, all symbols) — no new entries today")

        return ok

    def _trading_hours_ok(self) -> bool:
        # trade_start_time / trade_end_time are IST wall-clock values, so
        # evaluate against current IST time regardless of server timezone.
        from backend.engine.history_loader import now_ist
        now = now_ist().replace(tzinfo=None)
        try:
            s = datetime.strptime(self.cfg.get("trade_start_time", "09:20"), "%H:%M")\
                .replace(year=now.year, month=now.month, day=now.day)
            e = datetime.strptime(self.cfg.get("trade_end_time",   "15:00"), "%H:%M")\
                .replace(year=now.year, month=now.month, day=now.day)
            return s <= now <= e
        except Exception:
            return True

    # ================================================================
    # PREMIUM MARKET STRUCTURE ANALYSIS (1-Minute)
    # ================================================================

    def _run_premium_analysis(self) -> bool:
        """
        Run full premium market structure analysis on the completed
        1-minute candle.  Called exactly once per closed premium bar
        from _on_bar_close().

        Returns True if analysis was freshly computed this call,
        False if already analysed for this minute (dedup guard).
        """
        now_min = self._cur_min or _ist_min()

        # Dedup: never analyse more than once per candle.
        if self._premium_analyzed_min == now_min:
            return False

        self._init_analysis_engines()
        if self.market_structure_engine is None:
            return False

        with self._df_lock:
            df = self.opt_df.copy()

        if df.empty or len(df) < 5:
            return False

        # Build candle dict from the last row of opt_df (just-closed bar).
        last = df.iloc[-1]
        candle = {
            "open":   float(last["open"]),
            "high":   float(last["high"]),
            "low":    float(last["low"]),
            "close":  float(last["close"]),
            "volume": float(last["volume"]) if "volume" in df.columns else 0,
            "time":   last.name if hasattr(last, "name") else last["time"],
        }

        try:
            self.premium_structure = self.market_structure_engine.update(
                candle, full_analysis=True)
            self._premium_analyzed_min = now_min
        except Exception as e:
            print(_now(), f"[{self.symbol}] ⚠️ Premium analysis error: {e}")
            return False

        return True

    # ================================================================
    # UNDERLYING MARKET STRUCTURE ANALYSIS (5-Minute)
    # ================================================================

    def _run_underlying_analysis(self, is_confirmed: bool = False) -> bool:
        """
        Run underlying market structure analysis.

        Called in two modes:
          1. Every minute (is_confirmed=False) — feeds the developing
             5-minute candle to update forming structure.  Gives early
             context but should not be the sole reason for a trade.
          2. On every 5-minute candle close (is_confirmed=True) —
             confirms major structural events (BOS, CHOCH, swing
             highs/lows, patterns, bias).

        Returns True if a fresh analysis result was stored.
        """
        now_min = _ist_min()

        # Even developing updates should only run once per minute.
        if not is_confirmed and self._last_underlying_min == now_min:
            return False

        self._init_analysis_engines()
        if self.underlying_structure_engine is None:
            return False

        with self._df_lock:
            df5 = self.ul_df_5m.copy()

        if df5.empty:
            return False

        # Use the most recent 5-minute candle (forming or closed).
        last = df5.iloc[-1]
        candle = {
            "open":   float(last["open"]),
            "high":   float(last["high"]),
            "low":    float(last["low"]),
            "close":  float(last["close"]),
            "volume": float(last["volume"]) if "volume" in df5.columns else 0,
            "time":   last.name if hasattr(last, "name") else last["time"],
        }

        try:
            # When is_confirmed, run full analysis to confirm structure.
            # When developing, run fast (no S/R/patterns/confidence).
            if is_confirmed:
                self.underlying_structure = self.underlying_structure_engine.update(
                    candle, full_analysis=True)
            else:
                # Fast update — just state machines, no expensive full analysis.
                self.underlying_structure_engine.update(candle, full_analysis=False)

                # Build a developing-structure readout from the engine state
                # without the overhead of a full UnderlyingStructureResult.
                trend = self.underlying_structure_engine._get_trend_metrics()
                phase_result = self.underlying_structure_engine._determine_market_phase()
                confidence = self.underlying_structure_engine._compute_confidence_score(
                    phase_result.phase)

                self.underlying_structure = (
                    self.underlying_structure_engine._build_result(
                        trend, phase_result, confidence))

            self._last_underlying_min = now_min
        except Exception as e:
            print(_now(), f"[{self.symbol}] ⚠️ Underlying analysis error: {e}")
            return False

        return True

    # ================================================================
    # INDICATORS  (EMA 9 / 15 / 21)
    # ================================================================

    def _get_opt_ind(self) -> Optional[dict]:
        with self._df_lock:
            df = self.opt_df.copy()
        if df.empty or len(df) < 23:
            return None
        lc = float(df["close"].iloc[-1])
        if lc <= 0.50:
            return None
        last  = df.iloc[-1]
        ef    = float(df["close"].ewm(span=9,  adjust=False).mean().iloc[-1])
        em    = float(df["close"].ewm(span=15, adjust=False).mean().iloc[-1])
        el    = float(df["close"].ewm(span=21, adjust=False).mean().iloc[-1])
        rng   = float(last["high"]) - float(last["low"])
        body  = abs(float(last["close"]) - float(last["open"]))
        df2        = df.copy()
        df2["TR"]  = df2["high"] - df2["low"]
        atr14      = float(df2["TR"].rolling(14).mean().iloc[-1]) if len(df2)>=14 else 0.0
        ema9_s     = df["close"].ewm(span=9, adjust=False).mean()
        ema9_slope = float(ema9_s.iloc[-1]-ema9_s.iloc[-4]) if len(ema9_s)>=4 else 0.0
        mom3       = float(df["close"].iloc[-1]-df["close"].iloc[-4]) if len(df)>=4 else 0.0
        vwap_s     = _calc_vwap(df)
        vwap       = float(vwap_s.iloc[-1]) if not vwap_s.empty else lc
        return {
            "ema_fast":    ef,     "ema_mid":   em,    "ema_long":   el,
            "last_close":  lc,     "vwap":      vwap,
            "body_ratio":  body/rng if rng>0 else 0.0,
            "wick_ratio":  (float(last["high"])-lc)/rng if rng>0 else 1.0,
            "is_bullish":  float(last["close"]) > float(last["open"]),
            "recent_lows": list(df["low"].iloc[-5:]),
            "recent_highs":list(df["high"].iloc[-5:]),
            "atr14":       atr14,
            "ema9_slope":  ema9_slope,
            "mom3":        mom3,
            "raw_df":      df,
            "bars":        len(df),
        }

    def _get_5m_ind(self) -> Optional[dict]:
        """
        5m OPT premium indicators (API). None if < 3 bars → pass-through.
        'bullish' means the 5m PREMIUM is in an uptrend (price>=EMA9>=EMA15)
        — same definition for CE and PE since we always BUY the option.
        """
        with self._df_lock:
            df5 = self.opt_df_5m.copy()
        if df5.empty or len(df5) < 3:
            return None
        lc  = float(df5["close"].iloc[-1])
        if lc <= 0.50:
            return None
        ef5 = float(df5["close"].ewm(span=9,  adjust=False).mean().iloc[-1])
        em5 = float(df5["close"].ewm(span=15, adjust=False).mean().iloc[-1])
        bull = (lc >= ef5*0.995 and ef5 >= em5*0.995)
        return {"ema_fast": ef5, "ema_mid": em5, "last_close": lc,
                "bullish": bull, "bars": len(df5)}

    def _get_ul_5m_ind(self) -> Optional[dict]:
        """5m UL. None if < 3 bars → no block."""
        with self._df_lock:
            df5 = self.ul_df_5m.copy()
        if df5.empty or len(df5) < 3:
            return None
        ef5 = float(df5["close"].ewm(span=9,  adjust=False).mean().iloc[-1])
        em5 = float(df5["close"].ewm(span=15, adjust=False).mean().iloc[-1])
        agrees = (ef5 > em5 if self.direction == "BULL" else ef5 < em5)
        return {"ema_fast": ef5, "ema_mid": em5,
                "last_close": float(df5["close"].iloc[-1]), "agrees": agrees}

    # ================================================================
    # MASTER FILTER  (10 checks)
    # ================================================================

    def _all_filters_pass(self, ind: dict, label: str) -> bool:
        df   = ind["raw_df"]
        lc   = ind["last_close"]
        ef   = ind["ema_fast"]
        em   = ind["ema_mid"]
        el   = ind["ema_long"]
        atr  = ind.get("atr14", 0)
        bull = (self.opt_type == "CE")

        # 0. Option Chain signal filter
        # NEUTRAL = no filter (price action decides)
        # BULLISH variants block PE entries
        # BEARISH variants block CE entries
        if self._oc is not None:
            oc_bias = self._oc.is_bullish()   # True=CE, False=PE, None=neutral
            if oc_bias is not None:
                if bull and oc_bias is False:
                    print(_now(), f"[{self.symbol}][{label}] ❌ OC BEARISH — blocks CE")
                    return False
                if not bull and oc_bias is True:
                    print(_now(), f"[{self.symbol}][{label}] ❌ OC BULLISH — blocks PE")
                    return False

        # 1. MTF 5m OPT (pass-through if no data)
        i5m = self._get_5m_ind()
        ul5 = self._get_ul_5m_ind()
        if i5m is not None and not i5m["bullish"]:
            print(_now(), f"[{self.symbol}][{label}] ❌ MTF 5m ({i5m['bars']}b)"); return False
        if ul5 is not None and not ul5["agrees"]:
            print(_now(), f"[{self.symbol}][{label}] ❌ MTF UL 5m"); return False

        # 2. Chop (EMA9-EMA21 spread >= 0.2%)
        spread = abs(ef-el)/el if el>0 else 0
        if spread < 0.002:
            print(_now(), f"[{self.symbol}][{label}] ❌ CHOP {spread:.4f}"); return False

        # 3. EMA compression
        if (abs(ef-em)/em if em>0 else 0)<0.0015 and \
           (abs(em-el)/el if el>0 else 0)<0.0015:
            print(_now(), f"[{self.symbol}][{label}] ❌ EMA COMPRESSION"); return False

        # 4. ATR (>= 0.3% of price)
        if atr < lc*0.003:
            print(_now(), f"[{self.symbol}][{label}] ❌ ATR {atr:.2f}"); return False

        # 5. Doji / overlap (>= 2 of last 3 bars with body < 20%)
        if len(df) >= 4:
            brs = [abs(r["close"]-r["open"])/(r["high"]-r["low"])
                   if (r["high"]-r["low"])>0 else 0
                   for _, r in df.iloc[-4:-1].iterrows()]
            if sum(1 for b in brs if b<0.20) >= 2:
                print(_now(), f"[{self.symbol}][{label}] ❌ DOJI/OVERLAP"); return False

        # 6. Re-entry cooldown
        cooldown = self.cfg.get("reentry_cooldown_sec", 120)
        if time.time()-self._last_exit_ts < cooldown:
            print(_now(), f"[{self.symbol}][{label}] ❌ COOLDOWN "
                  f"{int(cooldown-(time.time()-self._last_exit_ts))}s"); return False

        # 6b. Same-strategy same-leg block after TARGET / NEAR_TARGET
        # Clears only when new pullback structure forms on the PREMIUM:
        #   A) Premium retraced >= 1% down from last entry AND touched EMA9/15/21
        #   B) Recovery candle formed (body >= 25%, premium bouncing up)
        # Same logic for CE and PE since df is the premium series and entries
        # always require the premium to be rising.
        if (self._last_exit_reason in ("TARGET", "NEAR_TARGET") and
                self._last_exit_side == self.opt_type and
                self._last_exit_strategy is not None and
                label.lower().startswith(self._last_exit_strategy.split("_")[0]) and
                self._last_entry_price is not None):
            cur = self._cur_candle.get("close") or lc
            new_structure = False
            # Premium retrace = price fell below last entry
            retrace_pct = (self._last_entry_price - cur) / self._last_entry_price
            if retrace_pct >= 0.01:
                ema_tol = 0.015
                recent  = df.iloc[-8:]
                evas    = [ef, em, el]
                ema_touched = any(
                    any(e*(1-ema_tol) <= float(r["low"]) <= e*(1+ema_tol) for e in evas)
                    for _, r in recent.iterrows())
                last_bar   = df.iloc[-1]
                bar_rng    = float(last_bar["high"]) - float(last_bar["low"])
                bar_body   = abs(float(last_bar["close"]) - float(last_bar["open"]))
                bar_body_r = bar_body / bar_rng if bar_rng > 0 else 0
                recovery   = (bar_body_r >= 0.25 and
                              float(last_bar["close"]) > float(last_bar["open"]))
                if ema_touched and recovery:
                    new_structure = True
            if not new_structure:
                print(_now(), f"[{self.symbol}][{label}] ❌ SAME-STRAT BLOCK "
                      f"(waiting for new pullback structure after TARGET)")
                return False

        # 7. Same-leg SL block (5% move required)
        if (self._last_exit_side == self.opt_type and
                self._last_exit_reason == "SL" and
                self._last_entry_price is not None):
            cur = self._cur_candle.get("close") or lc
            if abs(cur-self._last_entry_price)/self._last_entry_price < 0.005:
                print(_now(), f"[{self.symbol}][{label}] ❌ SAME-LEG BLOCK"); return False

        # 8. Leg reset (4% from last SL)
        if self._last_sl_price is not None:
            cur = self._cur_candle.get("close") or lc
            if abs(cur-self._last_sl_price)/self._last_sl_price < 0.004:
                print(_now(), f"[{self.symbol}][{label}] ❌ LEG RESET"); return False

        # 9. Quality — body >= 20%
        if ind["body_ratio"] < 0.20:
            print(_now(), f"[{self.symbol}][{label}] ❌ BODY {ind['body_ratio']:.2f}"); return False

        # 10. UL 1m EMA9/15 trend alignment
        with self._df_lock:
            ul_df = self.ul_df.copy()
        if len(ul_df) >= 15:
            ul_ef = float(ul_df["close"].ewm(span=9,  adjust=False).mean().iloc[-1])
            ul_em = float(ul_df["close"].ewm(span=15, adjust=False).mean().iloc[-1])
            if (ul_ef > ul_em) != bull:
                print(_now(), f"[{self.symbol}][{label}] ❌ TREND ALIGN"); return False

        return True

    # ================================================================
    # STRATEGY 1 — PULLBACK  (EMA 9/15/21 touch)
    # ================================================================

    def _eval_pullback(self):
        """
        7 conditions — same for CE and PE since opt_df is the OPTION'S OWN
        PREMIUM series. We only ever BUY an option, so an entry requires the
        PREMIUM to be in an uptrend (EMA9>EMA15>EMA21), regardless of whether
        the contract is a CE or PE. (PE premium rises when underlying falls —
        that's the whole point of buying a PE.)

        1.  EMA9>EMA15>EMA21 stack on premium + price > EMA21
        2.  5m API confirms — pass-through if < 3 bars
        3.  Premium touched EMA9, EMA15, or EMA21 in last 8 bars (1.5% tol)
        4.  Pullback volume < 3x avg (relaxed for trending days)
        5.  Recovery candle: body >= 25%, bullish (premium recovering)
        6.  LTP breaks trigger bar high (premium breaking out upward)
        7.  Breakout volume >= 0.3x avg
        """
        if self.position or not self.instrument_key or not self.direction: return
        i = self._get_opt_ind()
        if i is None or i["bars"] < 25: return
        df = i["raw_df"]

        # 1. Premium EMA stack must be rising
        if not (i["ema_fast"] > i["ema_mid"] > i["ema_long"] and
                i["last_close"] > i["ema_long"]):
            return

        i5m = self._get_5m_ind()
        if i5m is not None and not i5m["bullish"]:
            print(_now(), f"[{self.symbol}][PULLBACK] ❌ 5m ({i5m['bars']}b)"); return

        # 3. Premium touched an EMA on pullback (last 8 bars, low side)
        tol  = 0.015
        rec  = df.iloc[-8:]
        evas = [i["ema_fast"], i["ema_mid"], i["ema_long"]]
        touched = (
            any(any(e*(1-tol) <= float(r["low"]) <= e*(1+tol) for e in evas)
                for _, r in rec.iterrows()) or
            any(i["ema_long"]*0.985 <= float(r["low"]) <= i["ema_fast"]*1.015
                for _, r in rec.iterrows()))
        if not touched:
            print(_now(), f"[{self.symbol}][PULLBACK] ❌ No EMA touch ({self.opt_type})"); return

        # 4. Pullback volume not excessive
        if "volume" in df.columns and df["volume"].sum() > 0:
            avg_vol    = float(df["volume"].rolling(20).mean().iloc[-1])
            pb_vol_avg = float(df.iloc[-4:-1]["volume"].mean()) if len(df)>=4 else avg_vol
            if avg_vol > 0 and pb_vol_avg > avg_vol*3.0:
                print(_now(), f"[{self.symbol}][PULLBACK] ❌ PB vol high"); return

        # 5. Recovery candle — premium bouncing up
        last_bar = df.iloc[-1]
        rng      = float(last_bar["high"]) - float(last_bar["low"])
        body_r   = abs(float(last_bar["close"])-float(last_bar["open"]))/rng if rng>0 else 0
        if body_r < 0.25:
            print(_now(), f"[{self.symbol}][PULLBACK] ❌ Body {body_r:.2f}"); return
        if float(last_bar["close"]) <= float(last_bar["open"]):
            print(_now(), f"[{self.symbol}][PULLBACK] ❌ Not a recovery candle"); return

        # 6. LTP breaks trigger bar high
        trigger_high = float(last_bar["high"])
        cur_ltp      = self._cur_candle.get("close") or i["last_close"]
        if cur_ltp <= trigger_high:
            print(_now(), f"[{self.symbol}][PULLBACK] ❌ LTP {cur_ltp:.2f}<={trigger_high:.2f}")
            return

        # 7. Breakout volume sufficient — compare the JUST-CLOSED bar's
        # volume (df last row) against its 20-bar average. Do NOT use
        # self._cur_candle here — at this point in _on_bar_close it has
        # already been reset to the new (barely-started) minute and would
        # always read near-zero, permanently failing this check.
        if "volume" in df.columns and df["volume"].sum() > 0:
            avg_vol = float(df["volume"].rolling(20).mean().iloc[-1])
            cur_vol = float(df["volume"].iloc[-1])
            if avg_vol > 0 and cur_vol < avg_vol*0.3:
                print(_now(), f"[{self.symbol}][PULLBACK] ❌ BK vol low "
                      f"({cur_vol:.0f} < {avg_vol*0.3:.0f})"); return

        if not self._all_filters_pass(i, "PULLBACK"): return

        entry_ref = cur_ltp
        lows = i["recent_lows"]
        sl   = round(max(min(lows)-0.05 if lows else trigger_high-0.05,
                         entry_ref*(1-self.cfg.get("sl_pct",0.003))), 2)

        print(_now(), f"[{self.symbol}] ✅ PULLBACK {self.opt_type} @ {entry_ref:.2f} "
              f"SL {sl:.2f} [{self._regime.regime}]")
        self._place_trade(entry_ref, sl, "pullback_1m")

    # ================================================================
    # STRATEGY 2 — TREND FOLLOW
    # ================================================================

    def _eval_trend_follow(self):
        """
        Momentum entry — TRENDING regime only.
        Premium EMA9>EMA15>EMA21 stack + EMA9 slope positive + premium > EMA9
        by 0.1% + 3-bar momentum positive + last candle bullish (premium rising).
        Same conditions for CE and PE since opt_df is the premium series.
        SL: below EMA15.
        """
        if self.position or not self.instrument_key or not self.direction: return
        i = self._get_opt_ind()
        if i is None or i["bars"] < 20: return

        if not (i["ema_fast"] > i["ema_mid"] > i["ema_long"]): return
        if i["ema9_slope"] <= 0: return

        i5m = self._get_5m_ind()
        if i5m is not None and not i5m["bullish"]:
            print(_now(), f"[{self.symbol}][TREND] ❌ 5m ({i5m['bars']}b)"); return

        pct = (i["last_close"]-i["ema_fast"])/i["ema_fast"] if i["ema_fast"]>0 else 0
        if pct < 0.001: return
        if i["mom3"] <= 0: return
        if not i["is_bullish"]: return

        if not self._all_filters_pass(i, "TREND"): return

        cur_ltp = self._cur_candle.get("close") or i["last_close"]
        sl = round(max(i["ema_mid"]*0.997, cur_ltp*(1-self.cfg.get("sl_pct",0.004))), 2)

        print(_now(), f"[{self.symbol}] ✅ TREND {self.opt_type} @ {cur_ltp:.2f} "
              f"SL {sl:.2f} slope={i['ema9_slope']:.3f} [{self._regime.regime}]")
        self._place_trade(cur_ltp, sl, "trend_follow")

    # ================================================================
    # STRATEGY 3 — BREAKOUT
    # ================================================================

    def _eval_breakout(self):
        """
        10-bar structure break on the PREMIUM (upward) + EMA alignment.
        Same logic for CE and PE — we always want premium breaking above
        its recent 10-bar high with a bullish candle and rising EMA stack.
        """
        if self.position or not self.instrument_key or not self.direction: return
        i = self._get_opt_ind()
        if i is None or i["bars"] < 12: return
        window  = i["raw_df"].iloc[-10:]
        s_level = float(window["high"].iloc[:-1].max())
        lc, pc  = float(window["close"].iloc[-1]), float(window["close"].iloc[-2])
        if not (lc > s_level and pc <= s_level): return
        if not (i["is_bullish"] and i["ema_fast"]>i["ema_mid"]>i["ema_long"]): return
        if not self._all_filters_pass(i, f"BREAKOUT_{self.opt_type}"): return
        entry_ref = self._cur_candle.get("close") or lc
        sl = round(max(s_level*0.997, entry_ref*(1-self.cfg.get("sl_pct",0.003))), 2)
        print(_now(), f"[{self.symbol}] ✅ BREAKOUT {self.opt_type} @ {entry_ref:.2f} "
              f"SL {sl:.2f} [{self._regime.regime}]")
        self._place_trade(entry_ref, sl, "breakout_1m")

    # ================================================================
    # STRATEGY 4 — VWAP BOUNCE
    # ================================================================

    def _eval_vwap_bounce(self):
        """
        Premium within 0.4% of its own VWAP, bouncing up off it
        (EMA9>EMA15, recovery candle). Same logic for CE and PE.
        SL: just below VWAP.
        """
        if self.position or not self.instrument_key or not self.direction: return
        i = self._get_opt_ind()
        if i is None or i["bars"] < 20: return
        vwap = i.get("vwap", 0)
        lc   = i["last_close"]
        if vwap <= 0: return
        if abs(lc-vwap)/vwap > VWAP_BAND_PCT: return
        if lc < vwap*0.997: return
        if not (i["ema_fast"] > i["ema_mid"]): return
        last_bar = i["raw_df"].iloc[-1]
        rng      = float(last_bar["high"]) - float(last_bar["low"])
        body_r   = abs(float(last_bar["close"])-float(last_bar["open"]))/rng if rng>0 else 0
        if body_r < 0.35: return
        if float(last_bar["close"]) <= float(last_bar["open"]): return
        if not self._all_filters_pass(i, "VWAP"): return
        cur_ltp = self._cur_candle.get("close") or lc
        sl = round(max(vwap*0.997, cur_ltp*(1-self.cfg.get("sl_pct",0.003))), 2)
        print(_now(), f"[{self.symbol}] ✅ VWAP_BOUNCE {self.opt_type} @ {cur_ltp:.2f} "
              f"SL {sl:.2f} VWAP={vwap:.2f}")
        self._place_trade(cur_ltp, sl, "vwap_bounce")

    # ================================================================
    # STRATEGY 5 — EMA CROSS  (EMA9 x EMA15)
    # ================================================================

    def _eval_ema_cross(self):
        """
        Fresh EMA9 x EMA15 bullish cross on the PREMIUM (last closed bar).
        Volume >= 0.6x avg. EMA21 must be at/below EMA9 (rising stack).
        Same logic for CE and PE — premium crossing up is what matters.
        """
        if self.position or not self.instrument_key or not self.direction: return
        i = self._get_opt_ind()
        if i is None or i["bars"] < 20: return
        df = i["raw_df"]
        if len(df) < 4: return
        ema9  = df["close"].ewm(span=9,  adjust=False).mean()
        ema15 = df["close"].ewm(span=15, adjust=False).mean()
        ef_now, ef_prev = float(ema9.iloc[-1]),  float(ema9.iloc[-2])
        em_now, em_prev = float(ema15.iloc[-1]), float(ema15.iloc[-2])
        if not (ef_now > em_now and ef_prev <= em_prev): return
        if "volume" in df.columns and df["volume"].sum() > 0:
            avg_vol   = float(df["volume"].rolling(20).mean().iloc[-1])
            cross_vol = float(df["volume"].iloc[-1])
            if avg_vol > 0 and cross_vol < avg_vol*0.6:
                print(_now(), f"[{self.symbol}][EMA_CROSS] ❌ Low vol"); return
        el_val = float(df["close"].ewm(span=21, adjust=False).mean().iloc[-1])
        if ef_now < el_val: return
        if not self._all_filters_pass(i, "EMA_CROSS"): return
        cur_ltp = self._cur_candle.get("close") or i["last_close"]
        lows = i["recent_lows"]
        sl   = round(max(min(lows)-0.05 if lows else cur_ltp*0.997,
                         cur_ltp*(1-self.cfg.get("sl_pct",0.003))), 2)
        print(_now(), f"[{self.symbol}] ✅ EMA_CROSS {self.opt_type} @ {cur_ltp:.2f} "
              f"SL {sl:.2f} EF={ef_now:.2f}xEM={em_now:.2f}")
        self._place_trade(cur_ltp, sl, "ema_cross")

    # ================================================================
    # STRATEGY 6 — VCGB  (Volatility-Compression Gamma-Burst)
    # ================================================================

    def _eval_vcgb(self):
        """
        BB(20,2) squeeze inside KC(20,1.5xATR) on the PREMIUM → breakout with volume.
        1. BB inside KC last 5 bars (squeeze memory)
        2. ADX > 20
        3. Volume >= 1.5x 10-bar avg
        4. Premium LTP > KC upper (upward burst — same for CE and PE)
        5. 5m confirms (pass-through if no data)
        SL: KC lower band - 0.3%.
        """
        if self.position or not self.instrument_key or not self.direction: return
        with self._df_lock:
            df = self.opt_df.copy()
        if len(df) < 22: return

        close  = df["close"]
        high   = df["high"]
        low    = df["low"]
        volume = df["volume"]

        bb_mid   = close.rolling(20).mean()
        bb_std   = close.rolling(20).std()
        bb_upper = bb_mid + 2.0 * bb_std
        bb_lower = bb_mid - 2.0 * bb_std

        kc_mid   = close.ewm(span=20, adjust=False).mean()
        tr       = pd.concat([
            high - low,
            (high - close.shift()).abs(),
            (low  - close.shift()).abs(),
        ], axis=1).max(axis=1)
        atr20    = tr.rolling(20).mean()
        kc_upper = kc_mid + 1.5 * atr20
        kc_lower = kc_mid - 1.5 * atr20

        squeeze = (bb_upper < kc_upper) & (bb_lower > kc_lower)
        if not squeeze.iloc[-5:].any(): return

        adx_val = MarketRegimeAnalyzer._calc_adx(df, 14)
        if adx_val <= 20:
            print(_now(), f"[{self.symbol}][VCGB] ❌ ADX {adx_val:.1f}"); return

        vol_ma = volume.rolling(10).mean()
        if float(volume.iloc[-1]) <= float(vol_ma.iloc[-1]) * 1.5:
            print(_now(), f"[{self.symbol}][VCGB] ❌ No vol spike"); return

        kc_up   = float(kc_upper.iloc[-1])
        kc_lo   = float(kc_lower.iloc[-1])
        cur_ltp = self._cur_candle.get("close") or float(close.iloc[-1])

        if cur_ltp <= kc_up:
            print(_now(), f"[{self.symbol}][VCGB] ❌ {self.opt_type} {cur_ltp:.2f}<={kc_up:.2f}"); return
        sl = round(max(kc_lo*0.997, cur_ltp*(1-self.cfg.get("sl_pct",0.003))), 2)

        i5m = self._get_5m_ind()
        if i5m is not None and not i5m["bullish"]:
            print(_now(), f"[{self.symbol}][VCGB] ❌ 5m ({i5m['bars']}b)"); return

        cooldown = self.cfg.get("reentry_cooldown_sec", 120)
        if time.time()-self._last_exit_ts < cooldown:
            print(_now(), f"[{self.symbol}][VCGB] ❌ COOLDOWN "
                  f"{int(cooldown-(time.time()-self._last_exit_ts))}s"); return

        if (self._last_exit_reason in ("TARGET","NEAR_TARGET") and
                self._last_exit_side == self.opt_type and
                self._last_exit_strategy == "vcgb" and
                self._last_entry_price is not None and
                abs(cur_ltp-self._last_entry_price)/self._last_entry_price < 0.008):
            print(_now(), f"[{self.symbol}][VCGB] ❌ SAME-STRAT TARGET BLOCK"); return

        print(_now(), f"[{self.symbol}] ✅ VCGB {self.opt_type} @ {cur_ltp:.2f} "
              f"SL {sl:.2f} ADX={adx_val:.1f} [{self._regime.regime}]")
        self._place_trade(cur_ltp, sl, "vcgb")

    # ================================================================
    # STRATEGY 7 — UNIFIED STRUCTURE-BASED  (CE / PE via direction)
    # ================================================================

    def _eval_unified_strategy(self, direction: str):
        """
        Single generic strategy for both CE and PE trades using the
        integrated analysis engines.

        Args:
            direction: "CE" or "PE"

        Flow (same for CE and PE — only the checks reverse):
          1. Premium Market Structure analysis checks
          2. Underlying Market Structure analysis checks
          3. Option Chain bias check
          4. Existing EMA/ATR/Volume filters
          5. Risk validation → trade entry

        Returns nothing; calls _place_trade() if all conditions pass.
        """
        if self.position:
            return

        is_ce = (direction == "CE")
        label = f"UNIFIED_{direction}"

        # ── 0. Premium structure must be available ────────────────
        ps = self.premium_structure
        if ps is None:
            print(_now(), f"[{self.symbol}][{label}] ❌ No premium structure")
            return

        # ── 1. Premium Trend ──────────────────────────────────────
        # CE requires BULLISH trend, PE requires BEARISH trend.
        from backend.engine.market_structure import TrendDirection as PremiumTrend
        expected_trend = PremiumTrend.BULLISH if is_ce else PremiumTrend.BEARISH
        if ps.trend.direction != expected_trend:
            print(_now(), f"[{self.symbol}][{label}] ❌ Premium trend "
                  f"{ps.trend.direction.value} != {expected_trend.value}")
            return

        # ── 2. Premium Structure Valid ────────────────────────────
        # Confidence must be above configurable threshold (default 50).
        conf_threshold = self.cfg.get("structure_confidence_threshold", 50)
        if ps.confidence_score < conf_threshold:
            print(_now(), f"[{self.symbol}][{label}] ❌ Premium confidence "
                  f"{ps.confidence_score:.0f} < {conf_threshold}")
            return

        # ── 3. Premium Pullback ───────────────────────────────────
        # Must have a valid pullback in the expected direction.
        # For CE (bullish): pullback type must be HEALTHY, DEEP, or NESTED.
        # For PE (bearish): same pullback types apply (the premium
        # dropped during an overall downtrend).
        from backend.engine.market_structure import PullbackType as PBPullbackType
        valid_pb_types = {PBPullbackType.HEALTHY, PBPullbackType.DEEP,
                          PBPullbackType.NESTED}
        if ps.pullback.type not in valid_pb_types:
            print(_now(), f"[{self.symbol}][{label}] ❌ Invalid pullback: "
                  f"{ps.pullback.type.value}")
            return

        # Pullback quality check
        pb_quality_threshold = self.cfg.get("pullback_quality_threshold", 30)
        if ps.pullback.quality < pb_quality_threshold:
            print(_now(), f"[{self.symbol}][{label}] ❌ Pullback quality "
                  f"{ps.pullback.quality:.0f} < {pb_quality_threshold}")
            return

        # ── 4. Premium Recovery Confirmed ─────────────────────────
        # Recovery must be CONFIRMED (the premium has broken back above
        # the pullback swing high/low).
        from backend.engine.market_structure import RecoveryStatus as RecStatus
        if ps.recovery.status != RecStatus.CONFIRMED:
            print(_now(), f"[{self.symbol}][{label}] ❌ Recovery not confirmed: "
                  f"{ps.recovery.status.value}")
            return

        # Recovery confidence check
        rec_conf_threshold = self.cfg.get("recovery_confidence_threshold", 40)
        if ps.recovery.confidence < rec_conf_threshold:
            print(_now(), f"[{self.symbol}][{label}] ❌ Recovery confidence "
                  f"{ps.recovery.confidence:.0f} < {rec_conf_threshold}")
            return

        # ── 5. Underlying Structure (Higher Timeframe Context) ───
        from backend.engine.underlying_market_structure import (
            TrendDirection as UnderTrend,
            MarketDirection as UnderBias,
            MarketPhase as UnderPhase,
            PatternState as UnderPatternState,
        )

        us = self.underlying_structure
        if us is None:
            print(_now(), f"[{self.symbol}][{label}] ❌ No underlying structure")
            return

        # 5a. Underlying Confirmed Trend
        expected_under_trend = UnderTrend.UPTREND if is_ce else UnderTrend.DOWNTREND
        if us.trend != expected_under_trend:
            print(_now(), f"[{self.symbol}][{label}] ❌ Underlying trend "
                  f"{us.trend.value} != {expected_under_trend.value}")
            return

        # 5b. Higher Timeframe Bias
        expected_bias = UnderBias.BULLISH if is_ce else UnderBias.BEARISH
        if us.market_bias != expected_bias:
            print(_now(), f"[{self.symbol}][{label}] ❌ Market bias "
                  f"{us.market_bias.value} != {expected_bias.value}")
            return

        # 5c. Underlying Confidence
        under_conf_threshold = self.cfg.get("underlying_confidence_threshold", 40)
        if us.confidence < under_conf_threshold:
            print(_now(), f"[{self.symbol}][{label}] ❌ Underlying confidence "
                  f"{us.confidence:.0f} < {under_conf_threshold}")
            return

        # 5d. Market Phase must be trending (not ranging/transition)
        if us.market_phase not in (UnderPhase.STRONG_TREND, UnderPhase.WEAK_TREND):
            print(_now(), f"[{self.symbol}][{label}] ❌ Market phase "
                  f"{us.market_phase.value} not trending")
            return

        # 5e. No opposing confirmed reversal pattern
        if (us.active_pattern is not None and
                us.pattern_state == UnderPatternState.CONFIRMED):
            pattern_reverses_ce = us.active_pattern.direction == UnderBias.BEARISH
            pattern_reverses_pe = us.active_pattern.direction == UnderBias.BULLISH
            if (is_ce and pattern_reverses_ce) or (not is_ce and pattern_reverses_pe):
                print(_now(), f"[{self.symbol}][{label}] ❌ Opposing pattern: "
                      f"{us.active_pattern.pattern_type.value}")
                return

        # 5f. No opposing confirmed liquidity event
        if us.liquidity_event is not None:
            from backend.engine.underlying_market_structure import LiquidityType as LiqType
            # A liquidity sweep / SFP on the opposite side blocks the trade
            liq_opposes_ce = us.liquidity_event.liquidity_type in (
                LiqType.LIQUIDITY_SWEEP_HIGH, LiqType.SFP_HIGH,
                LiqType.FALSE_BREAKOUT, LiqType.BREAKOUT_TRAP)
            liq_opposes_pe = us.liquidity_event.liquidity_type in (
                LiqType.LIQUIDITY_SWEEP_LOW, LiqType.SFP_LOW,
                LiqType.FALSE_BREAKOUT, LiqType.BREAKOUT_TRAP)
            if (is_ce and liq_opposes_ce) or (not is_ce and liq_opposes_pe):
                if us.liquidity_event.confidence >= self.cfg.get(
                        "liquidity_confidence_threshold", 70):
                    print(_now(), f"[{self.symbol}][{label}] ❌ Opposing liquidity: "
                          f"{us.liquidity_event.liquidity_type.value}")
                    return

        # ── 6. Option Chain Bias ──────────────────────────────────
        # Already checked in _all_filters_pass, but also check here
        # for the unified strategy.
        if self._oc is not None:
            oc_bias = self._oc.is_bullish()
            if oc_bias is not None:
                if is_ce and oc_bias is False:
                    print(_now(), f"[{self.symbol}][{label}] ❌ OC bearish blocks CE")
                    return
                if not is_ce and oc_bias is True:
                    print(_now(), f"[{self.symbol}][{label}] ❌ OC bullish blocks PE")
                    return

        # ── 7. Existing filters (EMA, ATR, Volume, Regime) ────────
        ind = self._get_opt_ind()
        if ind is None:
            return

        # EMAs must align with direction (premium rising for both CE/PE)
        if not (ind["ema_fast"] > ind["ema_mid"] > ind["ema_long"] and
                ind["last_close"] > ind["ema_long"]):
            print(_now(), f"[{self.symbol}][{label}] ❌ Premium EMA stack not aligned")
            return

        if not self._all_filters_pass(ind, label):
            return

        # ── 8. All checks passed — place trade ───────────────────
        cur_ltp = self._cur_candle.get("close") or ind["last_close"]

        # SL: below recent swing low for CE, but since we always BUY
        # options the SL logic is the same — place below the current
        # premium structure low.
        lows = ind["recent_lows"]
        sl_pct = self.cfg.get("sl_pct", 0.003)
        sl = round(max(min(lows) - 0.05 if lows else cur_ltp * 0.997,
                       cur_ltp * (1 - sl_pct)), 2)

        print(_now(), f"[{self.symbol}] ✅ {label} @ {cur_ltp:.2f} "
              f"SL {sl:.2f} | PremConf={ps.confidence_score:.0f} "
              f"UnderConf={us.confidence:.0f} "
              f"UnderTrend={us.trend.value} "
              f"MarketPhase={us.market_phase.value}")
        self._place_trade(cur_ltp, sl, f"unified_{direction.lower()}")

    # ================================================================
    # ORDER PLACEMENT
    # ================================================================

    def _api_client(self):
        cfg = upstox_client.Configuration()
        cfg.access_token = self.access_token
        return upstox_client.ApiClient(cfg)

    def _place_order(self, side: str, qty: int,
                     order_type: str = "MARKET",
                     trigger: float = 0,
                     key_override: str = None) -> Optional[str]:
        key = key_override or self.instrument_key
        if not key: return None
        # Encode user_id in tag so webhook can resolve which user this
        # order belongs to without scanning all active bots in Redis.
        tag = f"algo_bot:{self.user_id}"
        if self.paper_mode:
            ltp = self._cur_candle.get("close") or 0
            o   = self._paper.place_market_order(side, qty, ltp, key, tag) \
                  if order_type == "MARKET" else \
                  self._paper.place_sl_order(side, qty, trigger, key, tag)
            oid = o["order_id"]
            print(_now(), f"[{self.symbol}] [PAPER] Order placed successfully: side={side} type={order_type} qty={qty} order_id={oid}")
        else:
            # Trade-time plan gate: re-verify the user's subscription
            # before a LIVE entry (bot start checked it once; a plan can
            # lapse or lose the symbol/limit mid-session). Exits and SL
            # placement are never blocked — only BUY entries.
            if side == "BUY":
                from backend.services.subscription_service import (
                    check_trading_permission_sync,
                )
                lots = max(1, int(self.cfg.get("order_qty", 1)))
                allowed, reason = check_trading_permission_sync(
                    self.user_id, self.symbol, lots)
                if not allowed:
                    print(_now(), f"[{self.symbol}] ⛔ [LIVE] Entry blocked "
                                  f"by plan check: {reason}")
                    return None
            order_type_api = order_type
            limit_price    = 0
            if order_type == "SL-M":
                order_type_api = "SL"
                limit_price    = round(trigger*0.995, 2) if side == "SELL" \
                                 else round(trigger*1.005, 2)
            api  = upstox_client.OrderApiV3(self._api_client())
            body = upstox_client.PlaceOrderV3Request(
                quantity=qty, product=self.cfg.get("product","I"),
                validity="DAY", price=limit_price, tag=tag,
                instrument_token=key, order_type=order_type_api,
                transaction_type=side, disclosed_quantity=0,
                trigger_price=trigger, is_amo=False, slice=False)
            body_dict = body.to_dict() if hasattr(body, 'to_dict') else str(body)
            print(_now(), f"[{self.symbol}] [LIVE] Placing order request payload: {body_dict}")
            try:
                resp = api.place_order(body)
                resp_dict = resp.to_dict() if hasattr(resp, 'to_dict') else str(resp)
                print(_now(), f"[{self.symbol}] [LIVE] Order response received: {resp_dict}")
                oid = self._get_order_id(resp)
                print(_now(), f"[{self.symbol}] [LIVE] Extracted order ID: {oid}")
            except ApiException as e:
                err_body = getattr(e, 'body', str(e))
                print(_now(), f"[{self.symbol}] ❌ [LIVE] Order placement failed: {err_body}")
                return None
            except Exception as e:
                print(_now(), f"[{self.symbol}] ❌ [LIVE] Order placement failed: {e}")
                return None
        # Register order_id in Redis so webhook can associate it with
        # this user (secondary lookup path if tag resolution fails).
        if oid:
            try:
                from backend.services.redis_client import get_redis_sync
                r = get_redis_sync()
                r.sadd(f"bot:orders:{self.user_id}", oid)
                r.expire(f"bot:orders:{self.user_id}", 86400)  # 24h TTL
            except Exception as e:
                print(_now(), f"[{self.symbol}] ⚠️ Failed to register order in Redis: {e}")
        return oid

    def _get_fill_price(self, order_id: str, timeout: int = 15) -> Optional[float]:
        """
        Wait for fill price, preferring the Upstox webhook (fast, no
        API rate-limit cost) with an automatic fallback to polling
        get_order_details if no webhook arrives in time.
        """
        if self.paper_mode:
            o = self._paper.get_order(order_id)
            return o["fill_price"] if o else None

        # 1. Webhook path (order_store.wait_for_fill_sync)
        # Blocks on Redis pub/sub until webhook delivers the fill
        # or `timeout` seconds elapse.
        try:
            from backend.services.order_store import wait_for_fill_sync
            fill = wait_for_fill_sync(order_id, timeout=float(timeout))
            if fill is not None:
                print(_now(), f"[{self.symbol}] ✅ Entry fill confirmed via Webhook. Price: ₹{fill}")
                return fill
        except Exception as e:
            print(_now(), f"[{self.symbol}] webhook fill wait exception: {e}")

        # 2. API fallback — Upstox webhook not configured or timed out
        print(_now(), f"[{self.symbol}] ⚠️  Webhook fill not received for {order_id} within timeout — falling back to API polling")
        api = upstox_client.OrderApiV3(self._api_client())
        t0  = time.time()
        while time.time()-t0 < 5:
            try:
                resp = api.get_order_details(order_id)
                resp_dict = resp.to_dict() if hasattr(resp, 'to_dict') else str(resp)
                print(_now(), f"[{self.symbol}] [LIVE] Fallback API poll response for {order_id}: {resp_dict}")
                if resp and hasattr(resp, "data"):
                    st = str(getattr(resp.data,"order_status",
                                     getattr(resp.data,"status",""))).lower()
                    if st in ("complete","filled"):
                        fill_p = float(resp.data.average_price)
                        print(_now(), f"[{self.symbol}] ✅ Entry fill confirmed via Fallback API Poll. Price: ₹{fill_p}")
                        return fill_p
            except Exception as e:
                print(_now(), f"[{self.symbol}] Fallback API poll error: {e}")
            time.sleep(0.3)
        print(_now(), f"[{self.symbol}] ❌ Failed to confirm entry fill via fallback API polling for {order_id}")
        return None

    @staticmethod
    def _get_order_id(resp) -> Optional[str]:
        try:
            if isinstance(resp, dict):
                d = resp.get("data", {})
                if isinstance(d, dict):
                    if "order_ids" in d: return str(d["order_ids"][0])
                    for k in ("order_id","orderId","id"):
                        if k in d: return str(d[k])
            if hasattr(resp,"data") and hasattr(resp.data,"order_ids"):
                return str(resp.data.order_ids[0])
            if hasattr(resp,"order_id"):
                return str(resp.order_id)
        except Exception:
            pass
        return None

    # ================================================================
    # EXECUTION LAYER — routes signals to Paper / Semi Auto / Auto
    # ================================================================

    def _build_trade_signal(self, entry_ref: float, initial_sl: float, strategy: str) -> TradeSignal:
        """Build a standardised TradeSignal from the current state."""
        num_lots = self.symbol_lots  # Per-symbol independent lot count
        custom_ls = self.cfg.get("custom_lot_sizes") or {}
        lot_size = get_lot_size(self.symbol, custom_ls)
        qty = num_lots * lot_size
        if self._regime.regime == "VOLATILE":
            qty = max(lot_size, (num_lots // 2) * lot_size)

        return TradeSignal(
            symbol=self.symbol,
            opt_type=self.opt_type or "CE",
            direction="BUY",
            entry_price=entry_ref,
            stop_loss=initial_sl,
            quantity=qty,
            confidence=None,
            strategy_name=strategy,
            timestamp=datetime.utcnow().isoformat(),
            instrument_key=self.instrument_key,
            trading_symbol=self.trading_symbol,
            strike=self.strike,
            regime=self._regime.regime,
        )

    def _route_via_execution_layer(self, signal: TradeSignal) -> dict:
        """
        Route a TradeSignal through the Execution Layer.
        Returns a dict with:
          - action: "execute" | "pending" | "blocked" | "failed"
          - result: ExecutionResult
          - pending_trade_id: optional int
        """
        exec_mode = self.cfg.get("execution_mode", "PAPER")
        mode = exec_mode.upper() if exec_mode else "PAPER"

        if mode == "PAPER":
            # Route via execution layer - sync (worker thread context)
            try:
                from backend.services.execution_layer import PaperExecutor
                result = PaperExecutor(self.user_id, signal).execute_sync()
                return {"action": "execute", "result": result}
            except Exception as e:
                print(_now(), f"[{self.symbol}] ⚠️ Execution layer error: {e}")
                # Fallback to direct paper mode
                return {"action": "fallback", "mode": "paper"}

        elif mode == "SEMI_AUTO":
            try:
                from backend.services.execution_layer import pending_trade_manager
                result = pending_trade_manager.create_pending_trade_sync(
                    self.user_id, signal
                )

                if result.status == ExecutionStatus.PENDING_APPROVAL:
                    print(_now(), f"[{self.symbol}] ⏳ Pending approval #{result.pending_trade_id}")
                    # Notify user via on_trade callback
                    self.on_trade({
                        "event": "PENDING_TRADE",
                        "user_id": self.user_id,
                        "mode": "semi_auto",
                        "symbol": self.symbol,
                        "trading_symbol": self.trading_symbol or self.symbol,
                        "opt_type": self.opt_type,
                        "strike": self.strike,
                        # NOTE: no entry_price / stop_loss — semi-auto
                        # fills at the current LTP on approval and SL is
                        # set from the actual fill, so the signal-time
                        # values are not sent to the user.
                        "quantity": signal.quantity,
                        "pending_trade_id": result.pending_trade_id,
                        "signal_id": result.signal_id,
                        "strategy": signal.strategy_name,
                        "expires_at": result.details.get("expires_at") if result.details else None,
                        "message": result.message,
                    })
                    return {"action": "pending", "result": result}
                else:
                    print(_now(), f"[{self.symbol}] ❌ Execution layer: {result.message}")
                    return {"action": "failed", "result": result}
            except Exception as e:
                print(_now(), f"[{self.symbol}] ⚠️ Semi-auto execution error: {e}")
                return {"action": "failed", "result": None}

        elif mode == "AUTO":
            try:
                result = _global_execution_router.execute_sync(
                    self.user_id, signal, place_order_fn=self._place_order
                )

                if result.status == ExecutionStatus.EXECUTED:
                    return {"action": "execute", "result": result}
                elif result.status == ExecutionStatus.BLOCKED:
                    print(_now(), f"[{self.symbol}] 🚫 Daily consent required for AUTO mode")
                    self._alert_order_failed(
                        f"⚠️ Daily risk disclosure not accepted — "
                        f"cannot execute automatic trade. Go to Settings to accept."
                    )
                    return {"action": "blocked", "result": result}
                else:
                    print(_now(), f"[{self.symbol}] ❌ Auto execution: {result.message}")
                    return {"action": "failed", "result": result}
            except Exception as e:
                print(_now(), f"[{self.symbol}] ⚠️ Auto execution error: {e}")
                return {"action": "failed", "result": None}

        else:
            # Unknown mode — fallback to paper
            print(_now(), f"[{self.symbol}] ⚠️ Unknown execution mode '{mode}' — falling back to PAPER")
            return {"action": "fallback", "mode": "paper"}

    # ================================================================
    # TRADE ENTRY  (SL-L only — no target order at broker)
    # ================================================================

    def _alert_sl_cancel(self, sl_trigger: float, reason: str = ""):
        """
        Fires SL_CANCEL through the same pipeline as ENTRY/EXIT/SL_TRAIL —
        confirms a stop-loss order was successfully cancelled at the
        broker (normal part of the exit sequence: cancel SL, then
        market-exit). Gated by telegram_on_exit since it's part of the
        exit flow.
        """
        mode = "paper" if self.paper_mode else "live"
        if self._tg_token and self._tg_chat and self.cfg.get("telegram_on_exit", True):
            tg.alert_sl_cancel(self._tg_token, self._tg_chat,
                              self.trading_symbol or self.symbol, sl_trigger, reason)
        self.on_trade({
            "event": "SL_CANCEL", "user_id": self.user_id, "mode": mode,
            "symbol": self.symbol, "trading_symbol": self.trading_symbol or self.symbol,
            "sl_trigger": sl_trigger, "reason": reason,
        })

    def _alert_order_failed(self, reason: str):
        """
        Fires the ORDER_ALERT event through the same pipeline as
        ENTRY/EXIT/SL_TRAIL — published via on_trade (Redis pub/sub ->
        WebSocket -> dashboard voice alert) and Telegram, so order
        failures/rejections are surfaced on every channel, not just
        printed to the worker log.
        """
        mode = "paper" if self.paper_mode else "live"
        print(_now(), f"[{self.symbol}] 🚨 ORDER FAILED: {reason}")
        if self._tg_token and self._tg_chat:
            tg.alert_order_failed(self._tg_token, self._tg_chat,
                                  self.trading_symbol or self.symbol, reason, mode)
        self.on_trade({
            "event": "ORDER_ALERT", "user_id": self.user_id, "mode": mode,
            "symbol": self.symbol, "trading_symbol": self.trading_symbol or self.symbol,
            "reason": reason,
        })

    def _place_trade(self, entry_ref: float, initial_sl: float, strategy: str):
        """
        1. Route through Execution Layer — handles Paper / Semi Auto / Auto
        2. For Paper and Auto modes: BUY at market, place SL-L order,
           no target at broker (cancel SL + market exit on near_target hit)
        3. For Semi Auto: creates PendingTrade record for user approval
        4. VOLATILE regime: lots halved (rounded down, min 1 lot)
        """
        # ── Build standardised signal ───────────────────────────
        signal = self._build_trade_signal(entry_ref, initial_sl, strategy)
        qty = signal.quantity

        # Route through execution layer
        route_result = self._route_via_execution_layer(signal)

        if route_result["action"] == "pending":
            # Semi Auto — pending approval, nothing more to do here
            return

        if route_result["action"] == "blocked":
            # Auto — consent not given
            return

        if route_result["action"] == "failed":
            # Execution layer rejected the trade
            msg = route_result.get("result")
            if msg:
                self._alert_order_failed(f"Trade rejected by execution layer: {msg}")
            return

        # ── Fallback to direct mode for backward compat ─────────
        is_paper = self.paper_mode
        if route_result["action"] == "fallback":
            is_paper = route_result.get("mode", "paper") == "paper"

        mode = "paper" if is_paper else "live"
        num_lots   = self.symbol_lots  # Per-symbol independent lot count
        custom_ls  = self.cfg.get("custom_lot_sizes") or {}
        lot_size   = get_lot_size(self.symbol, custom_ls)
        if self._regime.regime == "VOLATILE":
            qty = max(lot_size, (num_lots // 2) * lot_size)   # halve lots, keep ≥ 1 lot

        print(_now(), f"[{self.symbol}] Placing {num_lots} lot(s) × {lot_size} = {qty} qty in {mode} mode "
              f"(exec_layer_fallback={'yes' if route_result.get('action')=='fallback' else 'no'})")

        eid = self._place_order("BUY", qty)
        if not eid:
            self._alert_order_failed(
                f"BUY order rejected ({self.opt_type} {self.strike}, qty {qty}) — "
                f"entry not taken.")
            return

        fill = self._get_fill_price(eid)
        if not fill:
            self._alert_order_failed(
                f"BUY order placed (id {eid}) but fill price could not be confirmed — "
                f"aborting trade entry to protect account.")
            return

        sl_id = self._place_order("SELL", qty, order_type="SL-M", trigger=initial_sl)
        if not sl_id:
            self._alert_order_failed(
                f"⚠️ ENTRY FILLED at ₹{fill} but STOP-LOSS order placement failed — "
                f"position is UNPROTECTED. Attempting emergency exit.")
            self._place_order("SELL", qty)
            return

        # --- STOP-LOSS ORDER VERIFICATION ---
        print(_now(), f"[{self.symbol}] Verifying stop-loss order {sl_id} reaches a valid broker state...")
        sl_verified = False
        rejection_reason = "No broker response / timeout"

        if is_paper:
            sl_verified = True
        else:
            t0 = time.time()
            from backend.services.order_store import get_order_update_sync
            while time.time() - t0 < 5.0:
                status = ""
                message = ""
                update = get_order_update_sync(sl_id)
                if update:
                    status = str(update.get("status", "")).lower().strip()
                    message = str(update.get("message", "Unknown"))
                else:
                    try:
                        api = upstox_client.OrderApiV3(self._api_client())
                        resp = api.get_order_details(sl_id)
                        if resp and hasattr(resp, "data"):
                            status = str(getattr(resp.data, "order_status", getattr(resp.data, "status", ""))).lower().strip()
                            message = str(getattr(resp.data, "status_message", "Unknown"))
                    except Exception:
                        pass
                if status:
                    from backend.services.order_store import is_filled_status, is_rejected_status
                    if status in ("trigger_pending", "open", "trigger pending") or is_filled_status(status):
                        print(_now(), f"[{self.symbol}] Stop-loss order {sl_id} verified. State: {status}")
                        sl_verified = True
                        break
                    elif is_rejected_status(status) or status in ("rejected", "cancelled"):
                        rejection_reason = message or f"Status: {status}"
                        sl_verified = False
                        break
                time.sleep(0.3)

        if not sl_verified:
            self._alert_order_failed(
                f"⚠️ ENTRY FILLED at ₹{fill} but STOP-LOSS order {sl_id} verification failed "
                f"(Reason: {rejection_reason}) — emergency exit.")
            self._place_order("SELL", qty)
            return

        rr       = self.cfg.get("target_rr", 1.3)
        risk     = abs(fill - initial_sl)
        target   = round(fill + risk * rr, 2)
        near_pct = self.cfg.get("target_near_pct", 0.003)
        self.position = {
            "entry_price": fill, "qty": qty,
            "entry_order_id": eid, "sl_order_id": sl_id,
            "sl_trigger": initial_sl, "target": target,
            "near_target": round(target*(1-near_pct), 2),
            "strategy": strategy, "entry_ts": datetime.utcnow(),
            "instrument_key": self.instrument_key,
            "trading_symbol": self.trading_symbol,
            "opt_type": self.opt_type, "strike": self.strike,
            "paper_mode": is_paper, "symbol": self.symbol,
            "regime": self._regime.regime,
        }
        self.sl_order_id       = sl_id
        self.trailing_sl       = initial_sl
        self._sl_mod_ts        = time.time()
        self._risk.record_entry()
        self._last_entry_price = fill
        print(_now(), f"[{self.symbol}{'📄' if is_paper else '💰'}] "
              f"{self.opt_type} [{strategy}] Entry:{fill} SL:{initial_sl} "
              f"Target:{target} [{self._regime.regime}]")
        if self._tg_token and self._tg_chat and self.cfg.get("telegram_on_entry", True):
            tg.alert_entry(self._tg_token, self._tg_chat,
                           self.trading_symbol or self.symbol,
                           self.opt_type or "", fill, initial_sl,
                           target, qty, strategy, mode)
        self.on_trade({"event":"ENTRY","user_id":self.user_id,"mode":mode,**self.position})

    # ================================================================
    # POSITION MANAGEMENT
    # ================================================================

    def _manage_position(self, ltp: float):
        pos         = self.position
        near_target = pos.get("near_target", pos["target"])
        target      = pos["target"]
        if ltp >= near_target:
            reason = "TARGET" if ltp >= target else "NEAR_TARGET"
            print(_now(), f"[{self.symbol}] 🎯 {reason} @ {ltp}")
            self._book_profit(ltp, pos["sl_order_id"], reason)
            return
        if self.paper_mode:
            fill = self._paper.check_sl_filled(pos["sl_order_id"], ltp)
            if fill is not None:
                self._record_exit(fill, "SL"); return
        else:
            if self._sl_filled_live(pos["sl_order_id"]):
                exit_p = self._get_fill_price(pos["sl_order_id"], timeout=5) \
                         or pos["sl_trigger"]
                self._record_exit(exit_p, "SL"); return
        self._maybe_trail(ltp)

    def _book_profit(self, ltp: float, sl_id: str, reason: str):
        """Cancel SL (frees margin) then market exit."""
        pos = self.position
        if self.paper_mode and self._paper:
            self._paper.cancel_order(sl_id)
        else:
            try:
                upstox_client.OrderApiV3(self._api_client()).cancel_order(sl_id)
            except Exception as e:
                print(_now(), f"[{self.symbol}] cancel SL: {e}")
        self._alert_sl_cancel(pos.get("sl_trigger", 0), reason)
        time.sleep(0.15)
        exit_id = self._place_order("SELL", pos["qty"])
        if not exit_id:
            self._alert_order_failed(
                f"⚠️ {reason} exit SELL order REJECTED for {pos['qty']} qty — "
                f"position may still be OPEN. Please check broker terminal and square off manually.")
        pnl = round((ltp-pos["entry_price"])*pos["qty"], 2)
        self._record_exit(ltp, reason, pnl)

    def _maybe_trail(self, ltp: float):
        """
        ATR(7) x 1.2 trailing SL using SL-L.
        SL-M banned for NSE/BSE options.
        limit_price = trigger x 0.995
        """
        if time.time()-self._sl_mod_ts < 8: return
        with self._df_lock:
            df = self.opt_df.copy()
        if len(df) < 8: return
        df["TR"] = df["high"] - df["low"]
        atr_val  = float(df["TR"].rolling(7).mean().iloc[-1])
        if pd.isna(atr_val) or atr_val <= 0: return
        proposed = round(ltp - atr_val*1.2, 2)
        if proposed <= self.trailing_sl or proposed >= ltp: return
        if abs((proposed-self.trailing_sl)/self.trailing_sl) < 0.0006: return
        old_sl      = self.trailing_sl
        limit_price = round(proposed*0.995, 2)
        print(_now(), f"[{self.symbol}] Trailing SL trigger proposed: old={old_sl} new={proposed} limit={limit_price}")
        if self.paper_mode and self._paper:
            self._paper.modify_sl_order(self.sl_order_id, proposed)
        else:
            try:
                body = upstox_client.ModifyOrderRequest(
                    order_id=self.sl_order_id,
                    price=limit_price,
                    trigger_price=proposed,
                    order_type="SL",
                    quantity=self.position["qty"],
                    validity="DAY"
                )
                print(_now(), f"[{self.symbol}] Live Trailing SL Request Payload: {body.to_dict() if hasattr(body, 'to_dict') else str(body)}")
                resp = upstox_client.OrderApiV3(self._api_client()).modify_order(body=body)
                print(_now(), f"[{self.symbol}] Live Trailing SL Response: {resp.to_dict() if hasattr(resp, 'to_dict') else str(resp)}")
            except Exception as e:
                print(_now(), f"[{self.symbol}] ❌ Trailing SL modify failed: {e}")
                return
        self.trailing_sl            = proposed
        self.position["sl_trigger"] = proposed
        self._sl_mod_ts             = time.time()
        print(_now(), f"[{self.symbol}] [TRAIL] {old_sl}->trigger={proposed} "
              f"limit={limit_price} LTP={ltp}")
        if self._tg_token and self._tg_chat and self.cfg.get("telegram_on_trail", False):
            tg.alert_sl_trail(self._tg_token, self._tg_chat, old_sl, proposed, ltp)
        self.on_trade({"event":"SL_TRAIL","user_id":self.user_id,
                       "symbol":self.symbol,"new_sl":proposed,"ltp":ltp})

    def _sl_filled_live(self, sl_id: str) -> bool:
        """
        Check if SL order is filled. Called on every tick in
        _manage_position. Uses webhook data (O(1) Redis GET) instead
        of a Upstox API call per tick — no rate-limit risk, instant.
        Falls back to a single API call if Redis has no data for this
        order (webhook not configured / first few ticks after order
        placement before webhook arrives).
        """
        # 1. Instant webhook check — no blocking
        try:
            from backend.services.order_store import is_order_filled_sync
            if is_order_filled_sync(sl_id):
                return True
        except Exception:
            pass

        # 2. Single API call fallback (no busy-wait loop here)
        try:
            resp = upstox_client.OrderApiV3(self._api_client()).get_order_details(sl_id)
            if resp and hasattr(resp, "data"):
                st = str(getattr(resp.data,"order_status",
                                 getattr(resp.data,"status",""))).lower()
                return st in ("complete","filled")
        except Exception:
            pass
        return False

    def _record_exit(self, exit_price: float, status: str, pnl: float = None):
        pos = self.position
        if pnl is None:
            pnl = round((exit_price-pos["entry_price"])*pos["qty"], 2)
        # Net P&L (wins offset losses) — matches the "Max Loss Per Day"
        # label's meaning: account stops once NET daily P&L breaches
        # -max_loss, not once gross losing trades alone sum past it.
        self._risk.record_pnl(pnl)
        self._last_exit_ts       = time.time()
        self._last_exit_side     = pos.get("opt_type")
        self._last_exit_reason   = status
        self._last_entry_price   = pos.get("entry_price")
        self._last_sl_price      = pos.get("sl_trigger")
        self._last_exit_strategy = pos.get("strategy")
        mode = "paper" if self.paper_mode else "live"
        trade_data = {
            "event":"EXIT","user_id":self.user_id,
            "status":status,"exit_price":exit_price,
            "pnl":pnl,"mode":mode,"symbol":self.symbol,"exit_ts":_now(),
            **{k:pos[k] for k in (
                "entry_price","qty","sl_trigger","target","strategy",
                "entry_ts","instrument_key","trading_symbol","opt_type","strike")},
        }
        if self._tg_token and self._tg_chat and self.cfg.get("telegram_on_exit", True):
            tg.alert_exit(self._tg_token, self._tg_chat,
                          pos.get("trading_symbol", self.symbol),
                          pos.get("opt_type",""),
                          pos["entry_price"], exit_price,
                          pnl, status, pos["qty"], mode)
        self.on_trade(trade_data)
        self.position    = None
        self.trailing_sl = None
        self.sl_order_id = None
        print(_now(), f"[{self.symbol}] EXIT {status} @ {exit_price} "
              f"P&L Rs{pnl} [{'PAPER' if self.paper_mode else 'LIVE'}]")

    def _emergency_exit(self):
        pos = self.position
        if not pos: return
        if self.paper_mode and self._paper:
            self._paper.cancel_order(pos["sl_order_id"])
        else:
            try:
                upstox_client.OrderApiV3(self._api_client()).cancel_order(pos["sl_order_id"])
            except Exception: pass
        self._alert_sl_cancel(pos.get("sl_trigger", 0), "DIRECTION_FLIP_EXIT")
        exit_id = self._place_order("SELL", pos["qty"], key_override=pos["instrument_key"])
        if not exit_id:
            self._alert_order_failed(
                f"⚠️ Direction-flip exit SELL order REJECTED for {pos['qty']} qty — "
                f"position may still be OPEN. Please check broker terminal and square off manually.")
        self._record_exit(self._cur_candle.get("close") or pos["entry_price"],
                          "DIRECTION_FLIP_EXIT")

    def _send_daily_summary(self):
        # Account-wide totals (shared RiskTracker, not per-symbol).
        # NOTE: "winners" count isn't tracked by RiskTracker (it only
        # tracks net P&L) — passing 0 here undercounts; a precise win/loss
        # breakdown would need a separate counter or a DB query. Net P&L
        # and trade count are accurate; win-rate in the summary message
        # should be treated as approximate until that's added.
        snap = self._risk.snapshot()
        tg.alert_daily_summary(self._tg_token, self._tg_chat,
                               self.symbol, snap["trades_today"], 0,
                               snap["net_pnl_today"],
                               "paper" if self.paper_mode else "live")


# ================================================================
# TRADING ENGINE  (multi-symbol orchestrator)
# ================================================================

class TradingEngine:

    def __init__(self, user_id: int, config: dict, access_token: str,
                 stop_event: threading.Event, on_trade: Callable):
        self.user_id      = user_id
        self.cfg          = config
        self.access_token = access_token
        self._stop        = stop_event
        self.on_trade     = on_trade
        self._engines     = []
        self._snapshot_lock = threading.Lock()
        self._snapshots: dict[str, dict] = {}   # symbol -> latest snapshot
        # ONE RiskTracker shared by every symbol's SymbolEngine for this
        # user — makes max_trades_per_day / max_loss_per_day genuinely
        # account-wide instead of multiplied per traded symbol.
        self._risk_tracker = RiskTracker()

    def _aggregate_snapshot(self, symbol: str, snapshot: dict):
        """
        Collects per-symbol snapshots and pushes the combined list to
        Redis under bot:positions:{user_id} — used when a bot trades
        multiple symbols so /api/position/ returns all of them in one
        call (same shape as the old in-process get_positions()).
        """
        from backend.services.state_store import set_positions_sync
        with self._snapshot_lock:
            self._snapshots[symbol] = snapshot
            combined = list(self._snapshots.values())
        set_positions_sync(self.user_id, combined)

    def run_sync(self):
        symbols = self._get_symbol_list()
        mode    = "PAPER" if self.cfg.get("paper_mode", True) else "LIVE"
        print(_now(), f"[user {self.user_id}] Symbols: {[s['symbol'] for s in symbols]} | {mode}")
        for entry in symbols:
            eng = self._make_engine(entry["symbol"], entry["token"], entry.get("lots"))
            if len(symbols) > 1:
                eng._snapshot_aggregator = self._aggregate_snapshot
            self._engines.append(eng)
        try:
            from backend.services.telegram_bot import register_engines
            register_engines(self.user_id, self._engines)
        except Exception as e:
            print(_now(), f"[user {self.user_id}] TG registry: {e}")
        try:
            if len(self._engines) == 1:
                self._engines[0].run()
            else:
                threads = []
                for eng in self._engines:
                    t = threading.Thread(target=eng.run, daemon=True,
                                         name=f"sym-{self.user_id}-{eng.symbol}")
                    threads.append(t)
                    t.start()
                while not self._stop.is_set():
                    time.sleep(1)
                for t in threads:
                    t.join(timeout=5)
        finally:
            try:
                from backend.services.telegram_bot import unregister_engines
                unregister_engines(self.user_id)
            except Exception: pass
            try:
                from backend.services.state_store import set_positions_sync
                set_positions_sync(self.user_id, [])   # clear stale positions
            except Exception: pass

    def _get_symbol_list(self) -> list:
        """
        Returns a list of dicts with symbol, token, and optional lots.
        Main symbol uses order_qty; additional symbols use their own config.
        """
        main_symbol = self.cfg.get("underlying_symbol", "NIFTY")
        main_token  = self.cfg.get("underlying_token", "NSE_INDEX|NIFTY 50")
        main_lots   = max(1, int(self.cfg.get("order_qty", 1)))
        result = [{"symbol": main_symbol, "token": main_token, "lots": main_lots}]

        extra_s = [s.strip() for s in
                   (self.cfg.get("extra_symbols") or "").split(",") if s.strip()]
        extra_t = [t.strip() for t in
                   (self.cfg.get("extra_tokens")  or "").split(",") if t.strip()]

        # Build a lookup from extra_symbol_configs for per-symbol lots
        extra_configs = self.cfg.get("extra_symbol_configs") or []
        config_by_symbol = {}
        for entry in extra_configs:
            sym = entry.get("symbol", "").upper()
            if entry.get("enabled", True) and sym:
                config_by_symbol[sym] = max(1, int(entry.get("lots", 1)))

        for i in range(min(len(extra_s), len(extra_t))):
            sym = extra_s[i].upper()
            tok = extra_t[i]
            # Look up per-symbol lots; fall back to main order_qty only if
            # no config exists (backward compatibility for existing users).
            lots = config_by_symbol.get(sym, main_lots)
            result.append({"symbol": sym, "token": tok, "lots": lots})

        return result

    def _make_engine(self, symbol: str, token: str, symbol_lots: int = None) -> SymbolEngine:
        return SymbolEngine(
            user_id=self.user_id, config=self.cfg,
            access_token=self.access_token, stop_event=self._stop,
            on_trade=self.on_trade, symbol=symbol, underlying_token=token,
            risk_tracker=self._risk_tracker, symbol_lots=symbol_lots)
