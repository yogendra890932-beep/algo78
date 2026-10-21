# backend/shared/range_detector.py
# ================================================================
# Pure helpers for the Range Fade strategy.
#
# A "range" is a flat horizontal channel on the option-premium 1m chart:
# price oscillates between a support and a resistance with at least two
# touches of the lower boundary and no meaningful drift.
#
# The strategy only buys premium at the lower boundary (a dip-buy inside
# the range) because the execution layer never shorts option premium.
# ================================================================

from typing import Optional

import pandas as pd

RANGE_LOOKBACK = 30          # bars used to define the channel
RANGE_TOUCH_TOL = 0.15       # a "touch" is within 15% of the channel width
RANGE_ENTRY_ZONE = 0.35      # last bar must be in the lower 35% of the channel
RANGE_FLAT_DRIFT = 0.35      # channel midpoint may drift at most 35% of width
RANGE_MIN_TOUCHES = 2        # lower-boundary touches required
RANGE_MIN_WIDTH_PCT = 0.006  # reject micro ranges (< 0.6% of price)
RANGE_MIN_WIDTH_ATR = 1.5    # channel must be at least 1.5x ATR(14)


def _atr(df: pd.DataFrame, period: int = 14) -> float:
    try:
        high, low, close = df["high"], df["low"], df["close"]
        tr = pd.concat(
            [
                high - low,
                (high - close.shift()).abs(),
                (low - close.shift()).abs(),
            ],
            axis=1,
        ).max(axis=1)
        val = tr.rolling(period).mean().iloc[-1]
        return float(val) if pd.notna(val) else 0.0
    except Exception:
        return 0.0


def detect_range_channel(df: Optional[pd.DataFrame]) -> Optional[dict]:
    """Return ``{high, low, width, touches}`` for a flat channel, else None."""
    if df is None or len(df) < RANGE_LOOKBACK:
        return None
    try:
        window = df.iloc[-RANGE_LOOKBACK:]
        high = float(window["high"].max())
        low = float(window["low"].min())
        width = high - low
        if low <= 0 or width <= 0:
            return None

        # Micro ranges are noise, not tradeable structure.
        if width / low < RANGE_MIN_WIDTH_PCT:
            return None
        atr = _atr(df)
        if atr > 0 and width < RANGE_MIN_WIDTH_ATR * atr:
            return None

        # A flat channel must not drift: compare the first and last five
        # closes inside the lookback window.
        closes = window["close"].astype(float)
        drift = abs(
            float(closes.iloc[-5:].mean()) - float(closes.iloc[:5].mean())
        )
        if drift > RANGE_FLAT_DRIFT * width:
            return None

        touches = int(
            (window["low"].astype(float) <= low + RANGE_TOUCH_TOL * width).sum()
        )
        if touches < RANGE_MIN_TOUCHES:
            return None

        return {"high": high, "low": low, "width": width, "touches": touches}
    except Exception:
        return None


def range_entry(df: Optional[pd.DataFrame], channel: Optional[dict]) -> Optional[dict]:
    """Return ``{entry, stop_loss}`` on a lower-boundary bullish bounce."""
    if df is None or not channel or len(df) == 0:
        return None
    try:
        bar = df.iloc[-1]
        open_ = float(bar["open"])
        high = float(bar["high"])
        low = float(bar["low"])
        close = float(bar["close"])
        width = channel["width"]

        # Must be testing the lower boundary, not chasing mid-range.
        if low > channel["low"] + RANGE_ENTRY_ZONE * width:
            return None

        # Bullish reversal candle closing in the top half of its own range.
        if close <= open_:
            return None
        bar_range = high - low
        if bar_range <= 0 or close < low + 0.5 * bar_range:
            return None

        return {
            "entry": close,
            "stop_loss": round(channel["low"] - 0.1 * width, 2),
        }
    except Exception:
        return None
