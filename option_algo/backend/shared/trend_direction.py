# backend/shared/trend_direction.py
# ================================================================
# Trend-aware direction resolver.
#
# The old logic flipped CE↔PE the instant the underlying EMA9/15
# crossed. A small pullback inside a healthy trend crosses those EMAs
# for a bar or two, which churned the option contract and threw away
# the trend.
#
# This resolver keeps the current direction while the underlying 5m
# structure still supports it, and only flips when the new side is
# confirmed by BOTH a sustained EMA cross and price beyond EMA21 (or
# the 5m structure already changed trend).
# ================================================================

from typing import Optional

import pandas as pd

EMA_CONFIRM_BARS = 3
NEUTRAL_TRENDS = ("", "SIDEWAYS", "TRANSITION")


def _emas(df: pd.DataFrame):
    close = df["close"]
    return (
        close.ewm(span=9, adjust=False).mean(),
        close.ewm(span=15, adjust=False).mean(),
        close.ewm(span=21, adjust=False).mean(),
    )


def basic_direction(df: Optional[pd.DataFrame]) -> Optional[str]:
    """Raw EMA9/15 direction (no hysteresis)."""
    if df is None or len(df) < 15:
        return None
    ema9, ema15, _ = _emas(df)
    return "BULL" if float(ema9.iloc[-1]) > float(ema15.iloc[-1]) else "BEAR"


def _ema_confirmed(df: pd.DataFrame, direction: str,
                   bars: int = EMA_CONFIRM_BARS) -> bool:
    if len(df) < max(15, bars):
        return False
    ema9, ema15, _ = _emas(df)
    for i in range(1, bars + 1):
        up = float(ema9.iloc[-i]) > float(ema15.iloc[-i])
        if direction == "BULL" and not up:
            return False
        if direction == "BEAR" and up:
            return False
    return True


def _price_confirmed(df: pd.DataFrame, direction: str) -> bool:
    _, _, ema21 = _emas(df)
    close = float(df["close"].iloc[-1])
    level = float(ema21.iloc[-1])
    return close > level if direction == "BULL" else close < level


def resolve_direction(df: Optional[pd.DataFrame],
                      prev: Optional[str] = None,
                      trend: Optional[str] = None) -> Optional[str]:
    """Return BULL/BEAR, ignoring pullbacks that fight an intact trend.

    ``prev`` is the currently traded direction, ``trend`` is the
    underlying 5m structure trend (UPTREND/DOWNTREND/SIDEWAYS/...).
    """
    basic = basic_direction(df)
    if basic is None:
        return prev
    if prev is None or basic == prev:
        return basic

    trend = (trend or "").upper()
    prev_trend = "UPTREND" if prev == "BULL" else "DOWNTREND"
    new_trend = "UPTREND" if basic == "BULL" else "DOWNTREND"

    # The 5m structure still supports the direction we already hold:
    # treat the EMA cross as a pullback, not a reversal.
    if trend == prev_trend:
        return prev

    # A genuine reversal still needs a sustained cross and price beyond
    # EMA21 before we roll the option.
    if trend == new_trend or trend in NEUTRAL_TRENDS:
        if _ema_confirmed(df, basic) and _price_confirmed(df, basic):
            return basic

    return prev
