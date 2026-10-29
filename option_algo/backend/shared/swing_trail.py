"""Swing-low trailing stop on the 1-minute premium chart.

Selected with `trail_mode = "swing"` (Settings → Trail Mode → Swing Low).
The stop follows the structure of completed 1-minute candles, adapting to
how fast the premium is moving:

  slow   (small candle)        -> last bearish candle's LOW
  fast   (big green candle)    -> that green candle's LOW
  medium (in between)          -> last bearish candle's LOW, but only once
                                  a strong green candle has completed

"Speed" is the size of the last completed candle's body relative to the
average body of the previous bars. The returned level is a *candidate*:
callers only move the stop up (never down) and never to/above the live
premium price.
"""
from __future__ import annotations
from typing import Iterable, Optional, Sequence

# Body vs average-body thresholds (ratio).
FAST_X = 1.5      # >= 1.5x average body  -> fast move
SLOW_X = 0.7      # <= 0.7x average body  -> slow move
STRONG_X = 1.2    # >= 1.2x average body  -> "strong" green candle
AVG_WINDOW = 10   # bars used for the average body
BEARISH_LOOKBACK = 6


def _rows(candles) -> list[dict]:
    """Normalise a DataFrame / list of bars into a list of dicts."""
    if candles is None:
        return []
    try:
        if hasattr(candles, "to_dict"):
            return list(candles.to_dict("records"))
    except Exception:
        pass
    return list(candles) if isinstance(candles, Iterable) else []


def _f(v) -> Optional[float]:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f


def classify_speed(body: float, avg_body: float,
                   fast_x: float = FAST_X, slow_x: float = SLOW_X) -> str:
    """'fast' | 'slow' | 'medium' for the last completed candle."""
    if not avg_body or avg_body <= 0:
        return "medium"
    ratio = abs(body) / avg_body
    if ratio >= fast_x:
        return "fast"
    if ratio <= slow_x:
        return "slow"
    return "medium"


def _last_bearish_low(rows: Sequence[dict], lookback: int) -> Optional[float]:
    """Low of the most recent bearish (close < open) candle."""
    for bar in reversed(list(rows)[-lookback:]):
        o, c, low = _f(bar.get("open")), _f(bar.get("close")), _f(bar.get("low"))
        if o is None or c is None or low is None:
            continue
        if c < o:
            return low
    return None


def swing_low_stop(candles, avg_window: int = AVG_WINDOW,
                   fast_x: float = FAST_X, slow_x: float = SLOW_X,
                   strong_x: float = STRONG_X,
                   lookback: int = BEARISH_LOOKBACK) -> tuple[Optional[float], str]:
    """Candidate trailing stop (and why) for a long premium position.

    `candles` must be COMPLETED 1-minute premium candles, oldest first
    (SharedOptionPremiumBuilder.get_1m_df already returns exactly that).
    Returns (None, reason) when the stop should stay where it is.
    """
    rows = _rows(candles)
    if len(rows) < avg_window + 2:
        return None, "insufficient history"

    last = rows[-1]
    o, c, low = _f(last.get("open")), _f(last.get("close")), _f(last.get("low"))
    if o is None or c is None or low is None:
        return None, "bad candle"

    bodies = []
    for bar in rows[-(avg_window + 1):-1]:          # exclude the last bar itself
        bo, bc = _f(bar.get("open")), _f(bar.get("close"))
        if bo is None or bc is None:
            continue
        bodies.append(abs(bc - bo))
    if not bodies:
        return None, "no body history"
    avg_body = sum(bodies) / len(bodies)
    if avg_body <= 0:
        return None, "flat history"

    body = c - o
    speed = classify_speed(body, avg_body, fast_x, slow_x)

    if speed == "slow":
        # Slow grind: the bears' lows are the only structure worth using.
        lvl = _last_bearish_low(rows, lookback)
        return (lvl, "slow: bearish candle low") if lvl is not None else (None, "slow: no bearish candle")

    if speed == "fast":
        # Fast expansion: a big green candle's own low is the tightest
        # structure that is still safe.
        if body > 0 and body >= fast_x * avg_body:
            return low, "fast: big green candle low"
        return None, "fast: hold (no big green candle)"

    # Medium: only step up once a strong green candle has COMPLETED, and
    # then to the low of the bearish candle it broke out of.
    if body > 0 and body >= strong_x * avg_body:
        lvl = _last_bearish_low(rows[:-1], lookback)
        return (lvl, "medium: bearish low after strong green") if lvl is not None else (None, "medium: no bearish candle")
    return None, "medium: hold"
