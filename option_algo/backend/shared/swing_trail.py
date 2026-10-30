"""Structure-aware swing-low trailing stop on the 1-minute premium chart.

Selected with `trail_mode = "swing"` (Settings → Trail Mode → Swing Low).

Rules:
  1. Trail below the most recent confirmed swing low.
  2. If price breaks above the last swing high (resistance), trail below that
     broken resistance level (now acting as support).
  3. Only move the stop up (never down), and never to/above the live premium.

A swing high/low is confirmed when price makes a higher high/lower low with
at least N intervening bars (default 2) to avoid false breaks.
"""
from __future__ import annotations
from typing import Iterable, Optional, Sequence, Tuple


def _rows(candles) -> list[dict]:
    """Normalize a DataFrame / list of bars into a list of dicts."""
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


def _find_pivot_highs_lows(
    rows: Sequence[dict], left: int = 2, right: int = 2
) -> Tuple[list[Tuple[int, float]], list[Tuple[int, float]]]:
    """Find confirmed pivot highs and lows (index, price).

    A pivot high has the highest 'high' among `left` bars before and `right`
    bars after. A pivot low has the lowest 'low' in the same window.
    """
    highs, lows = [], []
    if len(rows) < left + right + 1:
        return highs, lows

    for i in range(left, len(rows) - right):
        cur_bar = rows[i]
        cur_high = _f(cur_bar.get("high"))
        cur_low = _f(cur_bar.get("low"))
        if cur_high is None or cur_low is None:
            continue

        # Check if this is a pivot high
        is_high = True
        for j in range(i - left, i + right + 1):
            if j == i:
                continue
            h = _f(rows[j].get("high"))
            if h is not None and h >= cur_high:
                is_high = False
                break
        if is_high:
            highs.append((i, cur_high))

        # Check if this is a pivot low
        is_low = True
        for j in range(i - left, i + right + 1):
            if j == i:
                continue
            l = _f(rows[j].get("low"))
            if l is not None and l <= cur_low:
                is_low = False
                break
        if is_low:
            lows.append((i, cur_low))

    return highs, lows


def swing_low_stop(
    candles,
    left: int = 2,
    right: int = 2,
    min_bars: int = 6,
) -> Tuple[Optional[float], str]:
    """Candidate trailing stop (and why) for a long premium position.

    `candles` must be COMPLETED 1-minute premium candles, oldest first.
    Returns (None, reason) when the stop should stay where it is.

    Logic:
      1. Find confirmed pivot highs and lows.
      2. Use the most recent pivot low as the base trailing level.
      3. If the current price has broken above the most recent pivot high
         (resistance), trail below that broken resistance instead.
      4. Return the higher of the two levels (more conservative trailing).
    """
    rows = _rows(candles)
    if len(rows) < min_bars:
        return None, "insufficient history"

    highs, lows = _find_pivot_highs_lows(rows, left, right)

    if not lows:
        return None, "no swing lows found"

    # Most recent confirmed swing low
    last_low_idx, last_low = lows[-1]

    # Current live price (use the last candle's close as proxy)
    last_bar = rows[-1]
    current_price = _f(last_bar.get("close")) or _f(last_bar.get("high")) or 0.0
    if current_price <= 0:
        return None, "no current price"

    # Default trail level = last swing low
    trail_level = last_low
    reason = f"swing low at {trail_level:.2f}"

    # Check if price has broken above any recent swing high
    if highs:
        last_high_idx, last_high = highs[-1]

        # If price is above the last swing high, use that as the trail level
        # (broken resistance becomes support)
        if current_price > last_high:
            # Trail below the broken resistance
            if last_high > trail_level:
                trail_level = last_high
                reason = f"broken resistance at {trail_level:.2f}"

        # Look for more recent swing highs after the last swing low
        for idx, high_val in reversed(highs):
            if idx > last_low_idx and current_price > high_val:
                if high_val > trail_level:
                    trail_level = high_val
                    reason = f"broken swing high at {trail_level:.2f}"
                break

    return trail_level, reason
