"""Structure-aware swing-low trailing stop on the 1-minute premium chart.

Selected with `trail_mode = "swing"` (Settings → Trail Mode → Swing Low).

Rules:
  1. Trail below the most recent confirmed swing low.
  2. If price breaks above the last swing high (resistance), trail below that
     broken resistance (now acting as support).
  3. If the last completed candle is a strong bullish candle (body >= 1.5x 
     average), trail below that green candle's low.
  4. Return the highest valid trailing level (most conservative).
  5. Only move the stop up (never down), and never to/above the live premium.

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


def _avg_body(rows: Sequence[dict], window: int = 10) -> float:
    """Calculate average candle body over the last `window` bars."""
    if len(rows) < window:
        rows = rows[-len(rows):] if rows else []
    else:
        rows = rows[-window:]
    
    bodies = []
    for bar in rows:
        o, c = _f(bar.get("open")), _f(bar.get("close"))
        if o is not None and c is not None:
            bodies.append(abs(c - o))
    
    return sum(bodies) / len(bodies) if bodies else 0.0


def swing_low_stop(
    candles,
    left: int = 2,
    right: int = 2,
    min_bars: int = 6,
    strong_x: float = 1.5,
) -> Tuple[Optional[float], str]:
    """Candidate trailing stop (and why) for a long premium position.

    `candles` must be COMPLETED 1-minute premium candles, oldest first.
    Returns (None, reason) when the stop should stay where it is.

    Logic (returns the highest valid level):
      1. Find confirmed pivot highs and lows.
      2. Use the most recent pivot low as the base trailing level.
      3. If price has broken above the most recent pivot high (resistance),
         trail below that broken resistance instead.
      4. If the last candle is a strong bullish candle (body >= 1.5x avg),
         trail below that green candle's low.
      5. Return the highest of all applicable levels.
    """
    rows = _rows(candles)
    if len(rows) < min_bars:
        return None, "insufficient history"

    # Get the last completed candle
    last_bar = rows[-1]
    last_open = _f(last_bar.get("open"))
    last_close = _f(last_bar.get("close"))
    last_low = _f(last_bar.get("low"))
    
    if last_open is None or last_close is None or last_low is None:
        return None, "bad candle data"

    is_bullish = last_close > last_open
    avg_body_val = _avg_body(rows[:-1], 10)  # exclude current candle
    is_strong_bullish = (
        is_bullish and 
        avg_body_val > 0 and 
        (last_close - last_open) >= strong_x * avg_body_val
    )

    # Current live price (use the last candle's close as proxy)
    current_price = last_close
    if current_price <= 0:
        return None, "no current price"

    trail_levels = []  # collect all candidate levels

    # Rule 1: Most recent confirmed swing low
    highs, lows = _find_pivot_highs_lows(rows, left, right)
    if lows:
        last_low_idx, last_swing_low = lows[-1]
        trail_levels.append((last_swing_low, f"swing low at {last_swing_low:.2f}"))

    # Rule 2: Broken resistance (price above last swing high)
    if highs:
        last_high_idx, last_swing_high = highs[-1]
        if current_price > last_swing_high:
            trail_levels.append((last_swing_high, f"broken resistance at {last_swing_high:.2f}"))
        
        # Check for more recent swing highs after the last swing low
        if lows:
            for idx, high_val in reversed(highs):
                if idx > last_low_idx and current_price > high_val:
                    trail_levels.append((high_val, f"broken swing high at {high_val:.2f}"))
                    break

    # Rule 3: Strong bullish candle -> trail below its low
    if is_strong_bullish:
        trail_levels.append((last_low, f"strong green candle low at {last_low:.2f}"))

    if not trail_levels:
        return None, "no valid trailing level"

    # Return the highest level (most conservative trailing)
    best_level, best_reason = max(trail_levels, key=lambda x: x[0])
    return best_level, best_reason
