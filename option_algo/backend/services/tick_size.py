# backend/services/tick_size.py
# ================================================================
# Price rounding for Upstox orders.
#
# Every NSE/BSE index option (NIFTY / SENSEX / BANKNIFTY) trades on a
# 0.05 tick, and Upstox rejects any order whose price or trigger is not
# an exact multiple of the tick. All broker-bound prices therefore pass
# through these helpers before an order is sent.
#
# Rounding rules:
#   * round_to_tick()      — half-up to the nearest tick. Decimal-based
#                            so floats like 123.45000000000002 do not
#                            land on the wrong side of a half-tick.
#   * sl_trigger_and_limit() — also rebuilds the SL limit from the
#                            trigger and guarantees a valid stop-limit
#                            gap (SELL: limit < trigger, BUY: limit >
#                            trigger) so tiny premiums cannot round the
#                            limit onto the wrong side of the trigger.
# ================================================================

from decimal import Decimal, ROUND_HALF_UP
from typing import Tuple

TICK_SIZE = 0.05

_SELL_LIMIT_BUFFER = 0.995
_BUY_LIMIT_BUFFER = 1.005


def round_to_tick(price: float, tick: float = TICK_SIZE) -> float:
    """Round *price* half-up to the nearest multiple of *tick*.

    Returns a float on the tick grid, never below one tick (Upstox
    rejects zero-priced limit/trigger values).
    """
    if tick <= 0:
        raise ValueError("tick must be positive")
    if price is None:
        return tick
    value = Decimal(str(price))
    step = Decimal(str(tick))
    steps = (value / step).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    rounded = steps * step
    if rounded <= 0:
        return float(step)
    # Normalise to the tick's own number of decimals (0.05 -> 2 dp).
    decimals = max(0, -step.as_tuple().exponent)
    return float(rounded.quantize(Decimal(1).scaleb(-decimals)))


def sl_trigger_and_limit(trigger: float, side: str,
                         tick: float = TICK_SIZE) -> Tuple[float, float]:
    """Tick-round an SL-M trigger and the SL limit derived from it.

    Mirrors the historical limit buffer (SELL x0.995, BUY x1.005) but on
    the tick grid, and enforces at least one tick between trigger and
    limit so the broker accepts the stop-limit order.
    """
    tick = float(tick)
    trig = round_to_tick(trigger, tick)
    if trig <= tick:
        # Keep room for a limit one tick away from the trigger.
        trig = round_to_tick(tick * 2, tick)
    buffer = _SELL_LIMIT_BUFFER if side == "SELL" else _BUY_LIMIT_BUFFER
    limit = round_to_tick(trig * buffer, tick)
    if side == "SELL":
        limit = min(limit, round_to_tick(trig - tick, tick))
    else:
        limit = max(limit, round_to_tick(trig + tick, tick))
    return trig, round_to_tick(max(limit, tick), tick)
