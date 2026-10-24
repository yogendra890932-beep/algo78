"""Entry-timing guard shared by every strategy.

Two failure modes make an otherwise valid signal a *late entry*:

1. The signal is stale — by the time we act, the setup is one or more
   bars old and the market may have moved on.
2. The price already ran in our direction — entering now means giving up
   a large slice of the planned reward/risk before the trade even starts.

The guard is intentionally strategy-agnostic: it compares the live LTP
with the signal's own entry and the account SL% / R:R, so it behaves the
same for pullback, breakout, trend, VWAP, Range Fade and the unified
strategies.
"""

from datetime import datetime, timezone
from typing import Optional

# Maximum price drift from the signal entry, as a fraction of the
# account risk (entry * sl_pct). 0.35 = at most 35% of the stop distance.
DEFAULT_MAX_DRIFT_FRAC = 0.35

# Maximum signal age (seconds) at the moment we act on it.
DEFAULT_MAX_AGE_SEC = 90


def _parse_ts(value) -> Optional[datetime]:
    if not value:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        try:
            dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except (TypeError, ValueError):
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def late_entry_reason(
    entry: float,
    stop_loss: Optional[float],
    target: Optional[float],
    current: Optional[float],
    sl_pct: float,
    target_rr: float,
    signal_ts=None,
    now: Optional[datetime] = None,
    max_drift_frac: Optional[float] = None,
    max_age_sec: Optional[float] = None,
    symbol: str = "",
) -> str:
    """Return a human-readable rejection reason, or "" when timely.

    A *missing* live price is not a rejection: with no tick there is no
    evidence of drift, so the signal is allowed through (the fill-time
    check runs again with the freshest LTP).
    """
    try:
        entry = float(entry or 0)
    except (TypeError, ValueError):
        return ""
    if entry <= 0:
        return ""

    sl_pct = float(sl_pct or 0.0)
    if sl_pct <= 0 or sl_pct >= 1:
        sl_pct = 0.05
    try:
        target_rr = float(target_rr or 0)
    except (TypeError, ValueError):
        target_rr = 1.5
    if target_rr <= 0:
        target_rr = 1.5

    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)

    max_age = DEFAULT_MAX_AGE_SEC if max_age_sec is None else float(max_age_sec)
    ts = _parse_ts(signal_ts)
    if ts is not None and max_age > 0:
        age = (now - ts).total_seconds()
        if age > max_age:
            return (f"stale signal ({age:.0f}s old > {max_age:.0f}s) "
                    f"for {symbol}".strip())

    if current is None:
        return ""
    try:
        current = float(current)
    except (TypeError, ValueError):
        return ""
    if current <= 0:
        return ""

    risk = entry * sl_pct
    # Setup already invalidated — price is at/below the account stop.
    if stop_loss:
        try:
            if current <= float(stop_loss):
                return (f"setup invalidated (LTP {current:.2f} <= "
                        f"SL {float(stop_loss):.2f}) for {symbol}".strip())
        except (TypeError, ValueError):
            pass
    else:
        if current <= entry * (1.0 - sl_pct):
            return (f"setup invalidated (LTP {current:.2f}) for "
                    f"{symbol}".strip())

    # Price already reached the target — there is nothing left to take.
    if target:
        try:
            if current >= float(target):
                return (f"price already at target {float(target):.2f} "
                        f"(LTP {current:.2f}) for {symbol}".strip())
        except (TypeError, ValueError):
            pass

    # Chasing: the price has already consumed too much of the stop
    # distance, so the remaining reward/risk at this entry is too poor.
    drift_frac = DEFAULT_MAX_DRIFT_FRAC if max_drift_frac is None \
        else float(max_drift_frac)
    if drift_frac > 0:
        drift = current - entry
        budget = risk * drift_frac
        if drift > budget:
            return (f"late entry — LTP {current:.2f} is {drift / risk * 100:.0f}% "
                    f"of risk above entry {entry:.2f} (max "
                    f"{drift_frac * 100:.0f}%) for {symbol}".strip())

    return ""
