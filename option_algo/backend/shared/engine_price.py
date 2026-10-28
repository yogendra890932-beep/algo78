"""Resolve an engine's current premium (option) price.

Manual SL modification / trailing must be validated against the LIVE
premium — not the entry price — so a stop can be trailed past the entry
to lock in profit once the premium has moved in our favour. Only the
wrong side of the live price is rejected (it would trigger immediately).
"""
from __future__ import annotations
from typing import Optional


def _price(v) -> Optional[float]:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if f > 0 else None


def engine_premium_price(eng, fallback=None) -> Optional[float]:
    """Best-effort live premium for `eng`; falls back to `fallback`."""
    # Shared mode: the manager keeps the latest tick per symbol.
    try:
        mgr = getattr(eng, "_mgr", None)
        if mgr is not None:
            v = _price(mgr._last_ltp.get(getattr(eng, "symbol", None)))
            if v is not None:
                return v
    except Exception:
        pass
    # Legacy engine_v6 (and simple mocks) expose ltp directly.
    for attr in ("ltp", "_ltp"):
        try:
            v = _price(getattr(eng, attr, None))
            if v is not None:
                return v
        except Exception:
            pass
    try:
        cur = getattr(eng, "_cur_candle", None) or {}
        v = _price(cur.get("close"))
        if v is not None:
            return v
    except Exception:
        pass
    return _price(fallback)
