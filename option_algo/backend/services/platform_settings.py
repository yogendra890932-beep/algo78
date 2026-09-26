# backend/services/platform_settings.py
# ================================================================
# Platform-wide settings accessor (singleton row id=1).
#
# Currently the only toggle is `auto_trading_enabled` — the global
# gate for the fully-automatic (AUTO) execution mode. An admin flips
# it from the admin panel; the value is read by:
#   - the API (async: is_auto_enabled_async) for mode-save / bot-start
#   - worker/engine threads (sync: is_auto_enabled_sync) for the
#     runtime AUTO-entry safety net
#
# Both paths are FAIL-SAFE: a missing row or any DB error means AUTO
# is treated as DISABLED, so a transient failure can never silently
# re-enable unattended live trading.
# ================================================================

import threading
import time

from sqlalchemy import select

from backend.db.models import PlatformSettings


# Short TTL so an admin toggle propagates to already-running worker
# processes within a few seconds without a DB hit per signal.
_SYNC_TTL_SEC = 5.0
_cache: dict = {"value": None, "ts": 0.0}
_lock = threading.Lock()


async def get_platform_settings(db):
    """Return the singleton row, creating a fail-safe default if absent."""
    res = await db.execute(
        select(PlatformSettings).where(PlatformSettings.id == 1)
    )
    row = res.scalar_one_or_none()
    if row is None:
        row = PlatformSettings(id=1, auto_trading_enabled=False)
        db.add(row)
        await db.commit()
        await db.refresh(row)
    return row


async def is_auto_enabled_async(db) -> bool:
    """Async reader for the API layer. Fail-safe: False on any error."""
    try:
        row = await get_platform_settings(db)
        return bool(row.auto_trading_enabled)
    except Exception as e:
        print(f"[platform_settings] read failed (treating AUTO as disabled): {e}")
        return False


def is_auto_enabled_sync() -> bool:
    """Sync reader for worker/engine threads, with a short TTL cache.

    Fail-safe: any DB error returns False (AUTO disabled).
    """
    now = time.time()
    with _lock:
        cached = _cache["value"]
        if cached is not None and (now - _cache["ts"]) < _SYNC_TTL_SEC:
            return cached

    value = False
    try:
        from backend.db.database import get_sync_session
        with get_sync_session() as db:
            row = db.execute(
                select(PlatformSettings).where(PlatformSettings.id == 1)
            ).scalar_one_or_none()
            value = bool(row.auto_trading_enabled) if row else False
    except Exception as e:
        print(f"[platform_settings] sync read failed "
              f"(treating AUTO as disabled): {e}")
        value = False

    with _lock:
        _cache["value"] = value
        _cache["ts"] = now
    return value


def refresh():
    """Drop the cached value — call after an admin update so the
    serving process reflects the change immediately."""
    with _lock:
        _cache["value"] = None
        _cache["ts"] = 0.0
