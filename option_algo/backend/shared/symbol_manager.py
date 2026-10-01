# backend/shared/symbol_manager.py
# ================================================================
# Dynamic Symbol Subscription Manager
#
# Tracks which symbols are actively needed and manages subscriptions.
# Only symbols with >= 1 user are actively subscribed to broker feeds.
# Unused symbols are unsubscribed after a configurable timeout.
#
# Responsibilities:
#   - Track active symbols via Redis Set
#   - Track subscriber counts per symbol
#   - Coordinate subscribe/unsubscribe with shared market workers
#   - Auto-recovery after restart
# ================================================================

from backend.shared.redis_infra import (
    active_symbols_key,
    symbol_subscriber_count,
    user_symbols_key,
)
from backend.services.redis_client import get_redis_sync


def _r():
    return get_redis_sync()


def _is_real_user(user_id) -> bool:
    """Only real users (id > 0) may hold a subscription.

    Sentinel/placeholder ids (0, -1, None) must never keep market data
    or the broker WebSocket alive when no bot is actually running.
    """
    try:
        return int(user_id) > 0
    except (TypeError, ValueError):
        return False


def add_subscriber(user_id: int, symbol: str) -> int:
    """
    Register a user as a subscriber for a symbol.
    Returns the new subscriber count.
    If count goes from 0→1, the symbol should be activated.
    """
    sym = symbol.upper()
    if not _is_real_user(user_id):
        print(f"[symbol_manager] Ignoring non-user subscriber "
              f"{user_id!r} for {sym}")
        return get_subscriber_count(sym)

    r = _r()

    # Track which symbols this user has
    r.sadd(user_symbols_key(user_id), sym)

    # Increment subscriber count for the symbol
    new_count = r.incr(symbol_subscriber_count(sym))

    # Add to active symbols set
    if new_count == 1:
        r.sadd(active_symbols_key(), sym)
        print(f"[symbol_manager] ACTIVATING: {sym} (first subscriber)")

    return new_count


def remove_subscriber(user_id: int, symbol: str) -> int:
    """
    Remove a user's subscription for a symbol.
    Returns the new subscriber count.
    If count hits 0, the symbol should be deactivated.
    """
    sym = symbol.upper()
    if not _is_real_user(user_id):
        return get_subscriber_count(sym)

    r = _r()

    r.srem(user_symbols_key(user_id), sym)

    new_count = max(0, r.decr(symbol_subscriber_count(sym)))

    if new_count <= 0:
        # Clean up — no subscribers left
        r.delete(symbol_subscriber_count(sym))
        r.srem(active_symbols_key(), sym)
        print(f"[symbol_manager] DEACTIVATING: {sym} (no subscribers)")

    return new_count


def get_subscriber_count(symbol: str) -> int:
    """Get current subscriber count for a symbol."""
    raw = _r().get(symbol_subscriber_count(symbol.upper()))
    return int(raw) if raw else 0


def get_active_symbols() -> set[str]:
    """Get all currently active (subscribed) symbols."""
    return _r().smembers(active_symbols_key())


def get_user_symbols(user_id: int) -> set[str]:
    """Get all symbols a user is subscribed to."""
    return _r().smembers(user_symbols_key(user_id))


def is_symbol_active(symbol: str) -> bool:
    """Check if a symbol has active subscribers."""
    return _r().sismember(active_symbols_key(), symbol.upper())


def clear_user_subscriptions(user_id: int):
    """Remove all subscriptions for a user (e.g., on bot stop)."""
    symbols = get_user_symbols(user_id)
    for sym in symbols:
        remove_subscriber(user_id, sym)
    _r().delete(user_symbols_key(user_id))


def recover_active_symbols():
    """
    Called at worker startup to reset active symbol state.

    A worker restart drops every in-memory execution manager, so NO
    bot is really running afterwards (users must press Start again).
    We therefore purge all ``user:*:symbols`` keys and rebuild an empty
    active set. Result: no market data feed (no broker WebSocket / no
    quote fetching) until a real user starts a bot.
    """
    print("[symbol_manager] Resetting active symbols (worker restart)...")
    r = _r()

    # Purge per-user subscriptions and stale subscriber counts.
    cursor = 0
    while True:
        cursor, keys = r.scan(cursor=cursor, match="user:*:symbols", count=100)
        for key in keys:
            r.delete(key)
        if cursor == 0:
            break

    for sym in r.smembers(active_symbols_key()):
        sym = sym.upper() if isinstance(sym, str) else sym.decode().upper()
        r.delete(symbol_subscriber_count(sym))

    r.delete(active_symbols_key())
    print("[symbol_manager] Active symbols reset — no feed until a bot starts")
    return set()
