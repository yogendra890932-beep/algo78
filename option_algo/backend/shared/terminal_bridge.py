# backend/shared/terminal_bridge.py
# ================================================================
# Legacy-mode Terminal Data Bridge
#
# The Trading Terminal (backend/services/terminal_service.py +
# backend/services/terminal_ws.py) reads the SHARED Redis keys and
# channels:
#
#   shared:tick_buffer:{symbol} / shared:tick_channel:{symbol}
#   shared:candles:1m|5m:{symbol}
#   shared:premium:*:{symbol}   (state / candles / current / close channel)
#   shared:indicators:*:{symbol}
#   shared:structure:1m|5m:{symbol}
#
# In SHARED mode the SharedWorkerOrchestrator runs the symbol-level
# data services that write those keys. In LEGACY mode the per-user
# engine (engine_v6) keeps all of that data in-memory and never
# writes it to Redis — so the terminal had no live data.
#
# This bridge fixes that: in LEGACY mode it starts the SAME shared
# symbol-level DATA services (market data, candle builder, option
# premium builder, indicators, market structure) alongside each
# running legacy bot, so the terminal works identically in both modes.
#
# It deliberately does NOT start SharedStrategyEngine or any user
# execution manager — in legacy mode execution stays entirely with
# the legacy BotThread/engine_v6.
# ================================================================

import threading

from backend.config import is_shared_mode

_lock = threading.Lock()
# symbol -> number of running legacy bots that want live data for it
_refcounts: dict[str, int] = {}
# user_id -> [symbols] — the symbols a legacy bot registered
_user_symbols: dict[int, list[str]] = {}


def _log(msg: str):
    print(f"{__name__}: {msg}")


def _symbols_from_config(config: dict) -> list:
    """Main + extra symbols a bot config wants."""
    out = []
    main = (config.get("underlying_symbol") or "NIFTY").upper()
    out.append(main)
    for s in (config.get("extra_symbols") or "").split(","):
        s = s.strip().upper()
        if s and s not in out:
            out.append(s)
    return out


def ensure_for_user(user_id: int, config: dict, access_token: str):
    """Start shared symbol data for a legacy bot (refcounted)."""
    if is_shared_mode():
        return  # shared worker already owns the data pipeline
    symbols = _symbols_from_config(config)
    fresh = []
    with _lock:
        _user_symbols[user_id] = symbols
        for sym in symbols:
            _refcounts[sym] = _refcounts.get(sym, 0) + 1
            if _refcounts[sym] == 1:
                fresh.append(sym)
    # Service init does warm-up network calls (Upstox history etc.) — run
    # in a daemon thread so the worker's command loop is never blocked.
    if fresh:
        threading.Thread(
            target=_start_services_batch, args=(fresh, access_token),
            daemon=True, name="term-bridge-start",
        ).start()
    _log(f"registered user {user_id} symbols={symbols}")


def _start_services_batch(symbols: list, access_token: str):
    for sym in symbols:
        try:
            _start_services(sym, access_token)
        except Exception as e:
            _log(f"start batch failed for {sym}: {e}")


def release_for_user(user_id: int):
    """Stop shared symbol data when a legacy bot stops (refcounted)."""
    if is_shared_mode():
        return
    with _lock:
        symbols = _user_symbols.pop(user_id, [])
        stopped = []
        for sym in symbols:
            n = _refcounts.get(sym, 0) - 1
            if n <= 0:
                _refcounts.pop(sym, None)
                stopped.append(sym)
            else:
                _refcounts[sym] = n
    for sym in stopped:
        _stop_services(sym)
    _log(f"released user {user_id} stopped={stopped}")


def stop_all():
    """Stop every shared data service started by this bridge (worker shutdown)."""
    with _lock:
        symbols = set(_refcounts.keys())
        _refcounts.clear()
        _user_symbols.clear()
    for sym in symbols:
        _stop_services(sym)
    _log(f"stop_all: stopped {len(symbols)} symbol pipeline(s)")


def _service_classes():
    from backend.shared.market_data_service import SharedMarketDataService
    from backend.shared.candle_builder import SharedCandleBuilder
    from backend.shared.option_premium_service import SharedOptionPremiumBuilder
    from backend.shared.indicator_engine import SharedIndicatorEngine
    from backend.shared.market_structure_engine import (
        SharedMarketStructureEngine,
        SharedUnderlyingMarketStructureEngine,
    )

    return [
        (SharedMarketDataService, True),
        (SharedCandleBuilder, True),
        (SharedOptionPremiumBuilder, True),
        (SharedIndicatorEngine, False),
        (SharedMarketStructureEngine, False),
        (SharedUnderlyingMarketStructureEngine, False),
    ]


def _start_services(symbol: str, access_token: str):
    """Idempotent — starts the shared symbol-level data services."""
    for cls, needs_token in _service_classes():
        try:
            if needs_token:
                cls.get_or_create(symbol, access_token)
            else:
                cls.get_or_create(symbol)
        except Exception as e:
            _log(f"start {cls.__name__} for {symbol} failed: {e}")
    _log(f"shared symbol data started for {symbol}")


def _stop_services(symbol: str):
    """Stop the shared symbol-level data services for one symbol."""
    for cls, _ in _service_classes():
        try:
            with cls._instances_lock:
                inst = cls._instances.pop(symbol.upper(), None)
            if inst:
                inst.stop()
        except Exception as e:
            _log(f"stop {cls.__name__} for {symbol} failed: {e}")
