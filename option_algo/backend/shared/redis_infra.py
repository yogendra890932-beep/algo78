# backend/shared/redis_infra.py
# ================================================================
# Redis Infrastructure — Central Channel/Key/Stream naming and helpers.
#
# This module defines ALL Redis key patterns, channel names, and stream
# names used across the shared architecture. Every module references
# these constants rather than hardcoding strings, ensuring consistency.
#
# Key Pattern Convention:
#   shared:market:{symbol}:*     — Shared market data (ticks, candles, indicators, structure)
#   shared:oc:{symbol}:{expiry}  — Shared option chain data
#   shared:signal:{symbol}       — Shared signal channels
#   user:{user_id}:*             — Per-user data (orders, positions, P&L, settings)
#   sys:*                        — System-wide state (workers, health, metrics)
# ================================================================

from typing import Optional
from backend.services.redis_client import get_redis_sync

# ================================================================
# SHARED MARKET DATA KEYS
# ================================================================

def shared_tick_stream(symbol: str) -> str:
    """Redis Stream: live tick data for a symbol."""
    return f"shared:tick:{symbol}"

def shared_tick_buffer(symbol: str) -> str:
    """Redis Hash: latest tick snapshot for a symbol."""
    return f"shared:tick_buffer:{symbol}"

def shared_candles_1m(symbol: str) -> str:
    """Redis JSON key: 1-minute candles DataFrame (serialized)."""
    return f"shared:candles:1m:{symbol}"

def shared_candles_5m(symbol: str) -> str:
    """Redis JSON key: 5-minute candles DataFrame (serialized)."""
    return f"shared:candles:5m:{symbol}"

def shared_candle_current_1m(symbol: str) -> str:
    """Redis Hash: current developing 1-minute candle (OHLCV)."""
    return f"shared:candle:cur_1m:{symbol}"

def shared_candle_current_5m(symbol: str) -> str:
    """Redis Hash: current developing 5-minute candle (OHLCV)."""
    return f"shared:candle:cur_5m:{symbol}"

def shared_premium_candles_1m(symbol: str) -> str:
    """Redis JSON key: option-premium 1-minute candles (serialized)."""
    return f"shared:premium:candles:1m:{symbol}"

def shared_premium_current_1m(symbol: str) -> str:
    """Redis Hash: current developing option-premium 1-minute candle (OHLCV)."""
    return f"shared:premium:candle:cur_1m:{symbol}"

def shared_premium_close_channel(symbol: str) -> str:
    """Redis Pub/Sub channel: option-premium candle-close notifications."""
    return f"shared:premium_close:{symbol}"

def shared_premium_state(symbol: str) -> str:
    """Redis Hash: currently selected option (instrument_key, opt_type, strike, expiry)."""
    return f"shared:premium:state:{symbol}"

# ================================================================
# SHARED INDICATOR KEYS
# ================================================================

def shared_indicators(symbol: str) -> str:
    """Redis Hash: indicator values for a symbol."""
    return f"shared:indicators:{symbol}"

# Cached historical indicator values for the full candle series
def shared_indicators_1m(symbol: str) -> str:
    return f"shared:indicators:1m:{symbol}"

def shared_indicators_5m(symbol: str) -> str:
    return f"shared:indicators:5m:{symbol}"

# ================================================================
# SHARED VWAP KEY
# ================================================================

def shared_vwap(symbol: str) -> str:
    """Redis key: VWAP value for today."""
    return f"shared:vwap:{symbol}"

# ================================================================
# SHARED MARKET STRUCTURE KEYS
# ================================================================

def shared_market_structure_1m(symbol: str) -> str:
    """Redis JSON key: 1-minute premium market structure result."""
    return f"shared:structure:1m:{symbol}"

def shared_market_structure_5m(symbol: str) -> str:
    """Redis JSON key: 5-minute underlying market structure result."""
    return f"shared:structure:5m:{symbol}"

def shared_market_structure_state(symbol: str, engine: str) -> str:
    """Redis key: internal engine state for recovery (engine='1m'|'5m')."""
    return f"shared:structure:state:{engine}:{symbol}"

# ================================================================
# SHARED OPTION CHAIN KEYS
# ================================================================

def shared_oc_analysis(symbol: str, expiry: str) -> str:
    """Redis JSON key: latest option chain analysis."""
    return f"shared:oc:analysis:{symbol}:{expiry}"

def shared_oc_chain_df(symbol: str, expiry: str) -> str:
    """Redis JSON key: latest option chain DataFrame snapshot."""
    return f"shared:oc:chain_df:{symbol}:{expiry}"

# ================================================================
# SHARED HISTORICAL DATA KEYS
# ================================================================

def shared_historical_1m(symbol: str) -> str:
    """Redis key: shared previous-day 1-min historical candles."""
    return f"shared:historical:1m:{symbol}"

def shared_historical_5m(symbol: str) -> str:
    """Redis key: shared previous-day 5-min historical candles."""
    return f"shared:historical:5m:{symbol}"

# ================================================================
# SHARED SIGNAL BUS CHANNELS
# ================================================================

def shared_signal_channel(symbol: str) -> str:
    """Redis Pub/Sub channel: trading signals for a symbol."""
    return f"shared:signal:{symbol}"

def shared_signal_stream(symbol: str) -> str:
    """Redis Stream: trading signal history (replayable)."""
    return f"shared:signal_stream:{symbol}"

# ================================================================
# SHARED MARKET DATA CHANNELS
# ================================================================

def shared_tick_channel(symbol: str) -> str:
    """Redis Pub/Sub channel: live tick publications."""
    return f"shared:tick_channel:{symbol}"

def shared_candle_close_channel(symbol: str) -> str:
    """Redis Pub/Sub channel: candle-close notifications."""
    return f"shared:candle_close:{symbol}"

def shared_indicator_update_channel(symbol: str) -> str:
    """Redis Pub/Sub channel: indicator update notifications."""
    return f"shared:indicator_update:{symbol}"

# ================================================================
# REFERENCE COUNTER KEYS
# ================================================================

def ref_counter_key(resource_type: str, identifier: str) -> str:
    """
    Redis key for reference counting shared resources.
    resource_type: 'ws', 'candle_builder', 'indicators', 'structure', 'oc', 'strategy'
    identifier: typically the symbol (e.g., 'NIFTY', 'BANKNIFTY')
    """
    return f"shared:ref:{resource_type}:{identifier}"

def ref_counter_timeout_key(resource_type: str, identifier: str) -> str:
    """Key tracking when to destroy a resource after ref count hits 0."""
    return f"shared:ref_timeout:{resource_type}:{identifier}"

# ================================================================
# DISTRIBUTED LOCK KEYS
# ================================================================

def lock_key(task: str, identifier: str = "") -> str:
    """Redis lock key for distributed task coordination."""
    base = f"shared:lock:{task}"
    return f"{base}:{identifier}" if identifier else base

# ================================================================
# SYMBOL SUBSCRIPTION KEYS
# ================================================================

def active_symbols_key() -> str:
    """Redis Set: currently active (subscribed) symbols."""
    return "shared:active_symbols"

def symbol_subscriber_count(symbol: str) -> str:
    """Redis key: number of users subscribing to a symbol."""
    return f"shared:subscribers:{symbol}"

def user_symbols_key(user_id: int) -> str:
    """Redis Set: symbols a user is currently trading."""
    return f"user:{user_id}:symbols"

# ================================================================
# PER-USER KEYS
# ================================================================

def user_execution_state(user_id: int) -> str:
    """Redis Hash: user's execution state (positions, risk, mode)."""
    return f"user:{user_id}:execution"

def user_position_snapshot(user_id: int, symbol: str) -> str:
    """Redis Hash: per-symbol position snapshot for a user."""
    return f"user:{user_id}:position:{symbol}"

def user_risk_snapshot(user_id: int) -> str:
    """Redis Hash: daily risk counters for a user."""
    return f"user:{user_id}:risk"

# ================================================================
# WORKER / SYSTEM KEYS
# ================================================================

def worker_heartbeat(worker_type: str, worker_id: str = "") -> str:
    """Redis key: worker heartbeat (TTL-based liveness check)."""
    wid = worker_id or "default"
    return f"sys:worker:{worker_type}:{wid}"

def worker_health_key() -> str:
    """Redis Set: all registered worker IDs."""
    return "sys:workers:registered"

def metrics_key(metric_name: str) -> str:
    """Redis key: system metric."""
    return f"sys:metrics:{metric_name}"

# ================================================================
# CHANNEL WILDCARD HELPERS
# ================================================================

def all_signal_channels() -> str:
    """Pattern to subscribe to ALL signal channels."""
    return "shared:signal:*"

def all_tick_channels() -> str:
    """Pattern to subscribe to ALL tick channels."""
    return "shared:tick_channel:*"

# ================================================================
# TTL CONSTANTS
# ================================================================

TICK_STREAM_MAXLEN    = 10000    # Max tick entries in stream
CANDLE_TTL_SEC        = 900      # 15 min for candle snapshots
INDICATOR_TTL_SEC     = 120      # 2 min for indicator snapshots
STRUCTURE_TTL_SEC     = 300      # 5 min for market structure results
OC_ANALYSIS_TTL_SEC   = 120      # 2 min for option chain analysis
HISTORICAL_TTL_SEC    = 86400    # 24h for shared historical data
HEARTBEAT_TTL_SEC     = 30       # Worker heartbeat TTL
REF_COUNTER_TTL_SEC   = 86400    # 24h for ref counters
EXECUTION_STATE_TTL   = 300      # 5 min for user execution state
