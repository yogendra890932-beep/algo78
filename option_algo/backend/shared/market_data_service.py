# backend/shared/market_data_service.py
# ================================================================
# Shared Market Data Service — ONE broker WebSocket for ALL symbols.
#
# Responsibilities:
#   - Maintain ONE Upstox WebSocket connection for all active symbols
#   - Receive live ticks from Upstox feed
#   - Validate and normalize market data
#   - Publish ticks to Redis Stream (tick buffer)
#   - Publish ticks to Redis Pub/Sub (real-time)
#   - Maintain latest tick snapshot in Redis Hash
#   - Auto-reconnect on disconnect
#   - Lifecycle managed via symbol_manager reference counts
# ================================================================

import json
import threading
import time
from datetime import datetime
from typing import Optional, Dict, Callable

import upstox_client

from backend.shared.redis_infra import (
    shared_tick_stream,
    shared_tick_buffer,
    shared_tick_channel,
    TICK_STREAM_MAXLEN,
)
from backend.shared.shared_cache import get_streamer_token
from backend.services.redis_client import get_redis_sync


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


class _GlobalStreamer:
    """Singleton Upstox WebSocket connection shared across all symbols."""
    
    _instance: Optional["_GlobalStreamer"] = None
    _lock = threading.Lock()
    
    def __new__(cls, access_token: str):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._initialized = False
            return cls._instance
    
    def __init__(self, access_token: str):
        if self._initialized:
            return
        self.access_token = access_token
        self._streamer: Optional[object] = None
        self._streamer_lock = threading.Lock()
        self._connected = threading.Event()
        self._stop_event = threading.Event()
        self._subscribed_tokens: set = set()
        self._tokens_lock = threading.Lock()
        self._tick_callbacks: Dict[str, Callable] = {}
        self._callbacks_lock = threading.Lock()
        self._thread: Optional[threading.Thread] = None
        self._initialized = True
    
    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._run, daemon=True, name="global-md-streamer")
        self._thread.start()
        print(f"{_now()} [global_md] Starting global streamer")
    
    def stop(self):
        self._stop_event.set()
        with self._streamer_lock:
            if self._streamer:
                try:
                    self._streamer.disconnect()
                except:
                    pass
        print(f"{_now()} [global_md] Stopped")
    
    def subscribe_token(self, token: str, callback: Callable):
        """Subscribe to a token's ticks and register callback."""
        with self._tokens_lock:
            if token not in self._subscribed_tokens:
                self._subscribed_tokens.add(token)
                if self._streamer and self._connected.is_set():
                    try:
                        self._streamer.subscribe([token], "full")
                        print(f"{_now()} [global_md] Subscribed: {token}")
                    except Exception as e:
                        print(f"{_now()} [global_md] Subscribe err for {token}: {e}")
        
        with self._callbacks_lock:
            self._tick_callbacks[token] = callback
    
    def unsubscribe_token(self, token: str):
        """Unsubscribe a token."""
        with self._tokens_lock:
            self._subscribed_tokens.discard(token)
        with self._callbacks_lock:
            self._tick_callbacks.pop(token, None)
        
        if self._streamer and self._connected.is_set():
            try:
                self._streamer.unsubscribe([token])
                print(f"{_now()} [global_md] Unsubscribed: {token}")
            except Exception as e:
                print(f"{_now()} [global_md] Unsubscribe err for {token}: {e}")
    
    def _run(self):
        while not self._stop_event.is_set():
            try:
                self._connect_and_stream()
            except Exception as e:
                print(f"{_now()} [global_md] Stream error: {e}")
            finally:
                with self._streamer_lock:
                    self._streamer = None
                    self._connected.clear()
            
            if not self._stop_event.is_set():
                print(f"{_now()} [global_md] Reconnecting in 3s...")
                time.sleep(3)
    
    def _connect_and_stream(self):
        cfg = upstox_client.Configuration()
        cfg.access_token = self.access_token
        print(f"{_now()} [global_md] Connecting...")
        streamer = upstox_client.MarketDataStreamerV3(upstox_client.ApiClient(cfg))
        with self._streamer_lock:
            self._streamer = streamer
        
        connection_closed = threading.Event()
        
        def on_open():
            self._connected.set()
            print(f"{_now()} [global_md] Stream open")
            with self._tokens_lock:
                tokens = list(self._subscribed_tokens)
            if tokens:
                try:
                    streamer.subscribe(tokens, "full")
                    print(f"{_now()} [global_md] Subscribed {len(tokens)} tokens")
                except Exception as e:
                    print(f"{_now()} [global_md] Subscribe err: {e}")
        
        def on_message(msg):
            try:
                feeds = msg.get("feeds", {})
                for token, feed in feeds.items():
                    if isinstance(feed, dict):
                        full = feed.get("fullFeed", {})
                        if isinstance(full, dict):
                            with self._callbacks_lock:
                                callback = self._tick_callbacks.get(token)
                            if callback:
                                callback(token, full)
            except Exception as e:
                print(f"{_now()} [global_md] msg err: {e}")
        
        def on_error(e):
            print(f"{_now()} [global_md] Stream err: {e}")
        
        def on_close(code, reason):
            self._connected.clear()
            connection_closed.set()
            print(f"{_now()} [global_md] Stream closed: {code}")
        
        streamer.on("open", on_open)
        streamer.on("message", on_message)
        streamer.on("error", on_error)
        streamer.on("close", on_close)
        
        streamer.connect()
        connected = self._connected.wait(timeout=10)
        if not connected:
            raise TimeoutError("WebSocket connection timeout")
        connection_closed.wait()


_global_streamer: Optional[_GlobalStreamer] = None
_streamer_init_lock = threading.Lock()


def _get_global_streamer(access_token: str) -> _GlobalStreamer:
    """Get or create the global streamer singleton."""
    global _global_streamer
    with _streamer_init_lock:
        if _global_streamer is None:
            _global_streamer = _GlobalStreamer(access_token)
            _global_streamer.start()
        return _global_streamer


class SharedMarketDataService:
    """
    Per-symbol shared market data feed. All symbols share ONE
    underlying WebSocket connection via _GlobalStreamer.
    """

    _instances: Dict[str, "SharedMarketDataService"] = {}
    _instances_lock = threading.Lock()

    def __init__(self, symbol: str, access_token: str):
        self.symbol = symbol.upper()
        self.access_token = access_token
        self._streamer = _get_global_streamer(access_token)
        self._token = get_streamer_token(self.symbol)
        print(f"{_now()} [shared_md:{self.symbol}] Token received: {access_token[:20]}...{access_token[-10:] if len(access_token) > 30 else '***'}")
        print(f"{_now()} [shared_md:{self.symbol}] Instrument token: {self._token}")
        self._additional_tokens: set = set()
        self._tokens_lock = threading.Lock()
        self._last_tick_log: float = 0.0

    def start(self):
        self._streamer.subscribe_token(self._token, self._on_tick)
        print(f"{_now()} [shared_md:{self.symbol}] Started")

    def stop(self):
        with self._tokens_lock:
            all_tokens = {self._token} | self._additional_tokens.copy()
        for token in all_tokens:
            self._streamer.unsubscribe_token(token)
        print(f"{_now()} [shared_md:{self.symbol}] Stopped")
        with self._instances_lock:
            self._instances.pop(self.symbol, None)

    def subscribe_option(self, instrument_key: str):
        with self._tokens_lock:
            if instrument_key not in self._additional_tokens:
                self._additional_tokens.add(instrument_key)
                self._streamer.subscribe_token(instrument_key, self._on_tick)

    def unsubscribe_option(self, instrument_key: str):
        with self._tokens_lock:
            if instrument_key in self._additional_tokens:
                self._additional_tokens.discard(instrument_key)
                self._streamer.unsubscribe_token(instrument_key)

    def _on_tick(self, token: str, feed: dict):
        """Process tick and publish to Redis."""
        r = get_redis_sync()
        now = datetime.now()
        now_str = now.strftime("%Y-%m-%d %H:%M:%S.%f")[:23]
        timestamp = now.timestamp()

        ltp = None
        ltq = 0.0
        full = feed.get("fullFeed", feed)
        if isinstance(full, dict):
            if full.get("marketFF", {}).get("ltpc"):
                ltpc = full["marketFF"]["ltpc"]
                ltp = ltpc.get("ltp")
                ltq = ltpc.get("ltq") or 0.0
            elif full.get("indexFF", {}).get("ltpc"):
                ltp = full["indexFF"]["ltpc"].get("ltp")
            elif full.get("ltp"):
                ltp = full["ltp"]

        if ltp is None:
            return

        tick = {
            "symbol": self.symbol,
            "token": token,
            "ltp": float(ltp),
            "ltq": float(ltq) if ltq else 0.0,
            "ts": now_str,
            "timestamp": timestamp,
        }

        now_t = time.time()
        if now_t - self._last_tick_log >= 5:
            self._last_tick_log = now_t
            print(f"{_now()} [shared_md:{self.symbol}] tick LTP={tick['ltp']} LTQ={tick['ltq']}")

        try:
            r.xadd(shared_tick_stream(self.symbol), {"data": json.dumps(tick)}, maxlen=TICK_STREAM_MAXLEN)
            r.hset(shared_tick_buffer(self.symbol), key=token, value=json.dumps(tick, default=str))
            r.publish(shared_tick_channel(self.symbol), json.dumps(tick, default=str))
        except Exception as e:
            print(f"{_now()} [shared_md:{self.symbol}] Redis err: {e}")

    @classmethod
    def get_or_create(cls, symbol: str, access_token: str) -> "SharedMarketDataService":
        with cls._instances_lock:
            if symbol.upper() not in cls._instances:
                svc = cls(symbol, access_token)
                cls._instances[symbol.upper()] = svc
                svc.start()
            return cls._instances[symbol.upper()]

    @classmethod
    def stop_all(cls):
        global _global_streamer
        with cls._instances_lock:
            for svc in list(cls._instances.values()):
                svc.stop()
            cls._instances.clear()
        if _global_streamer:
            _global_streamer.stop()
            _global_streamer = None

    @classmethod
    def active_symbols(cls) -> list:
        with cls._instances_lock:
            return list(cls._instances.keys())


def read_latest_tick(symbol: str, token: str) -> Optional[dict]:
    raw = get_redis_sync().hget(shared_tick_buffer(symbol), token)
    if raw:
        return json.loads(raw)
    return None


def read_all_latest_ticks(symbol: str) -> Dict[str, dict]:
    raw = get_redis_sync().hgetall(shared_tick_buffer(symbol))
    return {k: json.loads(v) for k, v in raw.items()}
