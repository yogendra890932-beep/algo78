# backend/shared/option_chain_service.py
# ================================================================
# Shared Option Chain Service — ONE per symbol/expiry.
#
# Replaces per-user OptionChainAnalyzer instances.
# One service polls the option chain and publishes results to Redis.
# All users trading the same symbol/expiry read the same data.
# ================================================================

import json
import threading
import time
from datetime import datetime
from typing import Optional

import pandas as pd
import upstox_client
from upstox_client.rest import ApiException

from backend.shared.redis_infra import (
    shared_oc_analysis,
    shared_oc_chain_df,
    OC_ANALYSIS_TTL_SEC,
)
from backend.shared.dist_locks import acquire_lock_wait, release_lock
from backend.services.redis_client import get_redis_sync


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


class SharedOptionChainService:
    """
    Maintains ONE OptionChainAnalyzer per symbol/expiry.
    Polls Upstox option chain every 30s.
    Stores results in Redis for all users to read.
    """

    REFRESH_SEC = 30

    def __init__(self, symbol: str, underlying_key: str,
                 expiry: str, access_token: str):
        self.symbol = symbol.upper()
        self.underlying_key = underlying_key
        self.expiry = expiry
        self.access_token = access_token
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._r = get_redis_sync()

        self._oc_analyzer: Optional[object] = None
        self._latest_analysis: Optional[dict] = None
        self._latest_chain_df: Optional[pd.DataFrame] = None
        self._lock = threading.Lock()

    def start(self):
        if self._thread and self._thread.is_alive():
            return

        self._init_analyzer()

        self._thread = threading.Thread(
            target=self._loop, daemon=True,
            name=f"oc-{self.symbol}-{self.expiry}",
        )
        self._thread.start()
        print(f"{_now()} [oc:{self.symbol}|{self.expiry}] Started")

    def stop(self):
        self._stop_event.set()
        if self._oc_analyzer and hasattr(self._oc_analyzer, 'stop'):
            try:
                self._oc_analyzer.stop()
            except Exception:
                pass
        with self._lock:
            self._latest_chain_df = None
            self._latest_analysis = None
        self._oc_analyzer = None
        print(f"{_now()} [oc:{self.symbol}|{self.expiry}] Stopped")

    def _init_analyzer(self):
        """Initialize the OptionChainAnalyzer."""
        try:
            from backend.engine.option_chain import OptionChainAnalyzer
            stop = threading.Event()
            self._oc_analyzer = OptionChainAnalyzer(
                symbol=self.symbol,
                underlying_key=self.underlying_key,
                expiry=self.expiry,
                access_token=self.access_token,
                stop_event=stop,
                on_update=None,
            )
        except Exception as e:
            print(f"{_now()} [oc:{self.symbol}|{self.expiry}] Init err: {e}")

    def _loop(self):
        """Poll option chain and store results in Redis."""
        while not self._stop_event.is_set():
            try:
                self._refresh()
            except Exception as e:
                print(f"{_now()} [oc:{self.symbol}|{self.expiry}] err: {e}")
            time.sleep(self.REFRESH_SEC)

    def _refresh(self):
        """Fetch and analyze option chain, store in Redis."""
        if self._oc_analyzer is None:
            return

        # Only one service refreshes per symbol/expiry
        lock_id = f"oc:{self.symbol}:{self.expiry}"
        if not acquire_lock_wait(lock_id, "", ttl=self.REFRESH_SEC, timeout=5):
            return

        try:
            df = self._oc_analyzer.fetch_option_chain()
            if df.empty:
                return

            if not hasattr(self._oc_analyzer, 'history') or len(self._oc_analyzer.history) == 0:
                self._oc_analyzer.history.append({"timestamp": _now(), "df": df.copy()})
                return

            prev_df = self._oc_analyzer.history[-1]["df"]
            result = self._oc_analyzer.analyze(df, prev_df)

            with self._lock:
                self._oc_analyzer.latest_analysis = result
                self._oc_analyzer.history.append({
                    "timestamp": _now(), "df": df.copy(), "analysis": result
                })
                if len(self._oc_analyzer.history) > 10:
                    self._oc_analyzer.history = self._oc_analyzer.history[-10:]
                self._latest_analysis = result
                self._latest_chain_df = df

            # Store in Redis
            self._r.set(shared_oc_analysis(self.symbol, self.expiry),
                        json.dumps(result, default=str),
                        ex=OC_ANALYSIS_TTL_SEC)

            # Store chain DataFrame snapshot
            chain_records = None
            if df is not None and not df.empty:
                chain_records = df[[
                    "strike", "ce_oi", "ce_volume", "ce_ltp",
                    "pe_oi", "pe_volume", "pe_ltp"
                ]].to_dict("records")
            if chain_records:
                self._r.set(shared_oc_chain_df(self.symbol, self.expiry),
                            json.dumps(chain_records, default=str),
                            ex=OC_ANALYSIS_TTL_SEC)

            print(f"{_now()} [oc:{self.symbol}|{self.expiry}] "
                  f"Signal={result.get('signal','?')} Score={result.get('flow_score','?')}")
        finally:
            release_lock(lock_id)

    def is_bullish(self) -> Optional[bool]:
        """True=CE bias, False=PE bias, None=neutral/no OC filter."""
        with self._lock:
            if not self._latest_analysis:
                return None
            sig = self._latest_analysis.get("signal", "")
            if "BULLISH" in sig:
                return True
            if "BEARISH" in sig:
                return False
            return None

    # ================================================================
    # STATIC READERS
    # ================================================================

    @staticmethod
    def get_analysis(symbol: str, expiry: str) -> Optional[dict]:
        """Get latest option chain analysis from Redis."""
        raw = get_redis_sync().get(shared_oc_analysis(symbol.upper(), expiry))
        if raw:
            return json.loads(raw)
        return None

    @staticmethod
    def get_chain_df(symbol: str, expiry: str) -> Optional[list]:
        """Get latest option chain DataFrame records from Redis."""
        raw = get_redis_sync().get(shared_oc_chain_df(symbol.upper(), expiry))
        if raw:
            return json.loads(raw)
        return None

    @staticmethod
    def is_bullish_from_redis(symbol: str, expiry: str) -> Optional[bool]:
        """Check OC bias from Redis (True=CE, False=PE, None=neutral)."""
        analysis = SharedOptionChainService.get_analysis(symbol, expiry)
        if not analysis:
            return None
        sig = analysis.get("signal", "")
        if "BULLISH" in sig:
            return True
        if "BEARISH" in sig:
            return False
        return None

    # ================================================================
    # INSTANCE MANAGEMENT
    # ================================================================

    _instances: dict[str, "SharedOptionChainService"] = {}
    _instances_lock = threading.Lock()

    @classmethod
    def _instance_key(cls, symbol: str, expiry: str) -> str:
        return f"{symbol.upper()}:{expiry}"

    @classmethod
    def get_or_create(cls, symbol: str, underlying_key: str,
                      expiry: str, access_token: str) -> "SharedOptionChainService":
        key = cls._instance_key(symbol, expiry)
        with cls._instances_lock:
            if key not in cls._instances:
                svc = cls(symbol.upper(), underlying_key, expiry, access_token)
                cls._instances[key] = svc
                svc.start()
            return cls._instances[key]

    @classmethod
    def stop_symbol_expiry(cls, symbol: str, expiry: str):
        key = cls._instance_key(symbol, expiry)
        with cls._instances_lock:
            svc = cls._instances.pop(key, None)
            if svc:
                svc.stop()

    def update_access_token(self, access_token: str) -> bool:
        """Hot-swap this service's broker token (no restart)."""
        if not access_token:
            return False
        self.access_token = access_token
        return True

    @classmethod
    def update_access_token_for_symbol(cls, symbol: str, access_token: str) -> int:
        """Hot-swap the token on all option-chain instances for a symbol."""
        prefix = f"{symbol.upper()}:"
        with cls._instances_lock:
            svcs = [s for k, s in cls._instances.items() if k.startswith(prefix)]
        n = 0
        for svc in svcs:
            if svc.update_access_token(access_token):
                n += 1
        return n

    @classmethod
    def stop_all(cls):
        with cls._instances_lock:
            for svc in list(cls._instances.values()):
                svc.stop()
            cls._instances.clear()
