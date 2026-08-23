# worker.py
# ================================================================
# AlgoBot WORKER PROCESS
#
# Runs SEPARATELY from the web (uvicorn/FastAPI) process. This is
# the ONLY process that holds live SymbolEngine / BotThread
# instances. It communicates with the web process(es) exclusively
# via Redis:
#
#   bot:commands          <- web pushes start/stop/modify commands
#   bot:cmd_result:{id}   -> worker posts command results
#   bot:status:{user}     -> worker heartbeats running bot status
#   bot:positions:{user}  -> SymbolEngine pushes live position snapshots
#   bot:oc:{user}         -> OptionChainAnalyzer pushes OC snapshots
#   events:{user}         -> worker publishes ENTRY/EXIT/SL_TRAIL/BOT_STATUS
#
# Two operating modes (controlled by USE_SHARED_WORKER in .env):
#
#   LEGACY  (USE_SHARED_WORKER=False, default off)
#     Creates one BotThread per user with full TradingEngine.
#     All market data and computations duplicated per user.
#
#   SHARED  (USE_SHARED_WORKER=True)
#     Uses SharedWorkerOrchestrator: one shared pipeline per symbol,
#     lightweight UserExecutionManager per user.
#     Market data / candles / indicators / signals computed once.
#
# Run with:
#   python worker.py
# ================================================================

import asyncio
import logging
import signal
import sys
import threading
import time
import traceback
from datetime import datetime, timezone

from sqlalchemy import select

from backend.config import get_settings
from backend.db.database import init_db, AsyncSessionLocal, close_db
from backend.db.models import BotConfig, BotStatus, Trade, TradeStatus
from backend.services.bot_manager import bot_manager, set_main_loop
from backend.services.bot_config_builder import resolve_start_inputs
from backend.services.command_queue import pop_command_sync, post_result_sync
from backend.services.event_bus import publish_event_sync
from backend.services.state_store import set_bot_status_sync, clear_bot_status_sync

# ── Structured logging ───────────────────────────────────────────
logger = logging.getLogger("worker")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [Worker] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

settings = get_settings()
USE_SHARED = settings.USE_SHARED_WORKER
USE_SHARED_MD = settings.USE_SHARED_MARKET_DATA and USE_SHARED
USE_SHARED_STRAT = settings.USE_SHARED_STRATEGY and USE_SHARED
USE_SHARED_WS = settings.USE_SHARED_WEBSOCKET and USE_SHARED

_now = lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S")

HEARTBEAT_SEC = 8

# Pending trades are auto-deleted from the pending-trade history after
# this many seconds (approval window is 30s; history kept briefly longer).
PENDING_TRADE_HISTORY_TTL_SEC = 60


def _log(msg: str):
    logger.info(msg)


# ================================================================
# ENGINE CALLBACKS  (called synchronously from BotThread / shared)
# ================================================================

from backend.services.push_notifications import send_push_sync


def _push(user_id: int, title: str, body: str, data: dict = None, tag: str = None):
    """Best-effort push — never let a notification failure break trading."""
    try:
        send_push_sync(user_id, title, body, data, tag)
    except Exception as e:
        _log(f"[push] error for user {user_id}: {e}")


def _send_pending_trade_telegram_alert(user_id: int, trade_data: dict):
    """
    Send a Telegram notification for a PENDING_TRADE event.
    Fetches the user's bot token and chat ID from BotConfig.
    Runs synchronously — safe to call from any thread.
    """
    try:
        from backend.db.database import get_sync_session
        from sqlalchemy import select
        
        with get_sync_session() as db:
            res = db.execute(
                select(BotConfig).where(BotConfig.user_id == user_id)
            )
            cfg = res.scalar_one_or_none()
            if not cfg:
                _log(f"No BotConfig for user {user_id} - cannot send pending trade alert")
                return
            
            bot_token = cfg.telegram_bot_token or ""
            chat_id = cfg.telegram_chat_id or ""

        if not bot_token or not chat_id:
            _log(f"No Telegram config for user {user_id} - cannot send pending trade alert")
            return

        from backend.services import telegram_alerts as tg
        trade_id = trade_data.get("pending_trade_id")
        symbol = trade_data.get("trading_symbol") or trade_data.get("symbol", "")
        opt_type = trade_data.get("opt_type", "")
        quantity = trade_data.get("quantity", 0)
        strategy = trade_data.get("strategy", "")
        confidence = trade_data.get("confidence")
        expires_at = trade_data.get("expires_at")
        trading_symbol = trade_data.get("trading_symbol")

        if trade_id:
            tg.alert_pending_trade(
                bot_token, chat_id,
                trade_id=trade_id,
                symbol=symbol,
                opt_type=opt_type,
                quantity=quantity,
                strategy=strategy,
                confidence=confidence,
                expires_at=expires_at,
                trading_symbol=trading_symbol or symbol,
            )
            _log(f"Telegram pending trade alert sent for trade #{trade_id}")
    except Exception as e:
        _log(f"Failed to send Telegram pending trade alert: {e}")


async def _on_status_change(user_id: int, status: BotStatus, error_msg: str = None):
    """
    Called on bot crash (status=error). Persists to DB, updates
    the Redis status snapshot, publishes BOT_STATUS event.
    """
    async with AsyncSessionLocal() as db:
        res = await db.execute(select(BotConfig).where(BotConfig.user_id == user_id))
        cfg = res.scalar_one_or_none()
        if cfg:
            cfg.status    = status
            cfg.error_msg = error_msg
            if status == BotStatus.running:
                cfg.last_started = datetime.now(timezone.utc).replace(tzinfo=None)
            else:
                cfg.last_stopped = datetime.now(timezone.utc).replace(tzinfo=None)
            db.add(cfg)
            await db.commit()

    set_bot_status_sync(user_id, status.value, error_msg)
    publish_event_sync(user_id, {
        "event": "BOT_STATUS", "status": status.value, "error": error_msg,
    })

    if status == BotStatus.error and error_msg:
        _push(user_id, "Bot Stopped — Error",
              error_msg[:180], data={"event": "BOT_STATUS"}, tag="bot-status")


async def _on_trade(user_id: int, trade_data: dict):
    """
    Called on ENTRY / EXIT / SL_TRAIL / ORDER_ALERT events.
    - Non-EXIT: publish immediately (no DB write needed)
    - EXIT: save to DB first, THEN publish (so dashboard query finds it)
    - Sends Web Push for ENTRY/EXIT/ORDER_ALERT.
    """
    event = trade_data.get("event")
    mode  = trade_data.get("mode", "live")

    if event != "EXIT":
        publish_event_sync(user_id, trade_data)
    sym   = trade_data.get("trading_symbol") or trade_data.get("symbol", "")
    mode_emoji = "P" if mode == "paper" else "L"

    if event == "ENTRY":
        _push(user_id, f"{mode_emoji} Entry: {sym}",
              f"{trade_data.get('opt_type','')} {trade_data.get('strike','')} "
              f"@ {trade_data.get('entry_price')} | SL {trade_data.get('sl_trigger')} "
              f"| Target {trade_data.get('target')}",
              data={"event": event}, tag=f"trade-{sym}")

    elif event == "EXIT":
        pnl    = trade_data.get("pnl", 0)
        status = trade_data.get("status", "")
        pnl_word = f"Profit {pnl}" if pnl >= 0 else f"Loss {abs(pnl)}"
        _push(user_id, f"{mode_emoji} Exit ({status}): {sym}",
              f"@ {trade_data.get('exit_price')} — {pnl_word}",
              data={"event": event}, tag=f"trade-{sym}")

    elif event == "ORDER_ALERT":
        _push(user_id, f"Order Alert: {sym}",
              trade_data.get("reason", "")[:180],
              data={"event": event}, tag=f"order-{sym}")

    elif event == "RISK_LIMIT_HIT":
        _push(user_id, f"Risk Limit Hit: {sym}",
              f"{trade_data.get('reason', '')} — no new entries today",
              data={"event": event}, tag="risk-limit")

    elif event == "PENDING_TRADE":
        _push(user_id, f"Pending Trade: {sym}",
              f"{trade_data.get('opt_type','')} {trade_data.get('strike','')} "
              f"— fills at current LTP on approval. Action required",
              data={"event": event}, tag=f"pending-{trade_data.get('pending_trade_id')}")
        _send_pending_trade_telegram_alert(user_id, trade_data)

    elif event == "SL_TRAIL":
        _push(user_id, f"SL Trailed: {sym}",
              f"New SL {trade_data.get('new_sl')} | LTP {trade_data.get('ltp')}",
              data={"event": event}, tag=f"trail-{sym}")

    elif event == "SL_CANCEL":
        reason = trade_data.get("reason", "")
        _push(user_id, f"SL Cancelled: {sym}",
              f"SL {trade_data.get('sl_trigger')} cancelled"
              + (f" — {reason}" if reason else ""),
              data={"event": event}, tag=f"slcancel-{sym}")

    if event != "EXIT":
        return

    raw_status = trade_data.get("status", "SL")
    status_map = {
        "NEAR_TARGET":          "TARGET",
        "TARGET":               "TARGET",
        "TARGET_HIT":           "TARGET",
        "SL":                   "SL",
        "SL_HIT":               "SL",
        "MANUAL_SQUAREOFF":     "MANUAL",
        "DIRECTION_FLIP_EXIT":  "DIRECTION_FLIP_EXIT",
    }
    db_status_str = status_map.get(raw_status, "SL")
    try:
        db_status = TradeStatus(db_status_str)
    except ValueError:
        db_status = TradeStatus("SL")

    async with AsyncSessionLocal() as db:
        raw_entry_ts = trade_data.get("entry_ts")
        if isinstance(raw_entry_ts, str):
            try:
                raw_entry_ts = datetime.strptime(raw_entry_ts, "%Y-%m-%d %H:%M:%S")
            except Exception:
                raw_entry_ts = datetime.now(timezone.utc).replace(tzinfo=None)
        elif raw_entry_ts is None:
            raw_entry_ts = datetime.now(timezone.utc).replace(tzinfo=None)

        t = Trade(
            user_id        = user_id,
            instrument_key = trade_data.get("instrument_key", ""),
            trading_symbol = trade_data.get("trading_symbol", ""),
            opt_type       = trade_data.get("opt_type", ""),
            strike         = float(trade_data.get("strike") or 0),
            expiry         = str(trade_data.get("expiry", "")),
            side           = "BUY",
            qty            = int(trade_data.get("qty", 0)),
            entry_price    = float(trade_data.get("entry_price", 0)),
            exit_price     = float(trade_data.get("exit_price", 0)),
            sl_trigger     = float(trade_data.get("sl_trigger", 0)),
            target         = float(trade_data.get("target", 0)),
            pnl            = float(trade_data.get("pnl", 0)),
            status         = db_status,
            strategy       = trade_data.get("strategy", ""),
            mode           = trade_data.get("mode", "live"),
            entry_ts       = raw_entry_ts,
            exit_ts        = datetime.now(timezone.utc).replace(tzinfo=None),
        )
        db.add(t)
        await db.commit()
        _log(f"[DB] Trade saved: {trade_data.get('trading_symbol')} "
             f"{raw_status}->{db_status_str} P&L {trade_data.get('pnl')}")

    if event == "EXIT":
        publish_event_sync(user_id, trade_data)


# ================================================================
# COMMAND HANDLERS  (work in both legacy and shared modes)
# ================================================================

def _get_engines(user_id: int):
    """Get engine-like objects for a user — delegates to bot_manager."""
    return bot_manager.get_engines_for_user(user_id)


def _handle_start(main_loop: asyncio.AbstractEventLoop, user_id: int) -> dict:
    try:
        fut = asyncio.run_coroutine_threadsafe(resolve_start_inputs(user_id), main_loop)
        config, access_token, error = fut.result(timeout=15)
    except Exception as e:
        return {"ok": False, "error": f"start failed: {e}"}

    if error:
        return {"ok": False, "error": error}

    if bot_manager.is_running(user_id):
        return {"ok": False, "error": "Bot already running"}

    started = bot_manager.start(
        user_id          = user_id,
        config           = config,
        access_token     = access_token,
        on_status_change = _on_status_change,
        on_trade         = _on_trade,
    )
    if not started:
        return {"ok": False, "error": "Bot already running"}

    set_bot_status_sync(user_id, "running")

    if not USE_SHARED:
        # Legacy mode: keep the shared symbol-level data pipeline alive so
        # the Trading Terminal has live data in both modes. Execution stays
        # with the legacy engine — this only feeds terminal snapshots.
        try:
            from backend.shared.terminal_bridge import ensure_for_user
            ensure_for_user(user_id, config, access_token)
        except Exception as e:
            _log(f"terminal bridge start failed: {e}")

    return {"ok": True, "status": "running"}


def _handle_stop(user_id: int) -> dict:
    bot_manager.stop(user_id)
    set_bot_status_sync(user_id, "stopped")

    if not USE_SHARED:
        # Legacy mode: release the shared symbol data pipeline once the
        # last bot watching those symbols has stopped.
        try:
            from backend.shared.terminal_bridge import release_for_user
            release_for_user(user_id)
        except Exception as e:
            _log(f"terminal bridge stop failed: {e}")

    if USE_SHARED:
        import threading
        def _stop_cleanup():
            try:
                from backend.shared.symbol_manager import (
                    clear_user_subscriptions, get_user_symbols,
                )
                symbols = get_user_symbols(user_id)
                clear_user_subscriptions(user_id)
                for sym in symbols:
                    count_str = "cleaned"  # symbol_manager cleans up on remove
                    _log(f"[SharedWorker] RefCount {sym} decreased (user {user_id} stopped)")
            except Exception:
                pass
        threading.Thread(target=_stop_cleanup, daemon=True).start()

    return {"ok": True, "status": "stopping"}


def _handle_modify_sl(user_id: int, payload: dict) -> dict:
    new_sl = payload.get("new_sl")
    symbol = payload.get("symbol")
    engines = _get_engines(user_id)
    if not engines:
        return {"ok": False, "error": "Bot not running"}
    changed, errors = [], []
    for eng in engines:
        if symbol and eng.symbol != symbol:
            continue
        pos = getattr(eng, "position", None)
        if not pos:
            continue
        entry = pos.get("entry_price", 0)
        if new_sl >= entry:
            errors.append(f"{eng.symbol}: SL must be below entry {entry}")
            continue
        try:
            eng._modify_sl_from_telegram(new_sl)
            changed.append(eng.symbol)
        except Exception as e:
            errors.append(f"{eng.symbol}: {e}")
    if not changed and not errors:
        return {"ok": False, "error": "No open positions found"}
    return {"ok": True, "changed": changed, "errors": errors}


def _handle_modify_target(user_id: int, payload: dict) -> dict:
    new_target = payload.get("new_target")
    symbol     = payload.get("symbol")
    engines = _get_engines(user_id)
    if not engines:
        return {"ok": False, "error": "Bot not running"}
    changed, errors = [], []
    for eng in engines:
        if symbol and eng.symbol != symbol:
            continue
        pos = getattr(eng, "position", None)
        if not pos:
            continue
        try:
            eng._modify_target_from_telegram(new_target)
            changed.append({
                "symbol":      eng.symbol,
                "new_target":  new_target,
                "near_target": eng.position["near_target"],
            })
        except Exception as e:
            errors.append(f"{eng.symbol}: {e}")
    if not changed and not errors:
        return {"ok": False, "error": "No open positions found"}
    return {"ok": True, "changed": changed, "errors": errors}


def _handle_trail_toggle(user_id: int, payload: dict) -> dict:
    """Enable/disable ATR trailing SL for a symbol (terminal chart toggle)."""
    symbol = (payload.get("symbol") or "").upper()
    enabled = bool(payload.get("enabled", True))
    if not symbol:
        return {"ok": False, "error": "symbol required"}
    engines = _get_engines(user_id)
    if not engines:
        return {"ok": False, "error": "Bot not running"}
    changed, errors = [], []
    for eng in engines:
        if eng.symbol != symbol:
            continue
        try:
            eng.set_trail(enabled)
            changed.append(eng.symbol)
        except Exception as e:
            errors.append(f"{eng.symbol}: {e}")
    if not changed:
        return {"ok": False, "error": f"No matching symbol {symbol}"}
    return {"ok": True, "changed": changed, "errors": errors,
            "enabled": enabled}


def _handle_squareoff(user_id: int, payload: dict) -> dict:
    symbol  = payload.get("symbol")
    engines = _get_engines(user_id)
    if not engines:
        return {"ok": False, "error": "Bot not running"}
    closed, errors = [], []
    for eng in engines:
        if symbol and eng.symbol != symbol:
            continue
        pos = getattr(eng, "position", None)
        if not pos:
            continue
        try:
            eng._squareoff_from_telegram()
            closed.append(eng.symbol)
        except Exception as e:
            errors.append(f"{eng.symbol}: {e}")
    if not closed and not errors:
        return {"ok": True, "message": "No open positions to close"}
    return {"ok": True, "closed": closed, "errors": errors}


def _handle_approve_pending_trade(user_id: int, payload: dict,
                                   main_loop: asyncio.AbstractEventLoop = None) -> dict:
    """Approve a pending trade and execute it."""
    trade_id = payload.get("trade_id")
    if not trade_id:
        return {"ok": False, "error": "trade_id required"}

    engines = _get_engines(user_id)

    from backend.services.execution_layer import (
        pending_trade_manager as ptm, place_order_from_engines,
    )

    def _place_order_from_signal(signal):
        return place_order_from_engines(
            user_id, engines, signal, trade_id=trade_id)

    if main_loop is not None:
        fut = asyncio.run_coroutine_threadsafe(
            ptm.approve(trade_id, user_id, _place_order_from_signal),
            main_loop,
        )
        try:
            result = fut.result(timeout=30)
        except Exception as e:
            return {"ok": False, "error": f"Approval failed: {e}"}
    else:
        try:
            result = ptm.approve_sync(trade_id, user_id, _place_order_from_signal)
        except Exception as e:
            return {"ok": False, "error": f"Approval failed: {e}"}

    if result.status.value in ("EXECUTED",):
        return {
            "ok": True, "status": "approved",
            "message": result.message, "trade_id": result.trade_id,
        }
    elif result.status.value == "EXPIRED":
        return {"ok": False, "error": result.message}
    else:
        return {"ok": False, "error": result.message}


def _handle_reject_pending_trade(user_id: int, payload: dict,
                                  main_loop: asyncio.AbstractEventLoop = None) -> dict:
    """Reject a pending trade."""
    trade_id = payload.get("trade_id")
    if not trade_id:
        return {"ok": False, "error": "trade_id required"}

    from backend.services.execution_layer import pending_trade_manager as ptm

    if main_loop is not None:
        fut = asyncio.run_coroutine_threadsafe(
            ptm.reject(trade_id, user_id), main_loop
        )
        try:
            result = fut.result(timeout=15)
        except Exception as e:
            return {"ok": False, "error": f"Rejection failed: {e}"}
    else:
        try:
            result = ptm.reject_sync(trade_id, user_id)
        except Exception as e:
            return {"ok": False, "error": f"Rejection failed: {e}"}

    if result.status.value in ("REJECTED",):
        return {"ok": True, "status": "rejected", "message": result.message}
    return {"ok": False, "error": result.message}


def _handle_pause_resume(user_id: int, paused: bool) -> dict:
    engines = _get_engines(user_id)
    if not engines:
        return {"ok": False, "error": "Bot not running"}
    for eng in engines:
        eng._paused = paused
    key = "paused" if paused else "resumed"
    return {"ok": True, key: [e.symbol for e in engines]}


_command_loop_main_loop = None


def _expire_stale_pending_trades():
    """Background task: mark expired pending trades (sync only)."""
    try:
        _expire_stale_sync()
    except Exception as e:
        _log(f"expire_stale: {e}")


def _expire_stale_sync():
    """Sync fallback for expiring stale pending trades."""
    from datetime import datetime as _dt, timezone as _tz, timedelta as _td
    from backend.db.database import get_sync_session
    from backend.db.models import PendingTrade
    from backend.services.execution_layer import PendingTradeStatus
    from sqlalchemy import select as _select, delete as _delete

    db = get_sync_session()
    try:
        now = _dt.now(_tz.utc).replace(tzinfo=None)
        expired = db.execute(
            _select(PendingTrade).where(
                PendingTrade.status == PendingTradeStatus.WAITING,
                PendingTrade.expires_at <= now,
            )
        ).scalars().all()
        for trade in expired:
            trade.status = PendingTradeStatus.EXPIRED
            db.add(trade)
        if expired:
            db.commit()
            _log(f"Expired {len(expired)} stale pending trade(s) [sync]")

        # Auto-delete pending trades from history once they are older
        # than PENDING_TRADE_HISTORY_TTL_SEC (approval already expired).
        cutoff = now - _td(seconds=PENDING_TRADE_HISTORY_TTL_SEC)
        purged = db.execute(
            _delete(PendingTrade).where(PendingTrade.created_at <= cutoff)
        )
        db.commit()
        if purged.rowcount:
            _log(f"Purged {purged.rowcount} pending trade(s) from "
                 f"history (> {PENDING_TRADE_HISTORY_TTL_SEC}s) [sync]")
    except Exception as e:
        db.rollback()
        raise
    finally:
        db.close()


# ================================================================
# COMMAND CONSUMER LOOP  (runs in a background thread)
# ================================================================

def _command_loop(main_loop: asyncio.AbstractEventLoop, stop_event: threading.Event):
    _log("Command consumer started"
         + (" [SHARED mode]" if USE_SHARED else " [LEGACY mode]"))
    global _command_loop_main_loop
    _command_loop_main_loop = main_loop
    expire_counter = 0

    while not stop_event.is_set():
        try:
            cmd = pop_command_sync(timeout=5)
        except Exception as e:
            _log(f"command pop error: {e}")
            continue
        if cmd is None:
            # Periodically expire stale pending trades (every ~10s so a
            # 30s approval window disappears promptly once elapsed)
            expire_counter += 1
            if expire_counter >= 2:  # every ~10s
                _expire_stale_pending_trades()
                expire_counter = 0
            continue

        cmd_id  = cmd.get("id")
        action  = cmd.get("action")
        user_id = cmd.get("user_id")
        payload = cmd.get("payload") or {}

        try:
            if action == "start":
                result = _handle_start(main_loop, user_id)
            elif action == "stop":
                result = _handle_stop(user_id)
            elif action == "modify_sl":
                result = _handle_modify_sl(user_id, payload)
            elif action == "modify_target":
                result = _handle_modify_target(user_id, payload)
            elif action == "trail_toggle":
                result = _handle_trail_toggle(user_id, payload)
            elif action == "squareoff":
                result = _handle_squareoff(user_id, payload)
            elif action == "pause":
                result = _handle_pause_resume(user_id, True)
            elif action == "resume":
                result = _handle_pause_resume(user_id, False)
            elif action == "approve_pending_trade":
                result = _handle_approve_pending_trade(user_id, payload, main_loop)
            elif action == "reject_pending_trade":
                result = _handle_reject_pending_trade(user_id, payload, main_loop)
            else:
                result = {"ok": False, "error": f"Unknown action: {action}"}
        except Exception:
            tb = traceback.format_exc()
            _log(f"command '{action}' for user {user_id} crashed:\n{tb}")
            result = {"ok": False, "error": "Internal worker error"}

        if cmd_id:
            try:
                post_result_sync(cmd_id, result)
            except Exception as e:
                _log(f"post_result error: {e}")

    _log("Command consumer stopped")


# ================================================================
# HEARTBEAT LOOP  (asyncio task — refreshes bot:status TTL)
# ================================================================

async def _heartbeat_loop(stop_event: threading.Event):
    while not stop_event.is_set():
        try:
            statuses = bot_manager.status_all()
            for user_id, status in statuses.items():
                if status == "running":
                    set_bot_status_sync(user_id, "running")
        except Exception as e:
            _log(f"heartbeat error: {e}")
        await asyncio.sleep(HEARTBEAT_SEC)


# ================================================================
# MAIN
# ================================================================

async def _order_update_push_loop(stop_event: threading.Event):
    """Subscribes to order_update_push channel for push notifications."""
    from backend.services.redis_client import get_redis
    r      = get_redis()
    pubsub = r.pubsub()
    await pubsub.subscribe("order_update_push")
    try:
        while not stop_event.is_set():
            msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
            if not msg:
                await asyncio.sleep(0.05)
                continue
            try:
                import json as _json
                data    = _json.loads(msg["data"])
                user_id = data.get("user_id")
                if not user_id:
                    continue
                sym     = data.get("trading_symbol") or data.get("symbol", "")
                st      = (data.get("status", "")).lower()
                icon    = "ok" if st == "complete" else ("fail" if "reject" in st or "cancel" in st else "info")
                if st == "complete":
                    body = (f"{data.get('side','')} {data.get('qty_filled','')} "
                            f"@ {data.get('average_price')} | {sym}")
                else:
                    body = f"{st}: {sym}" + (f" — {data.get('message','')}" if data.get("message") else "")
                _push(user_id, f"{icon} Order {st.title()}: {sym}", body[:180],
                      data={"event": "ORDER_UPDATE"}, tag=f"order-update-{sym}")
            except Exception as e:
                _log(f"order_update_push: {e}")
    finally:
        await pubsub.unsubscribe("order_update_push")
        await pubsub.close()


async def main():
    await init_db()

    # ── Preload instrument master ─────────────────────────────────
    _log("Preloading instrument master...")
    from backend.engine.instruments import preload_instruments
    await asyncio.to_thread(preload_instruments)
    _log("Instrument master ready")

    loop = asyncio.get_event_loop()
    set_main_loop(loop)

    stop_event = threading.Event()

    # ── Shared worker initialization ──────────────────────────────
    if USE_SHARED:
        _log("Initializing SharedWorkerOrchestrator...")
        from backend.shared.shared_worker import SharedWorkerOrchestrator
        from backend.shared.monitoring import metrics_collector
        from backend.shared.fault_tolerance import fault_manager

        orchestrator = SharedWorkerOrchestrator()
        bot_manager.init_shared(
            orchestrator,
            main_loop=loop,
            on_status_change=_on_status_change,
            on_trade=_on_trade,
        )

        fault_manager.start(on_redis_reconnect=lambda: _log("Redis reconnected"))

        orchestrator.start()
        metrics_collector.start()
        _log("SharedWorkerOrchestrator started")

    # ── Command consumer thread ───────────────────────────────────
    cmd_thread = threading.Thread(
        target=_command_loop, args=(loop, stop_event),
        daemon=True, name="command-consumer")
    cmd_thread.start()

    heartbeat_task  = asyncio.create_task(_heartbeat_loop(stop_event))
    order_push_task = asyncio.create_task(_order_update_push_loop(stop_event))

    # ── Graceful shutdown ────────────────────────────────────────
    shutdown_event = asyncio.Event()

    def _signal_handler():
        _log("Shutdown signal received")
        shutdown_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _signal_handler)
        except NotImplementedError:
            pass

    _log(f"AlgoBot worker started — waiting for commands"
         f" [mode={'SHARED' if USE_SHARED else 'LEGACY'}]")
    await shutdown_event.wait()

    # ── Shutdown sequence ────────────────────────────────────────
    _log("Initiating graceful shutdown...")

    # 1. Stop accepting commands
    stop_event.set()

    # 2. Stop all users (unsubscribes, stops execution managers)
    #    Shared mode also stops strategy engines and market data via _cleanup_shared_services()
    _log("Stopping all users...")
    bot_manager.stop_all(join_timeout=10.0)

    # 2b. Legacy mode: stop the shared symbol data pipeline the terminal bridge started
    if not USE_SHARED:
        try:
            from backend.shared.terminal_bridge import stop_all as _bridge_stop_all
            _bridge_stop_all()
        except Exception as e:
            _log(f"terminal bridge stop_all error: {e}")

    # 3. Stop monitoring and fault tolerance (shared-only services)
    if USE_SHARED:
        _log("Stopping monitoring...")
        from backend.shared.monitoring import metrics_collector
        metrics_collector.stop()
        _log("Stopping fault tolerance...")
        from backend.shared.fault_tolerance import fault_manager
        fault_manager.stop()

    # 4. Cancel background tasks
    heartbeat_task.cancel()
    order_push_task.cancel()

    # 5. Clear status keys
    for user_id in bot_manager.status_all().keys():
        clear_bot_status_sync(user_id)

    # 6. Close Redis connections
    _log("Closing Redis...")
    try:
        from backend.services.redis_client import close as close_redis
        await close_redis()
    except Exception as e:
        _log(f"Redis close error: {e}")

    # 7. Close WebSocket / DB connections
    await close_db()

    _log("Shutdown complete — exit cleanly")


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
