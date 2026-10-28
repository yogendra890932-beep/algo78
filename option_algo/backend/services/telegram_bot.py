# backend/services/telegram_bot.py
# ================================================================
# Telegram Command Bot
# Users can control their bot directly from Telegram:
#
#   /status         — show current position & bot status
#   /sl <price>     — modify stop-loss to <price>
#   /target <price> — modify target to <price>
#   /squareoff      — immediately close open position
#   /pause          — pause trading (no new entries)
#   /resume         — resume trading
#   /pnl            — show today's P&L summary
#   /help           — show all commands
#
# How it works:
#   A background thread polls Telegram getUpdates every 2 seconds.
#   Commands are matched to users via chat_id stored in their BotConfig.
#   Position changes are applied live to the running SymbolEngine.
# ================================================================

import threading
import time
import requests
from datetime import datetime
from typing import Optional, Callable

from backend.shared.engine_price import engine_premium_price

_now = lambda: datetime.now().strftime("%H:%M:%S")


# ── Global engine registry (user_id → list of SymbolEngine) ──────
# Populated by bot_manager when engines start
_engine_registry: dict[int, list] = {}
_registry_lock   = threading.Lock()


def register_engines(user_id: int, engines: list):
    with _registry_lock:
        _engine_registry[user_id] = engines


def unregister_engines(user_id: int):
    with _registry_lock:
        _engine_registry.pop(user_id, None)


def get_engines(user_id: int) -> list:
    with _registry_lock:
        return _engine_registry.get(user_id, [])


# ── Telegram API helpers ──────────────────────────────────────────

def _tg_get(bot_token: str, method: str, params: dict = None) -> Optional[dict]:
    try:
        url  = f"https://api.telegram.org/bot{bot_token}/{method}"
        resp = requests.get(url, params=params or {}, timeout=5)
        if resp.status_code == 200:
            return resp.json()
    except Exception:
        pass
    return None


def _tg_send(bot_token: str, chat_id: str, text: str) -> bool:
    try:
        url  = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        resp = requests.post(url, json={
            "chat_id":    chat_id,
            "text":       text,
            "parse_mode": "HTML",
        }, timeout=5)
        return resp.status_code == 200
    except Exception:
        return False


# ── Command handlers ──────────────────────────────────────────────

def _handle_status(user_id: int, bot_token: str, chat_id: str):
    engines = get_engines(user_id)
    if not engines:
        _tg_send(bot_token, chat_id, "🔴 <b>Bot is not running.</b>")
        return

    lines = [f"🟢 <b>Bot Running</b> — {len(engines)} symbol(s)\n"]
    for eng in engines:
        sym  = getattr(eng, "symbol", "?")
        mode = "📄 Paper" if getattr(eng, "paper_mode", True) else "💰 Live"
        pos  = getattr(eng, "position", None)
        ltp  = getattr(eng, "_cur_candle", {}).get("close", "—")
        paused = getattr(eng, "_paused", False)
        state  = "⏸ Paused" if paused else "▶ Active"

        lines.append(f"<b>{sym}</b> [{mode}] {state}")
        if pos:
            entry  = pos.get("entry_price", 0)
            sl     = pos.get("sl_trigger", 0)
            target = pos.get("target", 0)
            pnl    = round((float(ltp) - entry) * pos.get("qty", 0), 2) if isinstance(ltp, float) else "—"
            lines.append(
                f"  📌 {pos.get('trading_symbol','?')} {pos.get('opt_type','')} {pos.get('strike','')}\n"
                f"  Entry:  ₹{entry}\n"
                f"  LTP:    ₹{ltp}\n"
                f"  SL:     ₹{sl}\n"
                f"  Target: ₹{target}\n"
                f"  P&L:    ₹{pnl}"
            )
        else:
            lines.append(f"  No open position | LTP: ₹{ltp}")
        lines.append("")

    _tg_send(bot_token, chat_id, "\n".join(lines))


def _handle_sl(user_id: int, bot_token: str, chat_id: str, args: list):
    if not args:
        _tg_send(bot_token, chat_id, "❌ Usage: /sl <price>\nExample: /sl 245.50")
        return
    try:
        new_sl = float(args[0])
    except ValueError:
        _tg_send(bot_token, chat_id, "❌ Invalid price. Example: /sl 245.50")
        return

    engines = get_engines(user_id)
    changed  = []
    for eng in engines:
        pos = getattr(eng, "position", None)
        if not pos:
            continue
        old_sl = pos.get("sl_trigger", 0)
        entry  = pos.get("entry_price", 0)
        # Trailing is validated against the live premium, so the stop can be
        # trailed past the entry to lock in profit.
        ref  = engine_premium_price(eng, entry) or entry
        side = str(pos.get("side") or "BUY").upper()
        wrong_side = (new_sl <= ref) if side == "SELL" else (new_sl >= ref)
        if wrong_side:
            _tg_send(bot_token, chat_id,
                     f"❌ SL ₹{new_sl} must stay "
                     + ("above" if side == "SELL" else "below")
                     + f" the premium price ₹{ref}")
            continue
        # Modify via engine method
        try:
            eng._modify_sl_from_telegram(new_sl)
            changed.append(f"{eng.symbol}: ₹{old_sl} → ₹{new_sl}")
        except Exception as e:
            _tg_send(bot_token, chat_id, f"❌ Failed to modify SL for {eng.symbol}: {e}")

    if changed:
        _tg_send(bot_token, chat_id,
                 "📍 <b>SL Modified</b>\n" + "\n".join(changed))
    elif not engines:
        _tg_send(bot_token, chat_id, "🔴 Bot not running or no open positions.")


def _handle_target(user_id: int, bot_token: str, chat_id: str, args: list):
    if not args:
        _tg_send(bot_token, chat_id, "❌ Usage: /target <price>\nExample: /target 320.00")
        return
    try:
        new_target = float(args[0])
    except ValueError:
        _tg_send(bot_token, chat_id, "❌ Invalid price.")
        return

    engines = get_engines(user_id)
    changed = []
    errors  = []
    for eng in engines:
        pos = getattr(eng, "position", None)
        if not pos:
            continue
        try:
            # Use engine method — also resets near_target automatically
            eng._modify_target_from_telegram(new_target)
            near = eng.position.get("near_target", "?")
            changed.append(
                f"{eng.symbol}: target → ₹{new_target} | near-target → ₹{near}"
            )
        except Exception as e:
            errors.append(f"{eng.symbol}: {str(e)}")

    if changed:
        _tg_send(bot_token, chat_id,
                 "🎯 <b>Target Modified</b>\n" + "\n".join(changed))
    if errors:
        _tg_send(bot_token, chat_id, "❌ " + "\n".join(errors))
    if not changed and not errors:
        _tg_send(bot_token, chat_id, "ℹ️ No open positions.")


def _handle_squareoff(user_id: int, bot_token: str, chat_id: str):
    engines = get_engines(user_id)
    if not engines:
        _tg_send(bot_token, chat_id, "🔴 Bot not running.")
        return
    closed = []
    errors = []
    for eng in engines:
        pos = getattr(eng, "position", None)
        if not pos:
            continue
        try:
            # Engine method: Step 1 cancel SL, Step 2 market exit
            eng._squareoff_from_telegram()
            closed.append(eng.symbol)
        except Exception as e:
            errors.append(f"{eng.symbol}: {str(e)}")
    if closed:
        _tg_send(bot_token, chat_id,
                 f"⚡ <b>Square Off Done</b>\n"
                 f"SL cancelled + Market exit: {', '.join(closed)}")
    if errors:
        _tg_send(bot_token, chat_id, "❌ " + "\n".join(errors))
    if not closed and not errors:
        _tg_send(bot_token, chat_id, "ℹ️ No open positions to close.")


def _handle_pause(user_id: int, bot_token: str, chat_id: str):
    engines = get_engines(user_id)
    for eng in engines:
        eng._paused = True
    count = len(engines)
    _tg_send(bot_token, chat_id,
             f"⏸ <b>Trading Paused</b>\n{count} engine(s) paused.\n"
             f"No new entries until /resume")


def _handle_resume(user_id: int, bot_token: str, chat_id: str):
    engines = get_engines(user_id)
    for eng in engines:
        eng._paused = False
    _tg_send(bot_token, chat_id, "▶ <b>Trading Resumed</b>\nBot will take new entries.")


def _handle_pnl(user_id: int, bot_token: str, chat_id: str):
    engines = get_engines(user_id)
    if not engines:
        _tg_send(bot_token, chat_id, "🔴 Bot not running.")
        return
    # trades_today/net_pnl_today are now ACCOUNT-WIDE via a RiskTracker
    # shared by every symbol's engine (see engine_v6.RiskTracker) — read
    # it once from any engine, do NOT sum across engines (that would
    # multiply the same shared total by the number of symbols traded).
    risk_snap = engines[0]._risk.snapshot()
    total_trades = risk_snap["trades_today"]
    total_pnl    = risk_snap["net_pnl_today"]

    lines = ["📊 <b>Today's P&L</b> (account-wide)\n"]
    for eng in engines:
        sym  = getattr(eng, "symbol", "?")
        mode = "📄" if getattr(eng, "paper_mode", True) else "💰"
        pos  = getattr(eng, "position", None)
        status = "open position" if pos else "no open position"
        lines.append(f"{mode} <b>{sym}</b>: {status}")
    lines.append(f"\nTotal trades today: {total_trades}")
    pnl_sign = "+" if total_pnl >= 0 else ""
    lines.append(f"Net P&L: {pnl_sign}₹{total_pnl:.0f}")
    _tg_send(bot_token, chat_id, "\n".join(lines))


def _handle_approve(user_id: int, bot_token: str, chat_id: str, args: list):
    """
    Approve a pending trade from Telegram.
    Usage: /approve_<trade_id>  or  /approve <trade_id>
    Uses the Redis command queue to send the approval to the worker.
    """
    trade_id = None
    if args:
        trade_id = args[0]
    if not trade_id:
        _tg_send(bot_token, chat_id, "❌ Usage: /approve_<trade_id>\nExample: /approve_42")
        return

    try:
        trade_id = int(trade_id)
    except ValueError:
        _tg_send(bot_token, chat_id, "❌ Invalid trade ID. Example: /approve_42")
        return

    from backend.services.command_queue import send_command_sync

    _tg_send(bot_token, chat_id, f"⏳ Approving trade #{trade_id}...")

    try:
        result = send_command_sync("approve_pending_trade", user_id,
                                   {"trade_id": trade_id}, timeout=15.0)
        if result.get("ok"):
            msg = result.get("message", f"Trade #{trade_id} approved ✅")
            _tg_send(bot_token, chat_id, f"✅ <b>Trade #{trade_id} Approved</b>\n{msg}")
        else:
            error = result.get("error", "Unknown error")
            _tg_send(bot_token, chat_id, f"❌ Trade #{trade_id} approval failed:\n{error}")
    except Exception as e:
        _tg_send(bot_token, chat_id, f"❌ Error: {e}")


def _handle_reject(user_id: int, bot_token: str, chat_id: str, args: list):
    """
    Reject a pending trade from Telegram.
    Usage: /reject_<trade_id>  or  /reject <trade_id>
    Uses the Redis command queue to send the rejection to the worker.
    """
    trade_id = None
    if args:
        trade_id = args[0]
    if not trade_id:
        _tg_send(bot_token, chat_id, "❌ Usage: /reject_<trade_id>\nExample: /reject_42")
        return

    try:
        trade_id = int(trade_id)
    except ValueError:
        _tg_send(bot_token, chat_id, "❌ Invalid trade ID. Example: /reject_42")
        return

    from backend.services.command_queue import send_command_sync

    _tg_send(bot_token, chat_id, f"⏳ Rejecting trade #{trade_id}...")

    try:
        result = send_command_sync("reject_pending_trade", user_id,
                                   {"trade_id": trade_id}, timeout=15.0)
        if result.get("ok"):
            _tg_send(bot_token, chat_id, f"❌ <b>Trade #{trade_id} Rejected</b>")
        else:
            error = result.get("error", "Unknown error")
            _tg_send(bot_token, chat_id, f"❌ Trade #{trade_id} rejection failed:\n{error}")
    except Exception as e:
        _tg_send(bot_token, chat_id, f"❌ Error: {e}")


def _handle_help(bot_token: str, chat_id: str):
    text = (
        "📋 <b>Available Commands</b>\n\n"
        "/status        — Current position & bot status\n"
        "/sl &lt;price&gt;    — Modify stop-loss\n"
        "/target &lt;price&gt; — Modify target\n"
        "/squareoff     — Close all open positions NOW\n"
        "/pause         — Pause new entries\n"
        "/resume        — Resume trading\n"
        "/pnl           — Today's P&L summary\n"
        "/help          — Show this message\n\n"
        "<i>Example: /sl 245.50</i>"
    )
    _tg_send(bot_token, chat_id, text)


# ── Command dispatcher ────────────────────────────────────────────

COMMANDS = {
    "/status":    lambda uid, tok, cid, args: _handle_status(uid, tok, cid),
    "/sl":        lambda uid, tok, cid, args: _handle_sl(uid, tok, cid, args),
    "/target":    lambda uid, tok, cid, args: _handle_target(uid, tok, cid, args),
    "/squareoff": lambda uid, tok, cid, args: _handle_squareoff(uid, tok, cid),
    "/pause":     lambda uid, tok, cid, args: _handle_pause(uid, tok, cid),
    "/resume":    lambda uid, tok, cid, args: _handle_resume(uid, tok, cid),
    "/pnl":       lambda uid, tok, cid, args: _handle_pnl(uid, tok, cid),
    "/approve":   lambda uid, tok, cid, args: _handle_approve(uid, tok, cid, args),
    "/reject":    lambda uid, tok, cid, args: _handle_reject(uid, tok, cid, args),
    "/help":      lambda uid, tok, cid, args: _handle_help(tok, cid),
    "/start":     lambda uid, tok, cid, args: _handle_help(tok, cid),
}


def _process_callback_query(bot_token: str, callback_query_id: str, action: str, trade_id: int,
                             user_id: int, chat_id: str):
    """Handle inline button callback from Telegram."""
    from backend.services.command_queue import send_command_sync
    
    cmd = "approve_pending_trade" if action == "approve" else "reject_pending_trade"
    
    try:
        result = send_command_sync(cmd, user_id, {"trade_id": trade_id}, timeout=15.0)
        if result.get("ok"):
            answer_text = f"✅ Trade #{trade_id} approved" if action == "approve" else f"❌ Trade #{trade_id} rejected"
        else:
            answer_text = f"❌ Error: {result.get('error', 'Unknown error')}"
    except Exception as e:
        answer_text = f"❌ Error: {e}"
    
    # Answer callback query (shows popup notification in Telegram)
    _tg_get(bot_token, "answerCallbackQuery", {
        "callback_query_id": callback_query_id,
        "text": answer_text,
        "show_alert": True,
    })


def dispatch_command(user_id: int, bot_token: str,
                     chat_id: str, text: str):
    parts   = text.strip().split()
    cmd     = parts[0].lower().split("@")[0]  # strip @botname suffix
    args    = parts[1:]

    # Parse /approve_42 or /reject_42 format (trade_id embedded in command)
    if cmd.startswith("/approve_") and len(cmd) > 9:
        trade_id_str = cmd[9:]  # extract "42" from "/approve_42"
        cmd = "/approve"
        args = [trade_id_str] + args
    elif cmd.startswith("/reject_") and len(cmd) > 8:
        trade_id_str = cmd[8:]  # extract "42" from "/reject_42"
        cmd = "/reject"
        args = [trade_id_str] + args

    handler = COMMANDS.get(cmd)
    if handler:
        try:
            handler(user_id, bot_token, chat_id, args)
        except Exception as e:
            _tg_send(bot_token, chat_id, f"❌ Error: {e}")
    else:
        _tg_send(bot_token, chat_id,
                 f"❓ Unknown command: {cmd}\nType /help for commands.")


# ── Polling loop (one per user bot_token) ─────────────────────────

_poll_threads: dict[str, threading.Thread] = {}  # bot_token → thread
_poll_lock    = threading.Lock()
_poll_offsets: dict[str, int] = {}  # bot_token → last update_id


def start_polling(user_id: int, bot_token: str, chat_id: str,
                  stop_event: threading.Event):
    """
    Start a background polling thread for this user's Telegram bot.
    Only one thread per bot_token (shared if same token across instances).
    """
    with _poll_lock:
        if bot_token in _poll_threads:
            t = _poll_threads[bot_token]
            if t.is_alive():
                return   # already polling

    def poll_loop():
        offset = _poll_offsets.get(bot_token, 0)
        print(f"[TG Poll] Starting for user {user_id}")
        while not stop_event.is_set():
            try:
                result = _tg_get(bot_token, "getUpdates", {
                    "offset":  offset,
                    "timeout": 2,
                    "allowed_updates": ["message", "callback_query"],
                })
                if result and result.get("ok"):
                    for update in result.get("result", []):
                        offset = update["update_id"] + 1
                        _poll_offsets[bot_token] = offset
                        
                        # Handle callback queries (inline button clicks)
                        if "callback_query" in update:
                            callback = update["callback_query"]
                            callback_data = callback.get("data", "")
                            callback_id = callback["id"]
                            
                            if callback_data.startswith("approve_") or callback_data.startswith("reject_"):
                                action = "approve" if callback_data.startswith("approve_") else "reject"
                                trade_id = int(callback_data.split("_")[1])
                                _process_callback_query(bot_token, callback_id, action, trade_id, user_id, chat_id)
                            continue
                        
                        # Handle regular messages
                        msg = update.get("message", {})
                        if not msg:
                            continue
                        incoming_chat = str(msg.get("chat", {}).get("id", ""))
                        text          = msg.get("text", "").strip()
                        if not text.startswith("/"):
                            continue
                        # Security: only respond to configured chat_id
                        if incoming_chat != str(chat_id):
                            _tg_send(bot_token, incoming_chat,
                                     "⛔ Unauthorized. This bot is private.")
                            continue
                        dispatch_command(user_id, bot_token, chat_id, text)
            except Exception as e:
                print(f"[TG Poll] Error: {e}")
            time.sleep(2)
        print(f"[TG Poll] Stopped for user {user_id}")

    t = threading.Thread(target=poll_loop, daemon=True,
                         name=f"tg-poll-{user_id}")
    t.start()
    with _poll_lock:
        _poll_threads[bot_token] = t
