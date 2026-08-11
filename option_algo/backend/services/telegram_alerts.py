# backend/services/telegram_alerts.py
# ================================================================
# Telegram Alert Service
# Sends trade signals, entries, exits, and daily summaries
# to a user's Telegram chat via bot.
#
# Setup (per user):
#   1. Create a bot via @BotFather → get bot_token
#   2. Start the bot in Telegram → get chat_id via /getUpdates
#   3. Save both in user's BotConfig (telegram_bot_token, telegram_chat_id)
# ================================================================

import asyncio
import requests
from datetime import datetime
from typing import Optional


def _now() -> str:
    return datetime.now().strftime("%H:%M:%S")


def _tts_mp3(text: str, lang: str = "en") -> Optional[bytes]:
    """
    Generate speech audio (MP3) for `text` using Google Translate TTS
    (free, no API key). Returns raw bytes or None on failure.
    """
    if not text:
        return None
    try:
        url = "https://translate.google.com/translate_tts"
        params = {"ie": "UTF-8", "q": text, "tl": lang, "client": "tw-ob"}
        resp = requests.get(url, params=params, timeout=10,
                            headers={"User-Agent": "Mozilla/5.0"})
        if resp.status_code == 200 and resp.content:
            return resp.content
        return None
    except Exception as e:
        print(f"[Telegram] TTS failed: {e}")
        return None


def send_voice_message(bot_token: str, chat_id: str, text: str,
                       lang: str = "en") -> bool:
    """
    Send a Telegram voice message generated from `text`.
    Returns True on success, False on failure (incl. TTS failure).
    """
    if not bot_token or not chat_id or not text:
        return False
    try:
        audio = _tts_mp3(text, lang=lang)
        if not audio:
            return False
        url = f"https://api.telegram.org/bot{bot_token}/sendVoice"
        resp = requests.post(
            url,
            data={"chat_id": chat_id},
            files={"voice": ("alert.mp3", audio, "audio/mpeg")},
            timeout=15,
        )
        return resp.status_code == 200
    except Exception as e:
        print(f"[Telegram] Voice send failed: {e}")
        return False


def _send_message(bot_token: str, chat_id: str, text: str,
                  parse_mode: str = "HTML", reply_markup: dict = None) -> bool:
    """
    Sends a Telegram message synchronously.
    Returns True on success, False on failure.
    Safe to call from any thread.
    """
    if not bot_token or not chat_id:
        return False
    try:
        url  = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        payload = {
            "chat_id":    chat_id,
            "text":       text,
            "parse_mode": parse_mode,
        }
        if reply_markup:
            payload["reply_markup"] = reply_markup
        resp = requests.post(url, json=payload, timeout=5)
        return resp.status_code == 200
    except Exception as e:
        print(f"[Telegram] Send failed: {e}")
        return False


# ── Pre-formatted message templates ────────────────────────────

def alert_generic(bot_token: str, chat_id: str, title: str, body: str) -> bool:
    """
    Generic one-off alert (e.g. subscription/billing notices — see
    backend.services.billing_notifications) that don't warrant their
    own dedicated template function.
    """
    text = f"<b>{title}</b>\n{body}\nTime: {_now()}"
    return _send_message(bot_token, chat_id, text)


def alert_bot_started(bot_token: str, chat_id: str,
                      symbol: str, strategy: str, mode: str):
    emoji = "🟡" if mode == "paper" else "🟢"
    text  = (
        f"{emoji} <b>Bot Started</b>\n"
        f"Symbol: <b>{symbol}</b>\n"
        f"Strategy: {strategy}\n"
        f"Mode: <b>{mode.upper()}</b>\n"
        f"Time: {_now()}"
    )
    _send_message(bot_token, chat_id, text)


def alert_bot_stopped(bot_token: str, chat_id: str, symbol: str):
    text = (
        f"🔴 <b>Bot Stopped</b>\n"
        f"Symbol: {symbol}\n"
        f"Time: {_now()}"
    )
    _send_message(bot_token, chat_id, text)


def alert_direction_change(bot_token: str, chat_id: str,
                           old_dir: str, new_dir: str,
                           opt_type: str, strike: float,
                           trading_symbol: str):
    emoji = "📈" if new_dir == "BULL" else "📉"
    text  = (
        f"{emoji} <b>Direction Flip</b>\n"
        f"{old_dir or 'NONE'} → <b>{new_dir}</b>\n"
        f"Now trading: <b>{opt_type}</b>\n"
        f"Strike: {strike}\n"
        f"Symbol: {trading_symbol}\n"
        f"Time: {_now()}"
    )
    _send_message(bot_token, chat_id, text)


def alert_signal(bot_token: str, chat_id: str,
                 strategy: str, opt_type: str,
                 strike: float, entry_price: float,
                 sl: float, target: float,
                 mode: str = "live"):
    mode_tag = "📄 PAPER" if mode == "paper" else "💰 LIVE"
    emoji    = {"pullback_1m": "🟢", "sweep_1m": "💧",
                "breakout_1m": "🚀"}.get(strategy, "📊")
    rr       = round((target - entry_price) / max(entry_price - sl, 0.01), 2)
    text     = (
        f"{emoji} <b>Signal — {strategy.upper()}</b> [{mode_tag}]\n"
        f"Option: <b>{opt_type} {strike}</b>\n"
        f"Entry:  ₹{entry_price}\n"
        f"SL:     ₹{sl}\n"
        f"Target: ₹{target}\n"
        f"R:R     1 : {rr}\n"
        f"Time: {_now()}"
    )
    _send_message(bot_token, chat_id, text)


def alert_entry(bot_token: str, chat_id: str,
                trading_symbol: str, opt_type: str,
                entry_price: float, sl: float,
                target: float, qty: int,
                strategy: str, mode: str = "live"):
    mode_tag = "📄 PAPER" if mode == "paper" else "💰 LIVE"
    text     = (
        f"🟢 <b>ENTRY</b> [{mode_tag}]\n"
        f"<b>{trading_symbol}</b> ({opt_type})\n"
        f"Entry:  ₹{entry_price}\n"
        f"SL:     ₹{sl}\n"
        f"Target: ₹{target}\n"
        f"Qty:    {qty}\n"
        f"Strategy: {strategy}\n"
        f"Time: {_now()}"
    )
    _send_message(bot_token, chat_id, text)


def alert_exit(bot_token: str, chat_id: str,
               trading_symbol: str, opt_type: str,
               entry_price: float, exit_price: float,
               pnl: float, status: str,
               qty: int, mode: str = "live"):
    mode_tag = "📄 PAPER" if mode == "paper" else "💰 LIVE"
    if status == "TARGET":
        emoji = "🎯"
    elif status == "SL":
        emoji = "🛑"
    else:
        emoji = "🔄"
    pnl_sign = "+" if pnl >= 0 else ""
    text = (
        f"{emoji} <b>EXIT — {status}</b> [{mode_tag}]\n"
        f"<b>{trading_symbol}</b> ({opt_type})\n"
        f"Entry:  ₹{entry_price}\n"
        f"Exit:   ₹{exit_price}\n"
        f"P&amp;L:   <b>₹{pnl_sign}{pnl}</b>\n"
        f"Qty:    {qty}\n"
        f"Time: {_now()}"
    )
    _send_message(bot_token, chat_id, text)


def alert_sl_trail(bot_token: str, chat_id: str,
                   old_sl: float, new_sl: float, ltp: float):
    text = (
        f"📍 <b>SL Trailed</b>\n"
        f"₹{old_sl} → <b>₹{new_sl}</b>\n"
        f"LTP: ₹{ltp}\n"
        f"Time: {_now()}"
    )
    _send_message(bot_token, chat_id, text)


def alert_sl_cancel(bot_token: str, chat_id: str,
                    trading_symbol: str, sl_trigger: float, reason: str = ""):
    """
    Sent when a stop-loss order is CANCELLED at the broker — normally
    a normal part of the exit sequence (cancel SL, then market-exit),
    but useful confirmation that the SL is no longer live.
    """
    text = (
        f"❎ <b>SL Order Cancelled</b>\n"
        f"<b>{trading_symbol}</b>\n"
        f"SL was: ₹{sl_trigger}\n"
        + (f"Reason: {reason}\n" if reason else "")
        + f"Time: {_now()}"
    )
    _send_message(bot_token, chat_id, text)


def alert_order_failed(bot_token: str, chat_id: str,
                       symbol: str, reason: str, mode: str = "live"):
    """
    Sent when an order placement is rejected/fails — i.e. the
    exchange/broker did NOT respond with a successful order. For
    LIVE mode, an SL-placement failure after a successful entry is
    especially urgent (naked position) and is worded accordingly.
    """
    mode_tag = "📄 PAPER" if mode == "paper" else "💰 LIVE"
    text = (
        f"🚨 <b>ORDER FAILED</b> [{mode_tag}]\n"
        f"<b>{symbol}</b>\n"
        f"{reason}\n"
        f"Time: {_now()}"
    )
    _send_message(bot_token, chat_id, text)


def alert_daily_summary(bot_token: str, chat_id: str,
                        symbol: str, total_trades: int,
                        winners: int, total_pnl: float,
                        mode: str = "live"):
    mode_tag  = "📄 PAPER" if mode == "paper" else "💰 LIVE"
    losers    = total_trades - winners
    win_rate  = round(winners / total_trades * 100, 1) if total_trades > 0 else 0
    pnl_emoji = "💚" if total_pnl >= 0 else "🔴"
    pnl_sign  = "+" if total_pnl >= 0 else ""
    text = (
        f"📊 <b>Daily Summary</b> [{mode_tag}]\n"
        f"Symbol: {symbol}\n"
        f"Trades:  {total_trades} "
        f"(✅{winners} ❌{losers})\n"
        f"Win Rate: {win_rate}%\n"
        f"{pnl_emoji} P&amp;L: <b>₹{pnl_sign}{total_pnl}</b>\n"
        f"Date: {datetime.now().strftime('%d %b %Y')}"
    )
    _send_message(bot_token, chat_id, text)


def alert_risk_limit_hit(bot_token: str, chat_id: str,
                         reason: str, value: float):
    text = (
        f"⚠️ <b>Risk Limit Hit — Bot Paused</b>\n"
        f"Reason: {reason}\n"
        f"Value: ₹{value}\n"
        f"Time: {_now()}\n"
        f"Bot will resume tomorrow."
    )
    _send_message(bot_token, chat_id, text)


def alert_pending_trade(bot_token: str, chat_id: str,
                        trade_id: int, symbol: str, opt_type: str,
                        quantity: int,
                        strategy: str, confidence: Optional[float] = None,
                        expires_at: Optional[str] = None,
                        trading_symbol: Optional[str] = None):
    """
    Sent when a SEMI_AUTO trade signal is generated and requires
    user approval. Includes trade details and inline buttons to
    approve or reject directly from Telegram.

    NOTE: no signal-time entry price or stop loss is shown — the
    trade fills at the current LTP on approval and SL is set from
    the actual fill.
    """
    sym_display = trading_symbol or symbol
    text = (
        f"⏳ <b>Pending Trade #{trade_id} — Action Required</b>\n\n"
        f"<b>{sym_display}</b> ({opt_type})\n"
        f"Qty:    {quantity}\n"
        f"Strategy: {strategy}\n"
    )
    if confidence is not None:
        text += f"Confidence: {confidence:.0f}%\n"
    if expires_at:
        text += f"\n⏰ Expires: {expires_at}\n"
    text += (
        f"\n"
        f"<i>Or use the dashboard to review all pending trades.</i>\n"
        f"Time: {_now()}"
    )
    
    # Inline keyboard with Approve/Reject buttons
    reply_markup = {
        "inline_keyboard": [
            [
                {"text": "✅ Approve", "callback_data": f"approve_{trade_id}"},
                {"text": "❌ Reject", "callback_data": f"reject_{trade_id}"}
            ]
        ]
    }
    
    _send_message(bot_token, chat_id, text, reply_markup=reply_markup)

    # Voice alert so the user is alerted even if not watching the chat
    voice_text = (
        f"Trade alert. Pending trade {trade_id}. "
        f"{sym_display} {opt_type}. "
        f"Quantity {quantity}. "
        f"Please approve or reject. Trade will fill at current LTP."
    )
    send_voice_message(bot_token, chat_id, voice_text)


def test_telegram(bot_token: str, chat_id: str) -> bool:
    """Test if the bot token and chat ID are valid."""
    text = (
        "✅ <b>Telegram connected!</b>\n"
        "AlgoBot bot is ready to send alerts.\n"
        f"Time: {_now()}"
    )
    return _send_message(bot_token, chat_id, text)
