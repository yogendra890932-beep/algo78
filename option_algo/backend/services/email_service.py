# backend/services/email_service.py
# ================================================================
# Email service — sends verification emails via SMTP.
# Works with Gmail (App Password), Mailgun, SendGrid, etc.
#
# Configuration (.env):
#   SMTP_HOST=smtp.gmail.com
#   SMTP_PORT=587
#   SMTP_USER=you@gmail.com
#   SMTP_PASSWORD=your-app-password   ← Gmail App Password, NOT your main password
#   SMTP_FROM=Optiscalper <you@gmail.com>  ← optional, defaults to SMTP_USER
#   APP_BASE_URL=https://yourdomain.com
#
# Gmail setup:
#   1. Enable 2-Step Verification on your Google account
#   2. Go to myaccount.google.com/apppasswords
#   3. Create an "App Password" for Optiscalper
#   4. Use that 16-char password as SMTP_PASSWORD
# ================================================================

import asyncio
import smtplib
import secrets
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Optional

from backend.config import get_settings

settings = get_settings()


def generate_verify_token() -> str:
    """URL-safe 48-char token — enough entropy for a one-time verification link."""
    return secrets.token_urlsafe(36)


def _build_verify_email(to_email: str, full_name: str, token: str) -> MIMEMultipart:
    verify_url = f"{settings.APP_BASE_URL}/api/auth/verify-email?token={token}"
    from_addr  = settings.SMTP_FROM or settings.SMTP_USER
    subject    = "Verify your Optiscalper email address"

    html = f"""
<!DOCTYPE html>
<html>
<body style="font-family:Arial,sans-serif;background:#0f1923;color:#e2e8f0;padding:32px">
  <div style="max-width:520px;margin:0 auto;background:#1a2636;border-radius:12px;
              padding:32px;border:1px solid #2a3a4a">
    <div style="font-size:28px;margin-bottom:8px">📈 Optiscalper</div>
    <h2 style="color:#10b981;margin-top:0">Verify your email</h2>
    <p>Hi {full_name or 'there'},</p>
    <p>Click the button below to verify your email address and activate your account.
    This link expires in <strong>24 hours</strong>.</p>
    <div style="text-align:center;margin:32px 0">
      <a href="{verify_url}"
         style="background:#10b981;color:#fff;padding:14px 32px;border-radius:8px;
                text-decoration:none;font-weight:700;font-size:15px;display:inline-block">
        ✅ Verify Email Address
      </a>
    </div>
    <p style="font-size:12px;color:#64748b">
      Or paste this link in your browser:<br>
      <a href="{verify_url}" style="color:#3b82f6">{verify_url}</a>
    </p>
    <hr style="border-color:#2a3a4a;margin:24px 0">
    <p style="font-size:12px;color:#64748b">
      If you didn't create an Optiscalper account, you can safely ignore this email.
    </p>
  </div>
</body>
</html>"""

    text = (f"Hi {full_name or 'there'},\n\n"
            f"Verify your Optiscalper email address:\n{verify_url}\n\n"
            f"This link expires in 24 hours.\n\n"
            f"If you didn't sign up, ignore this email.")

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = from_addr
    msg["To"]      = to_email
    msg.attach(MIMEText(text, "plain"))
    msg.attach(MIMEText(html,  "html"))
    return msg


def _send_smtp(msg: MIMEMultipart, to_email: str):
    """Blocking SMTP send — call via asyncio.to_thread."""
    from_addr = settings.SMTP_FROM or settings.SMTP_USER
    with smtplib.SMTP(settings.SMTP_HOST, settings.SMTP_PORT, timeout=15) as smtp:
        smtp.ehlo()
        smtp.starttls()
        smtp.login(settings.SMTP_USER, settings.SMTP_PASSWORD)
        smtp.sendmail(from_addr, [to_email], msg.as_string())


async def send_verification_email(to_email: str, full_name: str, token: str) -> bool:
    """
    Sends a verification email. Returns True on success.
    Safe to call from FastAPI async context — SMTP is run in a
    thread pool (non-blocking).
    If email is not configured (SMTP_USER/PASSWORD missing),
    prints the verify URL to the server log instead so development
    works without an email setup.
    """
    if not settings.email_enabled:
        verify_url = f"{settings.APP_BASE_URL}/api/auth/verify-email?token={token}"
        print(f"\n[email] SMTP not configured — verification URL for {to_email}:\n{verify_url}\n")
        return True   # silently succeed in dev
    try:
        msg = _build_verify_email(to_email, full_name, token)
        await asyncio.to_thread(_send_smtp, msg, to_email)
        print(f"[email] Verification sent to {to_email}")
        return True
    except Exception as e:
        print(f"[email] Failed to send to {to_email}: {e}")
        return False


def _build_welcome_email(to_email: str, full_name: str, method: str) -> MIMEMultipart:
    from_addr = settings.SMTP_FROM or settings.SMTP_USER
    html = f"""
<!DOCTYPE html>
<html>
<body style="font-family:Arial,sans-serif;background:#0f1923;color:#e2e8f0;padding:32px">
  <div style="max-width:520px;margin:0 auto;background:#1a2636;border-radius:12px;
              padding:32px;border:1px solid #2a3a4a">
    <div style="font-size:28px;margin-bottom:8px">📈 Optiscalper</div>
    <h2 style="color:#10b981;margin-top:0">Welcome aboard!</h2>
    <p>Hi {full_name or 'there'},</p>
    <p>Your Optiscalper account has been created{' via ' + method if method else ''}. 
    You can now log in and configure your trading bot.</p>
    <div style="text-align:center;margin:32px 0">
      <a href="{settings.APP_BASE_URL}"
         style="background:#3b82f6;color:#fff;padding:14px 32px;border-radius:8px;
                text-decoration:none;font-weight:700;font-size:15px;display:inline-block">
        Go to Dashboard →
      </a>
    </div>
    <p style="font-size:12px;color:#64748b">
      Next steps: connect your Upstox account in Settings to start trading.
    </p>
  </div>
</body>
</html>"""
    msg = MIMEMultipart("alternative")
    msg["Subject"] = "Welcome to Optiscalper"
    msg["From"]    = from_addr
    msg["To"]      = to_email
    msg.attach(MIMEText(html, "html"))
    return msg


async def send_welcome_email(to_email: str, full_name: str, method: str = ""):
    if not settings.email_enabled:
        return
    try:
        msg = _build_welcome_email(to_email, full_name, method)
        await asyncio.to_thread(_send_smtp, msg, to_email)
    except Exception as e:
        print(f"[email] Welcome email failed for {to_email}: {e}")


def _build_reset_email(to_email: str, full_name: str, token: str) -> MIMEMultipart:
    reset_url = f"{settings.APP_BASE_URL}/reset-password?token={token}"
    from_addr = settings.SMTP_FROM or settings.SMTP_USER
    subject   = "Reset your Optiscalper password"

    html = f"""
<!DOCTYPE html>
<html>
<body style="font-family:Arial,sans-serif;background:#0f1923;color:#e2e8f0;padding:32px">
  <div style="max-width:520px;margin:0 auto;background:#1a2636;border-radius:12px;
              padding:32px;border:1px solid #2a3a4a">
    <div style="font-size:28px;margin-bottom:8px">📈 Optiscalper</div>
    <h2 style="color:#ef4444;margin-top:0">Password Reset</h2>
    <p>Hi {full_name or 'there'},</p>
    <p>Someone requested a password reset for your Optiscalper account.
    Click the button below to set a new password.
    This link expires in <strong>1 hour</strong>.</p>
    <div style="text-align:center;margin:32px 0">
      <a href="{reset_url}"
         style="background:#ef4444;color:#fff;padding:14px 32px;border-radius:8px;
                text-decoration:none;font-weight:700;font-size:15px;display:inline-block">
        🔑 Reset Password
      </a>
    </div>
    <p style="font-size:12px;color:#64748b">
      Or paste this link in your browser:<br>
      <a href="{reset_url}" style="color:#3b82f6">{reset_url}</a>
    </p>
    <hr style="border-color:#2a3a4a;margin:24px 0">
    <p style="font-size:12px;color:#64748b">
      If you did not request a password reset, ignore this email — your
      password will not be changed and this link will expire in 1 hour.
    </p>
  </div>
</body>
</html>"""

    text = (f"Hi {full_name or 'there'},\n\n"
            f"Reset your Optiscalper password:\n{reset_url}\n\n"
            f"This link expires in 1 hour.\n\n"
            f"If you didn't request this, ignore this email.")

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = from_addr
    msg["To"]      = to_email
    msg.attach(MIMEText(text, "plain"))
    msg.attach(MIMEText(html, "html"))
    return msg


async def send_html_email(to_email: str, subject: str, html: str, text: str = "") -> bool:
    """
    Generic sender for one-off notification emails (billing/subscription
    alerts — see backend.services.billing_notifications) that don't
    warrant their own dedicated _build_*_email function. Same dev-mode
    console fallback as the other senders when SMTP isn't configured.
    """
    if not settings.email_enabled:
        print(f"\n[email] SMTP not configured — '{subject}' for {to_email} not sent:\n{text or html}\n")
        return True
    try:
        from_addr = settings.SMTP_FROM or settings.SMTP_USER
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = from_addr
        msg["To"]      = to_email
        if text:
            msg.attach(MIMEText(text, "plain"))
        msg.attach(MIMEText(html, "html"))
        await asyncio.to_thread(_send_smtp, msg, to_email)
        print(f"[email] '{subject}' sent to {to_email}")
        return True
    except Exception as e:
        print(f"[email] '{subject}' failed for {to_email}: {e}")
        return False


async def send_password_reset_email(to_email: str, full_name: str, token: str) -> bool:
    """
    Sends a password reset email. Returns True on success.
    In dev (no SMTP config), prints the reset URL to the server log.
    """
    if not settings.email_enabled:
        reset_url = f"{settings.APP_BASE_URL}/reset-password?token={token}"
        print(f"\n[email] SMTP not configured — password reset URL for {to_email}:\n{reset_url}\n")
        return True
    try:
        msg = _build_reset_email(to_email, full_name, token)
        await asyncio.to_thread(_send_smtp, msg, to_email)
        print(f"[email] Password reset sent to {to_email}")
        return True
    except Exception as e:
        print(f"[email] Reset email failed for {to_email}: {e}")
        return False
