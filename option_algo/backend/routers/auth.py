# backend/routers/auth.py
# ================================================================
# Authentication routes:
#
#   POST /api/auth/register          — email + password signup
#                                      (sends verification email)
#   POST /api/auth/login             — email + password login
#   GET  /api/auth/verify-email      — one-click email verification
#   POST /api/auth/resend-verify     — resend verification email
#   POST /api/auth/refresh           — JWT token refresh
#
#   GET  /api/auth/google/login      — redirect to Google consent screen
#   GET  /api/auth/google/callback   — Google OAuth callback (code exchange)
#
# Email verification flow:
#   1. User registers → is_active=False, token stored in DB
#   2. Verification email sent (or URL printed to log in dev)
#   3. User clicks link → GET /verify-email?token=...
#   4. is_active=True, email_verified=True, token cleared
#   5. Redirect to / with ?verified=1
#
# Google Sign-In flow:
#   1. GET /google/login → redirect to Google
#   2. User logs in on Google
#   3. GET /google/callback?code=&state= → upsert User
#      (Google accounts are pre-verified → is_active=True immediately)
#   4. Redirect to /dashboard with tokens in cookie / query
# ================================================================

from datetime import datetime, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import RedirectResponse
from fastapi.security import OAuth2PasswordRequestForm
import re
from pydantic import BaseModel, EmailStr, Field, field_validator
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from backend.db.database import get_db
from backend.db.models import User, BotConfig, UserRole
from backend.services.auth_service import (
    hash_password, verify_password,
    create_access_token, create_refresh_token, decode_token,
    get_current_user as get_current_user_dep,
)
from backend.services.email_service import (
    generate_verify_token, send_verification_email, send_welcome_email,
    send_password_reset_email,
)
from backend.config import get_settings

router   = APIRouter(prefix="/api/auth", tags=["auth"])
settings = get_settings()

VERIFY_TOKEN_TTL_HOURS = 24


# ── Models ──────────────────────────────────────────────────────

_MOBILE_RE = re.compile(r"^[6-9]\d{9}$")  # 10-digit Indian mobile number


class RegisterIn(BaseModel):
    email:          EmailStr
    password:       str = Field(min_length=8)
    full_name:      str = Field(min_length=1)
    mobile_number:  str

    @field_validator("full_name", "mobile_number", mode="before")
    @classmethod
    def _strip(cls, v):
        return v.strip() if isinstance(v, str) else v

    @field_validator("mobile_number")
    @classmethod
    def _validate_mobile(cls, v: str) -> str:
        if not _MOBILE_RE.match(v):
            raise ValueError("Enter a valid 10-digit mobile number")
        return v


class TokenOut(BaseModel):
    access_token:  str
    refresh_token: str
    token_type:    str = "bearer"


# ── Helpers ─────────────────────────────────────────────────────

async def _create_user_and_config(db: AsyncSession, email: str,
                                   full_name: str,
                                   hashed_pw: Optional[str],
                                   google_id: Optional[str] = None,
                                   avatar_url: Optional[str] = None,
                                   pre_verified: bool = False,
                                   mobile_number: Optional[str] = None,
                                   role: UserRole = UserRole.user) -> User:
    user = User(
        email           = email,
        hashed_password = hashed_pw or "",
        full_name       = full_name,
        mobile_number   = mobile_number,
        google_id       = google_id,
        avatar_url      = avatar_url,
        email_verified  = pre_verified,
        is_active       = pre_verified,   # active immediately only for Google sign-in / admin
        role            = role,
    )
    if not pre_verified:
        user.email_verify_token   = generate_verify_token()
        user.email_verify_sent_at = datetime.utcnow()
    db.add(user)
    await db.flush()
    db.add(BotConfig(user_id=user.id))
    await db.commit()
    await db.refresh(user)
    return user


def _issue_tokens(user: User) -> TokenOut:
    data = {"sub": str(user.id)}
    return TokenOut(
        access_token  = create_access_token(data),
        refresh_token = create_refresh_token(data),
    )


async def _maybe_start_trial(db: AsyncSession, user: User, request: Optional[Request]) -> None:
    """
    Grants the one-time 7-day paper-trading trial on first successful
    login (email/password or Google) — see backend.services.
    subscription_service.start_trial(), which is a no-op if
    user.trial_used is already True (trials can't be reclaimed).
    """
    from backend.services.subscription_service import start_trial
    from backend.services.audit_log import log_event
    from backend.services.billing_notifications import notify

    if user.role == UserRole.admin:
        # Admins are never trial-limited — no subscription required.
        return

    sub = await start_trial(db, user)
    if sub is None:
        return
    ip = request.client.host if request and request.client else None
    await log_event(db, user.id, "trial_created", "7-day free trial started", ip_address=ip)
    await db.commit()
    # notify() only does one quick query itself, then fires the actual
    # email/Telegram/push sends as background tasks — await it directly
    # rather than wrapping in create_task, since `db` may be closed by
    # the request teardown before a wrapped task gets to run.
    await notify(db, user, "trial_started", days=7)


# ── REGISTER ────────────────────────────────────────────────────

@router.post("/register", response_model=TokenOut)
async def register(body: RegisterIn, db: AsyncSession = Depends(get_db)):
    # body.password / body.full_name / body.mobile_number are already
    # format-validated by RegisterIn (min length, mobile pattern) — a
    # malformed request never reaches here, FastAPI returns 422 first.
    res = await db.execute(select(User).where(User.email == body.email))
    existing = res.scalar_one_or_none()
    if existing:
        if existing.google_id and not existing.hashed_password:
            # Google account with same email — prompt them to sign in with Google
            raise HTTPException(400,
                "This email is linked to a Google account — please use 'Sign in with Google'")
        raise HTTPException(400, "Email already registered")

    mres = await db.execute(select(User).where(User.mobile_number == body.mobile_number))
    if mres.scalar_one_or_none():
        raise HTTPException(400, "Mobile number already registered")

    user = await _create_user_and_config(
        db, body.email, body.full_name, hash_password(body.password),
        mobile_number=body.mobile_number)

    # Send verification email (non-blocking — fire and forget)
    import asyncio
    asyncio.create_task(
        send_verification_email(user.email, user.full_name, user.email_verify_token))

    # Return tokens immediately — user can explore the app but bot
    # start is blocked until is_active=True (email verified).
    return _issue_tokens(user)


# ── LOGIN ────────────────────────────────────────────────────────

@router.post("/login", response_model=TokenOut)
async def login(request: Request, form: OAuth2PasswordRequestForm = Depends(),
                db: AsyncSession = Depends(get_db)):
    res  = await db.execute(select(User).where(User.email == form.username))
    user = res.scalar_one_or_none()

    # ── Admin login (ADMIN_EMAIL / ADMIN_PASSWORD from env) ──
    # Logging in with the server's configured admin email + password
    # auto-provisions an admin account: role=admin, verified, active.
    if form.username.strip().lower() == settings.ADMIN_EMAIL.strip().lower():
        import hmac

        if not hmac.compare_digest(form.password, settings.ADMIN_PASSWORD):
            raise HTTPException(401, "Invalid credentials")
        if not user:
            user = await _create_user_and_config(
                db, settings.ADMIN_EMAIL, "Administrator",
                hash_password(settings.ADMIN_PASSWORD),
                pre_verified=True, role=UserRole.admin)
        else:
            user.role          = UserRole.admin
            user.email_verified = True
            user.is_active      = True
            db.add(user)
            await db.commit()
        await _maybe_start_trial(db, user, request)
        return _issue_tokens(user)

    if not user:
        raise HTTPException(401, "Invalid credentials")
    if user.google_id and not user.hashed_password:
        raise HTTPException(401,
            "This account uses Google Sign-In — please click 'Sign in with Google'")
    if not verify_password(form.password, user.hashed_password or ""):
        raise HTTPException(401, "Invalid credentials")
    if not user.email_verified:
        raise HTTPException(403,
            "Email not verified — check your inbox or click 'Resend verification email'")
    if not user.is_active:
        raise HTTPException(403, "Account disabled by administrator")

    # First successful login after verification -> grant the one-time
    # 7-day paper-trading trial (no-op if already used).
    await _maybe_start_trial(db, user, request)
    return _issue_tokens(user)


# ── EMAIL VERIFICATION ───────────────────────────────────────────

@router.get("/verify-email")
async def verify_email(token: str = "", db: AsyncSession = Depends(get_db)):
    """
    One-click verification from the link in the email.
    Redirects to / with ?verified=1 on success, or ?error=... on failure.
    """
    if not token:
        return RedirectResponse("/login?error=missing_token")

    res  = await db.execute(
        select(User).where(User.email_verify_token == token))
    user = res.scalar_one_or_none()
    if not user:
        return RedirectResponse("/login?error=invalid_token")

    # Check TTL
    if user.email_verify_sent_at:
        age = datetime.utcnow() - user.email_verify_sent_at
        if age > timedelta(hours=VERIFY_TOKEN_TTL_HOURS):
            return RedirectResponse("/login?error=token_expired")

    user.email_verified      = True
    user.is_active           = True
    user.email_verify_token  = None
    user.email_verify_sent_at = None
    db.add(user)
    await db.commit()

    # Send welcome email
    import asyncio
    asyncio.create_task(send_welcome_email(user.email, user.full_name, "email"))

    return RedirectResponse("/login?verified=1")


@router.post("/resend-verify")
async def resend_verify(body: dict, db: AsyncSession = Depends(get_db)):
    email = body.get("email", "").strip().lower()
    if not email:
        raise HTTPException(400, "email required")
    res  = await db.execute(select(User).where(User.email == email))
    user = res.scalar_one_or_none()
    if not user:
        # Don't reveal whether email exists
        return {"ok": True}
    if user.email_verified:
        return {"ok": True, "message": "Already verified"}

    # Rate-limit: don't send more than once per 2 minutes
    if user.email_verify_sent_at:
        since = (datetime.utcnow() - user.email_verify_sent_at).total_seconds()
        if since < 120:
            raise HTTPException(429, f"Please wait {int(120-since)}s before resending")

    user.email_verify_token   = generate_verify_token()
    user.email_verify_sent_at = datetime.utcnow()
    db.add(user)
    await db.commit()

    import asyncio
    asyncio.create_task(
        send_verification_email(user.email, user.full_name, user.email_verify_token))
    return {"ok": True}


RESET_TOKEN_TTL_HOURS = 1   # short window — password reset is sensitive


# ── FORGOT PASSWORD ──────────────────────────────────────────────

@router.post("/forgot-password")
async def forgot_password(body: dict, db: AsyncSession = Depends(get_db)):
    """
    Sends a password reset link to the given email address.
    Always returns {"ok": True} regardless of whether the email
    exists — prevents email enumeration.
    Rate-limit: one reset email per 5 minutes per address.
    """
    email = body.get("email", "").strip().lower()
    if not email:
        raise HTTPException(400, "email required")

    res  = await db.execute(select(User).where(User.email == email))
    user = res.scalar_one_or_none()

    # Don't reveal whether email exists
    if not user:
        return {"ok": True}

    # Google-only accounts have no password to reset
    if user.google_id and not user.hashed_password:
        return {"ok": True}   # silently skip — don't reveal account type

    # Rate-limit: max one reset per 5 minutes
    if user.password_reset_sent_at:
        since = (datetime.utcnow() - user.password_reset_sent_at).total_seconds()
        if since < 300:
            raise HTTPException(429, f"Please wait {int(300 - since)}s before requesting another reset")

    token = generate_verify_token()   # reuse same secure token generator
    user.password_reset_token   = token
    user.password_reset_sent_at = datetime.utcnow()
    db.add(user)
    await db.commit()

    import asyncio
    asyncio.create_task(
        send_password_reset_email(user.email, user.full_name, token))

    return {"ok": True}


@router.get("/verify-reset-token")
async def verify_reset_token(token: str = "", db: AsyncSession = Depends(get_db)):
    """
    Called by the reset-password page on load to confirm the token
    is still valid before showing the new-password form.
    Returns {"valid": True/False, "email": "..."}.
    """
    if not token:
        return {"valid": False, "reason": "missing"}

    res  = await db.execute(select(User).where(User.password_reset_token == token))
    user = res.scalar_one_or_none()
    if not user:
        return {"valid": False, "reason": "invalid"}

    if user.password_reset_sent_at:
        age = datetime.utcnow() - user.password_reset_sent_at
        if age > timedelta(hours=RESET_TOKEN_TTL_HOURS):
            return {"valid": False, "reason": "expired"}

    return {"valid": True, "email": user.email}


@router.post("/reset-password")
async def reset_password(body: dict, db: AsyncSession = Depends(get_db)):
    """
    Accepts the reset token + new password, updates the hash,
    clears the token, and returns fresh JWT tokens so the user
    is automatically logged in after reset.
    """
    token       = body.get("token", "").strip()
    new_password = body.get("password", "")

    if not token:
        raise HTTPException(400, "token required")
    if len(new_password) < 8:
        raise HTTPException(400, "Password must be at least 8 characters")

    res  = await db.execute(select(User).where(User.password_reset_token == token))
    user = res.scalar_one_or_none()
    if not user:
        raise HTTPException(400, "Invalid or expired reset link")

    if user.password_reset_sent_at:
        age = datetime.utcnow() - user.password_reset_sent_at
        if age > timedelta(hours=RESET_TOKEN_TTL_HOURS):
            raise HTTPException(400, "Reset link has expired — please request a new one")

    user.hashed_password        = hash_password(new_password)
    user.password_reset_token   = None
    user.password_reset_sent_at = None
    # Ensure account is active (edge case: reset before email verify)
    user.email_verified = True
    user.is_active      = True
    db.add(user)
    await db.commit()

    return _issue_tokens(user)


# ── TOKEN REFRESH ────────────────────────────────────────────────

@router.post("/refresh", response_model=TokenOut)
async def refresh(body: dict):
    rt = body.get("refresh_token")
    if not rt:
        raise HTTPException(400, "refresh_token required")
    payload = decode_token(rt)
    if payload.get("type") != "refresh":
        raise HTTPException(401, "Invalid refresh token")
    data = {"sub": payload["sub"]}
    return TokenOut(
        access_token  = create_access_token(data),
        refresh_token = create_refresh_token(data),
    )


# ── GOOGLE SIGN-IN ───────────────────────────────────────────────

@router.get("/me")
async def me(user: User = Depends(get_current_user_dep)):
    """Returns current user profile + verification status."""
    return {
        "id":             user.id,
        "email":          user.email,
        "full_name":      user.full_name,
        "email_verified": user.email_verified,
        "is_active":      user.is_active,
        "avatar_url":     user.avatar_url,
        "has_google":     bool(user.google_id),
        "role":           user.role.value,
    }


@router.get("/google/status")
async def google_status():
    """Public — lets the login page know if Google Sign-In is available."""
    return {"enabled": settings.google_enabled}


@router.get("/google/login")
async def google_login():
    if not settings.google_enabled:
        raise HTTPException(501, "Google Sign-In is not configured on this server")
    from backend.services.google_oauth import build_login_url
    return RedirectResponse(build_login_url())


@router.get("/google/callback")
async def google_callback(
    code:  Optional[str] = None,
    state: Optional[str] = None,
    error: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
):
    """
    Google redirects here after the user logs in. Exchanges the
    auth code for an ID token, upserts the User row, issues JWTs,
    and redirects to /dashboard with tokens as query params
    (the frontend reads them, stores in localStorage, removes from URL).
    """
    import json as _json
    import urllib.parse

    def _fail(msg: str):
        return RedirectResponse(f"/login?error={urllib.parse.quote(msg)}")

    if error:
        return _fail(f"Google login error: {error}")
    if not code or not state:
        return _fail("Missing code or state from Google")

    from backend.services.google_oauth import verify_state, exchange_code, get_user_info
    if not verify_state(state):
        return _fail("Invalid or expired login link — please try again")

    # Exchange code for tokens
    try:
        token_resp = await exchange_code(code)
    except Exception as e:
        return _fail(f"Token exchange failed: {e}")

    access_token = token_resp.get("access_token")
    if not access_token:
        return _fail("Google did not return an access token")

    # Get user profile
    try:
        info = await get_user_info(access_token)
    except Exception as e:
        return _fail(f"Failed to get user info: {e}")

    google_id  = info.get("sub", "")
    email      = info.get("email", "").lower().strip()
    full_name  = info.get("name", "")
    avatar_url = info.get("picture", "")
    g_verified = info.get("email_verified", False)

    if not email or not google_id:
        return _fail("Google account missing email or ID")

    # Upsert user
    res  = await db.execute(select(User).where(User.google_id == google_id))
    user = res.scalar_one_or_none()

    if not user:
        # Try matching by email (might have registered with password first)
        res2 = await db.execute(select(User).where(User.email == email))
        user = res2.scalar_one_or_none()

    if user:
        # Link Google account to existing user
        if not user.google_id:
            user.google_id = google_id
        if avatar_url:
            user.avatar_url = avatar_url
        if not user.email_verified and g_verified:
            user.email_verified = True
            user.is_active      = True
        db.add(user)
        await db.commit()
    else:
        # New user via Google — pre-verified (Google verified the email)
        user = await _create_user_and_config(
            db, email, full_name,
            hashed_pw    = None,
            google_id    = google_id,
            avatar_url   = avatar_url,
            pre_verified = True,
        )
        import asyncio
        asyncio.create_task(send_welcome_email(email, full_name, "Google"))

    if not user.is_active:
        return _fail("Account disabled by administrator")

    # First successful login via Google -> grant the one-time 7-day
    # paper-trading trial (no-op if already used).
    await _maybe_start_trial(db, user, None)

    tokens = _issue_tokens(user)

    # Redirect to dashboard with tokens in URL — JS reads and removes them
    params = urllib.parse.urlencode({
        "access_token":  tokens.access_token,
        "refresh_token": tokens.refresh_token,
    })
    return RedirectResponse(f"/dashboard?{params}")
