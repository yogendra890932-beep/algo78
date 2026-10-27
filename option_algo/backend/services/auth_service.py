# backend/services/auth_service.py
from __future__ import annotations
import secrets
from datetime import datetime, timedelta
from typing import Optional
from argon2 import PasswordHasher
from argon2.exceptions import VerifyMismatchError, VerificationError, InvalidHashError
from jose import JWTError, jwt
from fastapi import Depends, HTTPException, Request, status
from fastapi.security import OAuth2PasswordBearer
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update
from backend.config import get_settings
from backend.db.database import get_db, AsyncSessionLocal
from backend.db.models import User, UserSession

settings = get_settings()
_ph      = PasswordHasher()          # argon2-cffi — works on all Python versions
oauth2   = OAuth2PasswordBearer(tokenUrl="/api/auth/login")


def hash_password(password: str) -> str:
    return _ph.hash(password)


def verify_password(plain: str, hashed: str) -> bool:
    try:
        return _ph.verify(hashed, plain)
    except (VerifyMismatchError, VerificationError, InvalidHashError):
        return False


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None,
                        jti: Optional[str] = None) -> str:
    to_encode = data.copy()
    expire    = datetime.utcnow() + (expires_delta or timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES))
    to_encode.update({"exp": expire, "type": "access"})
    if jti:
        to_encode["jti"] = jti
    return jwt.encode(to_encode, settings.SECRET_KEY, algorithm=settings.ALGORITHM)


def create_refresh_token(data: dict, jti: Optional[str] = None) -> str:
    to_encode = data.copy()
    expire    = datetime.utcnow() + timedelta(days=settings.REFRESH_TOKEN_EXPIRE_DAYS)
    to_encode.update({"exp": expire, "type": "refresh",
                      # random nonce so every rotation mints a distinct token
                      "nonce": secrets.token_urlsafe(8)})
    if jti:
        to_encode["jti"] = jti
    return jwt.encode(to_encode, settings.SECRET_KEY, algorithm=settings.ALGORITHM)


# ── Sliding sessions (48h idle timeout) ─────────────────────────
# Each login creates a UserSession row. A session stays valid while it is
# used: every authenticated request and every token refresh reads the row
# and bumps last_seen_at (the write is throttled to once per _TOUCH_INTERVAL
# seconds). Once a session has been idle for SESSION_IDLE_HOURS (default 48)
# it is invalid, the refresh token is rejected, and the user must log in
# again. The read is a single indexed lookup, so the check is exact.

_TOUCH_INTERVAL = 300          # seconds between persisted last_seen bumps


def new_session_id() -> str:
    return secrets.token_urlsafe(24)


async def start_session(db: AsyncSession, user_id: int,
                        request: Optional[Request] = None) -> tuple[str, str]:
    """Create a new login session and return (access_token, refresh_token)."""
    jti = new_session_id()
    ua = None
    ip = None
    if request is not None:
        try:
            ua = (request.headers.get("user-agent") or "")[:255] or None
            ip = (request.client.host if request.client else None)
        except Exception:
            ua = ip = None
    now = datetime.utcnow()
    db.add(UserSession(user_id=user_id, jti=jti, last_seen_at=now,
                       created_at=now, user_agent=ua, ip=ip))
    await db.commit()
    data = {"sub": str(user_id)}
    return create_access_token(data, jti=jti), create_refresh_token(data, jti=jti)


async def validate_session(db: AsyncSession, jti: str) -> Optional[UserSession]:
    """Return the session row if it is still valid, else None.

    Invalid = unknown jti, revoked, or idle for more than SESSION_IDLE_HOURS.
    """
    res = await db.execute(select(UserSession).where(UserSession.jti == jti))
    row = res.scalar_one_or_none()
    if row is None or row.revoked:
        return None
    last = row.last_seen_at or row.created_at or datetime.utcnow()
    if datetime.utcnow() - last > timedelta(hours=settings.SESSION_IDLE_HOURS):
        return None
    return row


async def touch_session(db: AsyncSession, jti: str) -> bool:
    """Slide the session's idle window on activity. False = session invalid."""
    row = await validate_session(db, jti)
    if row is None:
        return False
    now = datetime.utcnow()
    last = row.last_seen_at or row.created_at
    if last is None or (now - last).total_seconds() >= _TOUCH_INTERVAL:
        row.last_seen_at = now
        db.add(row)
        await db.commit()
    return True


async def revoke_session(db: AsyncSession, jti: str) -> None:
    await db.execute(update(UserSession)
                     .where(UserSession.jti == jti)
                     .values(revoked=True))
    await db.commit()


async def revoke_user_sessions(db: AsyncSession, user_id: int) -> None:
    await db.execute(update(UserSession)
                     .where(UserSession.user_id == user_id)
                     .values(revoked=True))
    await db.commit()


def decode_token(token: str) -> dict:
    try:
        return jwt.decode(token, settings.SECRET_KEY, algorithms=[settings.ALGORITHM])
    except JWTError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")


async def get_current_user(
    token: str = Depends(oauth2),
    db: AsyncSession = Depends(get_db),
) -> User:
    payload = decode_token(token)
    if payload.get("type") != "access":
        raise HTTPException(status_code=401, detail="Invalid token type")
    user_id = payload.get("sub")
    if not user_id:
        raise HTTPException(status_code=401, detail="Invalid token payload")
    # Sliding session: reject once the login session has been idle for
    # SESSION_IDLE_HOURS (48h). Tokens minted before sessions existed have
    # no jti and keep working until they expire (60 min).
    jti = payload.get("jti")
    if jti and not await touch_session(db, jti):
        raise HTTPException(status_code=401,
                            detail="Session expired — please log in again")
    result = await db.execute(select(User).where(User.id == int(user_id)))
    user   = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=401, detail="User not found")
    # Note: is_active=False (unverified) users can still call endpoints
    # — the bot start check in bot_config_builder blocks unverified
    # users from actually trading. Some endpoints (settings, profile)
    # should remain accessible so they can see the "verify email" prompt.
    return user


async def get_admin_user(user: User = Depends(get_current_user)) -> User:
    if user.role.value != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    return user


def encrypt_token(raw: str) -> str:
    return get_settings().fernet.encrypt(raw.encode()).decode()


def decrypt_token(enc: str) -> str:
    return get_settings().fernet.decrypt(enc.encode()).decode()
