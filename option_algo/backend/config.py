# backend/config.py
import os
import re
from functools import lru_cache
from typing import Dict, List
from dotenv import load_dotenv

load_dotenv()  # reads .env file automatically


class Settings:
    SECRET_KEY: str                   = os.getenv("SECRET_KEY", "dev-secret-change-me-32chars-min!")
    ALGORITHM: str                    = os.getenv("ALGORITHM", "HS256")
    ACCESS_TOKEN_EXPIRE_MINUTES: int  = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "60"))
    REFRESH_TOKEN_EXPIRE_DAYS: int    = int(os.getenv("REFRESH_TOKEN_EXPIRE_DAYS", "7"))
    FERNET_KEY: str                   = os.getenv("FERNET_KEY", "")
    DATABASE_URL: str                 = os.getenv("DATABASE_URL", "postgresql+asyncpg://algo_bot:algo_bot@localhost:5432/algo_bot")
    REDIS_URL: str                    = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    ADMIN_EMAIL: str                  = os.getenv("ADMIN_EMAIL", "admin@algo_bot.com")
    ADMIN_PASSWORD: str               = os.getenv("ADMIN_PASSWORD", "admin123")
    HOST: str                         = os.getenv("HOST", "0.0.0.0")
    PORT: int                         = int(os.getenv("PORT", "8000"))
    DEBUG: bool                       = os.getenv("DEBUG", "true").lower() == "true"
    ALLOWED_ORIGINS: str              = os.getenv("ALLOWED_ORIGINS", "http://localhost:8000,http://localhost:3000")

    # Upstox OAuth — this exact URL must be registered as the
    # "Redirect URI" in EVERY user's Upstox developer app (same URL
    # for all users; the user is identified via a signed `state`
    # parameter, not the URL itself).
    UPSTOX_REDIRECT_URI: str          = os.getenv("UPSTOX_REDIRECT_URI",
                                                    "http://localhost:8000/api/users/upstox/callback")

    # Web Push (VAPID) — for browser push notifications on live trade
    # events. Generate once with: python scripts/generate_vapid_keys.py
    VAPID_PUBLIC_KEY:  str            = os.getenv("VAPID_PUBLIC_KEY", "")
    VAPID_PRIVATE_KEY: str            = os.getenv("VAPID_PRIVATE_KEY", "")
    VAPID_CLAIM_EMAIL: str            = os.getenv("VAPID_CLAIM_EMAIL", "admin@algo_bot.com")

    # Upstox webhook signature secret. NOTE: Upstox order-update
    # postbacks are sent UNSIGNED — there is no postback secret/HMAC.
    # Signature verification is therefore OFF by default and only runs
    # when WEBHOOK_ENFORCE_SIGNATURE=true (for brokers/proxies that do
    # sign). Never let a missing signature drop a real order update.
    WEBHOOK_SECRET: str               = os.getenv("WEBHOOK_SECRET", "")
    WEBHOOK_ENFORCE_SIGNATURE: bool   = (
        os.getenv("WEBHOOK_ENFORCE_SIGNATURE", "false").lower() == "true"
    )

    # ── Shared Worker Feature Flags ────────────────────────────────
    # When USE_SHARED_WORKER=true, the worker process uses the
    # SharedWorkerOrchestrator instead of per-user BotThread
    # instances. Shared services (market data, candles, indicators,
    # signals) run once per symbol and are consumed by all users.
    # Set to false to revert to the legacy per-user architecture.
    USE_SHARED_WORKER: bool = (
        os.getenv("USE_SHARED_WORKER", "true").lower() == "true"
    )
    # Individual feature toggles — require USE_SHARED_WORKER=true
    USE_SHARED_MARKET_DATA: bool = (
        os.getenv("USE_SHARED_MARKET_DATA", "true").lower() == "true"
    )
    USE_SHARED_STRATEGY: bool = (
        os.getenv("USE_SHARED_STRATEGY", "true").lower() == "true"
    )
    USE_SHARED_WEBSOCKET: bool = (
        os.getenv("USE_SHARED_WEBSOCKET", "true").lower() == "true"
    )

    # ── Google OAuth ─────────────────────────────────────────────
    # Create at https://console.cloud.google.com/apis/credentials
    # Set Authorised redirect URI to:
    #   {APP_BASE_URL}/api/auth/google/callback
    GOOGLE_CLIENT_ID:     str         = os.getenv("GOOGLE_CLIENT_ID", "")
    GOOGLE_CLIENT_SECRET: str         = os.getenv("GOOGLE_CLIENT_SECRET", "")
    APP_BASE_URL:         str         = os.getenv("APP_BASE_URL", "http://localhost:8000")

    @property
    def google_redirect_uri(self) -> str:
        return f"{self.APP_BASE_URL}/api/auth/google/callback"

    @property
    def google_enabled(self) -> bool:
        return bool(self.GOOGLE_CLIENT_ID and self.GOOGLE_CLIENT_SECRET)

    # ── Email (SMTP) — for verification emails ───────────────────
    # Works with Gmail (use an App Password, not your main password)
    # or any SMTP provider (Mailgun, SendGrid, etc.)
    SMTP_HOST:     str                = os.getenv("SMTP_HOST", "smtp.gmail.com")
    SMTP_PORT:     int                = int(os.getenv("SMTP_PORT", "587"))
    SMTP_USER:     str                = os.getenv("SMTP_USER", "")
    SMTP_PASSWORD: str                = os.getenv("SMTP_PASSWORD", "")
    SMTP_FROM:     str                = os.getenv("SMTP_FROM", "")   # defaults to SMTP_USER if blank

    @property
    def email_enabled(self) -> bool:
        return bool(self.SMTP_USER and self.SMTP_PASSWORD)

    @property
    def push_enabled(self) -> bool:
        return bool(self.VAPID_PUBLIC_KEY and self.VAPID_PRIVATE_KEY)

    @property
    def origins_list(self):
        return [o.strip() for o in self.ALLOWED_ORIGINS.split(",")]

    # ── Razorpay — subscription payments ──────────────────────────
    # Create an app at https://dashboard.razorpay.com/app/keys
    # Webhook secret is set separately under Settings → Webhooks.
    RAZORPAY_KEY_ID:         str        = os.getenv("RAZORPAY_KEY_ID", "")
    RAZORPAY_KEY_SECRET:     str        = os.getenv("RAZORPAY_KEY_SECRET", "")
    RAZORPAY_WEBHOOK_SECRET: str        = os.getenv("RAZORPAY_WEBHOOK_SECRET", "")

    @property
    def razorpay_enabled(self) -> bool:
        return bool(self.RAZORPAY_KEY_ID and self.RAZORPAY_KEY_SECRET)

    @property
    def fernet(self):
        from cryptography.fernet import Fernet
        if not self.FERNET_KEY:
            raise RuntimeError(
                "FERNET_KEY not set in .env\n"
                "Run: python -c \"from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())\"\n"
                "Then paste into .env as FERNET_KEY=..."
            )
        return Fernet(self.FERNET_KEY.encode())


@lru_cache()
def get_settings() -> Settings:
    return Settings()


def is_shared_mode() -> bool:
    return get_settings().USE_SHARED_WORKER


def _raw_env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def _is_valid_bool_env(name: str) -> bool:
    val = _raw_env(name, "true").lower()
    return val in ("true", "false")


_REDIS_URL_RE = re.compile(r"^rediss?://")


def _is_valid_redis_url(url: str) -> bool:
    return bool(url and _REDIS_URL_RE.match(url))


def validate_config() -> Dict[str, List[str]]:
    errors: List[str] = []
    warnings: List[str] = []
    settings = get_settings()

    # 1. REDIS_URL
    redis_url = settings.REDIS_URL
    if not redis_url:
        errors.append("REDIS_URL is empty — must be set (e.g. redis://... or rediss://...)")
    elif not _is_valid_redis_url(redis_url):
        errors.append(
            f"REDIS_URL has invalid format: '{redis_url}' — "
            "must start with redis:// or rediss://"
        )

    # 2. DATABASE_URL
    if not settings.DATABASE_URL:
        errors.append("DATABASE_URL is empty — must be set")

    # 3. Upstox global API credentials (used by shared worker for broker connectivity)
    upstox_api_key = _raw_env("UPSTOX_API_KEY")
    upstox_api_secret = _raw_env("UPSTOX_API_SECRET")
    if not upstox_api_key:
        errors.append("UPSTOX_API_KEY is empty — must be set for broker connectivity")
    if not upstox_api_secret:
        errors.append("UPSTOX_API_SECRET is empty — must be set for broker connectivity")

    # 4. Feature flags — must be valid boolean strings
    for flag_name in (
        "USE_SHARED_WORKER",
        "USE_SHARED_MARKET_DATA",
        "USE_SHARED_STRATEGY",
        "USE_SHARED_WEBSOCKET",
    ):
        if not _is_valid_bool_env(flag_name):
            raw_val = _raw_env(flag_name, "true")
            errors.append(
                f"{flag_name} must be 'true' or 'false', got '{raw_val}'"
            )

    # 5. SHARED_TELEGRAM_BOT_TOKEN — required when shared worker is enabled
    shared_tg_token = _raw_env("SHARED_TELEGRAM_BOT_TOKEN")
    if settings.USE_SHARED_WORKER and not shared_tg_token:
        errors.append(
            "SHARED_TELEGRAM_BOT_TOKEN is empty — required when USE_SHARED_WORKER=true"
        )

    # 6. Production-safety checks (warnings, not errors)
    if settings.SECRET_KEY == "dev-secret-change-me-32chars-min!":
        warnings.append(
            "SECRET_KEY is using the INSECURE DEFAULT — "
            "set a unique random value (min 32 chars)"
        )
    if not settings.FERNET_KEY:
        warnings.append(
            "FERNET_KEY is not set — Upstox token encryption will fail. "
            "Generate with: python -c \"from cryptography.fernet import "
            "Fernet; print(Fernet.generate_key().decode())\""
        )
    if settings.ADMIN_PASSWORD == "admin123":
        warnings.append(
            "ADMIN_PASSWORD is using the INSECURE DEFAULT 'admin123' — "
            "change it before exposing this server publicly"
        )
    if settings.DEBUG:
        warnings.append(
            "DEBUG=true — disable in production (enables verbose SQL "
            "echo and other dev-only behaviour)"
        )
    if settings.WEBHOOK_ENFORCE_SIGNATURE and not settings.WEBHOOK_SECRET:
        warnings.append(
            "WEBHOOK_ENFORCE_SIGNATURE=true but WEBHOOK_SECRET is empty — "
            "signature verification cannot run; all webhook postbacks "
            "would be rejected"
        )
    if settings.USE_SHARED_WORKER:
        if not _raw_env("UPSTOX_API_KEY"):
            warnings.append(
                "UPSTOX_API_KEY not set — shared worker will be unable "
                "to connect to broker APIs"
            )
        if not _raw_env("UPSTOX_API_SECRET"):
            warnings.append(
                "UPSTOX_API_SECRET not set — shared worker will be unable "
                "to connect to broker APIs"
            )

    return {"errors": errors, "warnings": warnings}
