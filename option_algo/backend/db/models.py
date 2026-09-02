# backend/db/models.py
from __future__ import annotations
from datetime import datetime
from typing import Optional, List
from sqlalchemy import (
    Integer,
    String,
    Boolean,
    Float,
    DateTime,
    ForeignKey,
    Text,
    Enum as SAEnum,
    UniqueConstraint,
    Numeric,
)
from sqlalchemy.orm import mapped_column, Mapped, relationship
from backend.db.database import Base
import enum


class UserRole(str, enum.Enum):
    user = "user"
    admin = "admin"


class BotStatus(str, enum.Enum):
    stopped = "stopped"
    running = "running"
    error = "error"


class TradeStatus(str, enum.Enum):
    TARGET = "TARGET"
    SL = "SL"
    DIRECTION_FLIP = "DIRECTION_FLIP_EXIT"
    MANUAL = "MANUAL"


class ExecutionMode(str, enum.Enum):
    PAPER = "PAPER"
    SEMI_AUTO = "SEMI_AUTO"
    AUTO = "AUTO"


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    email: Mapped[str] = mapped_column(
        String(255), unique=True, index=True, nullable=False
    )
    hashed_password: Mapped[Optional[str]] = mapped_column(
        String(255), nullable=True
    )  # nullable for Google-only accounts
    full_name: Mapped[str] = mapped_column(String(255), default="")
    role: Mapped[UserRole] = mapped_column(SAEnum(UserRole), default=UserRole.user)
    is_active: Mapped[bool] = mapped_column(
        Boolean, default=False
    )  # False until email verified
    execution_mode: Mapped[ExecutionMode] = mapped_column(
        SAEnum(ExecutionMode), default=ExecutionMode.SEMI_AUTO, nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    # ── Email verification ─────────────────────────────────────
    email_verified: Mapped[bool] = mapped_column(Boolean, default=False)
    email_verify_token: Mapped[Optional[str]] = mapped_column(
        String(255), nullable=True, index=True
    )
    email_verify_sent_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime, nullable=True
    )

    # ── Password reset ─────────────────────────────────────────
    password_reset_token: Mapped[Optional[str]] = mapped_column(
        String(255), nullable=True, index=True
    )
    password_reset_sent_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime, nullable=True
    )

    # ── Google OAuth ───────────────────────────────────────────
    # google_id is the stable "sub" from the Google ID token.
    # Accounts created via Google skip email verification (Google
    # already verified the email).
    google_id: Mapped[Optional[str]] = mapped_column(
        String(255), nullable=True, unique=True, index=True
    )
    avatar_url: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)

    upstox_token_enc: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # ── Upstox OAuth (one-time setup, daily refresh via button) ──
    # API Key/Secret are configured ONCE per user in their Upstox
    # developer app — used to exchange the daily login "code" for a
    # fresh access token without manual copy/paste.
    upstox_api_key_enc: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    upstox_api_secret_enc: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    # Upstox access tokens are valid until ~3:30 AM IST the next day —
    # used to show "expired, please refresh" in the UI and to block
    # bot start with a clear error instead of a cryptic Upstox 401.
    upstox_token_expires_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime, nullable=True
    )

    # ── Subscription / billing ───────────────────────────────────
    # Nullable so existing rows aren't broken by the migration —
    # required (unique, non-empty) at the Pydantic layer for new
    # registrations only. trial_used is a permanent one-shot guard
    # that outlives the trial's `subscriptions` row, so a trial can
    # never be reclaimed even if that row is later deleted/archived.
    mobile_number: Mapped[Optional[str]] = mapped_column(
        String(15), nullable=True, unique=True, index=True
    )
    trial_used: Mapped[bool] = mapped_column(Boolean, default=False)

    config: Mapped[Optional[BotConfig]] = relationship(
        "BotConfig", back_populates="user", uselist=False, cascade="all, delete-orphan"
    )
    trades: Mapped[List[Trade]] = relationship(
        "Trade", back_populates="user", cascade="all, delete-orphan"
    )


class BotConfig(Base):
    __tablename__ = "bot_configs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), unique=True)

    underlying_symbol: Mapped[str] = mapped_column(String(20), default="NIFTY")
    underlying_token: Mapped[str] = mapped_column(String(50), default="NSE_INDEX|13")
    itm_depth: Mapped[int] = mapped_column(Integer, default=1)
    strategy: Mapped[str] = mapped_column(String(20), default="both")
    order_qty: Mapped[int] = mapped_column(Integer, default=25)
    product: Mapped[str] = mapped_column(String(5), default="I")
    trail_mode: Mapped[str] = mapped_column(String(10), default="atr")
    target_rr: Mapped[float] = mapped_column(Float, default=2.5)
    sl_pct: Mapped[float] = mapped_column(Float, default=0.003)

    max_trades_per_day: Mapped[int] = mapped_column(Integer, default=5)
    max_loss_per_day: Mapped[float] = mapped_column(Float, default=5000.0)
    trade_start_time: Mapped[str] = mapped_column(String(5), default="09:20")
    trade_end_time: Mapped[str] = mapped_column(String(5), default="15:00")

    status: Mapped[BotStatus] = mapped_column(
        SAEnum(BotStatus), default=BotStatus.stopped
    )
    last_started: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    last_stopped: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    error_msg: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    execution_mode: Mapped[ExecutionMode] = mapped_column(
        SAEnum(ExecutionMode), default=ExecutionMode.PAPER
    )
    # Backward compatibility for older configs and UI wiring
    paper_mode: Mapped[bool] = mapped_column(Boolean, default=True)

    # ── Telegram alerts ───────────────────────────────────────
    telegram_bot_token: Mapped[Optional[str]] = mapped_column(
        String(100), nullable=True
    )
    telegram_chat_id: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    telegram_on_entry: Mapped[bool] = mapped_column(Boolean, default=True)
    telegram_on_exit: Mapped[bool] = mapped_column(Boolean, default=True)
    telegram_on_trail: Mapped[bool] = mapped_column(Boolean, default=False)
    telegram_on_summary: Mapped[bool] = mapped_column(Boolean, default=True)

    # ── Multi-symbol: comma-separated symbols ─────────────────
    # e.g. "NIFTY,BANKNIFTY"  — each gets its own bot thread
    extra_symbols: Mapped[Optional[str]] = mapped_column(String(200), nullable=True)
    extra_tokens: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    # JSON string of per-symbol lot-size overrides, e.g. {"NIFTY":75,"BANKNIFTY":30}
    # Used when exchanges revise lot sizes or for symbols not in NSE_LOT_SIZES.
    custom_lot_sizes: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)

    # JSON string of per-symbol additional symbol configs.
    # Array of objects, each with: symbol, enabled, trade_mode, lots.
    # Example: [{"symbol":"BANKNIFTY","enabled":true,"trade_mode":"SEMI_AUTO","lots":2}]
    # Each Additional Symbol stores its own independent lot count.
    extra_symbol_config: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    user: Mapped[User] = relationship("User", back_populates="config")


class Trade(Base):
    __tablename__ = "trades"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), index=True)
    instrument_key: Mapped[str] = mapped_column(String(50), default="")
    trading_symbol: Mapped[str] = mapped_column(String(100), default="")
    opt_type: Mapped[str] = mapped_column(String(5), default="")
    strike: Mapped[float] = mapped_column(Float, default=0.0)
    expiry: Mapped[str] = mapped_column(String(20), default="")

    side: Mapped[str] = mapped_column(String(5), default="BUY")
    qty: Mapped[int] = mapped_column(Integer, default=0)
    entry_price: Mapped[float] = mapped_column(Float, default=0.0)
    exit_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    sl_trigger: Mapped[float] = mapped_column(Float, default=0.0)
    target: Mapped[float] = mapped_column(Float, default=0.0)
    pnl: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    status: Mapped[TradeStatus] = mapped_column(
        SAEnum(TradeStatus), default=TradeStatus.SL
    )
    strategy: Mapped[str] = mapped_column(String(30), default="")
    mode: Mapped[str] = mapped_column(String(10), default="live")  # "paper" | "live"

    entry_ts: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    exit_ts: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    user: Mapped[User] = relationship("User", back_populates="trades")


class ExchangeHoliday(Base):
    """
    Admin-managed exchange trading holidays. Read by the engine/worker
    (via backend.services.admin_config_cache) to skip trading on
    holidays, in addition to the hardcoded fallback set in
    backend.engine.history_loader.
    """

    __tablename__ = "exchange_holidays"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    exchange: Mapped[str] = mapped_column(String(10), default="NSE")
    holiday_date: Mapped[str] = mapped_column(String(10), index=True)  # "YYYY-MM-DD"
    description: Mapped[Optional[str]] = mapped_column(String(200), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    __table_args__ = (
        UniqueConstraint("exchange", "holiday_date", name="uq_holiday_exchange_date"),
    )


class StreamerSymbolToken(Base):
    """
    Admin-managed symbol -> streamer/history instrument key mapping
    (e.g. NIFTY -> "NSE_INDEX|Nifty 50"). Overrides the hardcoded
    KNOWN_INDEX_KEYS/KNOWN_STEPS dicts in backend.engine.instruments
    when an active row exists for the symbol.
    """

    __tablename__ = "streamer_symbol_tokens"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    symbol: Mapped[str] = mapped_column(String(20), unique=True, index=True)
    exchange: Mapped[str] = mapped_column(String(10), default="NSE")
    streamer_token: Mapped[str] = mapped_column(String(50))
    history_key: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    strike_step: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )


class PushSubscription(Base):
    """
    Web Push subscription (one row per browser/device the user has
    enabled notifications on). Created via the PushManager API in
    the browser, sent to POST /api/push/subscribe.

    endpoint is unique — re-subscribing the same browser updates
    (upserts) its keys rather than creating duplicates.
    """

    __tablename__ = "push_subscriptions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), index=True)
    endpoint: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    p256dh: Mapped[str] = mapped_column(String(255), nullable=False)
    auth: Mapped[str] = mapped_column(String(255), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    user: Mapped[User] = relationship("User")


# ================================================================
# SUBSCRIPTION / BILLING / PAYMENT SYSTEM
#
# Trading-permission enforcement lives in
# backend.services.subscription_service.check_trading_permission(),
# called from backend.services.bot_config_builder.resolve_start_inputs()
# — NOT from backend.engine.engine_v6, which this feature never touches.
# ================================================================


class SubscriptionStatus(str, enum.Enum):
    trial = "trial"
    active = "active"
    pending_payment = "pending_payment"
    expired = "expired"
    cancelled = "cancelled"
    suspended = "suspended"


class PaymentStatus(str, enum.Enum):
    created = "created"
    pending = "pending"
    success = "success"
    failed = "failed"
    refunded = "refunded"
    cancelled = "cancelled"
    # Manual payment statuses
    pending_verification = "pending_verification"
    approved = "approved"
    rejected = "rejected"


class PaymentProvider(str, enum.Enum):
    RAZORPAY = "RAZORPAY"
    MANUAL = "MANUAL"


class BillingProfile(Base):
    """One-per-user billing address, collected before first payment."""

    __tablename__ = "billing_profiles"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(
        ForeignKey("users.id"), unique=True, index=True
    )
    address_line1: Mapped[str] = mapped_column(String(255))
    address_line2: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    city: Mapped[str] = mapped_column(String(100))
    state: Mapped[str] = mapped_column(String(100))
    pincode: Mapped[str] = mapped_column(String(10))
    country: Mapped[str] = mapped_column(String(100), default="India")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    user: Mapped[User] = relationship("User")


class SubscriptionPlan(Base):
    """
    Admin-managed plan catalog (Starter/Basic/Professional/Enterprise
    by default, but plan_code/name/pricing/symbols are all editable
    from the admin panel — nothing about a plan is hardcoded).
    """

    __tablename__ = "subscription_plans"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    plan_code: Mapped[str] = mapped_column(String(30), unique=True, index=True)
    name: Mapped[str] = mapped_column(String(100))
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    monthly_price: Mapped[float] = mapped_column(Numeric(10, 2), default=0)
    gst_percentage: Mapped[float] = mapped_column(Numeric(5, 2), default=18)
    duration_days: Mapped[int] = mapped_column(Integer, default=30)
    # Enterprise-style plans show "Contact Sales" instead of a Buy button —
    # actual entitlements for such users live in CustomSubscription, not here.
    is_contact_sales: Mapped[bool] = mapped_column(Boolean, default=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    symbols: Mapped[List["SubscriptionPlanSymbol"]] = relationship(
        "SubscriptionPlanSymbol", cascade="all, delete-orphan"
    )


class SubscriptionPlanSymbol(Base):
    """
    One row per symbol allowed on a plan, with its own lot limit —
    this single shape covers Starter (1 symbol), Basic (shared limit
    across 2 symbols — admin just sets the same number twice) and
    Professional (a distinct limit per symbol) with no special-casing.
    Symbols are plain strings (admin picks from the existing
    StreamerSymbolToken list in the UI) so new symbols never require
    a code change.
    """

    __tablename__ = "subscription_plan_symbols"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    plan_id: Mapped[int] = mapped_column(
        ForeignKey("subscription_plans.id"), index=True
    )
    symbol: Mapped[str] = mapped_column(String(20))
    lot_limit: Mapped[int] = mapped_column(Integer, default=1)
    # True = subscriber can pick this as their single main symbol
    # (a plan may mark one or more symbols as main options; the user
    # chooses only one of them). False = additional symbol, traded
    # simultaneously alongside the chosen main symbol.
    is_main: Mapped[bool] = mapped_column(Boolean, default=False)

    __table_args__ = (UniqueConstraint("plan_id", "symbol", name="uq_plan_symbol"),)


class CustomSubscription(Base):
    """Enterprise-style per-user custom entitlement, assigned manually by an admin."""

    __tablename__ = "custom_subscriptions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), index=True)
    label: Mapped[str] = mapped_column(String(100))
    custom_price: Mapped[float] = mapped_column(Numeric(10, 2), default=0)
    custom_gst_percentage: Mapped[Optional[float]] = mapped_column(
        Numeric(5, 2), nullable=True
    )
    duration_days: Mapped[int] = mapped_column(Integer, default=30)
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_by_admin_id: Mapped[int] = mapped_column(ForeignKey("users.id"))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    symbols: Mapped[List["CustomSubscriptionSymbol"]] = relationship(
        "CustomSubscriptionSymbol", cascade="all, delete-orphan"
    )


class CustomSubscriptionSymbol(Base):
    """Mirrors SubscriptionPlanSymbol for a CustomSubscription."""

    __tablename__ = "custom_subscription_symbols"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    custom_subscription_id: Mapped[int] = mapped_column(
        ForeignKey("custom_subscriptions.id"), index=True
    )
    symbol: Mapped[str] = mapped_column(String(20))
    lot_limit: Mapped[int] = mapped_column(Integer, default=1)

    __table_args__ = (
        UniqueConstraint(
            "custom_subscription_id", "symbol", name="uq_custom_sub_symbol"
        ),
    )


class Subscription(Base):
    """
    Per-user subscription history — one row per trial/period/plan
    change (never overwritten in place), so history is preserved.
    The "current" row for a user is the one with is_current=True,
    maintained by backend.services.subscription_service (flips the
    prior current row to False in the same transaction that creates
    a new one — not DB-enforced, since SQLite/Postgres partial-unique
    index support differs and this app runs on both).
    """

    __tablename__ = "subscriptions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), index=True)
    plan_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("subscription_plans.id"), nullable=True
    )
    custom_subscription_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("custom_subscriptions.id"), nullable=True
    )
    status: Mapped[SubscriptionStatus] = mapped_column(
        SAEnum(SubscriptionStatus), default=SubscriptionStatus.trial, index=True
    )
    is_current: Mapped[bool] = mapped_column(Boolean, default=True, index=True)
    start_date: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    end_date: Mapped[datetime] = mapped_column(DateTime)
    # Set when a downgrade/queued-upgrade is scheduled to replace this
    # subscription once end_date passes (see billing_settings.upgrade_mode).
    pending_plan_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("subscription_plans.id"), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    user: Mapped[User] = relationship("User")
    plan: Mapped[Optional[SubscriptionPlan]] = relationship(
        "SubscriptionPlan", foreign_keys=[plan_id]
    )


class Payment(Base):
    """
    Payment record — one row per order created, updated through its lifecycle.
    Supports both Razorpay and Manual (PhonePe/UPI) payment providers.
    Razorpay-specific fields are nullable for manual payments.
    """

    __tablename__ = "payments"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), index=True)
    subscription_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("subscriptions.id"), nullable=True
    )
    plan_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("subscription_plans.id"), nullable=True
    )
    # ── Payment provider ──────────────────────────────────────────
    payment_provider: Mapped[str] = mapped_column(
        String(20), default=PaymentProvider.RAZORPAY.value, index=True
    )
    # ── Razorpay fields (nullable for manual payments) ────────────
    razorpay_order_id: Mapped[Optional[str]] = mapped_column(
        String(64), nullable=True, unique=True, index=True
    )
    razorpay_payment_id: Mapped[Optional[str]] = mapped_column(
        String(64), nullable=True, unique=True, index=True
    )
    razorpay_signature: Mapped[Optional[str]] = mapped_column(
        String(255), nullable=True
    )
    # ── Manual payment fields ─────────────────────────────────────
    utr_number: Mapped[Optional[str]] = mapped_column(
        String(64), nullable=True, index=True
    )
    screenshot_path: Mapped[Optional[str]] = mapped_column(
        String(512), nullable=True
    )
    verified_by: Mapped[Optional[int]] = mapped_column(
        ForeignKey("users.id"), nullable=True, index=True
    )
    verified_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    remarks: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    # ── Common fields ─────────────────────────────────────────────
    base_amount: Mapped[float] = mapped_column(Numeric(10, 2))
    gst_amount: Mapped[float] = mapped_column(Numeric(10, 2), default=0)
    total_amount: Mapped[float] = mapped_column(Numeric(10, 2))
    currency: Mapped[str] = mapped_column(String(5), default="INR")
    payment_method: Mapped[Optional[str]] = mapped_column(String(30), nullable=True)
    status: Mapped[PaymentStatus] = mapped_column(
        SAEnum(PaymentStatus), default=PaymentStatus.created, index=True
    )
    failure_reason: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    paid_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    # Prevent duplicate UTR submissions
    __table_args__ = (
        UniqueConstraint("utr_number", name="uq_payment_utr"),
    )

    user: Mapped[User] = relationship("User", foreign_keys=[user_id])
    verifier: Mapped[Optional[User]] = relationship("User", foreign_keys=[verified_by])


class InvoiceSequence(Base):
    """
    year -> last_number, incremented atomically inside the same
    transaction as invoice creation to avoid the race condition of
    a naive MAX(sequence)+1 query.
    """

    __tablename__ = "invoice_sequences"

    year: Mapped[int] = mapped_column(Integer, primary_key=True)
    last_number: Mapped[int] = mapped_column(Integer, default=0)


class Invoice(Base):
    """
    Invoice metadata + a point-in-time snapshot of amounts/billing
    details. The PDF itself is rendered on-demand at download time
    (backend.services.invoice_service.render_invoice_pdf) rather than
    stored on disk, avoiding filesystem/cleanup concerns on the VPS
    deploy target.
    """

    __tablename__ = "invoices"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    invoice_number: Mapped[str] = mapped_column(String(30), unique=True, index=True)
    payment_id: Mapped[int] = mapped_column(ForeignKey("payments.id"), unique=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), index=True)
    plan_name_snapshot: Mapped[str] = mapped_column(String(100))
    base_amount: Mapped[float] = mapped_column(Numeric(10, 2))
    gst_amount: Mapped[float] = mapped_column(Numeric(10, 2), default=0)
    total_amount: Mapped[float] = mapped_column(Numeric(10, 2))
    # Serialized (JSON) name/email/mobile/address at time of purchase —
    # later billing-profile edits must not rewrite past invoices.
    billing_snapshot: Mapped[str] = mapped_column(Text)
    issued_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    user: Mapped[User] = relationship("User")
    payment: Mapped[Payment] = relationship("Payment")


class PaymentLog(Base):
    """
    Raw audit trail for payment-related events (order created, webhook
    received, signature invalid, retry attempted) — separate from the
    generic AuditLog since these carry the raw Razorpay payload.
    """

    __tablename__ = "payment_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    payment_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("payments.id"), nullable=True, index=True
    )
    event_type: Mapped[str] = mapped_column(String(50), index=True)
    razorpay_event_id: Mapped[Optional[str]] = mapped_column(
        String(100), nullable=True, index=True
    )
    payload: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, index=True
    )


class PendingTradeStatus(str, enum.Enum):
    WAITING = "WAITING"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"


class PendingTrade(Base):
    __tablename__ = "pending_trades"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), index=True)
    signal_id: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    symbol: Mapped[str] = mapped_column(String(50), default="")
    opt_type: Mapped[str] = mapped_column(String(10), default="")
    strategy: Mapped[str] = mapped_column(String(50), default="")
    entry_price: Mapped[float] = mapped_column(Float, default=0.0)
    stop_loss: Mapped[float] = mapped_column(Float, default=0.0)
    quantity: Mapped[int] = mapped_column(Integer, default=0)
    confidence: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    status: Mapped[PendingTradeStatus] = mapped_column(
        SAEnum(PendingTradeStatus), default=PendingTradeStatus.WAITING
    )
    expires_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    approved_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    signal_payload: Mapped[Optional[str]] = mapped_column(Text, nullable=True)


class DailyAutoConsent(Base):
    __tablename__ = "daily_auto_consents"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), index=True)
    consent_date: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, index=True
    )
    accepted: Mapped[bool] = mapped_column(Boolean, default=False)
    accepted_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    valid_until: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    risk_version: Mapped[str] = mapped_column(String(20), default="v1.0")
    risk_text_snapshot: Mapped[str] = mapped_column(Text)
    ip_address: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    device_information: Mapped[Optional[str]] = mapped_column(
        String(255), nullable=True
    )
    browser_information: Mapped[Optional[str]] = mapped_column(
        String(255), nullable=True
    )
    user_agent: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    audit_hash: Mapped[str] = mapped_column(String(128), default="")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class AuditLog(Base):
    """
    Generic activity log for subscription/billing/security events
    (trial created/expired, payment initiated/verified, subscription
    activated/renewed/upgraded/downgraded/cancelled, invoice
    generated, etc). No such table existed anywhere in this codebase
    before this feature — see backend.services.audit_log.log_event().
    """

    __tablename__ = "audit_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("users.id"), nullable=True, index=True
    )
    event_type: Mapped[str] = mapped_column(String(50), index=True)
    description: Mapped[str] = mapped_column(Text)
    ip_address: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    log_metadata: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, index=True
    )


class BillingSettings(Base):
    """
    Singleton row (id=1) of global billing toggles — DB-backed (not
    .env) so admins can change them without a redeploy, same
    philosophy as ExchangeHoliday/StreamerSymbolToken.
    """

    __tablename__ = "billing_settings"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    gst_enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    upgrade_mode: Mapped[str] = mapped_column(
        String(10), default="immediate"
    )  # "immediate" | "queued"
    invoice_prefix: Mapped[str] = mapped_column(String(10), default="INV")
    # ── Payment mode settings ──────────────────────────────────────
    payment_mode: Mapped[str] = mapped_column(
        String(20), default=PaymentProvider.RAZORPAY.value
    )  # "RAZORPAY" | "MANUAL"
    manual_upi_id: Mapped[Optional[str]] = mapped_column(
        String(100), nullable=True
    )
    manual_qr_code_path: Mapped[Optional[str]] = mapped_column(
        String(512), nullable=True
    )
    manual_instructions: Mapped[Optional[str]] = mapped_column(
        Text, nullable=True
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )


class ContactSalesLead(Base):
    """Enterprise 'Contact Sales' lead capture — admin follows up and manually creates a CustomSubscription."""

    __tablename__ = "contact_sales_leads"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("users.id"), nullable=True
    )
    name: Mapped[str] = mapped_column(String(100))
    email: Mapped[str] = mapped_column(String(255))
    mobile_number: Mapped[Optional[str]] = mapped_column(String(15), nullable=True)
    message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(
        String(20), default="new"
    )  # new|contacted|converted|closed
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class PartnerRewardStatus(str, enum.Enum):
    Pending = "Pending"
    Verified = "Verified"
    Rejected = "Rejected"


class PartnerRewardRequest(Base):
    __tablename__ = "partner_reward_requests"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), index=True)
    client_id: Mapped[str] = mapped_column(
        String(100), unique=True, index=True, nullable=False
    )
    comments: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    status: Mapped[PartnerRewardStatus] = mapped_column(
        SAEnum(PartnerRewardStatus), default=PartnerRewardStatus.Pending, index=True
    )
    reward_start_date: Mapped[Optional[datetime]] = mapped_column(
        DateTime, nullable=True
    )
    reward_end_date: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    submitted_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    verified_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    verified_by: Mapped[Optional[int]] = mapped_column(
        ForeignKey("users.id"), nullable=True
    )
    admin_notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    user: Mapped[User] = relationship("User", foreign_keys=[user_id])
    verifier: Mapped[Optional[User]] = relationship("User", foreign_keys=[verified_by])


class CampaignSettings(Base):
    __tablename__ = "campaign_settings"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    enable_campaign: Mapped[bool] = mapped_column(Boolean, default=False)
    campaign_title: Mapped[str] = mapped_column(
        String(255), default="🎁 Get 7 Days FREE Paper Trading"
    )
    campaign_description: Mapped[str] = mapped_column(
        Text,
        default="Open your Upstox Demat account using our official partner referral link and receive 7 Days of FREE Paper Trading after successful verification.",
    )
    partner_referral_url: Mapped[str] = mapped_column(String(512), default="")
    button_text: Mapped[str] = mapped_column(String(100), default="Open Upstox Account")
    banner_image: Mapped[str] = mapped_column(String(512), default="")
    terms_conditions: Mapped[str] = mapped_column(Text, default="")
    campaign_start_date: Mapped[Optional[datetime]] = mapped_column(
        DateTime, nullable=True
    )
    campaign_end_date: Mapped[Optional[datetime]] = mapped_column(
        DateTime, nullable=True
    )
