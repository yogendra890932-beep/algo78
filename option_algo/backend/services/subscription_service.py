# backend/services/subscription_service.py
# ================================================================
# Core subscription business logic: trial lifecycle, activation,
# renew/upgrade/downgrade, admin actions, and the trading-permission
# check enforced from backend.services.bot_config_builder.resolve_start_inputs()
# (NEVER from backend.engine.engine_v6 — this module has no import
# of, or dependency on, the trading engine).
#
# check_trading_permission() always queries fresh via the AsyncSession
# already open in its caller (not the TTL-cached billing_cache) —
# a subscription that just activated after payment must be honored
# immediately, not after up to 5 minutes of cache staleness.
# ================================================================

from __future__ import annotations
from datetime import datetime, timedelta
from typing import Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.db.models import (
    Subscription, SubscriptionStatus, SubscriptionPlan, SubscriptionPlanSymbol,
    CustomSubscription, CustomSubscriptionSymbol, User,
)
from backend.repositories.subscription_repository import SubscriptionRepository


# ── Trial ───────────────────────────────────────────────────────

async def start_trial(db: AsyncSession, user: User) -> Optional[Subscription]:
    """
    Idempotent: grants the one-time 7-day paper-trading trial. Returns
    None (no-op) if the user has already used their trial — trials
    can never be restarted, extended, or reclaimed (user.trial_used
    is a permanent flag independent of this Subscription row).
    """
    if user.trial_used:
        return None

    repo = SubscriptionRepository(db)
    await repo.clear_current(user.id)
    now = datetime.utcnow()
    sub = await repo.create(Subscription(
        user_id=user.id, status=SubscriptionStatus.trial,
        is_current=True, start_date=now, end_date=now + timedelta(days=7),
    ))
    user.trial_used = True
    db.add(user)
    await db.flush()
    return sub


# ── Queries ─────────────────────────────────────────────────────

async def get_current_subscription(db: AsyncSession, user_id: int) -> Optional[Subscription]:
    return await SubscriptionRepository(db).get_current(user_id)


async def symbols_and_limits_for_subscription(db: AsyncSession, sub: Subscription) -> dict:
    """Returns {SYMBOL: lot_limit} for the plan or custom-subscription behind `sub`."""
    if sub.plan_id:
        res = await db.execute(
            select(SubscriptionPlanSymbol).where(SubscriptionPlanSymbol.plan_id == sub.plan_id))
        return {row.symbol.upper(): row.lot_limit for row in res.scalars().all()}
    if sub.custom_subscription_id:
        res = await db.execute(
            select(CustomSubscriptionSymbol)
            .where(CustomSubscriptionSymbol.custom_subscription_id == sub.custom_subscription_id))
        return {row.symbol.upper(): row.lot_limit for row in res.scalars().all()}
    return {}


async def main_symbols_for_subscription(db: AsyncSession, sub: Subscription) -> list:
    """
    Returns the plan's main-symbol options — the symbols the user may
    pick as their single main symbol. Falls back to ALL plan symbols
    when none are explicitly flagged as main (legacy plans).
    """
    if not sub.plan_id:
        return []
    res = await db.execute(
        select(SubscriptionPlanSymbol).where(SubscriptionPlanSymbol.plan_id == sub.plan_id))
    rows = list(res.scalars().all())
    if not rows:
        return []
    mains = [r.symbol.upper() for r in rows if r.is_main]
    if mains:
        return mains
    return [r.symbol.upper() for r in rows]


async def validate_plan_config(db: AsyncSession, sub: Subscription,
                               main_symbol: str, main_lots: int,
                               additional: Optional[list] = None) -> tuple[bool, Optional[str]]:
    """
    Validates a config save against the plan: every requested symbol must
    be in the plan's allowed list and each lot count must not exceed the
    per-symbol limit. `additional` is an optional list of
    (symbol, lots) pairs for the extra symbols. Returns (ok, reason).
    """
    allowed_symbols = await symbols_and_limits_for_subscription(db, sub)
    if not allowed_symbols:
        return False, "Your subscription plan has no trading symbols configured — contact support."
    for symbol, lots in [(main_symbol, main_lots)] + list(additional or []):
        sym = str(symbol or "").strip().upper()
        if not sym:
            continue
        if sym not in allowed_symbols:
            return False, f"Your subscription does not include {sym}."
        if int(lots or 0) > allowed_symbols[sym]:
            return False, (f"Maximum lot limit for {sym} exceeded — your plan allows "
                           f"up to {allowed_symbols[sym]} lot(s).")
    return True, None


# ── Activation / renewal / plan changes ────────────────────────

async def activate_subscription(db: AsyncSession, user_id: int, *, plan_id: Optional[int] = None,
                                 custom_subscription_id: Optional[int] = None,
                                 duration_days: int) -> Subscription:
    """
    Called after Razorpay signature verification succeeds. Creates
    the new current Subscription row (status=active) and marks any
    prior current row as no longer current — never activates without
    a verified payment (see subscription_router.verify_payment).
    """
    repo = SubscriptionRepository(db)
    await repo.clear_current(user_id)
    now = datetime.utcnow()
    sub = await repo.create(Subscription(
        user_id=user_id, plan_id=plan_id, custom_subscription_id=custom_subscription_id,
        status=SubscriptionStatus.active, is_current=True,
        start_date=now, end_date=now + timedelta(days=duration_days),
    ))
    return sub


async def renew_subscription(db: AsyncSession, current: Subscription, duration_days: int) -> Subscription:
    """Extends the current expiry date (does not lose remaining time if renewed early)."""
    base = current.end_date if current.end_date > datetime.utcnow() else datetime.utcnow()
    current.end_date = base + timedelta(days=duration_days)
    current.status = SubscriptionStatus.active
    db.add(current)
    await db.flush()
    return current


async def upgrade_subscription(db: AsyncSession, user_id: int, current: Optional[Subscription],
                                new_plan_id: int, duration_days: int,
                                upgrade_mode: str) -> Subscription:
    """
    upgrade_mode="immediate" (default): replaces the current plan right away.
    upgrade_mode="queued": current plan keeps running; new plan is
    recorded on pending_plan_id and takes effect at end_date via the
    daily job (scripts/subscription_daily_job.py).
    """
    if upgrade_mode == "queued" and current is not None:
        current.pending_plan_id = new_plan_id
        db.add(current)
        await db.flush()
        return current
    return await activate_subscription(db, user_id, plan_id=new_plan_id, duration_days=duration_days)


async def downgrade_subscription(db: AsyncSession, current: Subscription, new_plan_id: int) -> Subscription:
    """Downgrades always take effect at the current period's end_date, regardless of upgrade_mode."""
    current.pending_plan_id = new_plan_id
    db.add(current)
    await db.flush()
    return current


# ── Admin actions ───────────────────────────────────────────────

async def cancel_subscription(db: AsyncSession, sub: Subscription) -> Subscription:
    sub.status = SubscriptionStatus.cancelled
    db.add(sub)
    await db.flush()
    return sub


async def suspend_subscription(db: AsyncSession, sub: Subscription) -> Subscription:
    sub.status = SubscriptionStatus.suspended
    db.add(sub)
    await db.flush()
    return sub


async def resume_subscription(db: AsyncSession, sub: Subscription) -> Subscription:
    if sub.status == SubscriptionStatus.suspended:
        sub.status = SubscriptionStatus.active if sub.end_date > datetime.utcnow() else SubscriptionStatus.expired
        db.add(sub)
        await db.flush()
    return sub


async def extend_subscription(db: AsyncSession, sub: Subscription, days: int) -> Subscription:
    sub.end_date = sub.end_date + timedelta(days=days)
    db.add(sub)
    await db.flush()
    return sub


# ── Trading-permission enforcement ─────────────────────────────
# Called ONLY from backend.services.bot_config_builder.resolve_start_inputs()
# and (for instant UX feedback) backend.routers.all_routers.start_bot() —
# never from backend.engine.engine_v6.

async def check_trading_permission(db: AsyncSession, user_id: int,
                                    requested_symbols: list[str],
                                    requested_lots: int,
                                    is_paper: bool) -> tuple[bool, Optional[str], bool]:
    """
    Returns (allowed, reason, force_paper).

    force_paper=True means the caller MUST run this bot in paper mode
    regardless of the user's saved BotConfig.paper_mode (trial users).
    """
    sub = await get_current_subscription(db, user_id)

    if sub is None:
        return False, "No active subscription — please subscribe to a plan to start trading.", False

    if sub.status == SubscriptionStatus.trial:
        if not is_paper:
            return False, "Your free trial only allows Paper Trading — live orders are disabled during the trial.", True
        return True, None, True

    if sub.status in (SubscriptionStatus.expired, SubscriptionStatus.cancelled, SubscriptionStatus.suspended):
        return False, "Your subscription has expired or is inactive — please renew to continue trading.", False

    if sub.status == SubscriptionStatus.pending_payment:
        return False, "Your subscription payment is pending — trading is disabled until payment completes.", False

    # status == active
    if sub.end_date < datetime.utcnow():
        return False, "Your subscription has expired — please renew to continue trading.", False

    allowed_symbols = await symbols_and_limits_for_subscription(db, sub)
    if not allowed_symbols:
        return False, "Your subscription plan has no trading symbols configured — contact support.", False

    requested = [s.upper() for s in requested_symbols if s]
    ok, reason = await validate_plan_config(
        db, sub,
        requested[0] if requested else "",
        requested_lots,
        [(s, requested_lots) for s in requested[1:]],
    )
    if not ok:
        return False, reason, False

    return True, None, False
