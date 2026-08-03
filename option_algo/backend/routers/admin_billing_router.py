# backend/routers/admin_billing_router.py
# ================================================================
# Admin-facing subscription/billing/payment management API.
# All endpoints gated by Depends(get_admin_user) — same dependency
# used by the existing admin_router (backend.routers.all_routers).
#
#   Plans:              GET/POST/PUT/DELETE /api/admin/billing/plans
#   Custom Subscriptions: GET/POST/PUT/DELETE /api/admin/billing/custom-subscriptions
#   Contact-Sales leads: GET /api/admin/billing/leads
#   Subscriptions:      GET /api/admin/billing/subscriptions
#                        POST .../{id}/extend|suspend|resume|cancel|convert-trial
#   Payments:           GET /api/admin/billing/payments
#   Revenue reports:    GET /api/admin/billing/revenue
#   Invoices:           GET /api/admin/billing/invoices, GET .../{id}/download
#   Settings:           GET/PUT /api/admin/billing/settings
#   User search:        GET /api/admin/billing/users?q=
# ================================================================

from __future__ import annotations
from datetime import datetime, timedelta
from typing import Optional, List

import re
from fastapi import APIRouter, Depends, HTTPException, Response
from pydantic import BaseModel, Field, field_validator, model_validator
from sqlalchemy import select, or_, func
from sqlalchemy.ext.asyncio import AsyncSession

from backend.db.database import get_db
from backend.db.models import (
    User, SubscriptionPlan, SubscriptionPlanSymbol, CustomSubscription,
    CustomSubscriptionSymbol, ContactSalesLead, BillingSettings, Subscription,
    SubscriptionStatus, Payment, PaymentStatus,
)
from backend.services.auth_service import get_admin_user
from backend.repositories.plan_repository import PlanRepository
from backend.repositories.subscription_repository import SubscriptionRepository
from backend.repositories.payment_repository import PaymentRepository
from backend.repositories.invoice_repository import InvoiceRepository
from backend.repositories.billing_profile_repository import BillingProfileRepository
from backend.db.models import AuditLog
from backend.services import subscription_service, invoice_service, audit_log
from backend.services import billing_cache

router = APIRouter(prefix="/api/admin/billing", tags=["admin-billing"])


_PLAN_CODE_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{1,29}$")


class SymbolLimitIn(BaseModel):
    symbol: str = Field(min_length=1, max_length=20)
    lot_limit: int = Field(default=1, ge=1)
    is_main: bool = False

    @field_validator("symbol", mode="before")
    @classmethod
    def _strip_upper(cls, v):
        return v.strip().upper() if isinstance(v, str) else v


# ── Plans ────────────────────────────────────────────────────────

class PlanIn(BaseModel):
    plan_code: str
    name: str = Field(min_length=1, max_length=100)
    description: Optional[str] = Field(default=None, max_length=1000)
    monthly_price: float = Field(default=0, ge=0)
    gst_percentage: float = Field(default=18, ge=0, le=100)
    duration_days: int = Field(default=30, ge=1)
    is_contact_sales: bool = False
    is_active: bool = True
    symbols: List[SymbolLimitIn] = []

    @field_validator("plan_code", mode="before")
    @classmethod
    def _normalize_code(cls, v):
        return v.strip().lower() if isinstance(v, str) else v

    @field_validator("plan_code")
    @classmethod
    def _validate_code(cls, v: str) -> str:
        if not _PLAN_CODE_RE.match(v or ""):
            raise ValueError("Plan code must be 2-30 lowercase letters/digits/underscores/hyphens")
        return v

    @model_validator(mode="after")
    def _require_price_unless_contact_sales(self):
        if not self.is_contact_sales and self.monthly_price <= 0:
            raise ValueError("Monthly price must be greater than 0 unless this is a Contact-Sales plan")
        return self


def _plan_out(plan: SubscriptionPlan) -> dict:
    return {
        "id": plan.id, "plan_code": plan.plan_code, "name": plan.name,
        "description": plan.description, "monthly_price": float(plan.monthly_price),
        "gst_percentage": float(plan.gst_percentage), "duration_days": plan.duration_days,
        "is_contact_sales": plan.is_contact_sales, "is_active": plan.is_active,
        "symbols": [{"symbol": s.symbol, "lot_limit": s.lot_limit, "is_main": s.is_main}
                    for s in plan.symbols],
    }


@router.get("/plans")
async def list_plans(admin=Depends(get_admin_user), db: AsyncSession = Depends(get_db)):
    plans = await PlanRepository(db).list()
    return [_plan_out(p) for p in plans]


@router.post("/plans")
async def create_plan(body: PlanIn, admin=Depends(get_admin_user), db: AsyncSession = Depends(get_db)):
    repo = PlanRepository(db)
    plan = SubscriptionPlan(
        plan_code=body.plan_code, name=body.name, description=body.description,
        monthly_price=body.monthly_price, gst_percentage=body.gst_percentage,
        duration_days=body.duration_days, is_contact_sales=body.is_contact_sales,
        is_active=body.is_active,
    )
    try:
        await repo.create(plan)
        for sym in body.symbols:
            await repo.add_symbol(SubscriptionPlanSymbol(
                plan_id=plan.id, symbol=sym.symbol.upper(), lot_limit=sym.lot_limit,
                is_main=sym.is_main))
        await db.commit()
    except Exception:
        await db.rollback()
        raise HTTPException(409, "A plan with that plan_code already exists")
    plan = await repo.get_by_id(plan.id)
    billing_cache.refresh(force=True)
    return _plan_out(plan)


@router.put("/plans/{plan_id}")
async def update_plan(plan_id: int, body: PlanIn, admin=Depends(get_admin_user), db: AsyncSession = Depends(get_db)):
    repo = PlanRepository(db)
    plan = await repo.get_by_id(plan_id)
    if not plan:
        raise HTTPException(404, "Plan not found")
    plan.plan_code = body.plan_code
    plan.name = body.name
    plan.description = body.description
    plan.monthly_price = body.monthly_price
    plan.gst_percentage = body.gst_percentage
    plan.duration_days = body.duration_days
    plan.is_contact_sales = body.is_contact_sales
    plan.is_active = body.is_active
    db.add(plan)
    try:
        await repo.delete_symbols(plan_id)
        await db.flush()
        for sym in body.symbols:
            await repo.add_symbol(SubscriptionPlanSymbol(
                plan_id=plan_id, symbol=sym.symbol.upper(), lot_limit=sym.lot_limit,
                is_main=sym.is_main))
        await db.commit()
    except Exception:
        await db.rollback()
        raise HTTPException(409, "A plan with that plan_code already exists")
    await db.refresh(plan, ["symbols"])
    plan = await repo.get_by_id(plan_id)
    billing_cache.refresh(force=True)
    return _plan_out(plan)


@router.delete("/plans/{plan_id}")
async def delete_plan(plan_id: int, admin=Depends(get_admin_user), db: AsyncSession = Depends(get_db)):
    repo = PlanRepository(db)
    plan = await repo.get_by_id(plan_id)
    if not plan:
        raise HTTPException(404, "Plan not found")
    await repo.delete(plan)
    await db.commit()
    billing_cache.refresh(force=True)
    return {"ok": True}


# ── Custom Subscriptions (Enterprise) ───────────────────────────

class CustomSubscriptionIn(BaseModel):
    user_id: int = Field(gt=0)
    label: str = Field(min_length=1, max_length=100)
    custom_price: float = Field(default=0, ge=0)
    custom_gst_percentage: Optional[float] = Field(default=None, ge=0, le=100)
    duration_days: int = Field(default=30, ge=1)
    notes: Optional[str] = Field(default=None, max_length=1000)
    symbols: List[SymbolLimitIn] = []

    @field_validator("label", "notes", mode="before")
    @classmethod
    def _strip(cls, v):
        return v.strip() if isinstance(v, str) else v


def _custom_out(c: CustomSubscription) -> dict:
    return {
        "id": c.id, "user_id": c.user_id, "label": c.label,
        "custom_price": float(c.custom_price),
        "custom_gst_percentage": float(c.custom_gst_percentage) if c.custom_gst_percentage is not None else None,
        "duration_days": c.duration_days, "notes": c.notes,
        "symbols": [{"symbol": s.symbol, "lot_limit": s.lot_limit} for s in c.symbols],
    }


@router.get("/custom-subscriptions")
async def list_custom_subscriptions(user_id: Optional[int] = None, admin=Depends(get_admin_user),
                                     db: AsyncSession = Depends(get_db)):
    if user_id:
        rows = await SubscriptionRepository(db).list_custom_for_user(user_id)
    else:
        res = await db.execute(select(CustomSubscription).order_by(CustomSubscription.created_at.desc()))
        rows = list(res.scalars().unique().all())
    out = []
    for c in rows:
        sres = await db.execute(select(CustomSubscriptionSymbol).where(
            CustomSubscriptionSymbol.custom_subscription_id == c.id))
        c.symbols = list(sres.scalars().all())
        out.append(_custom_out(c))
    return out


@router.post("/custom-subscriptions")
async def create_custom_subscription(body: CustomSubscriptionIn, admin=Depends(get_admin_user),
                                      db: AsyncSession = Depends(get_db)):
    ures = await db.execute(select(User).where(User.id == body.user_id))
    if not ures.scalar_one_or_none():
        raise HTTPException(404, "User not found")

    repo = SubscriptionRepository(db)
    custom = await repo.create_custom(CustomSubscription(
        user_id=body.user_id, label=body.label, custom_price=body.custom_price,
        custom_gst_percentage=body.custom_gst_percentage, duration_days=body.duration_days,
        notes=body.notes, created_by_admin_id=admin.id,
    ))
    for sym in body.symbols:
        await repo.add_custom_symbol(CustomSubscriptionSymbol(
            custom_subscription_id=custom.id, symbol=sym.symbol.upper(), lot_limit=sym.lot_limit))
    await audit_log.log_event(db, body.user_id, "custom_subscription_created",
                               f"Custom plan '{body.label}' assigned by admin #{admin.id}")
    await db.commit()
    custom.symbols = [SubscriptionPlanSymbol(symbol=s.symbol, lot_limit=s.lot_limit) for s in body.symbols]
    return _custom_out(custom)


@router.post("/custom-subscriptions/{custom_id}/activate")
async def activate_custom_subscription(custom_id: int, admin=Depends(get_admin_user),
                                        db: AsyncSession = Depends(get_db)):
    """Immediately activates the Enterprise custom subscription for its user (no payment flow)."""
    custom = await SubscriptionRepository(db).get_custom(custom_id)
    if not custom:
        raise HTTPException(404, "Custom subscription not found")
    repo = SubscriptionRepository(db)
    await repo.clear_current(custom.user_id)
    now = datetime.utcnow()
    sub = await repo.create(Subscription(
        user_id=custom.user_id, custom_subscription_id=custom.id,
        status=SubscriptionStatus.active, is_current=True,
        start_date=now, end_date=now + timedelta(days=custom.duration_days),
    ))
    await audit_log.log_event(db, custom.user_id, "subscription_activated",
                               f"Custom plan '{custom.label}' activated by admin #{admin.id}")
    await db.commit()
    return {"ok": True, "subscription_id": sub.id}


# ── Contact-Sales leads ──────────────────────────────────────────

@router.get("/leads")
async def list_leads(admin=Depends(get_admin_user), db: AsyncSession = Depends(get_db)):
    res = await db.execute(select(ContactSalesLead).order_by(ContactSalesLead.created_at.desc()))
    return [{
        "id": l.id, "user_id": l.user_id, "name": l.name, "email": l.email,
        "mobile_number": l.mobile_number, "message": l.message, "status": l.status,
        "created_at": l.created_at.isoformat(),
    } for l in res.scalars().all()]


@router.put("/leads/{lead_id}/status")
async def update_lead_status(lead_id: int, body: dict, admin=Depends(get_admin_user),
                              db: AsyncSession = Depends(get_db)):
    res = await db.execute(select(ContactSalesLead).where(ContactSalesLead.id == lead_id))
    lead = res.scalar_one_or_none()
    if not lead:
        raise HTTPException(404, "Lead not found")
    lead.status = body.get("status", lead.status)
    db.add(lead)
    await db.commit()
    return {"ok": True}


# ── Subscription management ─────────────────────────────────────

@router.get("/subscriptions")
async def list_subscriptions(status: Optional[str] = None, trial_only: bool = False,
                              admin=Depends(get_admin_user), db: AsyncSession = Depends(get_db)):
    stmt = select(Subscription).where(Subscription.is_current.is_(True))
    if trial_only:
        stmt = stmt.where(Subscription.status == SubscriptionStatus.trial)
    elif status:
        stmt = stmt.where(Subscription.status == status)
    res = await db.execute(stmt.order_by(Subscription.created_at.desc()))
    subs = res.scalars().all()
    out = []
    for s in subs:
        ures = await db.execute(select(User).where(User.id == s.user_id))
        u = ures.scalar_one_or_none()
        out.append({
            "id": s.id, "user_id": s.user_id,
            "user_email": u.email if u else None, "user_name": u.full_name if u else None,
            "plan_id": s.plan_id, "status": s.status.value,
            "start_date": s.start_date.isoformat(), "end_date": s.end_date.isoformat(),
            "pending_plan_id": s.pending_plan_id,
        })
    return out


@router.post("/subscriptions/{sub_id}/extend")
async def extend_sub(sub_id: int, body: dict, admin=Depends(get_admin_user), db: AsyncSession = Depends(get_db)):
    sub = await SubscriptionRepository(db).get_by_id(sub_id)
    if not sub:
        raise HTTPException(404, "Subscription not found")
    days = int(body.get("days", 0))
    await subscription_service.extend_subscription(db, sub, days)
    await audit_log.log_event(db, sub.user_id, "subscription_extended", f"Extended by {days} day(s) by admin #{admin.id}")
    await db.commit()
    return {"ok": True, "end_date": sub.end_date.isoformat()}


@router.post("/subscriptions/{sub_id}/suspend")
async def suspend_sub(sub_id: int, admin=Depends(get_admin_user), db: AsyncSession = Depends(get_db)):
    sub = await SubscriptionRepository(db).get_by_id(sub_id)
    if not sub:
        raise HTTPException(404, "Subscription not found")
    await subscription_service.suspend_subscription(db, sub)
    await audit_log.log_event(db, sub.user_id, "subscription_suspended", f"Suspended by admin #{admin.id}")
    await db.commit()
    return {"ok": True}


@router.post("/subscriptions/{sub_id}/resume")
async def resume_sub(sub_id: int, admin=Depends(get_admin_user), db: AsyncSession = Depends(get_db)):
    sub = await SubscriptionRepository(db).get_by_id(sub_id)
    if not sub:
        raise HTTPException(404, "Subscription not found")
    await subscription_service.resume_subscription(db, sub)
    await audit_log.log_event(db, sub.user_id, "subscription_resumed", f"Resumed by admin #{admin.id}")
    await db.commit()
    return {"ok": True}


@router.post("/subscriptions/{sub_id}/cancel")
async def cancel_sub(sub_id: int, admin=Depends(get_admin_user), db: AsyncSession = Depends(get_db)):
    sub = await SubscriptionRepository(db).get_by_id(sub_id)
    if not sub:
        raise HTTPException(404, "Subscription not found")
    await subscription_service.cancel_subscription(db, sub)
    await audit_log.log_event(db, sub.user_id, "subscription_cancelled", f"Cancelled by admin #{admin.id}")
    await db.commit()
    return {"ok": True}


@router.post("/subscriptions/{sub_id}/convert-trial")
async def convert_trial(sub_id: int, body: dict, admin=Depends(get_admin_user), db: AsyncSession = Depends(get_db)):
    """Admin manually converts a trial user to a paid plan without a payment flow."""
    sub = await SubscriptionRepository(db).get_by_id(sub_id)
    if not sub or sub.status != SubscriptionStatus.trial:
        raise HTTPException(400, "Subscription is not an active trial")
    plan_id = body.get("plan_id")
    plan = await PlanRepository(db).get_by_id(plan_id) if plan_id else None
    if not plan:
        raise HTTPException(404, "Plan not found")
    new_sub = await subscription_service.activate_subscription(
        db, sub.user_id, plan_id=plan.id, duration_days=plan.duration_days)
    await audit_log.log_event(db, sub.user_id, "subscription_activated",
                               f"Trial converted to plan {plan.plan_code} by admin #{admin.id}")
    await db.commit()
    return {"ok": True, "subscription_id": new_sub.id}


# ── Payments / Revenue ───────────────────────────────────────────

@router.get("/payments")
async def list_payments(status: Optional[str] = None, admin=Depends(get_admin_user),
                         db: AsyncSession = Depends(get_db)):
    payments = await PaymentRepository(db).list_all(status=status)
    return [{
        "id": p.id, "user_id": p.user_id, "plan_id": p.plan_id,
        "razorpay_order_id": p.razorpay_order_id, "razorpay_payment_id": p.razorpay_payment_id,
        "base_amount": float(p.base_amount), "gst_amount": float(p.gst_amount),
        "total_amount": float(p.total_amount), "status": p.status.value,
        "created_at": p.created_at.isoformat(),
        "paid_at": p.paid_at.isoformat() if p.paid_at else None,
    } for p in payments]


@router.get("/revenue")
async def revenue_report(admin=Depends(get_admin_user), db: AsyncSession = Depends(get_db)):
    all_time = await PaymentRepository(db).revenue_summary()
    last_30d = await PaymentRepository(db).revenue_summary(start=datetime.utcnow() - timedelta(days=30))
    by_plan_res = await db.execute(
        select(Payment.plan_id, func.count(Payment.id), func.sum(Payment.total_amount))
        .where(Payment.status == PaymentStatus.success).group_by(Payment.plan_id))
    by_plan = [{"plan_id": r[0], "count": r[1], "total": float(r[2] or 0)} for r in by_plan_res.all()]
    return {"all_time": all_time, "last_30_days": last_30d, "by_plan": by_plan}


# ── Invoices ─────────────────────────────────────────────────────

@router.get("/invoices")
async def list_all_invoices(admin=Depends(get_admin_user), db: AsyncSession = Depends(get_db)):
    invoices = await InvoiceRepository(db).list_all()
    return [{
        "id": i.id, "invoice_number": i.invoice_number, "user_id": i.user_id,
        "plan_name": i.plan_name_snapshot, "total_amount": float(i.total_amount),
        "issued_at": i.issued_at.isoformat(),
    } for i in invoices]


@router.get("/invoices/{invoice_id}/download")
async def download_invoice_admin(invoice_id: int, admin=Depends(get_admin_user), db: AsyncSession = Depends(get_db)):
    invoice = await InvoiceRepository(db).get_by_id(invoice_id)
    if not invoice:
        raise HTTPException(404, "Invoice not found")
    pdf_bytes = invoice_service.render_invoice_pdf(invoice)
    return Response(content=pdf_bytes, media_type="application/pdf", headers={
        "Content-Disposition": f'attachment; filename="{invoice.invoice_number}.pdf"'
    })


# ── Billing settings (GST toggle, upgrade mode, invoice prefix) ─

class BillingSettingsIn(BaseModel):
    gst_enabled: bool = True
    upgrade_mode: str = "immediate"
    invoice_prefix: str = Field(default="INV", min_length=1, max_length=10)

    @field_validator("invoice_prefix", mode="before")
    @classmethod
    def _strip(cls, v):
        return v.strip() if isinstance(v, str) else v


@router.get("/settings")
async def get_settings_row(admin=Depends(get_admin_user), db: AsyncSession = Depends(get_db)):
    res = await db.execute(select(BillingSettings).where(BillingSettings.id == 1))
    row = res.scalar_one_or_none()
    if not row:
        return BillingSettingsIn().model_dump()
    return {"gst_enabled": row.gst_enabled, "upgrade_mode": row.upgrade_mode, "invoice_prefix": row.invoice_prefix}


@router.put("/settings")
async def update_settings_row(body: BillingSettingsIn, admin=Depends(get_admin_user), db: AsyncSession = Depends(get_db)):
    if body.upgrade_mode not in ("immediate", "queued"):
        raise HTTPException(400, "upgrade_mode must be 'immediate' or 'queued'")
    res = await db.execute(select(BillingSettings).where(BillingSettings.id == 1))
    row = res.scalar_one_or_none()
    if not row:
        row = BillingSettings(id=1)
    row.gst_enabled = body.gst_enabled
    row.upgrade_mode = body.upgrade_mode
    row.invoice_prefix = body.invoice_prefix
    db.add(row)
    await db.commit()
    billing_cache.refresh(force=True)
    return {"ok": True}


# ── User search (billing-aware) ─────────────────────────────────

@router.get("/users")
async def search_users(q: Optional[str] = None, admin=Depends(get_admin_user), db: AsyncSession = Depends(get_db)):
    stmt = select(User)
    if q:
        like = f"%{q}%"
        stmt = stmt.where(or_(User.full_name.ilike(like), User.email.ilike(like),
                               User.mobile_number.ilike(like)))
    res = await db.execute(stmt.order_by(User.created_at.desc()))
    users = res.scalars().all()
    out = []
    for u in users:
        sub = await subscription_service.get_current_subscription(db, u.id)
        out.append({
            "id": u.id, "email": u.email, "full_name": u.full_name,
            "mobile_number": u.mobile_number, "trial_used": u.trial_used,
            "subscription_status": sub.status.value if sub else None,
        })
    return out


# ── Per-user billing profile / audit logs (read-only — UI redesign) ──
# Additive-only endpoints: surface data that already exists (BillingProfile,
# AuditLog) for the admin user-drawer. No existing endpoint, schema, or
# business logic changes.

@router.get("/users/{user_id}/billing-profile")
async def get_user_billing_profile(user_id: int, admin=Depends(get_admin_user), db: AsyncSession = Depends(get_db)):
    profile = await BillingProfileRepository(db).get_for_user(user_id)
    if not profile:
        return None
    return {
        "address_line1": profile.address_line1, "address_line2": profile.address_line2,
        "city": profile.city, "state": profile.state, "pincode": profile.pincode,
        "country": profile.country,
    }


@router.get("/users/{user_id}/audit-logs")
async def get_user_audit_logs(user_id: int, admin=Depends(get_admin_user), db: AsyncSession = Depends(get_db)):
    res = await db.execute(
        select(AuditLog).where(AuditLog.user_id == user_id).order_by(AuditLog.created_at.desc()).limit(200))
    return [{
        "id": a.id, "event_type": a.event_type, "description": a.description,
        "ip_address": a.ip_address, "created_at": a.created_at.isoformat(),
    } for a in res.scalars().all()]
