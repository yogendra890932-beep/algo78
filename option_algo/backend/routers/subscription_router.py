# backend/routers/subscription_router.py
# ================================================================
# User-facing subscription/payment/billing API.
#
#   GET  /api/subscription/plans                 — public plan catalog
#   GET  /api/subscription/me                    — current subscription status
#   GET  /api/subscription/billing-profile
#   POST /api/subscription/billing-profile
#   POST /api/subscription/order                 — create Razorpay order (new/renew/upgrade)
#   POST /api/subscription/verify-payment         — server-side signature check -> activate
#   POST /api/subscription/renew
#   POST /api/subscription/upgrade
#   POST /api/subscription/downgrade              — no payment; queues at period end
#   GET  /api/subscription/payments
#   GET  /api/subscription/invoices/{id}/download
#   POST /api/subscription/contact-sales
#   POST /api/subscription/webhook/razorpay       — server-to-server, no auth
#
# Security: Razorpay's frontend response is NEVER trusted for
# activation — only verify_payment_signature()/verify_webhook_signature()
# (backend.services.razorpay_service, pure HMAC) gate activation.
# ================================================================

from __future__ import annotations
import re
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from pydantic import BaseModel, EmailStr, Field, field_validator
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

_MOBILE_RE  = re.compile(r"^[6-9]\d{9}$")   # 10-digit Indian mobile number
_PINCODE_RE = re.compile(r"^\d{6}$")        # 6-digit Indian PIN code

from backend.db.database import get_db
from backend.db.models import (
    User, SubscriptionPlan, SubscriptionStatus, PaymentStatus, Payment,
    Subscription, ContactSalesLead, PaymentLog, PaymentProvider,
)
from backend.services.auth_service import get_current_user
from backend.services.rate_limit import rate_limit
from backend.repositories.plan_repository import PlanRepository
from backend.repositories.payment_repository import PaymentRepository
from backend.repositories.invoice_repository import InvoiceRepository
from backend.repositories.billing_profile_repository import BillingProfileRepository
from backend.services import razorpay_service, subscription_service, invoice_service, audit_log
from backend.services.billing_notifications import notify
from backend.services.billing_cache import get_billing_settings
from backend.services.payment_manager import get_payment_manager
from backend.config import get_settings

router = APIRouter(prefix="/api/subscription", tags=["subscription"])
settings = get_settings()


def _client_ip(request: Request) -> Optional[str]:
    return request.client.host if request and request.client else None


def _plan_out(plan: SubscriptionPlan) -> dict:
    return {
        "id": plan.id, "plan_code": plan.plan_code, "name": plan.name,
        "description": plan.description, "monthly_price": float(plan.monthly_price),
        "gst_percentage": float(plan.gst_percentage), "duration_days": plan.duration_days,
        "is_contact_sales": plan.is_contact_sales, "is_active": plan.is_active,
        "symbols": [{"symbol": s.symbol, "lot_limit": s.lot_limit} for s in plan.symbols],
    }


def _compute_amounts(base_price: float, gst_percentage: float) -> tuple[float, float, float]:
    gst_enabled = get_billing_settings().get("gst_enabled", True)
    gst_amount = round(base_price * float(gst_percentage) / 100, 2) if gst_enabled else 0.0
    total = round(base_price + gst_amount, 2)
    return base_price, gst_amount, total


async def _subscription_out(db: AsyncSession, sub: Optional[Subscription]) -> dict:
    if sub is None:
        return {"status": "none", "plan": None}
    symbols = await subscription_service.symbols_and_limits_for_subscription(db, sub)
    main_symbols = await subscription_service.main_symbols_for_subscription(db, sub)
    plan_name = None
    if sub.plan_id:
        res = await db.execute(select(SubscriptionPlan).where(SubscriptionPlan.id == sub.plan_id))
        plan = res.scalar_one_or_none()
        plan_name = plan.name if plan else None
    remaining = max((sub.end_date - datetime.utcnow()).days, 0)
    return {
        "status": sub.status.value, "plan_id": sub.plan_id, "plan_name": plan_name,
        "is_trial": sub.status == SubscriptionStatus.trial,
        "start_date": sub.start_date.isoformat(), "end_date": sub.end_date.isoformat(),
        "remaining_days": remaining, "allowed_symbols": symbols,
        "main_symbols": main_symbols,
        "pending_plan_id": sub.pending_plan_id,
    }


# ── Plans ────────────────────────────────────────────────────────

@router.get("/plans")
async def list_plans(db: AsyncSession = Depends(get_db)):
    plans = await PlanRepository(db).list(active_only=True)
    return [_plan_out(p) for p in plans]


# ── Current subscription ────────────────────────────────────────

@router.get("/me")
async def my_subscription(user: User = Depends(get_current_user), db: AsyncSession = Depends(get_db)):
    sub = await subscription_service.get_current_subscription(db, user.id)
    return await _subscription_out(db, sub)


# ── Billing profile ──────────────────────────────────────────────

class BillingProfileIn(BaseModel):
    address_line1: str = Field(min_length=1, max_length=255)
    address_line2: Optional[str] = Field(default=None, max_length=255)
    city:          str = Field(min_length=1, max_length=100)
    state:         str = Field(min_length=1, max_length=100)
    pincode:       str
    country:       str = Field(default="India", min_length=1, max_length=100)

    @field_validator("address_line1", "address_line2", "city", "state", "pincode", "country", mode="before")
    @classmethod
    def _strip(cls, v):
        return v.strip() if isinstance(v, str) else v

    @field_validator("pincode")
    @classmethod
    def _validate_pincode(cls, v: str) -> str:
        if not _PINCODE_RE.match(v or ""):
            raise ValueError("Enter a valid 6-digit PIN code")
        return v


@router.get("/billing-profile")
async def get_billing_profile(user: User = Depends(get_current_user), db: AsyncSession = Depends(get_db)):
    profile = await BillingProfileRepository(db).get_for_user(user.id)
    if not profile:
        return None
    return {
        "address_line1": profile.address_line1, "address_line2": profile.address_line2,
        "city": profile.city, "state": profile.state, "pincode": profile.pincode,
        "country": profile.country,
    }


@router.post("/billing-profile")
async def save_billing_profile(body: BillingProfileIn, user: User = Depends(get_current_user),
                                db: AsyncSession = Depends(get_db)):
    await BillingProfileRepository(db).upsert(user.id, body.model_dump())
    await db.commit()
    return {"ok": True}


# ── Order creation (new / renew / upgrade all funnel through here) ─

class OrderIn(BaseModel):
    plan_id: int


@router.post("/order")
async def create_order(body: OrderIn, request: Request, user: User = Depends(get_current_user),
                        db: AsyncSession = Depends(get_db),
                        _rl: None = Depends(rate_limit("subscription_order", 3))):
    plan = await PlanRepository(db).get_by_id(body.plan_id)
    if not plan or not plan.is_active:
        raise HTTPException(404, "Plan not found or inactive")
    if plan.is_contact_sales:
        raise HTTPException(400, "This plan requires contacting sales — see /contact-sales")

    base, gst, total = _compute_amounts(float(plan.monthly_price), float(plan.gst_percentage))

    # Use PaymentManager to determine payment flow
    pm = get_payment_manager()
    is_manual = pm.is_manual_mode()

    if is_manual:
        # Return manual payment instructions — user pays via QR/UPI
        receipt = f"user{user.id}-plan{plan.id}-{int(datetime.utcnow().timestamp())}"
        order_data = pm.create_order(total, receipt=receipt, notes={"user_id": str(user.id), "plan_id": str(plan.id)})
        return {
            "payment_mode": PaymentProvider.MANUAL.value,
            "plan_id": plan.id,
            "plan_name": plan.name,
            "base_amount": base,
            "gst_amount": gst,
            "total_amount": total,
            "receipt": receipt,
            "upi_id": order_data.get("upi_id", ""),
            "qr_code_path": order_data.get("qr_code_path", ""),
            "instructions": order_data.get("instructions", ""),
        }

    # Razorpay flow (unchanged)
    if not settings.razorpay_enabled:
        raise HTTPException(503, "Payments are not configured on this server yet")

    try:
        order = razorpay_service.create_order(
            total, receipt=f"user{user.id}-plan{plan.id}-{int(datetime.utcnow().timestamp())}",
            notes={"user_id": str(user.id), "plan_id": str(plan.id)})
    except Exception as e:
        raise HTTPException(502, f"Failed to create payment order: {e}")

    payment = await PaymentRepository(db).create(Payment(
        user_id=user.id, plan_id=plan.id, razorpay_order_id=order["id"],
        base_amount=base, gst_amount=gst, total_amount=total, status=PaymentStatus.created,
    ))
    await audit_log.log_event(db, user.id, "payment_initiated",
                               f"Order {order['id']} created for plan {plan.plan_code}",
                               ip_address=_client_ip(request))
    await db.commit()

    return {
        "payment_mode": PaymentProvider.RAZORPAY.value,
        "order_id": order["id"], "amount": order["amount"], "currency": order["currency"],
        "key_id": settings.RAZORPAY_KEY_ID, "payment_id": payment.id,
        "plan_name": plan.name, "base_amount": base, "gst_amount": gst, "total_amount": total,
    }


@router.post("/renew")
async def renew(request: Request, user: User = Depends(get_current_user), db: AsyncSession = Depends(get_db),
                 _rl: None = Depends(rate_limit("subscription_order", 3))):
    current = await subscription_service.get_current_subscription(db, user.id)
    if not current or not current.plan_id:
        raise HTTPException(400, "No current paid plan to renew")
    return await create_order(OrderIn(plan_id=current.plan_id), request, user, db, None)


@router.post("/upgrade")
async def upgrade(body: OrderIn, request: Request, user: User = Depends(get_current_user),
                   db: AsyncSession = Depends(get_db),
                   _rl: None = Depends(rate_limit("subscription_order", 3))):
    return await create_order(body, request, user, db, None)


@router.post("/downgrade")
async def downgrade(body: OrderIn, request: Request, user: User = Depends(get_current_user),
                     db: AsyncSession = Depends(get_db)):
    """
    Downgrades never require immediate payment — they always take
    effect at the current period's end_date (backend.services.
    subscription_service.downgrade_subscription), processed by
    scripts/subscription_daily_job.py.
    """
    current = await subscription_service.get_current_subscription(db, user.id)
    if not current or current.status != SubscriptionStatus.active:
        raise HTTPException(400, "No active subscription to downgrade")
    plan = await PlanRepository(db).get_by_id(body.plan_id)
    if not plan or not plan.is_active or plan.is_contact_sales:
        raise HTTPException(404, "Plan not found or unavailable")
    await subscription_service.downgrade_subscription(db, current, plan.id)
    await audit_log.log_event(db, user.id, "subscription_downgraded",
                               f"Downgrade to plan {plan.plan_code} scheduled for {current.end_date}",
                               ip_address=_client_ip(request))
    await db.commit()
    return {"ok": True, "effective_date": current.end_date.isoformat()}


# ── Payment verification ────────────────────────────────────────

class VerifyPaymentIn(BaseModel):
    razorpay_order_id: str
    razorpay_payment_id: str
    razorpay_signature: str


async def _finalize_successful_payment(db: AsyncSession, payment: Payment) -> Subscription:
    """
    Shared by /verify-payment and the Razorpay webhook — activates the
    subscription, generates the invoice, and sends notifications.
    Only ever called after signature verification has succeeded.
    """
    ures = await db.execute(select(User).where(User.id == payment.user_id))
    user = ures.scalar_one_or_none()
    plan = await PlanRepository(db).get_by_id(payment.plan_id) if payment.plan_id else None

    current = await subscription_service.get_current_subscription(db, payment.user_id)
    billing_settings = get_billing_settings()
    event_type = "subscription_activated"

    if current is None or current.status in (
            SubscriptionStatus.expired, SubscriptionStatus.cancelled, SubscriptionStatus.trial):
        sub = await subscription_service.activate_subscription(
            db, payment.user_id, plan_id=payment.plan_id, duration_days=plan.duration_days if plan else 30)
    elif current.plan_id == payment.plan_id:
        sub = await subscription_service.renew_subscription(
            db, current, plan.duration_days if plan else 30)
        event_type = "subscription_renewed"
    else:
        sub = await subscription_service.upgrade_subscription(
            db, payment.user_id, current, payment.plan_id,
            plan.duration_days if plan else 30, billing_settings.get("upgrade_mode", "immediate"))
        event_type = "subscription_upgraded"

    payment.subscription_id = sub.id
    db.add(payment)

    billing_profile = await BillingProfileRepository(db).get_for_user(payment.user_id)
    symbols = await subscription_service.symbols_and_limits_for_subscription(db, sub)
    invoice = await invoice_service.create_invoice_record(
        db, payment, user, plan.name if plan else "Subscription", symbols, billing_profile)

    # Payment provider agnostic audit log
    payment_ref = payment.razorpay_payment_id or payment.utr_number or f"#{payment.id}"
    await audit_log.log_event(db, payment.user_id, "payment_verified",
                               f"Payment {payment_ref} verified ({payment.payment_provider})")
    await audit_log.log_event(db, payment.user_id, event_type,
                               f"Subscription {event_type.split('_')[-1]} — plan {plan.plan_code if plan else ''}")
    await audit_log.log_event(db, payment.user_id, "invoice_generated", f"Invoice {invoice.invoice_number}")
    await db.commit()

    if user:
        await notify(db, user, "payment_success", amount=float(payment.total_amount),
                     plan_name=plan.name if plan else "", invoice_number=invoice.invoice_number)
        await notify(db, user, event_type, plan_name=plan.name if plan else "",
                     end_date=sub.end_date.strftime("%d %b %Y"))
    return sub


@router.post("/verify-payment")
async def verify_payment(body: VerifyPaymentIn, request: Request, user: User = Depends(get_current_user),
                          db: AsyncSession = Depends(get_db),
                          _rl: None = Depends(rate_limit("subscription_verify", 3))):
    payment = await PaymentRepository(db).get_by_order_id(body.razorpay_order_id)
    if not payment or payment.user_id != user.id:
        raise HTTPException(404, "Payment order not found")
    if payment.status == PaymentStatus.success:
        return {"ok": True, "message": "Already verified"}

    valid = razorpay_service.verify_payment_signature(
        body.razorpay_order_id, body.razorpay_payment_id, body.razorpay_signature)

    if not valid:
        payment.status = PaymentStatus.failed
        payment.failure_reason = "Signature verification failed"
        db.add(payment)
        await PaymentRepository(db).add_log(
            PaymentLog(payment_id=payment.id, event_type="signature_invalid"))
        await audit_log.log_event(db, user.id, "payment_failed",
                                   f"Signature invalid for order {body.razorpay_order_id}",
                                   ip_address=_client_ip(request))
        await db.commit()
        await notify(db, user, "payment_failed", plan_name="")
        raise HTTPException(400, "Payment verification failed — signature mismatch")

    payment.razorpay_payment_id = body.razorpay_payment_id
    payment.razorpay_signature = body.razorpay_signature
    payment.status = PaymentStatus.success
    payment.paid_at = datetime.utcnow()
    db.add(payment)
    await db.flush()

    sub = await _finalize_successful_payment(db, payment)
    return {"ok": True, "subscription": await _subscription_out(db, sub)}


# ── Payment history / invoices ──────────────────────────────────

@router.get("/payments")
async def payment_history(user: User = Depends(get_current_user), db: AsyncSession = Depends(get_db)):
    payments = await PaymentRepository(db).list_for_user(user.id)
    return [{
        "id": p.id, "razorpay_order_id": p.razorpay_order_id,
        "razorpay_payment_id": p.razorpay_payment_id, "base_amount": float(p.base_amount),
        "gst_amount": float(p.gst_amount), "total_amount": float(p.total_amount),
        "status": p.status.value, "created_at": p.created_at.isoformat(),
        "paid_at": p.paid_at.isoformat() if p.paid_at else None,
        "payment_provider": p.payment_provider or "RAZORPAY",
        "utr_number": p.utr_number,
        "screenshot_path": p.screenshot_path,
        "remarks": p.remarks,
    } for p in payments]


@router.get("/invoices")
async def list_invoices(user: User = Depends(get_current_user), db: AsyncSession = Depends(get_db)):
    invoices = await InvoiceRepository(db).list_for_user(user.id)
    return [{
        "id": i.id, "invoice_number": i.invoice_number, "plan_name": i.plan_name_snapshot,
        "total_amount": float(i.total_amount), "issued_at": i.issued_at.isoformat(),
    } for i in invoices]


@router.get("/invoices/{invoice_id}/download")
async def download_invoice(invoice_id: int, user: User = Depends(get_current_user),
                            db: AsyncSession = Depends(get_db)):
    invoice = await InvoiceRepository(db).get_by_id(invoice_id)
    if not invoice or invoice.user_id != user.id:
        raise HTTPException(404, "Invoice not found")
    pdf_bytes = invoice_service.render_invoice_pdf(invoice)
    return Response(content=pdf_bytes, media_type="application/pdf", headers={
        "Content-Disposition": f'attachment; filename="{invoice.invoice_number}.pdf"'
    })


# ── Contact Sales (Enterprise lead capture) ─────────────────────

class ContactSalesIn(BaseModel):
    name:          str = Field(min_length=1, max_length=150)
    email:         EmailStr
    mobile_number: Optional[str] = None
    message:       Optional[str] = Field(default=None, max_length=2000)

    @field_validator("name", "mobile_number", "message", mode="before")
    @classmethod
    def _strip(cls, v):
        return v.strip() if isinstance(v, str) else v

    @field_validator("mobile_number")
    @classmethod
    def _validate_mobile(cls, v):
        if v and not _MOBILE_RE.match(v):
            raise ValueError("Enter a valid 10-digit mobile number")
        return v or None


@router.post("/contact-sales")
async def contact_sales(body: ContactSalesIn, user: User = Depends(get_current_user),
                         db: AsyncSession = Depends(get_db)):
    db.add(ContactSalesLead(
        user_id=user.id, name=body.name, email=body.email,
        mobile_number=body.mobile_number, message=body.message,
    ))
    await audit_log.log_event(db, user.id, "contact_sales_lead", f"Lead submitted by {body.email}")
    await db.commit()
    return {"ok": True}


# ── Razorpay webhook (server-to-server, no auth) ────────────────

@router.post("/webhook/razorpay")
async def razorpay_webhook(request: Request, db: AsyncSession = Depends(get_db)):
    raw = await request.body()
    signature = request.headers.get("X-Razorpay-Signature", "")
    if not razorpay_service.verify_webhook_signature(raw, signature):
        raise HTTPException(400, "Invalid webhook signature")

    import json
    body = json.loads(raw)
    event = body.get("event", "")
    event_id = request.headers.get("X-Razorpay-Event-Id") or body.get("id") or ""

    if await PaymentRepository(db).event_already_processed(event_id):
        return {"ok": True, "duplicate": True}

    payload = body.get("payload", {}).get("payment", {}).get("entity", {})
    order_id = payload.get("order_id")
    payment = await PaymentRepository(db).get_by_order_id(order_id) if order_id else None

    await PaymentRepository(db).add_log(PaymentLog(
        payment_id=payment.id if payment else None, event_type=event,
        razorpay_event_id=event_id, payload=raw.decode(errors="ignore")))

    if payment and event == "payment.captured" and payment.status != PaymentStatus.success:
        payment.razorpay_payment_id = payload.get("id")
        payment.status = PaymentStatus.success
        payment.paid_at = datetime.utcnow()
        db.add(payment)
        await db.flush()
        await _finalize_successful_payment(db, payment)
    elif payment and event == "payment.failed" and payment.status != PaymentStatus.success:
        payment.status = PaymentStatus.failed
        payment.failure_reason = payload.get("error_description") or "Payment failed"
        db.add(payment)
        await db.commit()
    else:
        await db.commit()

    return {"ok": True}
