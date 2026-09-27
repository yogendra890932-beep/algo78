# backend/services/invoice_service.py
# ================================================================
# Invoice numbering + PDF rendering.
#
# The PDF is generated on-demand at download time from the Invoice
# row's snapshot fields (see backend.db.models.Invoice) rather than
# stored on disk — avoids filesystem/cleanup concerns on the VPS
# deploy target, and later billing-profile edits never rewrite a
# past invoice's recorded name/address.
# ================================================================

from __future__ import annotations
import io
import json
from datetime import datetime
from typing import Optional

from sqlalchemy.ext.asyncio import AsyncSession

from backend.db.models import Invoice, Payment, User, BillingProfile
from backend.repositories.invoice_repository import InvoiceRepository
from backend.services.billing_cache import get_billing_settings


async def next_invoice_number(db: AsyncSession) -> str:
    year = datetime.utcnow().year
    prefix = get_billing_settings().get("invoice_prefix", "INV")
    seq = await InvoiceRepository(db).next_sequence_number(year)
    return f"{prefix}-{year}-{seq:06d}"


async def create_invoice_record(db: AsyncSession, payment: Payment, user: User,
                                 plan_name: str, symbols_limits: dict,
                                 billing_profile: Optional[BillingProfile]) -> Invoice:
    invoice_number = await next_invoice_number(db)
    snapshot = {
        "customer_name":  user.full_name or user.email,
        "customer_email": user.email,
        "mobile_number":  user.mobile_number or "",
        "address_line1":  billing_profile.address_line1 if billing_profile else "",
        "address_line2":  billing_profile.address_line2 if billing_profile else "",
        "city":           billing_profile.city if billing_profile else "",
        "state":          billing_profile.state if billing_profile else "",
        "pincode":        billing_profile.pincode if billing_profile else "",
        "country":        billing_profile.country if billing_profile else "India",
        "plan_name":      plan_name,
        "symbols":        symbols_limits,
        "payment_id":     payment.razorpay_payment_id or "",
    }
    invoice = Invoice(
        invoice_number=invoice_number,
        payment_id=payment.id,
        user_id=user.id,
        plan_name_snapshot=plan_name,
        base_amount=payment.base_amount,
        gst_amount=payment.gst_amount,
        total_amount=payment.total_amount,
        billing_snapshot=json.dumps(snapshot, default=str),
        issued_at=datetime.utcnow(),
    )
    return await InvoiceRepository(db).create(invoice)


def _make_qr_flowable(data: str, size_mm: float = 22):
    """Renders a QR code (invoice #/amount/payment ID, plain text — no
    verification backend involved) as a reportlab Image flowable."""
    import qrcode
    from reportlab.lib.units import mm
    from reportlab.platypus import Image

    qr = qrcode.QRCode(border=1, box_size=6)
    qr.add_data(data)
    qr.make(fit=True)
    img = qr.make_image(fill_color="#0f172a", back_color="white")
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    buf.seek(0)
    return Image(buf, width=size_mm * mm, height=size_mm * mm)


def render_invoice_pdf(invoice: Invoice) -> bytes:
    """
    Renders a professional business invoice PDF from the invoice's
    stored snapshot — layout/styling only, same inputs/outputs as
    before (no change to when/how invoices are created or numbered).
    """
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.units import mm
    from reportlab.lib import colors
    from reportlab.platypus import (
        SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, HRFlowable,
    )
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.enums import TA_RIGHT, TA_CENTER

    snap = json.loads(invoice.billing_snapshot)

    BRAND = colors.HexColor("#0f172a")     # dark navy — header band
    ACCENT = colors.HexColor("#10b981")    # green — brand accent / paid badge
    MUTED = colors.HexColor("#64748b")
    LIGHT_BG = colors.HexColor("#f1f5f9")

    styles = getSampleStyleSheet()
    brand_style = ParagraphStyle("Brand", parent=styles["Title"], fontSize=22,
                                  textColor=colors.white, leading=26)
    tagline_style = ParagraphStyle("Tagline", parent=styles["Normal"], fontSize=8.5,
                                    textColor=colors.HexColor("#94a3b8"))
    h2_style = ParagraphStyle("H2", parent=styles["Heading2"], fontSize=10.5,
                               textColor=BRAND, spaceBefore=12, spaceAfter=4)
    normal = ParagraphStyle("NormalSm", parent=styles["Normal"], fontSize=9.5, leading=13)
    muted = ParagraphStyle("Muted", parent=normal, textColor=MUTED, fontSize=8.5)
    right = ParagraphStyle("Right", parent=normal, alignment=TA_RIGHT)
    right_muted = ParagraphStyle("RightMuted", parent=muted, alignment=TA_RIGHT)
    center_small = ParagraphStyle("CenterSmall", parent=muted, alignment=TA_CENTER, fontSize=7.5)

    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4, topMargin=0, bottomMargin=18 * mm,
                             leftMargin=18 * mm, rightMargin=18 * mm)
    elements = []

    # ── Header band (brand wordmark + tagline, invoice title) ──────
    header_table = Table([[
        Paragraph("Optiscalper", brand_style),
        Paragraph("INVOICE", ParagraphStyle("InvTitle", parent=styles["Title"],
                                             fontSize=20, textColor=colors.white, alignment=TA_RIGHT)),
    ]], colWidths=[95 * mm, 97 * mm])
    header_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), BRAND),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("TOPPADDING", (0, 0), (-1, -1), 18),
        ("LEFTPADDING", (0, 0), (0, 0), 18 * mm - 6),
        ("RIGHTPADDING", (1, 0), (1, 0), 18 * mm - 6),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]))
    elements.append(header_table)

    tag_table = Table([[
        Paragraph("Algorithmic Options Trading Platform &nbsp;|&nbsp; support@optiscalper.com", tagline_style),
        Paragraph("", tagline_style),
    ]], colWidths=[95 * mm, 97 * mm])
    tag_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), BRAND),
        ("TOPPADDING", (0, 0), (-1, -1), 0),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 18),
        ("LEFTPADDING", (0, 0), (0, 0), 18 * mm - 6),
    ]))
    elements.append(tag_table)
    elements.append(Spacer(1, 14))

    # ── Invoice meta + PAID status badge ────────────────────────────
    # Every Invoice row is only ever created after a successful, verified
    # payment (see subscription_router._finalize_successful_payment) —
    # "PAID" is a structural fact about this table, not re-derived logic.
    paid_badge = Table([["PAID"]], colWidths=[22 * mm])
    paid_badge.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), ACCENT),
        ("TEXTCOLOR", (0, 0), (-1, -1), colors.white),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("FONTNAME", (0, 0), (-1, -1), "Helvetica-Bold"),
        ("TOPPADDING", (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
    ]))
    meta_table = Table([
        [Paragraph(f"<b>Invoice Number</b><br/>{invoice.invoice_number}", normal),
         Paragraph(f"<b>Invoice Date</b><br/>{invoice.issued_at.strftime('%d %b %Y')}", normal),
         Paragraph(f"<b>Payment Method</b><br/>{snap.get('payment_method') or 'Online (Razorpay)'}", normal),
         paid_badge],
    ], colWidths=[55 * mm, 45 * mm, 55 * mm, 22 * mm])
    meta_table.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("ALIGN", (3, 0), (3, 0), "RIGHT"),
    ]))
    elements.append(meta_table)
    elements.append(Spacer(1, 4))
    elements.append(HRFlowable(width="100%", thickness=0.75, color=colors.HexColor("#e2e8f0")))

    # ── Billed To ────────────────────────────────────────────────────
    elements.append(Paragraph("BILLED TO", h2_style))
    address_lines = [snap.get("address_line1", ""), snap.get("address_line2") or "",
                      f"{snap.get('city','')}, {snap.get('state','')} {snap.get('pincode','')}".strip(", "),
                      snap.get("country", "India")]
    address_lines = [line for line in address_lines if line.strip()]
    billed_to_html = "<br/>".join([
        f"<b>{snap['customer_name']}</b>", snap["customer_email"],
    ] + ([f"Mobile: {snap['mobile_number']}"] if snap.get("mobile_number") else []) + address_lines)
    elements.append(Paragraph(billed_to_html, normal))

    # ── Subscription / plan details ──────────────────────────────────
    elements.append(Paragraph("SUBSCRIPTION DETAILS", h2_style))
    symbols = snap.get("symbols") or {}
    symbol_items = list(symbols.items()) or [("-", "-")]
    body_rows = []
    for i, (sym, lim) in enumerate(symbol_items):
        lot_text = f"Max {lim} lot{'s' if lim != 1 else ''}" if lim != "-" else "-"
        body_rows.append([snap["plan_name"] if i == 0 else "", sym, lot_text])

    plan_table = Table([["Plan", "Allowed Symbols", "Lot Limit"]] + body_rows,
                        colWidths=[55 * mm, 80 * mm, 42 * mm])
    table_style = [
        ("BACKGROUND", (0, 0), (-1, 0), LIGHT_BG),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("TEXTCOLOR", (0, 0), (-1, 0), BRAND),
        ("TOPPADDING", (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ("LINEBELOW", (0, 0), (-1, -2), 0.5, colors.HexColor("#e2e8f0")),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
    ]
    if len(body_rows) > 1:
        table_style.append(("SPAN", (0, 1), (0, len(body_rows))))
    plan_table.setStyle(TableStyle(table_style))
    elements.append(plan_table)

    # ── Pricing breakdown ─────────────────────────────────────────────
    elements.append(Paragraph("PRICING BREAKDOWN", h2_style))
    amount_rows = [
        ["Description", "Amount (Rs.)"],
        ["Base Price", f"{float(invoice.base_amount):,.2f}"],
        ["GST", f"{float(invoice.gst_amount):,.2f}"],
        ["Grand Total", f"{float(invoice.total_amount):,.2f}"],
    ]
    amount_table = Table(amount_rows, colWidths=[142 * mm, 35 * mm])
    amount_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), BRAND),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("ALIGN", (1, 0), (1, -1), "RIGHT"),
        ("FONTSIZE", (0, 0), (-1, -1), 9.5),
        ("FONTNAME", (0, -1), (-1, -1), "Helvetica-Bold"),
        ("FONTSIZE", (0, -1), (-1, -1), 11),
        ("BACKGROUND", (0, -1), (-1, -1), LIGHT_BG),
        ("LINEABOVE", (0, -1), (-1, -1), 1, BRAND),
        ("TOPPADDING", (0, 0), (-1, -1), 6),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
    ]))
    elements.append(amount_table)
    if snap.get("payment_id"):
        elements.append(Spacer(1, 4))
        elements.append(Paragraph(f"Payment ID: {snap['payment_id']}", muted))

    # ── QR code + footer ──────────────────────────────────────────────
    elements.append(Spacer(1, 18))
    elements.append(HRFlowable(width="100%", thickness=0.75, color=colors.HexColor("#e2e8f0")))
    elements.append(Spacer(1, 8))

    qr_data = f"Optiscalper Invoice {invoice.invoice_number} | Rs.{float(invoice.total_amount):.2f} | Payment {snap.get('payment_id','-')}"
    try:
        qr_img = _make_qr_flowable(qr_data)
    except Exception:
        qr_img = Paragraph("", normal)

    footer_table = Table([[
        qr_img,
        Paragraph(
            "<b>Terms &amp; Conditions</b><br/>"
            "This invoice confirms a successful subscription payment. Subscriptions are billed in "
            "advance for the plan duration shown above and are non-refundable except where required by law.<br/><br/>"
            "<b>Support</b><br/>Questions about this invoice? Contact support@optiscalper.com",
            muted),
    ]], colWidths=[30 * mm, 147 * mm])
    footer_table.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("ALIGN", (0, 0), (0, 0), "CENTER"),
    ]))
    elements.append(footer_table)
    elements.append(Spacer(1, 10))
    elements.append(Paragraph("Thank you for subscribing to Optiscalper — this is a system-generated invoice.",
                               center_small))

    doc.build(elements)
    return buf.getvalue()
