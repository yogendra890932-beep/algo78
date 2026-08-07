#!/usr/bin/env python
# scripts/init_db.py — Run: python scripts/init_db.py
# Safe to re-run: skips existing data, adds missing columns

import asyncio, sys, os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.db.database import init_db, AsyncSessionLocal, engine
from backend.db.models import (
    User,
    BotConfig,
    UserRole,
    ExchangeHoliday,
    StreamerSymbolToken,
    SubscriptionPlan,
    SubscriptionPlanSymbol,
    BillingSettings,
)
from backend.services.auth_service import hash_password
from backend.config import get_settings
from sqlalchemy import select, text, func

settings = get_settings()

NEW_COLUMNS = [
    ("bot_configs", "paper_mode", "BOOLEAN DEFAULT 1"),
    ("bot_configs", "execution_mode", "VARCHAR(20) DEFAULT 'PAPER'"),
    ("bot_configs", "telegram_bot_token", "VARCHAR(100)"),
    ("bot_configs", "telegram_bot_chat_id", "VARCHAR(50)"),
    ("bot_configs", "telegram_on_entry", "BOOLEAN DEFAULT 1"),
    ("bot_configs", "telegram_on_exit", "BOOLEAN DEFAULT 1"),
    ("bot_configs", "telegram_on_trail", "BOOLEAN DEFAULT 0"),
    ("bot_configs", "telegram_on_summary", "BOOLEAN DEFAULT 1"),
    ("bot_configs", "extra_symbols", "VARCHAR(200)"),
    ("bot_configs", "extra_tokens", "VARCHAR(500)"),
    ("bot_configs", "custom_lot_sizes", "VARCHAR(500)"),
    # Upstox OAuth — one-time API key/secret + daily token expiry
    ("users", "upstox_api_key_enc", "TEXT"),
    ("users", "upstox_api_secret_enc", "TEXT"),
    ("users", "upstox_token_expires_at", "TIMESTAMP"),
    # Google OAuth + email verification
    ("users", "email_verified", "BOOLEAN DEFAULT 0"),
    ("users", "email_verify_token", "VARCHAR(255)"),
    ("users", "email_verify_sent_at", "TIMESTAMP"),
    ("users", "google_id", "VARCHAR(255)"),
    ("users", "avatar_url", "VARCHAR(512)"),
    ("users", "password_reset_token", "VARCHAR(255)"),
    ("users", "password_reset_sent_at", "TIMESTAMP"),
    # Trade mode (paper/live) — was missing, broke trade history mode filter
    ("trades", "mode", "VARCHAR(10) DEFAULT 'live'"),
    # Subscription/billing — mobile number (unique, collected at registration)
    # and the one-time trial guard flag
    ("users", "mobile_number", "VARCHAR(15)"),
    ("users", "trial_used", "BOOLEAN DEFAULT 0"),
]


async def migrate():
    """
    Add any missing columns. Works for both Postgres (information_schema)
    and SQLite (PRAGMA table_info) — detects dialect automatically.
    After adding columns, runs backfill statements to set sensible values
    for existing rows (e.g. existing active users get email_verified=True
    so they aren't locked out after the column is first added).
    """
    is_sqlite = settings.DATABASE_URL.startswith("sqlite")
    newly_added = set()

    async with engine.begin() as conn:
        for table, col, col_type in NEW_COLUMNS:
            if is_sqlite:
                result = await conn.execute(text(f"PRAGMA table_info({table})"))
                existing = [row[1] for row in result.fetchall()]
            else:
                result = await conn.execute(
                    text(
                        "SELECT column_name FROM information_schema.columns "
                        "WHERE table_name = :table"
                    ),
                    {"table": table},
                )
                existing = [row[0] for row in result.fetchall()]

            if col not in existing:
                pg_type = col_type
                if not is_sqlite:
                    pg_type = pg_type.replace("DEFAULT 1", "DEFAULT TRUE").replace(
                        "DEFAULT 0", "DEFAULT FALSE"
                    )
                await conn.execute(
                    text(f"ALTER TABLE {table} ADD COLUMN {col} {pg_type}")
                )
                newly_added.add(f"{table}.{col}")
                print(f"  + Added: {table}.{col}")

    # ── Always-run idempotent backfill ───────────────────────────
    # Runs every time init_db.py is executed — safe because it's a
    # WHERE-guarded UPDATE that's a no-op once already applied.
    # Catches the case where the column was added in a previous run
    # (so "users.email_verified" is NOT in newly_added) but the
    # backfill was never run (e.g. the admin or early users are
    # sitting with email_verified=False even though is_active=True).
    async with engine.begin() as conn:
        true_val = "1" if is_sqlite else "TRUE"
        false_val = "0" if is_sqlite else "FALSE"
        try:
            result = await conn.execute(
                text(
                    f"UPDATE users SET email_verified = {true_val} "
                    f"WHERE is_active = {true_val} "
                    f"AND (email_verified IS NULL OR email_verified = {false_val})"
                )
            )
            count = result.rowcount
            if count:
                print(
                    f"  ✓ Backfill: set email_verified=True for {count} existing active user(s)"
                )
        except Exception as e:
            # Column may not exist yet on a brand-new DB (created in this
            # same run) — that's fine, the column default and the newly-
            # created admin row already carry email_verified=True.
            print(f"  (backfill skipped: {e})")

        try:
            result = await conn.execute(
                text(
                    f"UPDATE bot_configs SET execution_mode = 'PAPER' "
                    f"WHERE execution_mode IS NULL "
                    f"AND (paper_mode IS NULL OR paper_mode = {true_val})"
                )
            )
            count2 = result.rowcount
            if count2:
                print(
                    f"  ✓ Backfill: set execution_mode=PAPER for {count2} existing paper-mode config(s)"
                )
            result = await conn.execute(
                text(
                    f"UPDATE bot_configs SET execution_mode = 'AUTO' "
                    f"WHERE execution_mode IS NULL AND paper_mode = {false_val}"
                )
            )
            count3 = result.rowcount
            if count3:
                print(
                    f"  ✓ Backfill: set execution_mode=AUTO for {count3} existing live-mode config(s)"
                )
        except Exception as e:
            print(f"  (execution_mode backfill skipped: {e})")
    from backend.engine.history_loader import NSE_HOLIDAYS
    from backend.engine.instruments import KNOWN_INDEX_KEYS, KNOWN_STEPS

    async with AsyncSessionLocal() as db:
        count = (await db.execute(select(func.count(ExchangeHoliday.id)))).scalar()
        if count == 0:
            for d in sorted(NSE_HOLIDAYS):
                db.add(ExchangeHoliday(exchange="NSE", holiday_date=d))
            await db.commit()
            print(f"  ✅ Seeded {len(NSE_HOLIDAYS)} NSE holiday(s)")
        else:
            print(f"  ℹ️  Already have {count} holiday(s)")

        count = (await db.execute(select(func.count(StreamerSymbolToken.id)))).scalar()
        if count == 0:
            for symbol, history_key in KNOWN_INDEX_KEYS.items():
                db.add(
                    StreamerSymbolToken(
                        symbol=symbol,
                        exchange="NSE",
                        streamer_token=history_key,
                        history_key=history_key,
                        strike_step=KNOWN_STEPS.get(symbol),
                    )
                )
            await db.commit()
            print(f"  ✅ Seeded {len(KNOWN_INDEX_KEYS)} streamer symbol token(s)")
        else:
            print(f"  ℹ️  Already have {count} streamer token(s)")


async def seed_default_plans():
    """
    Seeds the four default plan tiers + a default BillingSettings row
    on first run only. All values here are admin-editable afterward
    (GET/PUT /api/admin/billing/plans, /api/admin/billing/settings) —
    this just gets a working catalog in place out of the box.
    """
    async with AsyncSessionLocal() as db:
        count = (await db.execute(select(func.count(SubscriptionPlan.id)))).scalar()
        if count == 0:
            plans = [
                dict(
                    plan_code="starter",
                    name="Starter",
                    monthly_price=1500,
                    gst_percentage=18,
                    duration_days=30,
                    is_contact_sales=False,
                    symbols=[("NIFTY", 1)],
                ),
                dict(
                    plan_code="basic",
                    name="Basic",
                    monthly_price=2500,
                    gst_percentage=18,
                    duration_days=30,
                    is_contact_sales=False,
                    symbols=[("NIFTY", 2), ("SENSEX", 2)],
                ),
                dict(
                    plan_code="professional",
                    name="Professional",
                    monthly_price=4000,
                    gst_percentage=18,
                    duration_days=30,
                    is_contact_sales=False,
                    symbols=[("NIFTY", 3), ("SENSEX", 3), ("BANKNIFTY", 2)],
                ),
                dict(
                    plan_code="enterprise",
                    name="Enterprise",
                    monthly_price=0,
                    gst_percentage=18,
                    duration_days=365,
                    is_contact_sales=True,
                    symbols=[],
                ),
            ]
            for p in plans:
                symbols = p.pop("symbols")
                plan = SubscriptionPlan(**p)
                db.add(plan)
                await db.flush()
                for symbol, lot_limit in symbols:
                    db.add(
                        SubscriptionPlanSymbol(
                            plan_id=plan.id, symbol=symbol, lot_limit=lot_limit
                        )
                    )
            await db.commit()
            print(f"  ✅ Seeded {len(plans)} subscription plan(s)")
        else:
            print(f"  ℹ️  Already have {count} plan(s)")

        settings_row = (
            await db.execute(select(BillingSettings).where(BillingSettings.id == 1))
        ).scalar_one_or_none()
        if not settings_row:
            db.add(
                BillingSettings(
                    id=1,
                    gst_enabled=True,
                    upgrade_mode="immediate",
                    invoice_prefix="INV",
                )
            )
            await db.commit()
            print(
                "  ✅ Seeded default billing settings (GST on, upgrade_mode=immediate)"
            )
        else:
            print("  ℹ️  Billing settings already configured")


async def main():
    print("=" * 50)
    print("AlgoBot — Database Setup")
    print("=" * 50)

    print("\n[1/5] Creating tables...")
    await init_db()
    print("  ✅ Tables ready")

    print("\n[2/5] Running migrations...")
    await migrate()
    print("  ✅ Schema up to date")

    print("\n[3/5] Exchange holidays & streamer tokens...")
    # Already seeded inside migrate() above.

    print("\n[4/5] Subscription plans & billing settings...")
    await seed_default_plans()

    print("\n[5/5] Admin user...")
    async with AsyncSessionLocal() as db:
        res = await db.execute(select(User).where(User.email == settings.ADMIN_EMAIL))
        if res.scalar_one_or_none():
            print(f"  ℹ️  Already exists: {settings.ADMIN_EMAIL}")
        else:
            admin = User(
                email=settings.ADMIN_EMAIL,
                hashed_password=hash_password(settings.ADMIN_PASSWORD),
                full_name="Admin",
                role=UserRole.admin,
                is_active=True,
                email_verified=True,
            )
            db.add(admin)
            await db.flush()
            db.add(BotConfig(user_id=admin.id, paper_mode=True))
            await db.commit()
            print(f"  ✅ Admin: {settings.ADMIN_EMAIL} / {settings.ADMIN_PASSWORD}")
            print("  ⚠️  Change password after first login!")

    print("\n✅ Done! Run: uvicorn backend.main:app --port 8000 --reload")
    print(f"   Then open: http://localhost:8000")


asyncio.run(main())
