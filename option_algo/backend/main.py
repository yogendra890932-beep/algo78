# backend/main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.requests import Request
from fastapi.responses import HTMLResponse
from contextlib import asynccontextmanager

from backend.config import get_settings
from backend.db.database import init_db, close_db
from backend.routers.auth import router as auth_router
from backend.routers.all_routers import (
    router        as users_router,
    bot_router,
    trades_router,
    admin_router,
    ws_router,
    oc_router,
    push_router,
)
from backend.routers.position import router as position_router
from backend.routers.webhook import router as webhook_router
from backend.routers.subscription_router import router as subscription_router
from backend.routers.admin_billing_router import router as admin_billing_router
from backend.routers.campaign_router import router as campaign_router
from backend.routers.manual_payment_router import router as manual_payment_router
from backend.routers.terminal import router as terminal_router

settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # ── Startup ──────────────────────────────────────────────────
    # NOTE: this is the WEB process. It does NOT manage bot threads —
    # those live in the separate worker process (worker.py), reached
    # via the Redis command queue / state store / event bus. See
    # SETUP_GUIDE.txt "PRODUCTION / MULTI-USER NOTES".
    _validate_production_config()
    await init_db()

    # Initialize campaign settings row
    from backend.db.database import AsyncSessionLocal
    from backend.services.campaign_service import get_campaign_settings
    async with AsyncSessionLocal() as db:
        await get_campaign_settings(db)

    yield
    # ── Shutdown ─────────────────────────────────────────────────
    # Gracefully close the async DB engine pool to prevent Windows
    # ProactorEventLoop "NoneType has no attribute 'send'" errors
    # when asyncpg connections outlive the event loop.
    await close_db()
    # Bot threads are owned and gracefully stopped by worker.py on
    # its own SIGTERM handler.


def _validate_production_config():
    """
    Warn loudly at startup if running with insecure defaults.
    Does not block startup (so local/dev usage still works out of the box),
    but makes misconfiguration impossible to miss in production logs.
    """
    warnings = []
    if settings.SECRET_KEY == "dev-secret-change-me-32chars-min!":
        warnings.append("SECRET_KEY is using the INSECURE DEFAULT — set a unique "
                         "random value in .env (min 32 chars).")
    if not settings.FERNET_KEY:
        warnings.append("FERNET_KEY is not set — Upstox token encryption will fail. "
                         "Generate with: python -c \"from cryptography.fernet import "
                         "Fernet; print(Fernet.generate_key().decode())\"")
    if settings.ADMIN_PASSWORD == "admin123":
        warnings.append("ADMIN_PASSWORD is using the INSECURE DEFAULT 'admin123' — "
                         "change it in .env before exposing this server publicly.")
    if settings.DEBUG:
        warnings.append("DEBUG=true — disable in production (enables verbose SQL "
                         "echo and other dev-only behaviour).")
    if "*" in settings.origins_list:
        warnings.append("ALLOWED_ORIGINS includes '*' — restrict to your actual "
                         "frontend domain(s) in production.")
    if settings.REDIS_URL.startswith("redis://localhost"):
        warnings.append("REDIS_URL points to localhost — fine for single-VM "
                         "deployments, but the worker process (worker.py) and "
                         "this web process must share the SAME Redis instance.")
    if warnings:
        print("=" * 70)
        print("⚠️  PRODUCTION CONFIG WARNINGS:")
        for w in warnings:
            print(f"   - {w}")
        print("=" * 70)


app = FastAPI(
    title="AlgoBot",
    description="Multi-user 1-minute options scalping bot",
    version="7.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.origins_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Static files and templates
app.mount("/static", StaticFiles(directory="frontend/static"), name="static")
app.mount("/uploads", StaticFiles(directory="uploads"), name="uploads")
templates = Jinja2Templates(directory="frontend/templates")

# API routers
app.include_router(auth_router)
app.include_router(users_router)
app.include_router(bot_router)
app.include_router(trades_router)
app.include_router(admin_router)
app.include_router(ws_router)
app.include_router(position_router)
app.include_router(oc_router)
app.include_router(push_router)
app.include_router(webhook_router)
app.include_router(subscription_router)
app.include_router(admin_billing_router)
app.include_router(campaign_router)
app.include_router(manual_payment_router)
app.include_router(terminal_router)


@app.get("/health")
async def health():
    """
    Lightweight health check for load balancers / process managers
    (e.g. systemd, Docker HEALTHCHECK, uptime monitors).
    Does NOT require auth — returns DB + Redis connectivity and the
    number of bots currently running (reported by the worker process
    via Redis; will show 0/unknown if the worker is down even if this
    web process itself is healthy).
    """
    from backend.db.database import AsyncSessionLocal
    from backend.services import state_store
    from backend.services.redis_client import ping as redis_ping
    from sqlalchemy import text

    db_ok = True
    try:
        async with AsyncSessionLocal() as db:
            await db.execute(text("SELECT 1"))
    except Exception:
        db_ok = False

    redis_ok = await redis_ping()

    bots_running = await state_store.count_running_bots() if redis_ok else 0

    status = "ok" if (db_ok and redis_ok) else "degraded"
    return {
        "status":        status,
        "database":      "ok" if db_ok else "error",
        "redis":         "ok" if redis_ok else "error",
        "bots_running":  bots_running,
    }


# ── Page routes ──────────────────────────────────────────────────
@app.get("/", response_class=HTMLResponse)
async def landing(request: Request):
    return templates.TemplateResponse("landing.html", {"request": request})

@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request):
    return templates.TemplateResponse("dashboard.html", {"request": request})

@app.get("/settings", response_class=HTMLResponse)
async def settings_page(request: Request):
    return templates.TemplateResponse("settings.html", {"request": request})

@app.get("/trades", response_class=HTMLResponse)
async def trades_page(request: Request):
    return templates.TemplateResponse("trades.html", {"request": request})

@app.get("/admin", response_class=HTMLResponse)
async def admin_page(request: Request):
    return templates.TemplateResponse("admin.html", {"request": request})

@app.get("/admin/rewards", response_class=HTMLResponse)
async def admin_rewards_page(request: Request):
    return templates.TemplateResponse("admin_rewards.html", {"request": request})

@app.get("/admin/rewards/settings", response_class=HTMLResponse)
async def admin_campaign_settings_page(request: Request):
    return templates.TemplateResponse("admin_campaign_settings.html", {"request": request})

@app.get("/admin/billing", response_class=HTMLResponse)
async def admin_billing_page(request: Request):
    # show_nav=False: this page has its own sidebar+topbar shell,
    # rendered instead of the shared navbar (see base.html).
    return templates.TemplateResponse("admin_billing.html", {"request": request, "show_nav": False})

@app.get("/claim-reward", response_class=HTMLResponse)
async def claim_reward_page(request: Request):
    return templates.TemplateResponse("claim_reward.html", {"request": request})

@app.get("/billing", response_class=HTMLResponse)
async def billing_page(request: Request):
    return templates.TemplateResponse("billing.html", {"request": request, "show_nav": False})

@app.get("/oc-dashboard", response_class=HTMLResponse)
async def oc_dashboard(request: Request):
    return templates.TemplateResponse("oc_dashboard.html", {"request": request})

@app.get("/terminal", response_class=HTMLResponse)
async def trading_terminal(request: Request):
    """Advanced Trading Terminal (additive page — existing pages untouched)."""
    return templates.TemplateResponse("terminal.html", {"request": request})

@app.get("/reset-password", response_class=HTMLResponse)
async def reset_password_page(request: Request):
    return templates.TemplateResponse("reset_password.html", {"request": request})

@app.get("/terms", response_class=HTMLResponse)
async def terms_page(request: Request):
    return templates.TemplateResponse("terms.html", {"request": request})

@app.get("/privacy", response_class=HTMLResponse)
async def privacy_page(request: Request):
    return templates.TemplateResponse("privacy.html", {"request": request})
