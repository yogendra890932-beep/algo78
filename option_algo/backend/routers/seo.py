# backend/routers/seo.py
# ================================================================
# Search-engine endpoints.
#
#   GET /robots.txt   — crawl directives (public pages allowed,
#                       authenticated app pages + APIs disallowed)
#   GET /sitemap.xml  — XML sitemap of the public, indexable pages
#
# The canonical host comes from APP_BASE_URL (backend/config.py),
# so the same code works on localhost and in production without a
# hard-coded domain.
# ================================================================

from datetime import date
from pathlib import Path

from fastapi import APIRouter
from fastapi.responses import FileResponse, PlainTextResponse, Response

from backend.config import get_settings

router = APIRouter(tags=["seo"])

_FAVICON_ICO = Path("frontend/static/favicon.ico")

# Paths that require login (or are internal) — never indexed.
_DISALLOW = [
    "/dashboard",
    "/terminal",
    "/trades",
    "/settings",
    "/admin",
    "/billing",
    "/oc-dashboard",
    "/claim-reward",
    "/reset-password",
    "/api/",
    "/uploads/",
]

# Public pages, in sitemap priority order.
# NOTE: /login is intentionally excluded — it is marked noindex
# (authenticated shell) and would otherwise conflict with the sitemap.
_PUBLIC_PAGES = [
    ("/", 1.0, "daily"),
    ("/terms", 0.3, "yearly"),
    ("/privacy", 0.3, "yearly"),
    ("/refund", 0.3, "yearly"),
]


def _base_url() -> str:
    return (get_settings().APP_BASE_URL or "https://optiscalper.com").rstrip("/")


@router.get("/robots.txt", response_class=PlainTextResponse, include_in_schema=False)
async def robots_txt() -> str:
    base = _base_url()
    lines = ["User-agent: *", "Allow: /"]
    lines += [f"Disallow: {path}" for path in _DISALLOW]
    lines += ["", f"Sitemap: {base}/sitemap.xml", ""]
    return "\n".join(lines)


@router.get("/favicon.ico", include_in_schema=False)
async def favicon_ico():
    # Browsers request /favicon.ico at the origin root even when a
    # <link rel="icon"> is present — serve the real file to avoid 404s.
    if _FAVICON_ICO.exists():
        return FileResponse(_FAVICON_ICO, media_type="image/x-icon")
    return Response(status_code=204)


@router.get("/sitemap.xml", include_in_schema=False)
async def sitemap_xml() -> Response:
    base = _base_url()
    today = date.today().isoformat()
    url_tags = []
    for path, priority, freq in _PUBLIC_PAGES:
        url_tags.append(
            "  <url>\n"
            f"    <loc>{base}{path}</loc>\n"
            f"    <lastmod>{today}</lastmod>\n"
            f"    <changefreq>{freq}</changefreq>\n"
            f"    <priority>{priority:.1f}</priority>\n"
            "  </url>"
        )
    body = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
        + "\n".join(url_tags)
        + "\n</urlset>\n"
    )
    return Response(content=body, media_type="application/xml")
