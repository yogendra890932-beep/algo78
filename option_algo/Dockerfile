# syntax=docker/dockerfile:1
# AlgoBot application image — used for BOTH the web (uvicorn) and
# worker (worker.py) services in docker-compose.yml.
#
#   Web   : uvicorn backend.main:app  (stateless, talks to Redis/Postgres)
#   Worker: python worker.py          (owns live engines — exactly ONE replica)

FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    TZ=Asia/Kolkata

# tzdata  -> zoneinfo.ZoneInfo("Asia/Kolkata") used for NSE trading hours
# curl    -> container healthcheck
# gcc     -> fallback for any dependency without a prebuilt wheel
RUN apt-get update \
 && apt-get install -y --no-install-recommends gcc tzdata curl \
 && ln -snf /usr/share/zoneinfo/$TZ /etc/localtime \
 && echo $TZ > /etc/timezone \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Runtime-writable directories (also declared as volumes in compose)
RUN mkdir -p candle_cache uploads/payment_screenshots logs

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=40s --retries=5 \
  CMD curl -fsS http://localhost:8000/health || exit 1

CMD ["uvicorn", "backend.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1", "--log-level", "info"]
