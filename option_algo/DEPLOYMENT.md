#!/bin/bash
# ================================================================
# DEPLOYMENT DOCUMENTATION
# Shared Multi-Tenant Trading Platform — Production Deployment
# ================================================================
#
# This document covers every operational aspect of deploying the
# shared architecture to production. It assumes Ubuntu 22.04+ with
# systemd. Adjust paths and versions for your distribution.
#
# Sections:
#   1.  Prerequisites
#   2.  Systemd Services
#   3.  Nginx Reverse Proxy + TLS
#   4.  Redis Persistence + Authentication
#   5.  PostgreSQL Tuning
#   6.  Uvicorn Configuration
#   7.  Health Monitoring
#   8.  Log Rotation
#   9.  Backup Strategy
#   10. Rolling Restart Procedure
#   11. Disaster Recovery
#   12. Deployment Checklist
# ================================================================

cat << 'DOC'

============================================================
1. PREREQUISITES
============================================================

Packages:
  apt install redis-server postgresql nginx certbot python3-certbot-nginx

Python:
  python3.11+ with pip
  pip install -r requirements.txt

Directories:
  /opt/option_algo/                    # application root
  /opt/option_algo/backend/            # FastAPI + shared modules
  /opt/option_algo/worker.py           # worker entry point
  /var/log/option_algo/                # log directory
  /etc/option_algo/                    # config

Environment file (/etc/option_algo/.env):

  # Required — production secrets
  SECRET_KEY=<random-64-char-string>
  FERNET_KEY=<generated-with-cryptography.fernet.Fernet.generate_key()>
  DATABASE_URL=postgresql+asyncpg://user:pass@localhost:5432/option_algo
  REDIS_URL=redis://:strongpass@localhost:6379/0

  # Required — Upstox broker
  UPSTOX_API_KEY=your_api_key
  UPSTOX_API_SECRET=your_api_secret
  UPSTOX_REDIRECT_URI=https://yourdomain.com/auth/callback

  # Feature flags — enable for shared architecture
  USE_SHARED_WORKER=true
  USE_SHARED_MARKET_DATA=true
  USE_SHARED_STRATEGY=true
  USE_SHARED_WEBSOCKET=true

  # Optional
  ADMIN_USERNAME=admin
  ADMIN_PASSWORD=<strong-password>
  DEBUG=false
  WEBHOOK_SECRET=<random-string>

Database setup:
  sudo -u postgres psql -c "CREATE USER option_algo WITH PASSWORD 'pass';"
  sudo -u postgres psql -c "CREATE DATABASE option_algo OWNER option_algo;"
  sudo -u postgres psql -c "GRANT ALL ON DATABASE option_algo TO option_algo;"
  python scripts/init_db.py   # creates tables


============================================================
2. SYSTEMD SERVICES
============================================================

Two services — one for the web API, one for the worker.

File: /etc/systemd/system/algo_bot-web.service

  [Unit]
  Description=Optiscalper Web API (FastAPI)
  After=network.target redis-server.service postgresql.service
  Wants=redis-server.service postgresql.service

  [Service]
  Type=simple
  User=algo
  Group=algo
  WorkingDirectory=/opt/option_algo
  EnvironmentFile=/etc/option_algo/.env
  ExecStart=/opt/option_algo/venv/bin/uvicorn backend.main:app \
      --host 127.0.0.1 --port 8000 \
      --workers 4 \
      --log-level info \
      --access-log
  Restart=on-failure
  RestartSec=5
  KillSignal=SIGTERM
  TimeoutStopSec=20

  [Install]
  WantedBy=multi-user.target

File: /etc/systemd/system/algo_bot-worker.service

  [Unit]
  Description=Optiscalper Worker (Shared Orchestrator)
  After=network.target redis-server.service postgresql.service
  Wants=redis-server.service postgresql.service
  Requires=algo_bot-web.service

  [Service]
  Type=simple
  User=algo
  Group=algo
  WorkingDirectory=/opt/option_algo
  EnvironmentFile=/etc/option_algo/.env
  ExecStart=/opt/option_algo/venv/bin/python worker.py
  Restart=on-failure
  RestartSec=5
  KillSignal=SIGTERM
  TimeoutStopSec=30
  LimitNOFILE=65536
  MemoryMax=2G

  [Install]
  WantedBy=multi-user.target

IMPORTANT:
  - Only ONE worker process must run. Web API can have 4+ workers.
  - Worker restart = all shared services restart = ~2s trading gap.
  - Set USE_SHARED_WORKER=false in .env to fail back to legacy mode.

Enable and start:
  systemctl daemon-reload
  systemctl enable algo_bot-web algo_bot-worker
  systemctl start algo_bot-web algo_bot-worker

Check status:
  systemctl status algo_bot-web algo_bot-worker
  journalctl -u algo_bot-worker -f


============================================================
3. NGINX REVERSE PROXY + TLS
============================================================

File: /etc/nginx/sites-available/algo_bot

  upstream algo_bot_backend {
      server 127.0.0.1:8000;
  }

  server {
      listen 80;
      server_name yourdomain.com;
      return 301 https://$host$request_uri;
  }

  server {
      listen 443 ssl http2;
      server_name yourdomain.com;

      ssl_certificate     /etc/letsencrypt/live/yourdomain.com/fullchain.pem;
      ssl_certificate_key /etc/letsencrypt/live/yourdomain.com/privkey.pem;
      ssl_protocols TLSv1.2 TLSv1.3;
      ssl_ciphers HIGH:!aNULL:!MD5;

      # WebSocket upgrade
      location /ws/ {
          proxy_pass http://algo_bot_backend;
          proxy_http_version 1.1;
          proxy_set_header Upgrade $http_upgrade;
          proxy_set_header Connection "upgrade";
          proxy_set_header Host $host;
          proxy_set_header X-Real-IP $remote_addr;
          proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
          proxy_set_header X-Forwarded-Proto $scheme;
          proxy_read_timeout 86400;
          proxy_send_timeout 86400;
      }

      # API
      location / {
          proxy_pass http://algo_bot_backend;
          proxy_set_header Host $host;
          proxy_set_header X-Real-IP $remote_addr;
          proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
          proxy_set_header X-Forwarded-Proto $scheme;
          proxy_read_timeout 60s;

          # Rate limiting
          limit_req zone=api burst=20 nodelay;
      }
  }

  # Rate limiting zone (10 req/s with burst)
  limit_req_zone $binary_remote_addr zone=api:10m rate=10r/s;

Enable:
  ln -sf /etc/nginx/sites-available/algo_bot /etc/nginx/sites-enabled/
  nginx -t && systemctl reload nginx

TLS certificate (Let's Encrypt):
  certbot --nginx -d yourdomain.com
  certbot renew --dry-run   # test auto-renewal


============================================================
4. REDIS PERSISTENCE + AUTHENTICATION
============================================================

File: /etc/redis/redis.conf

  # ---- Authentication ----
  requirepass strongpass

  # ---- Persistence (RDB snapshots) ----
  save 900 1
  save 300 10
  save 60 10000
  dbfilename dump.rdb
  dir /var/lib/redis

  # ---- AOF (Append-Only File) — recommended for trading ----
  appendonly yes
  appendfsync everysec
  auto-aof-rewrite-percentage 100
  auto-aof-rewrite-min-size 64mb

  # ---- Memory ----
  maxmemory 2gb
  maxmemory-policy allkeys-lru

  # ---- Network ----
  bind 127.0.0.1
  port 6379
  tcp-keepalive 300
  timeout 300

Restart:
  systemctl restart redis-server

Verify persistence:
  redis-cli -a strongpass CONFIG GET save
  redis-cli -a strongpass CONFIG GET appendonly

Key keyspace breakdown (for monitoring):
  shared:*   — market data, candles, indicators, signals (~80% of memory)
  bot:*      — command queue, bot status, positions (~10%)
  user:*     — per-user subscriptions, execution state (~5%)
  events:*   — per-user event channels (~2%)
  sys:*      — worker health, metrics (~2%)
  inttest:*  — integration test keys (~1%, ephemeral)


============================================================
5. POSTGRESQL TUNING
============================================================

File: /etc/postgresql/<version>/main/postgresql.conf

  shared_buffers = 512MB
  effective_cache_size = 1GB
  work_mem = 16MB
  maintenance_work_mem = 128MB
  max_connections = 100
  wal_buffers = 16MB
  checkpoint_completion_target = 0.9
  random_page_cost = 1.1
  effective_io_concurrency = 200

Application pool (already configured in database.py):
  pool_size = 10
  max_overflow = 20
  pool_pre_ping = True

Restart:
  systemctl restart postgresql

Monitor connections:
  SELECT count(*) FROM pg_stat_activity;


============================================================
6. UVICORN CONFIGURATION
============================================================

Web API (algo_bot-web.service):
  uvicorn backend.main:app --host 127.0.0.1 --port 8000 --workers 4

  Workers: Start with 4 (2 * CPU cores).
  Increase to 8 under heavy load.
  Web processes are STATELESS — all state is in Redis.

Worker (algo_bot-worker.service):
  python worker.py

  Single worker process only.
  Uses USE_SHARED_WORKER env flag to choose shared vs legacy mode.
  Worker holds all engine instances in memory.
  Do NOT run multiple workers — they will compete for command queue.


============================================================
7. HEALTH MONITORING
============================================================

Endpoint: GET /health

  curl https://yourdomain.com/health

  Returns:
  {
    "status": "ok" | "degraded",
    "database": "ok" | "error",
    "redis": "ok" | "error",
    "bots_running": N,
    "timestamp": "..."
  }

Health check for systemd watchdog:
  Add WatchdogSec=30 to worker service
  Worker heartbeat loop refreshes status in Redis every 8s
  If status key expires (> 30s), service is unhealthy

Queue monitoring (wired into worker heartbeat):
  Every 48s (6 * HEARTBEAT_SEC):
    - check_and_warn_queue() — logs at 500/1000 depth
    - trim_queue() — LTRIM to MAX_QUEUE_LENGTH

Alert thresholds:
  - Queue depth >= 500: WARNING log
  - Queue depth >= 1000: CRITICAL log + commands rejected at web layer
  - Redis disconnected: RedisHealthMonitor log + status goes "degraded"
  - Worker not heartbeat-ing: /health returns bots_running=unknown

Monitor worker logs:
  journalctl -u algo_bot-worker -f | grep -E 'WARNING|CRITICAL|error|Token Expired'


============================================================
8. LOG ROTATION
============================================================

File: /etc/logrotate.d/algo_bot

  /var/log/option_algo/*.log {
      daily
      rotate 14
      compress
      delaycompress
      missingok
      notifempty
      create 644 algo algo
      sharedscripts
      postrotate
          systemctl kill -s HUP algo_bot-web algo_bot-worker 2>/dev/null || true
      endscript
  }

  /var/log/option_algo/access*.log {
      daily
      rotate 30
      compress
      missingok
      notifempty
  }


============================================================
9. BACKUP STRATEGY
============================================================

PostgreSQL:
  pg_dump -U option_algo option_algo | gzip > backup-$(date +%Y%m%d).sql.gz

  Cron: 0 2 * * * (daily at 2 AM)
  Keep: 7 daily + 4 weekly + 3 monthly

Redis:
  Backup RDB file: /var/lib/redis/dump.rdb
  Backup AOF file: /var/lib/redis/appendonly.aof

  Cron: 0 * * * * cp /var/lib/redis/dump.rdb /backup/redis/dump-$(date +%Y%m%d-%H).rdb

Application code + config:
  tar czf /backup/app/app-$(date +%Y%m%d).tar.gz /opt/option_algo /etc/option_algo

  Cron: 0 3 * * *

Restore procedure (see Section 11).


============================================================
10. ROLLING RESTART PROCEDURE
============================================================

GOAL: Restart the worker with zero user impact.

Current limitation: Worker restart disconnects all shared services.
                  ~2 second gap in market data processing.

Procedure:

1. Announce maintenance window (if needed for production users).

2. Restart web API (zero-downtime — nginx load balances):
     systemctl restart algo_bot-web
   Wait 5s, verify:
     curl https://yourdomain.com/health

3. Restart worker (brief gap):
     systemctl restart algo_bot-worker
   Wait 10s, verify:
     systemctl status algo_bot-worker
     journalctl -u algo_bot-worker --since "1 min ago" | grep "shared-orch.*Started"

4. Users must press "Start" to resume their bots:
   Worker restart is a clean start — _restore_running_users() recovers
   active symbols but does NOT auto-restart user bots (token refresh
   required).

5. Verify shared services:
   curl https://yourdomain.com/health
   Should show bots_running >= 0, status = "ok"

Future improvement: Persist running user list in Redis and add
auto-restart with token refresh. Currently requires manual re-start.


============================================================
11. DISASTER RECOVERY
============================================================

SCENARIO A: Redis Crash
  - Worker logs: "Redis disconnected" (RedisHealthMonitor)
  - /health returns redis="error", status="degraded"
  - Web API continues serving (stateless, reads from PostgreSQL)
  - Worker pubsub loops reconnect automatically (resilient_pubsub_consumer)
  - Recovery time: < 30s (health check interval + reconnect backoff)
  - With persistence: no data loss
  - Without persistence: shared market data lost, candles rebuild from live feed

  Manual recovery (if auto-reconnect fails):
    systemctl restart redis-server
    systemctl restart algo_bot-worker

SCENARIO B: PostgreSQL Crash
  - Web API returns 503 for auth/start endpoints (no DB)
  - Worker continues trading (symbol config cached in memory)
  - Recovery time: DB restart + pool_pre_ping detects healthy connections
  - No intervention needed if auto-restarted by systemd

SCENARIO C: Broker (Upstox) Disconnect
  - SharedMarketDataService._run() auto-reconnects every 3s
  - During gap: tick data lost, candles and signals pause
  - After reconnect: ticks resume, candles continue from last bar
  - Users with expired tokens: UserExecutionManager pauses automatically
    (when 401 detected — currently requires manual pause trigger)

SCENARIO D: Worker Crash
  - systemd Restart=on-failure restarts worker
  - _restore_running_users() recovers active symbols from Redis
  - Shared services re-initialized for active symbols
  - User bots must be manually restarted (token refresh required)
  - Closed trades persisted in PostgreSQL (safe)
  - Open positions lost (in-memory only — risk)

SCENARIO E: Full Server Reboot
  - systemd auto-starts all services on boot
  - PostgreSQL + Redis start first
  - Web API starts → serves health/status endpoints
  - Worker starts → recovers active symbols → waits for commands
  - Users must manually restart bots
  - Estimated full recovery: 60-90 seconds

SCENARIO F: Complete Data Loss (Redis + DB corruption)
  1. Stop all services: systemctl stop algo_bot-worker algo_bot-web
  2. Restore PostgreSQL: pg_restore from latest backup
  3. Restore Redis: copy dump.rdb to /var/lib/redis/
  4. Start services: systemctl start redis-server postgresql
  5. Start app: systemctl start algo_bot-web algo_bot-worker
  6. Verify: /health returns "ok"
  7. Notify users to restart bots

SCENARIO G: Token Expiry (per-user, automated)
  1. UserExecutionManager detects expired token
  2. Manager.pause() — pauses ONLY that user
  3. WebSocket event: {"event": "token_expired", "user_id": N}
  4. Other users continue trading unaffected
  5. User submits new token via frontend
  6. Worker command: update_token → validates → resumes
  7. User resumes trading immediately
  8. No worker restart, no shared service restart


============================================================
12. DEPLOYMENT CHECKLIST
============================================================

[ ] Server provisioned (Ubuntu 22.04+, 4+ vCPU, 8+ GB RAM)
[ ] Python 3.11+ installed
[ ] Redis installed and configured (auth + persistence + maxmemory)
[ ] PostgreSQL installed and configured (pool connections, backups)
[ ] Nginx installed and configured (TLS + WebSocket + rate limiting)
[ ] TLS certificate obtained (Let's Encrypt or commercial)
[ ] Application code deployed to /opt/option_algo
[ ] .env file populated with production secrets
[ ] Database tables created (scripts/init_db.py)
[ ] Systemd unit files installed and enabled
[ ] Log rotation configured
[ ] Backup cron jobs active
[ ] Firewall: 80/443 open, 6379/5432/8000 restricted to localhost

[ ] SECRET_KEY != "dev-secret-change-me"
[ ] FERNET_KEY is set
[ ] ADMIN_PASSWORD is not default
[ ] DEBUG = false
[ ] REDIS_URL has password (redis://:pass@host...)
[ ] REDIS_URL uses localhost (not 0.0.0.0)
[ ] USE_SHARED_WORKER = true (or false for legacy)

Startup verification:
[ ] systemctl start algo_bot-web → curl /health returns "ok"
[ ] systemctl start algo_bot-worker → journalctl shows "Orchestrator started"
[ ] systemctl enable algo_bot-web algo_bot-worker
[ ] nginx -t → syntax OK → systemctl reload nginx
[ ] curl -k https://localhost/health returns 200
[ ] wscat -c wss://yourdomain.com/ws/1?token=<valid_token> connects

Load test (optional):
[ ] Run stress_test.py with 10, 50, 100 users
[ ] Verify queue depth < 500
[ ] Verify latency p99 < 500ms
[ ] Verify zero cross-user data leaks

DOC
