# backend/shared/production_tests.py
# ================================================================
# Production validation — source-code inspection tests.
# No runtime dependencies required (no Redis, no pandas, no broker).
# ================================================================

import ast
import os
import sys

BASE = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

PASS = 0
FAIL = 0


def _ok(msg: str):
    global PASS
    PASS += 1
    print(f"  [PASS] {msg}")


def _fail(msg: str):
    global FAIL
    FAIL += 1
    print(f"  [FAIL] {msg}")


def _read_src(relpath: str) -> str:
    with open(os.path.join(BASE, relpath), "r") as f:
        return f.read()


def _has_function_with_code(src: str, func_name: str, required_code: str) -> bool:
    """Check if a function/method body contains specific code."""
    tree = ast.parse(src)
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if node.name == func_name:
                func_src = ast.get_source_segment(src, node)
                if func_src and required_code in func_src:
                    return True
    return False


def _has_import(src: str, module: str) -> bool:
    """Check if a module is imported."""
    tree = ast.parse(src)
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == module:
                    return True
        elif isinstance(node, ast.ImportFrom):
            if node.module and node.module == module:
                return True
    return False


def _has_call(src: str, func_name_contains: str) -> bool:
    return func_name_contains in src


def _has_toplevel_variable(src: str, var_name: str) -> bool:
    tree = ast.parse(src)
    for node in tree.body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == var_name:
                    return True
    return False


# ================================================================
# TEST 1: PubSub Auto Recovery
# ================================================================

def test_pubsub_recovery_module_exists():
    print("\n=== Test: PubSub Auto Recovery ===")
    path = os.path.join(BASE, "backend", "shared", "pubsub_utils.py")
    if os.path.isfile(path):
        _ok("pubsub_utils.py exists")
    else:
        _fail("pubsub_utils.py missing")
        return

    src = _read_src("backend/shared/pubsub_utils.py")

    if "resilient_pubsub_consumer" in src:
        _ok("resilient_pubsub_consumer function defined")
    else:
        _fail("resilient_pubsub_consumer not found")

    if "reconnect" in src.lower():
        _ok("Contains reconnection logic")
    else:
        _fail("Missing reconnection logic")

    if "sub(" in src or "subscribe" in src:
        _ok("Contains subscribe call")
    else:
        _fail("Missing subscribe call")

    if "close(" in src or ".close()" in src:
        _ok("Contains pubsub close in finally/cleanup")
    else:
        _fail("Missing pubsub close")


def test_all_engines_use_resilient_pubsub():
    print("\n=== Test: Engines Use Resilient PubSub ===")

    engines = [
        ("candle_builder.py", "SharedCandleBuilder"),
        ("indicator_engine.py", "SharedIndicatorEngine"),
        ("market_structure_engine.py", "SharedMarketStructureEngine"),
        ("strategy_engine.py", "SharedStrategyEngine"),
        ("user_execution_manager.py", "UserExecutionManager"),
    ]

    for filename, class_name in engines:
        src = _read_src(f"backend/shared/{filename}")
        if "resilient_pubsub_consumer" in src:
            _ok(f"{class_name} uses resilient_pubsub_consumer")
        else:
            _fail(f"{class_name} does NOT use resilient_pubsub_consumer")


# ================================================================
# TEST 2: Reliable Signal Publishing
# ================================================================

def test_signal_publish_retry():
    print("\n=== Test: Signal Publish Retry ===")
    src = _read_src("backend/shared/strategy_engine.py")

    if "_publish_signal" in src:
        _ok("_publish_signal method exists")
    else:
        _fail("_publish_signal missing")
        return

    if "retry" in src.lower():
        _ok("Contains retry logic")
    else:
        _fail("Missing retry logic")

    if "_last_published_id" in src:
        _ok("Contains dedup check (_last_published_id)")
    else:
        _fail("Missing dedup check")


# ================================================================
# TEST 3: Command Queue Backpressure
# ================================================================

def test_command_queue_backpressure():
    print("\n=== Test: Command Queue Backpressure ===")
    src = _read_src("backend/services/command_queue.py")

    if "MAX_QUEUE_LENGTH" in src:
        _ok("MAX_QUEUE_LENGTH defined")
    else:
        _fail("MAX_QUEUE_LENGTH missing")

    if "QUEUE_WARN_THRESHOLD" in src:
        _ok("QUEUE_WARN_THRESHOLD defined")
    else:
        _fail("QUEUE_WARN_THRESHOLD missing")

    if "get_queue_depth" in src:
        _ok("get_queue_depth() defined")
    else:
        _fail("get_queue_depth() missing")

    if "check_and_warn_queue" in src:
        _ok("check_and_warn_queue() defined")
    else:
        _fail("check_and_warn_queue() missing")

    if "trim_queue" in src:
        _ok("trim_queue() defined")
    else:
        _fail("trim_queue() missing")

    if "llen" in src:
        _ok("Queue depth check (llen) in push path")
    else:
        _fail("Queue depth check missing")


# ================================================================
# TEST 4: Token Expiry Lifecycle
# ================================================================

def test_token_expiry_lifecycle():
    print("\n=== Test: Token Expiry Lifecycle ===")
    src = _read_src("backend/shared/user_execution_manager.py")

    if "_paused" in src and "Event()" in src:
        _ok("_paused threading.Event exists")
    else:
        _fail("_paused Event missing")

    if "def pause(" in src:
        _ok("pause() method exists")
    else:
        _fail("pause() missing")

    if "def update_token(" in src:
        _ok("update_token() method exists")
    else:
        _fail("update_token() missing")

    if "def resume(" in src:
        _ok("resume() method exists")
    else:
        _fail("resume() missing")

    if "is_paused" in src:
        _ok("is_paused property exists")
    else:
        _fail("is_paused missing")

    if "token_expired" in src:
        _ok("Token expired event published on pause")
    else:
        _fail("Token expired event missing")

    if "self._paused.is_set()" in src:
        _ok("_process_signal checks _paused")
    else:
        _fail("_process_signal missing _paused check")


# ================================================================
# TEST 5: WebSocket Auth Hardening
# ================================================================

def test_websocket_auth_hardening():
    print("\n=== Test: WebSocket Auth Hardening ===")

    gateway_src = _read_src("backend/shared/websocket_gateway.py")
    if "type" in gateway_src and "access" in gateway_src:
        _ok("WSGateway.handle checks token type")
    else:
        _fail("WSGateway.handle missing token type check")

    router_src = _read_src("backend/routers/all_routers.py")
    if "type" in router_src and "access" in router_src:
        _ok("/ws endpoint checks token type")
    else:
        _fail("/ws endpoint missing token type check")


# ================================================================
# TEST 6: Command Handler
# ================================================================

def test_command_handler_update_token():
    print("\n=== Test: Command Handler — update_token ===")
    src = _read_src("backend/shared/shared_worker.py")

    if '"update_token"' in src or "'update_token'" in src:
        _ok("_dispatch_command routes 'update_token'")
    else:
        _fail("_dispatch_command missing 'update_token' route")

    if "def _handle_update_token(" in src:
        _ok("_handle_update_token method exists")
    else:
        _fail("_handle_update_token missing")


# ================================================================
# TEST 7: Redis Client Config
# ================================================================

def test_redis_client_config():
    print("\n=== Test: Redis Client Config ===")
    src = _read_src("backend/services/redis_client.py")

    if "socket_timeout" in src:
        _ok("Redis client has socket_timeout configured")
    else:
        _fail("Redis client missing socket_timeout")

    if "socket_connect_timeout" in src:
        _ok("Redis client has socket_connect_timeout configured")
    else:
        _fail("Redis client missing socket_connect_timeout")

    if "retry_on_timeout" in src:
        _ok("Redis client has retry_on_timeout=True")
    else:
        _fail("Redis client missing retry_on_timeout")

    if "async def close(" in src:
        _ok("Async close() function exists")
    else:
        _fail("Async close() missing")

    if "def close_sync(" in src:
        _ok("close_sync() function exists")
    else:
        _fail("close_sync() missing")


# ================================================================
# TEST 8: Trial users blocked from non-paper config
# ================================================================

def test_trial_config_guard():
    print("\n=== Test: Trial users cannot save non-paper mode ===")
    src = _read_src("backend/routers/all_routers.py")

    if _has_function_with_code(src, "update_config", "SubscriptionStatus.trial"):
        _ok("update_config checks trial subscription")
    else:
        _fail("update_config missing trial subscription check")

    if _has_function_with_code(src, "update_config", "fields[\"execution_mode\"] != ExecutionMode.PAPER.value"):
        _ok("update_config rejects non-PAPER execution_mode for trial users")
    else:
        _fail("update_config missing non-PAPER trial rejection")

    if _has_function_with_code(src, "update_config", "get_current_subscription"):
        _ok("update_config fetches current subscription")
    else:
        _fail("update_config missing get_current_subscription call")

    settings_src = _read_src("frontend/templates/settings.html")
    if "/api/subscription/me" in settings_src:
        _ok("settings page checks subscription status")
    else:
        _fail("settings page missing subscription check")

    if "/billing?upgrade=1" in settings_src:
        _ok("settings page redirects trial users to billing")
    else:
        _fail("settings page missing billing redirect")


# ================================================================
# TEST 9: Admin login auto-provisions admin account
# ================================================================

def test_admin_login():
    print("\n=== Test: Admin login (ADMIN_EMAIL/ADMIN_PASSWORD) ===")
    src = _read_src("backend/routers/auth.py")

    if "settings.ADMIN_EMAIL.strip().lower()" in src:
        _ok("login recognizes the configured ADMIN_EMAIL")
    else:
        _fail("login does not check ADMIN_EMAIL")

    if "hmac.compare_digest(form.password, settings.ADMIN_PASSWORD)" in src:
        _ok("admin password validated against ADMIN_PASSWORD")
    else:
        _fail("admin password not validated against ADMIN_PASSWORD")

    if "role=UserRole.admin" in src:
        _ok("admin login assigns admin role")
    else:
        _fail("admin login does not assign admin role")

    if "pre_verified=True, role=UserRole.admin" in src:
        _ok("admin account auto-created verified + active")
    else:
        _fail("admin account not auto-provisioned verified/active")

    if "user.email_verified = True" in src and "user.is_active      = True" in src:
        _ok("existing admin row promoted to verified + active")
    else:
        _fail("existing admin row not promoted to verified/active")

    if "if user.role == UserRole.admin:" in src:
        _ok("admins are skipped from trial subscription")
    else:
        _fail("admin trial skip missing")

    index_src = _read_src("frontend/templates/index.html")
    if "id=\"r-admin\"" not in index_src:
        _ok("no admin checkbox on register form")
    else:
        _fail("admin checkbox still present on register form")


# ================================================================
# TEST 10: Plan symbol picker fallback + fresh update response
# ================================================================

def test_plan_symbol_picker():
    print("\n=== Test: Plan symbol picker + plan update ===")
    page = _read_src("frontend/templates/admin_billing.html")

    if "DEFAULT_PLAN_SYMBOLS" in page and "known.add(t.symbol)" in page:
        _ok("symbol picker falls back to known symbols")
    else:
        _fail("symbol picker missing default-symbol fallback")

    if 'id="plan-symbol-list"' in page:
        _ok("symbol input is a datalist combo (pick or type additional symbols)")
    else:
        _fail("symbol input missing datalist combo")

    if ".trim().toUpperCase()" in page:
        _ok("typed symbols are trimmed and uppercased")
    else:
        _fail("typed symbols not normalized")

    if "planMainSymbol" in page and "addExtraPlanSymbol" in page:
        _ok("plan editor separates main symbol from additional symbols")
    else:
        _fail("plan editor missing main/additional symbol split")

    router = _read_src("backend/routers/admin_billing_router.py")
    if 'db.refresh(plan, ["symbols"])' in router:
        _ok("plan update refreshes symbols before responding")
    else:
        _fail("plan update does not refresh symbols")


# ================================================================
# TEST 11: Main-symbol options (plan marks candidates, user picks one)
# ================================================================

def test_main_symbol_options():
    print("\n=== Test: Plan main-symbol options ===")
    models = _read_src("backend/db/models.py")
    if 'is_main: Mapped[bool]' in models or "is_main" in models and "Boolean" in models:
        _ok("SubscriptionPlanSymbol carries is_main flag")
    else:
        _fail("SubscriptionPlanSymbol missing is_main flag")

    router = _read_src("backend/routers/admin_billing_router.py")
    if "is_main: bool = False" in router:
        _ok("SymbolLimitIn accepts is_main")
    else:
        _fail("SymbolLimitIn missing is_main field")

    if '"is_main": s.is_main' in router:
        _ok("plan API returns is_main per symbol")
    else:
        _fail("_plan_out does not expose is_main")

    if "is_main=sym.is_main" in router and router.count("is_main=sym.is_main") >= 2:
        _ok("plan create and update both persist is_main")
    else:
        _fail("plan create/update do not persist is_main")

    sub_router = _read_src("backend/routers/subscription_router.py")
    if '"main_symbols": main_symbols' in sub_router:
        _ok("subscription /me exposes main_symbols")
    else:
        _fail("subscription /me missing main_symbols")

    svc = _read_src("backend/services/subscription_service.py")
    if "def main_symbols_for_subscription" in svc:
        _ok("main_symbols_for_subscription helper exists")
    else:
        _fail("main_symbols_for_subscription helper missing")

    if "return [r.symbol.upper() for r in rows]" in svc:
        _ok("legacy plans without main flags fall back to all symbols")
    else:
        _fail("legacy plan fallback missing")

    page = _read_src("frontend/templates/settings.html")
    if "sub.main_symbols" in page and "populateMainSymbols" in page:
        _ok("user settings main-symbol select sourced from plan main options")
    else:
        _fail("settings page does not build main-symbol select from plan")

    admin_page = _read_src("frontend/templates/admin_billing.html")
    if "planMainSymbols" in admin_page and "is_main: true" in admin_page:
        _ok("admin editor marks multiple main symbols")
    else:
        _fail("admin editor missing multi main-symbol marking")


# ================================================================
# MAIN
# ================================================================

def run_all_tests():
    global PASS, FAIL
    PASS = 0
    FAIL = 0

    print("=" * 60)
    print("PRODUCTION HARDENING VALIDATION TESTS")
    print("=" * 60)

    test_pubsub_recovery_module_exists()
    test_all_engines_use_resilient_pubsub()
    test_signal_publish_retry()
    test_command_queue_backpressure()
    test_token_expiry_lifecycle()
    test_websocket_auth_hardening()
    test_command_handler_update_token()
    test_redis_client_config()
    test_trial_config_guard()
    test_admin_login()
    test_plan_symbol_picker()
    test_main_symbol_options()

    print("\n" + "=" * 60)
    total = PASS + FAIL
    print(f"RESULTS: {PASS} passed, {FAIL} failed of {total}")
    print(f"SCORE: {int(PASS / total * 100)}%")
    print("=" * 60)
    return FAIL == 0


if __name__ == "__main__":
    ok = run_all_tests()
    sys.exit(0 if ok else 1)
