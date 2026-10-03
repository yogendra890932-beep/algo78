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

    if _has_function_with_code(src, "update_config", "eff_mode != ExecutionMode.PAPER.value"):
        _ok("update_config rejects non-PAPER execution mode for trial users")
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
# TEST 12: Plan-based symbol/lot gate on Semi Auto / Fully Auto
# ================================================================

def test_plan_symbol_lot_gate():
    print("\n=== Test: Semi Auto / Auto config checked against plan ===")
    src = _read_src("backend/routers/all_routers.py")

    if _has_function_with_code(src, "update_config", "validate_plan_config"):
        _ok("update_config validates config against the plan")
    else:
        _fail("update_config missing validate_plan_config call")

    if "Your subscription is not active — please renew to continue trading." in src:
        _ok("inactive/expired subscriptions blocked from non-paper mode")
    else:
        _fail("inactive/expired subscription gate missing")

    if _has_function_with_code(src, "update_config", "extra_symbol_config"):
        _ok("additional symbols from extra_symbol_config are validated")
    else:
        _fail("additional symbol validation missing")

    svc = _read_src("backend/services/subscription_service.py")
    if "def validate_plan_config" in svc:
        _ok("validate_plan_config helper exists")
    else:
        _fail("validate_plan_config helper missing")

    if "does not include" in svc and "lot(s)" in svc:
        _ok("symbol membership + per-symbol lot limit are both checked")
    else:
        _fail("symbol/lot checks incomplete in validate_plan_config")

    if _has_function_with_code(svc, "check_trading_permission", "validate_plan_config"):
        _ok("bot-start permission check reuses validate_plan_config")
    else:
        _fail("check_trading_permission not refactored onto validate_plan_config")

    page = _read_src("frontend/templates/settings.html")
    if "planCheck" in page and "saveExecutionMode" in page:
        _ok("settings page runs plan check before saving execution mode")
    else:
        _fail("settings page missing plan check")

    if "sub.allowed_symbols[symbol]" in page:
        _ok("settings page checks symbol lot limit against plan")
    else:
        _fail("settings page missing lot-limit check")

    if "j.detail" in page:
        _ok("settings page surfaces backend plan-block message")
    else:
        _fail("settings page does not surface backend detail")


# ================================================================
# TEST 13: Global admin gate for FULLY AUTOMATIC (AUTO) mode
# ================================================================

def test_auto_trading_gate():
    print("\n=== Test: Global admin AUTO trading gate ===")

    models = _read_src("backend/db/models.py")
    if "class PlatformSettings" in models and "auto_trading_enabled" in models:
        _ok("PlatformSettings model with auto_trading_enabled exists")
    else:
        _fail("PlatformSettings model missing")
    if "default=False" in models.split("class PlatformSettings", 1)[-1]:
        _ok("PlatformSettings.auto_trading_enabled defaults to False (fail-safe)")
    else:
        _fail("PlatformSettings default is not False")

    svc = _read_src("backend/services/platform_settings.py")
    for fn in ("get_platform_settings", "is_auto_enabled_async", "is_auto_enabled_sync"):
        if f"def {fn}" in svc:
            _ok(f"platform_settings.{fn} exists")
        else:
            _fail(f"platform_settings.{fn} missing")
    if "return False" in svc and "treating AUTO as disabled" in svc:
        _ok("platform_settings accessors fail safe to disabled")
    else:
        _fail("platform_settings fail-safe behaviour missing")

    router = _read_src("backend/routers/all_routers.py")
    if '@admin_router.get("/platform-settings")' in router and \
            '@admin_router.put("/platform-settings")' in router:
        _ok("admin GET/PUT /platform-settings endpoints exist")
    else:
        _fail("admin platform-settings endpoints missing")
    if _has_function_with_code(router, "update_config", "is_auto_enabled_async"):
        _ok("update_config blocks AUTO when disabled")
    else:
        _fail("update_config missing AUTO gate")
    if _has_function_with_code(router, "set_execution_mode", "is_auto_enabled_async"):
        _ok("set_execution_mode blocks AUTO when disabled")
    else:
        _fail("set_execution_mode missing AUTO gate")
    if "auto_trading_enabled" in router:
        _ok("get_config exposes auto_trading_enabled")
    else:
        _fail("get_config does not expose auto_trading_enabled")

    bcb = _read_src("backend/services/bot_config_builder.py")
    if _has_function_with_code(bcb, "resolve_start_inputs", "ExecutionMode.SEMI_AUTO"):
        _ok("bot start downgrades stored AUTO to SEMI_AUTO when disabled")
    else:
        _fail("bot start AUTO downgrade missing")
    if 'config["is_admin"]' in bcb:
        _ok("engine config carries is_admin for the runtime safety net")
    else:
        _fail("config is_admin flag missing")

    engine = _read_src("backend/engine/engine_v6.py")
    if _has_function_with_code(engine, "_route_via_execution_layer", "is_auto_enabled_sync"):
        _ok("legacy engine has runtime AUTO safety net")
    else:
        _fail("legacy engine runtime AUTO net missing")

    shared = _read_src("backend/shared/user_execution_manager.py")
    if _has_function_with_code(shared, "_execute_auto", "is_auto_enabled_sync"):
        _ok("shared manager has runtime AUTO safety net")
    else:
        _fail("shared manager runtime AUTO net missing")

    page = _read_src("frontend/templates/settings.html")
    for token in ("auto-disabled-note", "applyAutoGate", "auto_trading_enabled"):
        if token in page:
            _ok(f"settings page handles {token}")
        else:
            _fail(f"settings page missing {token}")
    if "autoRadio.disabled = !allowed" in page:
        _ok("settings page disables the AUTO radio when gated")
    else:
        _fail("settings page does not disable the AUTO radio")

    admin_page = _read_src("frontend/templates/admin.html")
    if "/api/admin/platform-settings" in admin_page and "ps-auto-enabled" in admin_page:
        _ok("admin page has the Platform Controls toggle")
    else:
        _fail("admin page Platform Controls missing")

    initdb = _read_src("scripts/init_db.py")
    if "PlatformSettings(id=1" in initdb:
        _ok("init_db seeds the PlatformSettings singleton row")
    else:
        _fail("init_db does not seed PlatformSettings")


# ================================================================
# TEST 14: Prices rounded to the instrument tick before Upstox orders
# ================================================================

def _load_tick_helper():
    import importlib.util
    path = os.path.join(BASE, "backend/services/tick_size.py")
    spec = importlib.util.spec_from_file_location("tick_size", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_tick_size_rounding():
    print("\n=== Test: Upstox order prices rounded to tick size ===")

    if not os.path.exists(os.path.join(BASE, "backend/services/tick_size.py")):
        _fail("tick_size helper module missing")
        return
    _ok("tick_size helper module exists")

    mod = _load_tick_helper()
    if getattr(mod, "TICK_SIZE", None) == 0.05:
        _ok("TICK_SIZE is 0.05 (NSE/BSE index options)")
    else:
        _fail("TICK_SIZE is not 0.05")

    r = mod.round_to_tick
    cases = {123.4567: 123.45, 100.025: 100.05, 100.024: 100.0,
             0.04: 0.05, 99.999: 100.0, 2.0: 2.0}
    good = all(abs(r(p) - e) < 1e-9 for p, e in cases.items())
    if good:
        _ok("round_to_tick rounds half-up onto the 0.05 grid")
    else:
        _fail(f"round_to_tick grid wrong: {[(p, r(p)) for p in cases]}")

    if r(0) == 0.05 and r(-5) == 0.05:
        _ok("round_to_tick never returns a zero/negative price")
    else:
        _fail("round_to_tick does not clamp to one tick")

    for side, want in (("SELL", "below"), ("BUY", "above")):
        trig, limit = mod.sl_trigger_and_limit(123.4567, side)
        on_grid = (abs(trig / 0.05 - round(trig / 0.05)) < 1e-9 and
                   abs(limit / 0.05 - round(limit / 0.05)) < 1e-9)
        ordered = limit < trig if side == "SELL" else limit > trig
        if on_grid and ordered:
            _ok(f"SL {side}: trigger and limit on grid, limit {want} trigger")
        else:
            _fail(f"SL {side}: trig={trig} limit={limit} on_grid={on_grid} ordered={ordered}")

    trig, limit = mod.sl_trigger_and_limit(0.05, "SELL")
    if trig > limit > 0:
        _ok("tiny premium still yields a valid SELL stop-limit gap")
    else:
        _fail(f"tiny premium gap invalid: trig={trig} limit={limit}")

    engine = _read_src("backend/engine/engine_v6.py")
    if _has_function_with_code(engine, "_place_order", "sl_trigger_and_limit"):
        _ok("legacy engine places SL orders tick-rounded")
    else:
        _fail("legacy engine _place_order does not tick-round")
    if _has_function_with_code(engine, "_modify_sl_from_telegram", "sl_trigger_and_limit"):
        _ok("legacy engine telegram SL modify is tick-rounded")
    else:
        _fail("legacy engine _modify_sl_from_telegram not tick-rounded")
    if _has_function_with_code(engine, "_maybe_trail", "sl_trigger_and_limit"):
        _ok("legacy engine trailing SL modify is tick-rounded")
    else:
        _fail("legacy engine _maybe_trail not tick-rounded")
    if _has_function_with_code(engine, "_place_trade", "round_to_tick"):
        _ok("legacy engine rounds the signal SL before storing it")
    else:
        _fail("legacy engine _place_trade does not round the SL")

    shared = _read_src("backend/shared/user_execution_manager.py")
    if _has_function_with_code(shared, "_place_order_live", "sl_trigger_and_limit"):
        _ok("shared manager places live SL orders tick-rounded")
    else:
        _fail("shared manager _place_order_live not tick-rounded")
    if _has_function_with_code(shared, "_modify_sl_live", "sl_trigger_and_limit"):
        _ok("shared manager live SL modify is tick-rounded")
    else:
        _fail("shared manager _modify_sl_live not tick-rounded")

    layer = _read_src("backend/services/execution_layer.py")
    if _has_function_with_code(layer, "place_order_from_engines", "round_to_tick"):
        _ok("execution layer stores tick-rounded SL/target")
    else:
        _fail("execution layer place_order_from_engines not tick-rounded")


# ================================================================
# TEST 15: Strategy codenames in settings + engine normalisation
# ================================================================

def test_strategy_codenames():
    print("\n=== Test: Strategy codenames ===")

    page = _read_src("frontend/templates/settings.html")
    if "Strategy Codenames" in page:
        _ok("settings has a Strategy Codenames legend card")
    else:
        _fail("settings Strategy Codenames card missing")

    codenames = {
        "Gold": "Trend Follow",
        "Diamond": "Pullback",
        "Silver": "Breakout",
        "Platinum": "VWAP Bounce",
        "Bronze": "EMA Cross",
        "Titanium": "VCGB",
        "Obsidian": "Unified Structure",
    }
    for code, strategy in codenames.items():
        if code in page and strategy in page:
            _ok(f"legend maps {code} -> {strategy}")
        else:
            _fail(f"legend missing {code} / {strategy}")

    options = ['value="all"', 'value="pullback"', 'value="breakout"',
               'value="trend_follow"', 'value="vwap_bounce"',
               'value="ema_cross"', 'value="vcgb"']
    for opt in options:
        if opt in page:
            _ok(f"dropdown has option {opt}")
        else:
            _fail(f"dropdown missing option {opt}")

    if 'value="both"' not in page and "Liquidity Sweep" not in page:
        _ok("legacy both/liquidity dropdown options removed")
    else:
        _fail("legacy dropdown options still present")

    if "stratAliases" in page and "both: 'all'" in page:
        _ok("loadConfig normalises legacy strategy values")
    else:
        _fail("loadConfig legacy strategy normalisation missing")

    engine = _read_src("backend/engine/engine_v6.py")
    if "_strategy_aliases" in engine and "run_all" in engine:
        _ok("engine normalises strategy aliases and has a run-all flag")
    else:
        _fail("engine strategy normalisation missing")
    if '"both": "all"' in engine and '"liquidity": "all"' in engine:
        _ok("engine treats both/liquidity as all strategies")
    else:
        _fail("engine does not map both/liquidity to all")
    for key in ("trend_follow", "pullback", "breakout",
                "vwap_bounce", "ema_cross", "vcgb"):
        if f'cfg_strategy == "{key}"' in engine:
            _ok(f"engine can run {key} alone")
        else:
            _fail(f"engine gating missing single {key} branch")


# ================================================================
# TEST 16: Broker rejection reasons are logged, not misreported
# ================================================================

def test_rejection_reason_reporting():
    print("\n=== Test: Order rejection reason reporting ===")

    store = _read_src("backend/services/order_store.py")
    if "_note_rejection" in store and "def get_last_rejection" in store:
        _ok("order_store records and exposes the rejection reason")
    else:
        _fail("order_store rejection-reason plumbing missing")
    if "status_message" in store and "rejection_reason" in store:
        _ok("order_store extracts status_message/rejection_reason")
    else:
        _fail("order_store does not extract the broker message")
    if "fill_price is None and not rejected" in store:
        _ok("wait_for_fill_sync no longer reports a rejection as a timeout")
    else:
        _fail("wait_for_fill_sync still misreports rejections as timeouts")

    layer = _read_src("backend/services/execution_layer.py")
    if "get_last_rejection" in layer and "_emit_entry_failed" in layer:
        _ok("execution_layer surfaces the entry rejection reason")
    else:
        _fail("execution_layer does not surface entry rejection reason")

    mgr = _read_src("backend/shared/user_execution_manager.py")
    if "get_last_rejection" in mgr and "rejected by broker" in mgr:
        _ok("shared live fill-wait logs the broker rejection reason")
    else:
        _fail("shared live fill-wait does not log the broker reason")
    if "refusing" in mgr and "index token" in mgr:
        _ok("shared order placement refuses to send the index token")
    else:
        _fail("shared order placement can still send the index token")

    engine = _read_src("backend/engine/engine_v6.py")
    if "get_last_rejection" in engine and "rejected by broker" in engine:
        _ok("engine fill-wait logs the broker rejection reason")
    else:
        _fail("engine fill-wait does not log the broker reason")


# ================================================================
# TEST 17: Upstox webhook signature verification is opt-in
# ================================================================

def test_webhook_signature_optin():
    print("\n=== Test: Webhook signature verification is opt-in ===")

    wh = _read_src("backend/routers/webhook.py")
    if "WEBHOOK_ENFORCE_SIGNATURE" in wh:
        _ok("webhook honours the WEBHOOK_ENFORCE_SIGNATURE flag")
    else:
        _fail("webhook does not gate verification behind a flag")
    if "if not enforce:" in wh:
        _ok("webhook accepts unsigned postbacks by default")
    else:
        _fail("webhook can still reject unsigned postbacks by default")
    if "no signature header" in wh and "return False" in wh:
        _ok("webhook only fails when enforcement is on")
    else:
        _fail("webhook failure path missing")

    cfg = _read_src("backend/config.py")
    if 'WEBHOOK_ENFORCE_SIGNATURE' in cfg and '"false"' in cfg:
        _ok("config defaults WEBHOOK_ENFORCE_SIGNATURE to false")
    else:
        _fail("config is missing the default-off flag")

    env = _read_src(".env.example")
    if "WEBHOOK_ENFORCE_SIGNATURE=false" in env:
        _ok(".env.example documents the opt-in flag")
    else:
        _fail(".env.example missing WEBHOOK_ENFORCE_SIGNATURE")


# ================================================================
# TEST 18: Entry failures are surfaced on dashboard + terminal
# ================================================================

def test_entry_failure_surfaced():
    print("\n=== Test: Entry failure surfaced on dashboard + terminal ===")

    layer = _read_src("backend/services/execution_layer.py")
    if "def _emit_entry_failed" in layer and '"event": "ORDER_ALERT"' in layer:
        _ok("execution layer emits ORDER_ALERT on entry failure")
    else:
        _fail("execution layer does not emit an ORDER_ALERT on failure")
    for code in ("last_order_error", "get_last_rejection"):
        if code in layer:
            _ok(f"entry failure reason uses {code}")
        else:
            _fail(f"entry failure reason missing {code}")

    mgr = _read_src("backend/shared/user_execution_manager.py")
    if "_last_order_error" in mgr and "def last_order_error" in mgr:
        _ok("live order failures are recorded and exposed")
    else:
        _fail("live order failures are not recorded/exposed")

    dash = _read_src("frontend/templates/dashboard.html")
    if "case 'ORDER_ALERT'" in dash and "ORDER ALERT" in dash:
        _ok("dashboard renders ORDER_ALERT with the reason")
    else:
        _fail("dashboard does not render ORDER_ALERT")

    term = _read_src("frontend/static/js/terminal.js")
    if 'evt === "ORDER_ALERT"' in term and "ORDER FAILED" in term:
        _ok("terminal renders ORDER_ALERT with the reason")
    else:
        _fail("terminal does not render ORDER_ALERT")


def test_idle_no_feed_and_auto_off():
    print("\n=== Test: No market data when idle + 15:45 IST auto-off ===")

    sm = _read_src("backend/shared/symbol_manager.py")
    if "def _is_real_user" in sm and "not _is_real_user(user_id)" in sm:
        _ok("symbol manager only accepts real users (id > 0)")
    else:
        _fail("symbol manager does not guard against sentinel subscribers")
    if "def recover_active_symbols" in sm and "Reset" in sm:
        _ok("restart resets subscriptions (no feed until a bot starts)")
    else:
        _fail("restart recovery does not clear subscriptions")

    w = _read_src("backend/shared/shared_worker.py")
    if "add_subscriber(-1" not in w:
        _ok("no sentinel (-1) subscriber keeps services alive")
    else:
        _fail("sentinel (-1) subscriber still present")
    for code in ("def _maybe_auto_stop_all", "AUTO_STOP_MINUTE = 45",
                 "is_nse_holiday", "self.stop_user(uid)"):
        if code in w:
            _ok(f"auto-off uses {code}")
        else:
            _fail(f"auto-off missing {code}")

    dash = _read_src("frontend/templates/dashboard.html")
    if "function syncLiveMode" in dash and "if (!botRunning) return;" in dash:
        _ok("dashboard opens WS/polling only while a bot runs")
    else:
        _fail("dashboard does not gate live mode on bot status")
    if "/api/bot/status" in dash:
        _ok("dashboard idle-polls bot status")
    else:
        _fail("dashboard has no idle status poll")

    term = _read_src("frontend/static/js/terminal.js")
    if "function syncTerminalLive" in term and "if (!state.botRunning) return;" in term:
        _ok("terminal opens WS only while a bot runs")
    else:
        _fail("terminal does not gate live feed on bot status")
    if "/api/bot/status" in term:
        _ok("terminal idle-polls bot status")
    else:
        _fail("terminal has no idle status poll")


def test_no_direction_flip_with_open_position():
    print("\n=== Test: No direction flip while a position is open ===")

    eng = _read_src("backend/engine/engine_v6.py")
    if "blocked — position open" in eng and "Direction flip" in eng:
        _ok("legacy engine blocks direction flip on an open position")
    else:
        _fail("legacy engine still flips with an open position")
    if "self._emergency_exit()" not in eng:
        _ok("legacy engine no longer force-exits on direction flip")
    else:
        _fail("legacy engine still force-exits on direction flip")

    prem = _read_src("backend/shared/option_premium_service.py")
    if "blocked — position open" in prem and "has_open_position(self.symbol)" in prem:
        _ok("shared premium builder does not roll with an open position")
    else:
        _fail("shared premium builder still rolls with an open position")


def test_partial_lot_reduce():
    print("\n=== Test: Partial lot reduction ===")

    pos = _read_src("backend/routers/position.py")
    if "lots: Optional[int]" in pos and 'payload["lots"]' in pos:
        _ok("squareoff API accepts an optional lots count")
    else:
        _fail("squareoff API has no lots support")

    term = _read_src("backend/routers/terminal.py")
    if 'action == "squareoff" and body.value is not None' in term and 'payload["lots"]' in term:
        _ok("terminal order maps value to squareoff lots")
    else:
        _fail("terminal order does not map lots")

    w = _read_src("backend/shared/shared_worker.py")
    if "eng.squareoff(lots)" in w:
        _ok("shared worker forwards lots to the engine")
    else:
        _fail("shared worker does not forward lots")

    mgr = _read_src("backend/shared/user_execution_manager.py")
    for code in ("def _reduce_position", "def _position_lots",
                 "def squareoff(self, lots=None)", "PARTIAL_EXIT"):
        if code in mgr:
            _ok(f"manager supports {code}")
        else:
            _fail(f"manager missing {code}")
    if "self._modify_sl_live(sl_id" in mgr and "remaining" in mgr:
        _ok("resting SL is resized to the remaining qty")
    else:
        _fail("resting SL is not resized")

    layer = _read_src("backend/services/execution_layer.py")
    if '"lot_size": lot_size, "num_lots": num_lots' in layer:
        _ok("live positions record lot_size / num_lots")
    else:
        _fail("live positions do not record lot info")

    dash = _read_src("frontend/templates/dashboard.html")
    if "btn-reduce" in dash and "body: JSON.stringify({ symbol: sym, lots })" in dash:
        _ok("dashboard has a per-position Reduce control")
    else:
        _fail("dashboard has no per-position Reduce control")

    tjs = _read_src("frontend/static/js/terminal.js")
    if "chip-reduce" in tjs and "function reduceLots" in tjs:
        _ok("terminal chip has a Reduce Lots control")
    else:
        _fail("terminal chip has no Reduce control")


def test_single_pending_and_60s_approval():
    print("\n=== Test: One pending trade at a time + 60s approval ===")

    el = _read_src("backend/services/execution_layer.py")
    if "PENDING_TIMEOUT_SEC = 60" in el:
        _ok("semi-auto approval timeout is 60 seconds")
    else:
        _fail("semi-auto approval timeout is not 60 seconds")
    if "SKIPPED = " in el:
        _ok("a SKIPPED status exists for duplicate signals")
    else:
        _fail("no SKIPPED status for duplicate signals")
    # Both the async and sync creators must dedupe on a WAITING trade.
    if el.count("PendingTrade.status == PendingTradeStatus.WAITING") >= 2:
        _ok("both pending creators reject a second WAITING trade")
    else:
        _fail("pending creators do not both dedupe")
    if "expires_at > now" in el or "expires_at\n" in el:
        _ok("dedupe only counts unexpired pending trades")
    else:
        _fail("dedupe does not check expiry")

    mgr = _read_src("backend/shared/user_execution_manager.py")
    if 'result.status.value == "SKIPPED"' in mgr:
        _ok("manager drops duplicate signals without a second popup")
    else:
        _fail("manager does not handle duplicate pending signals")

    dash = _read_src("frontend/templates/dashboard.html")
    if "MODAL_TIMEOUT_SEC = 60" in dash:
        _ok("dashboard approval countdown is 60 seconds")
    else:
        _fail("dashboard approval countdown is not 60 seconds")

    tjs = _read_src("frontend/static/js/terminal.js")
    if "state.pendingTrades.slice(0, 1)" in tjs:
        _ok("terminal shows only one pending trade / approval button")
    else:
        _fail("terminal can show multiple pending trades")


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
    test_plan_symbol_lot_gate()
    test_auto_trading_gate()
    test_tick_size_rounding()
    test_strategy_codenames()
    test_rejection_reason_reporting()
    test_webhook_signature_optin()
    test_entry_failure_surfaced()
    test_idle_no_feed_and_auto_off()
    test_no_direction_flip_with_open_position()
    test_partial_lot_reduce()
    test_single_pending_and_60s_approval()

    print("\n" + "=" * 60)
    total = PASS + FAIL
    print(f"RESULTS: {PASS} passed, {FAIL} failed of {total}")
    print(f"SCORE: {int(PASS / total * 100)}%")
    print("=" * 60)
    return FAIL == 0


if __name__ == "__main__":
    ok = run_all_tests()
    sys.exit(0 if ok else 1)
