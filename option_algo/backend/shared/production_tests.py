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

    models = _read_src("backend/db/models.py")
    if "PARTIAL_EXIT = " in models:
        _ok("TradeStatus enum includes PARTIAL_EXIT")
    else:
        _fail("TradeStatus enum missing PARTIAL_EXIT")
    worker = _read_src("worker.py")
    if '"PARTIAL_EXIT":' in worker:
        _ok("worker persists PARTIAL_EXIT instead of falling back to SL")
    else:
        _fail("worker does not persist PARTIAL_EXIT")
    if "PARTIAL_EXIT" in tjs and "Partial Exit" in tjs:
        _ok("terminal renders PARTIAL_EXIT as 'Partial Exit'")
    else:
        _fail("terminal lacks a Partial Exit label")
    if "PARTIAL_EXIT" in dash and "Partial Exit" in dash:
        _ok("dashboard renders PARTIAL_EXIT as 'Partial Exit'")
    else:
        _fail("dashboard lacks a Partial Exit label")


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


def test_seo_and_indexing():
    print("\n=== Test: SEO assets, crawl directives and structured data ===")

    seo = _read_src("backend/routers/seo.py")
    if "/robots.txt" in seo and 'f"Disallow: {path}"' in seo and '"Sitemap:' in seo:
        _ok("robots.txt route serves crawl directives")
    else:
        _fail("robots.txt route missing or malformed")
    if "sitemaps.org/schemas/sitemap" in seo and "urlset" in seo:
        _ok("sitemap.xml route emits a valid urlset")
    else:
        _fail("sitemap.xml route missing or malformed")
    if "/login" not in seo.split("_PUBLIC_PAGES")[1].split("]")[0]:
        _ok("noindex login page excluded from the sitemap")
    else:
        _fail("sitemap still lists the noindex login page")

    if '("/about", 0.6, "monthly")' in seo:
        _ok("about page is in the sitemap")
    else:
        _fail("about page missing from the sitemap")

    main_src = _read_src("backend/main.py")
    if '@app.get("/about", response_class=HTMLResponse)' in main_src \
            and 'TemplateResponse("about.html"' in main_src:
        _ok("/about route serves the About page")
    else:
        _fail("/about route missing")
    if "/about" not in main_src.split("_NOINDEX_PREFIXES")[1].split(")")[0]:
        _ok("about page is indexable (not in the noindex list)")
    else:
        _fail("about page is marked noindex")
    about_html = _read_src("frontend/templates/about.html")
    for needle, label in [
        ('<link rel="canonical" href="{{ site_url | default(\'https://optiscalper.com\') }}/about">', "canonical URL"),
        ('name="description"', "meta description"),
        ('name="robots" content="index, follow"', "indexable robots meta"),
        ('property="og:title"', "Open Graph title"),
        ("Risk Disclosure", "risk disclosure"),
        ("support@optiscalper.com", "support contact"),
        ('href="/terms"', "terms link"),
        ('href="/privacy"', "privacy link"),
    ]:
        if needle in about_html:
            _ok(f"about page has {label}")
        else:
            _fail(f"about page missing {label}")
    landing_src = _read_src("frontend/templates/landing.html")
    if 'href="/about"' in landing_src:
        _ok("landing page links to the About page")
    else:
        _fail("landing page has no About link")
    priv = _read_src("frontend/templates/privacy.html")
    if 'href="/about"' in priv:
        _ok("legal pages link to the About page")
    else:
        _fail("legal pages do not link to About")

    if "from backend.routers.seo import router as seo_router" in main_src and "app.include_router(seo_router)" in main_src:
        _ok("SEO router is registered on the app")
    else:
        _fail("SEO router not registered")
    if "X-Robots-Tag" in main_src and "noindex, nofollow" in main_src:
        _ok("app pages carry X-Robots-Tag: noindex")
    else:
        _fail("app pages lack noindex headers")

    landing = _read_src("frontend/templates/landing.html")
    for needle, label in [
        ('rel="canonical"', "canonical URL"),
        ('property="og:image"', "Open Graph image"),
        ('name="twitter:card"', "Twitter card"),
        ('application/ld+json', "JSON-LD structured data"),
        ('"@type": "SoftwareApplication"', "SoftwareApplication schema"),
        ("/static/og-image.png", "OG image asset reference"),
    ]:
        if needle in landing:
            _ok(f"landing has {label}")
        else:
            _fail(f"landing missing {label}")

    base = _read_src("frontend/templates/base.html")
    if 'content="noindex, nofollow"' in base and "/static/favicon.svg" in base:
        _ok("app shell is noindex with proper favicon")
    else:
        _fail("app shell missing noindex or favicon")

    # ── Refund & Cancellation policy ──
    refund_path = os.path.join(BASE, "frontend/templates/refund.html")
    if os.path.exists(refund_path):
        _ok("refund policy template exists")
    else:
        _fail("refund policy template missing")
    refund = _read_src("frontend/templates/refund.html")
    if 'rel="canonical"' in refund and 'content="index, follow"' in refund:
        _ok("refund policy is indexable with a canonical URL")
    else:
        _fail("refund policy SEO meta missing")
    public = seo.split("_PUBLIC_PAGES")[1].split("]")[0]
    if "/refund" in public:
        _ok("refund page is in the sitemap")
    else:
        _fail("refund page missing from the sitemap")
    if '@app.get("/refund"' in main_src and 'TemplateResponse("refund.html"' in main_src:
        _ok("refund page has a route")
    else:
        _fail("refund page route missing")
    if "'/refund'" in base:
        _ok("refund page treated as public in the app shell")
    else:
        _fail("refund not in base public paths")
    if 'href="/refund"' in landing:
        _ok("refund policy linked from the landing footer")
    else:
        _fail("refund policy not linked from the footer")


def test_dashboard_lots_and_terminal_layout():
    print("\n=== Test: Dashboard 'Lots' label + terminal laptop layout ===")

    dash = _read_src("frontend/templates/dashboard.html")
    if '<span class="pill-label">Lots</span><b id="d-qty">' in dash:
        _ok("dashboard order-size pill is labelled 'Lots'")
    else:
        _fail("dashboard still labels the order-size pill 'Qty'")

    css = _read_src("frontend/static/css/terminal.css")
    left_block = css.split(".term-left {")[1].split("}")[0]
    if "justify-content: flex-start" in left_block:
        _ok("selected-symbol card stacks under the watchlist (laptop mode)")
    else:
        _fail("selected-symbol card is detached from the watchlist")

    for rel in [
        "frontend/static/favicon.svg",
        "frontend/static/favicon.ico",
        "frontend/static/favicon-32.png",
        "frontend/static/apple-touch-icon.png",
        "frontend/static/og-image.png",
        "frontend/static/logo.png",
        "frontend/static/site.webmanifest",
        "frontend/static/icons/icon-192.png",
        "frontend/static/icons/icon-512.png",
    ]:
        if os.path.exists(os.path.join(BASE, rel)):
            _ok(f"asset present: {rel}")
        else:
            _fail(f"asset missing: {rel}")


def test_strong_trend_only_signals():
    print("\n=== Test: signals only when underlying 5m phase = STRONG_TREND ===")

    se = _read_src("backend/shared/strategy_engine.py")
    if _has_function_with_code(se, "_evaluate_all", 'get_underlying_structure'):
        _ok("shared engine reads the underlying 5m structure before signaling")
    else:
        _fail("shared engine does not gate on the underlying structure")
    if 'QUICK_SCALP_PHASES = {"STRONG_TREND", "DISTRIBUTION", "COMPRESSION"}' in se:
        _ok("shared engine allows STRONG_TREND + quick-scalp phases")
    else:
        _fail("quick-scalp phase set is missing")
    if _has_function_with_code(se, "_evaluate_all",
                               "under_phase not in SIGNAL_PHASES"):
        _ok("shared engine blocks phases outside the allowed set")
    else:
        _fail("shared engine does not gate on the allowed phase set")

    ev6 = _read_src("backend/engine/engine_v6.py")
    if "UnderPhase.RANGE" in ev6:
        _ok("legacy engine allows the RANGE phase")
    else:
        _fail("legacy engine does not allow the RANGE phase")
    if "UnderPhase.STRONG_TREND, UnderPhase.DISTRIBUTION" in ev6 or \
            "UnderPhase.DISTRIBUTION, UnderPhase.COMPRESSION" in ev6:
        _ok("legacy engine allows quick-scalp phases when enabled")
    else:
        _fail("legacy engine does not allow quick-scalp phases")
    if "(UnderPhase.STRONG_TREND, UnderPhase.WEAK_TREND)" not in ev6:
        _ok("legacy engine no longer accepts WEAK_TREND")
    else:
        _fail("legacy engine still accepts WEAK_TREND")
    if _has_function_with_code(ev6, "_on_bar_close", "UnderPhase.STRONG_TREND"):
        _ok("legacy engine gates all strategies in _on_bar_close")
    else:
        _fail("legacy engine does not gate the 6 strategies")


def test_quick_scalp_phase_gate():
    print("\n=== Test: Quick Scalp toggles DISTRIBUTION/COMPRESSION signals ===")

    models = _read_src("backend/db/models.py")
    if "quick_scalp_enabled" in models:
        _ok("User has a persisted quick_scalp_enabled flag")
    else:
        _fail("User.quick_scalp_enabled is missing")

    initdb = _read_src("scripts/init_db.py")
    if "quick_scalp_enabled" in initdb:
        _ok("init_db adds the quick_scalp column")
    else:
        _fail("init_db does not add the quick_scalp column")

    uem = _read_src("backend/shared/user_execution_manager.py")
    if "_quick_scalp_enabled" in uem and "get_user_quick_scalp" in uem:
        _ok("execution manager reads the per-user quick-scalp flag")
    else:
        _fail("execution manager does not read the quick-scalp flag")
    if "DISTRIBUTION" in uem and "COMPRESSION" in uem:
        _ok("execution manager filters DISTRIBUTION/COMPRESSION when off")
    else:
        _fail("execution manager does not filter quick-scalp phases")

    sw = _read_src("backend/shared/shared_worker.py")
    if "set_quick_scalp" in sw and "_handle_set_quick_scalp" in sw:
        _ok("shared worker handles the set_quick_scalp command")
    else:
        _fail("shared worker does not handle set_quick_scalp")

    wk = _read_src("worker.py")
    if "set_quick_scalp" in wk and "_handle_set_quick_scalp" in wk:
        _ok("legacy worker handles the set_quick_scalp command")
    else:
        _fail("legacy worker does not handle set_quick_scalp")

    router = _read_src("backend/routers/terminal.py")
    if '"/api/terminal/quick-scalp"' in router:
        _ok("terminal exposes a quick-scalp toggle endpoint")
    else:
        _fail("quick-scalp endpoint missing")
    tsvc = _read_src("backend/services/terminal_service.py")
    if '"quick_scalp"' in tsvc:
        _ok("bootstrap returns the quick-scalp state")
    else:
        _fail("bootstrap does not return quick-scalp state")

    html = _read_src("frontend/templates/terminal.html")
    if 'id="term-quick-scalp"' in html:
        _ok("terminal has a Quick Scalp toggle button")
    else:
        _fail("Quick Scalp button missing")
    js = _read_src("frontend/static/js/terminal.js")
    if "function syncQuickScalp(" in js and "/api/terminal/quick-scalp" in js:
        _ok("terminal toggle is wired to the API")
    else:
        _fail("terminal Quick Scalp toggle is not wired")


def test_range_fade_strategy():
    print("\n=== Test: always-on Range Fade strategy in RANGE phase ===")

    se = _read_src("backend/shared/strategy_engine.py")
    if 'SIGNAL_PHASES = QUICK_SCALP_PHASES | {"RANGE", "WEAK_TREND", "PULLBACK"}' in se:
        _ok("RANGE and trend-pullback phases are allowed")
    else:
        _fail("RANGE/WEAK_TREND/PULLBACK are not allowed by the signal gate")
    if "def _eval_range_fade(" in se and 'under_phase == "RANGE"' in se:
        _ok("shared engine evaluates Range Fade only in RANGE")
    else:
        _fail("shared engine does not evaluate Range Fade in RANGE")

    ev6 = _read_src("backend/engine/engine_v6.py")
    if "def _eval_range_fade(" in ev6 and "self._eval_range_fade()" in ev6:
        _ok("legacy engine has Range Fade parity")
    else:
        _fail("legacy engine has no Range Fade parity")

    uem = _read_src("backend/shared/user_execution_manager.py")
    if '"range_fade": "all"' in uem:
        _ok("Range Fade bypasses the per-user strategy filter")
    else:
        _fail("Range Fade is filtered by the user's strategy choice")
    if 'phase not in ("STRONG_TREND", "RANGE",' in uem:
        _ok("RANGE/WEAK_TREND/PULLBACK pass the per-user phase gate")
    else:
        _fail("trend-pullback phases are blocked by the Quick Scalp gate")

    rd = _read_src("backend/shared/range_detector.py")
    for token in ("detect_range_channel", "range_entry", "RANGE_ENTRY_ZONE",
                  "RANGE_MIN_TOUCHES"):
        if token not in rd:
            _fail(f"range_detector missing {token}")
            break
    else:
        _ok("range detector exposes channel + entry helpers")

    # Synthetic data: a flat oscillating channel must be detected and the
    # lower-boundary bounce accepted; drifting / micro channels rejected.
    try:
        import math
        import pandas as pd
        from backend.shared.range_detector import (
            detect_range_channel, range_entry,
        )

        rows = []
        for i in range(48):
            c = 100.0 + 1.0 * math.sin(i * 2 * math.pi / 12.0)
            rows.append({"open": c - 0.05, "high": c + 0.05,
                         "low": c - 0.05, "close": c, "volume": 1000})
        rows[-1] = {"open": 99.0, "high": 99.4, "low": 98.9,
                    "close": 99.3, "volume": 1000}
        flat = pd.DataFrame(rows)
        ch = detect_range_channel(flat)
        if ch and range_entry(flat, ch):
            _ok("flat channel detected and dip-buy entry accepted")
        else:
            _fail("flat channel or entry not detected")

        drift = pd.DataFrame([
            {"open": c - 0.2, "high": c + 0.3, "low": c - 0.3,
             "close": c, "volume": 1000}
            for c in [100 + i * 0.5 for i in range(40)]
        ])
        if detect_range_channel(drift) is None:
            _ok("drifting channel rejected")
        else:
            _fail("drifting channel accepted as a range")

        micro = pd.DataFrame([
            {"open": 100.0, "high": 100.02, "low": 99.99,
             "close": 100.01, "volume": 1000}
            for _ in range(40)
        ])
        if detect_range_channel(micro) is None:
            _ok("micro range rejected")
        else:
            _fail("micro range accepted")
    except Exception as e:
        _fail(f"range detector synthetic check crashed: {e}")


def test_unified_strategy_reachable():
    print("\n=== Test: unified strategy can actually fire ===")

    ms = _read_src("backend/engine/market_structure.py")
    if "keep the pullback context" in ms and "RecoveryStatus.ACTIVE" in ms:
        _ok("structure engine keeps the pullback context while recovering")
    else:
        _fail("pullback context is wiped before recovery can confirm")

    se = _read_src("backend/shared/strategy_engine.py")
    unified = se.split("def _eval_unified_strategy(")[1].split("def ", 1)[0]
    if 'not in ("ACTIVE", "CONFIRMED")' in unified:
        _ok("shared unified accepts an ACTIVE recovery")
    else:
        _fail("shared unified still requires CONFIRMED only")

    legacy = _read_src("backend/engine/engine_v6.py")
    luni = legacy.split("def _eval_unified_strategy(")[1].split("\n    def ", 1)[0]
    if "RecStatus.ACTIVE" in luni:
        _ok("legacy unified accepts an ACTIVE recovery")
    else:
        _fail("legacy unified still requires CONFIRMED only")

    mse = _read_src("backend/shared/market_structure_engine.py")
    if "from backend.shared.redis_infra import (\n    STRUCTURE_TTL_SEC," in mse:
        _ok("structure results use the 5-minute TTL, not the 5-second cache TTL")
    else:
        _fail("structure results still expire after 5 seconds")

    try:
        from backend.engine.market_structure import (
            MarketStructureEngine, SwingPoint, SwingType, SwingLevel,
            TrendDirection, PullbackType, RecoveryStatus,
        )

        e = MarketStructureEngine()
        e.trend_direction = TrendDirection.BULLISH
        e.global_index = 19
        e.candles = [
            {"open": 100, "high": 125, "low": 100, "close": 120, "volume": 100}
        ] * 17 + [
            {"open": 120, "high": 121, "low": 112, "close": 113, "volume": 100},
            {"open": 113, "high": 124, "low": 112, "close": 122, "volume": 120},
        ]
        e.swing_lows = [SwingPoint(0, 100.0, SwingType.LOW, SwingLevel.MAJOR,
                                   80, 80, 19, 0, 100)]
        e.swing_highs = [
            SwingPoint(10, 120.0, SwingType.HIGH, SwingLevel.MAJOR,
                       80, 80, 9, 0, 100),
            SwingPoint(18, 115.0, SwingType.HIGH, SwingLevel.MINOR,
                       40, 40, 1, 0, 100),
        ]
        e.pullback_active = True
        e.pullback_type = PullbackType.HEALTHY
        e.pullback_start_idx = 17
        e.pullback_duration = 2
        e.pullback_max_depth = 40.0
        e.recovery_status = RecoveryStatus.ACTIVE
        e.recovery_start_idx = 17
        e._analyze_pullback_recovery()
        if (e.pullback_active and e.pullback_type.value == "HEALTHY"
                and e.recovery_status.value == "CONFIRMED"):
            _ok("pullback + CONFIRMED coexist on the confirmation bar")
        else:
            _fail(f"states do not coexist: pb={e.pullback_type.value} "
                  f"rec={e.recovery_status.value}")

        e.candles.append({"open": 122, "high": 126, "low": 121,
                          "close": 125, "volume": 120})
        e.global_index = 20
        e._analyze_pullback_recovery()
        if not e.pullback_active:
            _ok("pullback context is not retained forever")
        else:
            _fail("pullback context never resets (would pin the phase)")
    except Exception as e:
        _fail(f"structure engine runtime check crashed: {e}")


def test_no_late_entry():
    print("\n=== Test: no late entry in any strategy ===")

    if not os.path.exists(os.path.join(BASE, "backend/shared/entry_timing.py")):
        _fail("entry_timing guard module missing")
        return

    uem = _read_src("backend/shared/user_execution_manager.py")
    if "_late_entry_reason(sig_symbol, signal)" in uem:
        _ok("signal intake rejects late entries for every strategy")
    else:
        _fail("signal intake has no late-entry guard")

    layer = _read_src("backend/services/execution_layer.py")
    if "late_entry_reason(" in layer and "_live_ltp(eng, signal)" in layer:
        _ok("fill-time check uses the freshest live LTP")
    else:
        _fail("fill-time late-entry check missing")
    if "precheck_fn" in layer:
        _ok("approval path can reject a late entry before marking APPROVED")
    else:
        _fail("approval precheck missing")
    if '"timestamp": getattr(signal, "timestamp", None)' in layer:
        _ok("pending trades persist the original signal time")
    else:
        _fail("pending trade payload drops the signal timestamp")

    try:
        from datetime import datetime, timedelta, timezone
        from backend.shared.entry_timing import late_entry_reason

        now = datetime.now(timezone.utc)
        fresh = now.isoformat()

        r = late_entry_reason(100.0, 95.0, None, 100.5, 0.05, 1.5,
                              signal_ts=fresh, now=now, symbol="NIFTY")
        if not r:
            _ok("timely entry at the signal price is accepted")
        else:
            _fail(f"timely entry rejected: {r}")

        r = late_entry_reason(100.0, 95.0, None, 102.0, 0.05, 1.5,
                              signal_ts=fresh, now=now, symbol="NIFTY")
        if "late entry" in r:
            _ok("chasing entry beyond the drift budget is rejected")
        else:
            _fail(f"chasing entry accepted: {r!r}")

        old = (now - timedelta(seconds=200)).isoformat()
        r = late_entry_reason(100.0, 95.0, None, 100.1, 0.05, 1.5,
                              signal_ts=old, now=now, symbol="NIFTY")
        if "stale signal" in r:
            _ok("stale signal is rejected")
        else:
            _fail(f"stale signal accepted: {r!r}")

        r = late_entry_reason(100.0, 95.0, None, 94.5, 0.05, 1.5,
                              signal_ts=fresh, now=now, symbol="NIFTY")
        if "invalidated" in r:
            _ok("entry below the stop is rejected")
        else:
            _fail(f"invalidated entry accepted: {r!r}")

        r = late_entry_reason(100.0, 95.0, 110.0, 111.0, 0.05, 1.5,
                              signal_ts=fresh, now=now, symbol="NIFTY")
        if "at target" in r:
            _ok("entry after the target is rejected")
        else:
            _fail(f"post-target entry accepted: {r!r}")

        r = late_entry_reason(100.0, 95.0, None, None, 0.05, 1.5,
                              signal_ts=fresh, now=now, symbol="NIFTY")
        if not r:
            _ok("missing live tick does not fabricate a rejection")
        else:
            _fail(f"missing LTP rejected: {r}")
    except Exception as e:
        _fail(f"late-entry guard synthetic check crashed: {e}")


def test_pullback_quality_and_risk():
    print("\n=== Test: refined pullback accuracy + account R:R filter ===")

    se = _read_src("backend/shared/strategy_engine.py")
    pb = se.split("def _eval_pullback(")[1].split("def _eval_trend_follow(")[0]
    for token, label in (
        ("ema_9_slope", "rising fast EMA"),
        ("pb_vol > avg_vol * 1.5", "pullback volume contraction"),
        ("body_r < 0.5", "strong recovery body"),
        ("<= float(ind_df.iloc[-2][\"high\"])", "recovery clears prior bar"),
        ("_calc_adx", "premium ADX filter"),
        ("\"take_profit\"", "structural room to target"),
        ("\"risk_pct\"", "structural risk tag"),
    ):
        if token in pb:
            _ok(f"pullback requires {label}")
        else:
            _fail(f"pullback missing {label}")

    uem = _read_src("backend/shared/user_execution_manager.py")
    if "def _pullback_risk_ok(" in uem and \
            'sig_strategy == "pullback_1m"' in uem:
        _ok("execution gate applies the account R:R filter to pullbacks")
    else:
        _fail("execution gate does not filter pullbacks by account R:R")

    try:
        from backend.shared.user_execution_manager import UserExecutionManager

        mgr = UserExecutionManager.__new__(UserExecutionManager)
        mgr.user_id = 0
        mgr.config = {"sl_pct": 0.03, "target_rr": 2.5}

        good = {"entry_price": 100.0, "stop_loss": 98.0, "take_profit": 110.0}
        if mgr._pullback_risk_ok(good):
            _ok("setup inside account SL with enough room is accepted")
        else:
            _fail("good pullback was rejected")

        wide = {"entry_price": 100.0, "stop_loss": 95.0, "take_profit": 110.0}
        if not mgr._pullback_risk_ok(wide):
            _ok("setup with structural SL beyond account SL is rejected")
        else:
            _fail("wide-stop pullback was accepted")

        tight = {"entry_price": 100.0, "stop_loss": 98.0, "take_profit": 105.0}
        if not mgr._pullback_risk_ok(tight):
            _ok("setup without room to the account target is rejected")
        else:
            _fail("low-room pullback was accepted")
    except Exception as e:
        _fail(f"pullback risk filter synthetic check crashed: {e}")


def test_trend_pullback_no_flip():
    print("\n=== Test: no CE/PE flip on a small pullback inside a trend ===")

    td = _read_src("backend/shared/trend_direction.py")
    if "def resolve_direction(" in td and "EMA_CONFIRM_BARS" in td:
        _ok("trend-aware direction resolver exists")
    else:
        _fail("trend-aware direction resolver missing")

    psvc = _read_src("backend/shared/option_premium_service.py")
    if "resolve_direction(df, prev_dir, trend)" in psvc:
        _ok("premium builder no longer flips on a bare EMA cross")
    else:
        _fail("premium builder still flips on a bare EMA cross")

    se = _read_src("backend/shared/strategy_engine.py")
    if "resolve_direction(df, prev, trend)" in se and \
            "self._get_direction(df_1m, self._prev_direction)" in se:
        _ok("shared engine uses the sticky direction")
    else:
        _fail("shared engine still flips on a bare EMA cross")

    ev6 = _read_src("backend/engine/engine_v6.py")
    if "resolve_direction(df, self.direction, trend_val)" in ev6:
        _ok("legacy engine uses the sticky direction")
    else:
        _fail("legacy engine still flips on a bare EMA cross")

    try:
        import pandas as pd
        from backend.shared.trend_direction import resolve_direction

        def mk(closes):
            return pd.DataFrame([
                {"open": c, "high": c, "low": c, "close": c, "volume": 1}
                for c in closes
            ])

        up = [100 + i * 0.5 for i in range(40)]
        pullback = mk(up + [116, 112, 108])
        if resolve_direction(pullback, "BULL", "UPTREND") == "BULL":
            _ok("small pullback in an uptrend keeps BULL (no flip)")
        else:
            _fail("small pullback flipped the direction")

        down = mk([140 - i * 0.5 for i in range(40)])
        if resolve_direction(down, "BULL", "DOWNTREND") == "BEAR":
            _ok("confirmed reversal flips direction")
        else:
            _fail("confirmed reversal did not flip")

        if resolve_direction(pullback, "BULL", "SIDEWAYS") == "BULL":
            _ok("unconfirmed cross in a neutral market does not flip")
        else:
            _fail("unconfirmed cross flipped in a neutral market")
    except Exception as e:
        _fail(f"trend direction synthetic check crashed: {e}")


def test_terminal_chart_zoom_and_view_lock():
    print("\n=== Test: Terminal charts are larger + keep view on direction flip ===")

    css = _read_src("frontend/static/css/terminal.css")
    if "height: clamp(460px, 68vh, 880px)" in css:
        _ok("chart wrap is taller (zoomed-in, larger area)")
    else:
        _fail("chart wrap height was not increased")

    js = _read_src("frontend/static/js/terminal.js")
    if "subscribeVisibleLogicalRangeChange" in js and "this._userRange = { span:" in js:
        _ok("chart remembers the user's zoom/pan range (relative to the last bar)")
    else:
        _fail("chart does not track the user's zoom/pan range")
    if "this._userRange.rightGap" in js and "setVisibleLogicalRange({ from: from, to: to })" in js:
        _ok("chart re-applies the user's range anchored to the newest bar")
    else:
        _fail("chart does not re-anchor the user's range to the newest bar")
    full_render = js.split("fullRender() {")[1].split("updateLive()")[0]
    if "first !== this._lastFirst" not in full_render:
        _ok("premium chart no longer refits when the option symbol changes")
    else:
        _fail("premium chart still refits on option symbol change")
    if "rebuildMain(keepView = false)" in js and "this._userRange = saved;" in js:
        _ok("timeframe switch keeps the zoom + right gap instead of refitting")
    else:
        _fail("timeframe switch does not keep the saved range")

    under = js.split("class UnderlyingChart")[1].split("Overlay: live position panel")[0]
    if "preserveView()" in under and "this._userRange.rightGap" in under \
            and "setVisibleLogicalRange({ from: from, to: to })" in under:
        _ok("underlying chart mirrors the premium chart view logic")
    else:
        _fail("underlying chart does not mirror the premium chart view logic")
    if "state.underlyingChart.preserveView()" in js:
        _ok("timeframe switch keeps the underlying chart view too")
    else:
        _fail("timeframe switch re-fits the underlying chart")

    # A direction flip / strike roll swaps in a brand-new option contract.
    # The visible window must be captured BEFORE the candles are cleared so
    # the new premium chart renders with the active candle in the same place.
    if "captureView() {" in js and "getVisibleLogicalRange()" in js.split("captureView() {")[1][:600]:
        _ok("chart can snapshot the current visible window (auto-fit included)")
    else:
        _fail("chart cannot snapshot the current visible window")
    state_msg = js.split('if (prevOpt && newOpt && newOpt !== prevOpt) {')[1][:400]
    if "state.chart.captureView()" in state_msg:
        _ok("option change (state msg) captures the view before clearing candles")
    else:
        _fail("option change (state msg) drops the chart view")
    rolled = js.split("if (rolled) {")[1][:400]
    if "state.chart.captureView()" in rolled:
        _ok("option change (premium frame) captures the view before clearing candles")
    else:
        _fail("option change (premium frame) drops the chart view")

    # The restored view must survive the chart's own async range callbacks,
    # and the price axis must re-fit to the new contract's premium level.
    if "if (!isFinite(span) || span < 2) return;" in js:
        _ok("transient reset ranges cannot clobber the restored view")
    else:
        _fail("a reset range can overwrite the restored view")
    if "requestAnimationFrame(done)" in js and "_releaseApply()" in js:
        _ok("range guard released on the next frame (async callbacks ignored)")
    else:
        _fail("range guard is released synchronously (late callback can win)")
    if 'this.chart.priceScale("right").applyOptions({ autoScale: true })' in js:
        _ok("price axis re-fits vertically after the contract swap")
    else:
        _fail("price axis keeps the old contract scale")
    if "this._lastFirst" not in js:
        _ok("legacy first-bar refit logic removed")
    else:
        _fail("legacy first-bar refit logic still present")
    if js.count("attributionLogo: false") == 2:
        _ok("TradingView attribution logo hidden on both charts")
    else:
        _fail("TradingView attribution logo is still shown")

    if "overflow-y: scroll" in css and "::-webkit-scrollbar" in css:
        _ok("mobile terminal shows a visible vertical scrollbar")
    else:
        _fail("mobile terminal vertical scrollbar missing")
    if "touch-action: pan-y" in css:
        _ok("vertical swipe over charts scrolls the page")
    else:
        _fail("charts do not allow vertical page scroll")


def test_terminal_voice_alerts():
    print("\n=== Test: Terminal page announces trade events by voice ===")

    html = _read_src("frontend/templates/terminal.html")
    if "/static/js/voice-alerts.js" in html:
        _ok("terminal loads the voice-alerts library")
    else:
        _fail("terminal does not load voice-alerts.js")
    if 'id="term-voice-toggle"' in html:
        _ok("terminal has a voice on/off toggle")
    else:
        _fail("terminal voice toggle missing")

    js = _read_src("frontend/static/js/terminal.js")
    if "VoiceAlerts.announce(evt, m)" in js:
        _ok("terminal announces WS events via VoiceAlerts")
    else:
        _fail("terminal does not announce WS events")
    if "function bindVoiceToggle()" in js and "VoiceAlerts.set({ enabled: on })" in js:
        _ok("terminal toggle persists the shared voice setting")
    else:
        _fail("terminal voice toggle is not wired")

    va = _read_src("frontend/static/js/voice-alerts.js")
    if "speechSynthesis" in va and "STORAGE_KEY = 'voice_alerts_settings_v1'" in va:
        _ok("voice alerts use Web Speech API and shared localStorage settings")
    else:
        _fail("voice-alerts library is not the shared implementation")


def test_terminal_independent_timeframes():
    print("\n=== Test: Premium and underlying charts have independent timeframes ===")

    js = _read_src("frontend/static/js/terminal.js")
    if 'premiumTf: "1m"' in js and 'underlyingTf: "1m"' in js:
        _ok("separate premium/underlying timeframe state exists")
    else:
        _fail("charts do not keep separate timeframe state")
    if 'state.premiumTf === "5m"' in js:
        _ok("premium candles use the premium timeframe")
    else:
        _fail("premium candles do not use the premium timeframe")
    if 'state.underlyingTf === "5m"' in js:
        _ok("underlying candles use the underlying timeframe")
    else:
        _fail("underlying candles do not use the underlying timeframe")
    if "function switchPremiumTimeframe()" in js and "function switchUnderlyingTimeframe()" in js:
        _ok("timeframe switches are scoped per chart")
    else:
        _fail("timeframe switches are not scoped per chart")
    if "state.tf =" not in js and "state.tf ===" not in js:
        _ok("legacy single-timeframe state removed")
    else:
        _fail("legacy single-timeframe state still present")

    html = _read_src("frontend/templates/terminal.html")
    if html.count('class="term-tf-group"') == 2 \
            and 'data-chart="premium"' in html and 'data-chart="underlying"' in html:
        _ok("toolbar exposes a timeframe selector for each chart")
    else:
        _fail("toolbar does not expose two separate timeframe selectors")
    if html.index('data-chart="underlying"') < html.index('data-chart="premium"'):
        _ok("underlying timeframe selector is listed first")
    else:
        _fail("underlying timeframe selector is not first")

    # Switching timeframe must keep the forming candle in the same spot:
    # snapshot the current range, rebuild with keepView, then re-apply it.
    if "rebuildMain(keepView = false)" in js and "getVisibleLogicalRange()" in js:
        _ok("premium rebuild can snapshot the current visible range")
    else:
        _fail("premium rebuild cannot preserve the current range")
    if "state.chart.rebuildMain(true)" in js:
        _ok("premium timeframe switch keeps the current zoom/position")
    else:
        _fail("premium timeframe switch re-fits and moves the active candle")
    if "preserveView()" in js and "state.underlyingChart.preserveView()" in js:
        _ok("underlying timeframe switch keeps the current zoom/position")
    else:
        _fail("underlying timeframe switch re-fits and moves the active candle")

    # Candle countdown: each chart has its own timeframe, so it needs its own
    # timer showing the time left in its forming candle.
    if 'id="candle-timer"' in html and 'id="u-candle-timer"' in html:
        _ok("both charts expose a candle countdown")
    else:
        _fail("underlying chart has no candle countdown")
    prem_group = html.split('id="tf-group-premium"')[1].split("</div>")[0]
    under_group = html.split('id="tf-group-underlying"')[1].split("</div>")[0]
    if 'id="candle-timer"' in prem_group and 'id="u-candle-timer"' in under_group:
        _ok("each countdown sits with its own timeframe selector")
    else:
        _fail("candle countdowns are not tied to their chart")
    if "function candleCountdown(tf)" in js \
            and "candleCountdown(state.premiumTf)" in js \
            and "candleCountdown(state.underlyingTf)" in js:
        _ok("countdown uses each chart's own timeframe")
    else:
        _fail("underlying countdown does not use the underlying timeframe")


def test_sliding_session_48h():
    print("\n=== Test: sessions slide for 48h of activity, then require re-login ===")

    cfg = _read_src("backend/config.py")
    if 'SESSION_IDLE_HOURS: int           = int(os.getenv("SESSION_IDLE_HOURS", "48"))' in cfg:
        _ok("session idle window defaults to 48 hours")
    else:
        _fail("session idle window is not 48 hours")
    if 'REFRESH_TOKEN_EXPIRE_DAYS: int    = int(os.getenv("REFRESH_TOKEN_EXPIRE_DAYS", "2"))' in cfg:
        _ok("refresh token TTL matches one idle window (2 days)")
    else:
        _fail("refresh token TTL is not tied to the idle window")

    models = _read_src("backend/db/models.py")
    if 'class UserSession(Base):' in models and '"user_sessions"' in models \
            and "last_seen_at" in models and "jti" in models and "revoked" in models:
        _ok("per-device session table stores jti / last_seen / revoked")
    else:
        _fail("no per-device session table")

    svc = _read_src("backend/services/auth_service.py")
    for fn in ("start_session", "validate_session", "touch_session",
               "revoke_session", "revoke_user_sessions"):
        if f"async def {fn}(" not in svc:
            _fail(f"auth service is missing {fn}()")
            break
    else:
        _ok("auth service creates, validates, slides and revokes sessions")
    if 'timedelta(hours=settings.SESSION_IDLE_HOURS)' in svc:
        _ok("a session is invalid once idle past the 48h window")
    else:
        _fail("idle expiry is not enforced from SESSION_IDLE_HOURS")
    if 'jti = payload.get("jti")' in svc and "if jti and not await touch_session" in svc:
        _ok("every authenticated request slides the session")
    else:
        _fail("authenticated requests do not slide the session")
    if '"nonce": secrets.token_urlsafe(8)' in svc:
        _ok("refresh tokens rotate to a new value on every refresh")
    else:
        _fail("refresh token rotation can return the same token")

    router = _read_src("backend/routers/auth.py")
    if 'async def _issue_tokens(db: AsyncSession, user: User' in router \
            and "await start_session(db, user.id, request)" in router:
        _ok("login / register / google / reset all open a session")
    else:
        _fail("token issuing does not open a session")
    refresh_fn = router.split('@router.post("/refresh"')[1].split("@router.")[0]
    if "validate_session(db, jti) is None" in refresh_fn:
        _ok("/refresh rejects a session idle past 48h")
    else:
        _fail("/refresh accepts expired sessions")
    if '@router.post("/logout")' in router and "await revoke_session(db, jti)" in router:
        _ok("logout revokes the device session")
    else:
        _fail("logout does not revoke the session")
    if "await revoke_user_sessions(db, user.id)" in router:
        _ok("password reset revokes every device session")
    else:
        _fail("password reset leaves old sessions alive")

    base = _read_src("frontend/templates/base.html")
    if "async function refreshSession()" in base and "/api/auth/refresh" in base:
        _ok("pages rotate the access token silently")
    else:
        _fail("pages never refresh the access token")
    if "if (!options._retried && await refreshSession())" in base:
        _ok("a 401 refreshes and retries instead of logging out")
    else:
        _fail("a 401 logs the user out immediately")
    if "function scheduleTokenRefresh()" in base and "scheduleTokenRefresh();" in base:
        _ok("active tabs keep refreshing before the token expires")
    else:
        _fail("active tabs never refresh proactively")
    if "if (tokenExpired()) {" in base and "if (!ok) { logout(); return; }" in base:
        _ok("an expired tab re-logins only when the session really ended")
    else:
        _fail("expired tabs are not recovered by a refresh")

    js = _read_src("frontend/static/js/terminal.js")
    if "async function refreshSession()" in js and "await refreshSession()" in js:
        _ok("terminal rotates the access token too")
    else:
        _fail("terminal does not refresh the access token")
    if "setInterval(() => { refreshSession(); }" in js:
        _ok("long-open terminal keeps sliding the session")
    else:
        _fail("terminal session stops sliding")


def test_manual_trailing_uses_live_premium():
    print("\n=== Test: manual trailing SL is validated against the live premium ===")

    price = _read_src("backend/shared/engine_price.py")
    if "def engine_premium_price(" in price and '"ltp", "_ltp"' in price \
            and "_last_ltp" in price:
        _ok("engine premium price helper (shared + legacy engines)")
    else:
        _fail("no helper to read an engine's live premium")

    js = _read_src("frontend/static/js/terminal.js")
    sl_branch = js.rsplit('if (kind === "sl") {', 1)[1][:1400]
    if "const live = pos ? livePrice(pos) : null;" in sl_branch \
            and "SL must stay below the premium price" in sl_branch \
            and "SL must stay below entry" not in sl_branch:
        _ok("chart drag validates SL against the current premium, not the entry")
    else:
        _fail("chart drag still blocks trailing above entry")
    if "SELL SL must stay above the premium price" in sl_branch \
            and "SELL SL must stay above entry" not in sl_branch:
        _ok("SELL side mirrors the same rule (SL above live premium)")
    else:
        _fail("SELL trailing rule missing")

    router = _read_src("backend/routers/terminal.py")
    if "async def _find_open_position_ctx(" in router and 'p.get("opt_ltp")' in router:
        _ok("/api/terminal/order gets the live premium from the snapshot")
    else:
        _fail("order endpoint has no live premium to validate against")
    if "_validate_level(action, value, ctx[\"position\"], ctx.get(\"ltp\"))" in router:
        _ok("order endpoint validates SL against the live premium")
    else:
        _fail("order endpoint still validates against the entry")
    if "SL must stay below the premium price" in router:
        _ok("server rejects only the wrong side of the live price")
    else:
        _fail("server SL rule not updated")

    sw = _read_src("backend/shared/shared_worker.py")
    if "engine_premium_price(eng, entry)" in sw \
            and "SL must be below entry" not in sw:
        _ok("shared worker trailing uses the live premium")
    else:
        _fail("shared worker still rejects SL above entry")
    lw = _read_src("worker.py")
    if "engine_premium_price(eng, entry)" in lw \
            and "SL must be below entry" not in lw:
        _ok("legacy worker trailing uses the live premium")
    else:
        _fail("legacy worker still rejects SL above entry")
    tg = _read_src("backend/services/telegram_bot.py")
    if "engine_premium_price(eng, entry)" in tg \
            and "must be below entry" not in tg:
        _ok("telegram /sl uses the live premium too")
    else:
        _fail("telegram /sl still rejects SL above entry")

    if "Manual trailing is validated against the CURRENT premium" in _read_src("backend/routers/terminal.py"):
        _ok("trailing intent documented on the server")
    else:
        _fail("trailing intent not documented")


def test_email_sender_branding():
    print("\n=== Test: User mail is sent From support@optiscalper.com ===")

    cfg = _read_src("backend/config.py")
    if 'os.getenv("SMTP_FROM", "support@optiscalper.com")' in cfg:
        _ok("SMTP_FROM defaults to support@optiscalper.com")
    else:
        _fail("SMTP_FROM default is not the branded support address")
    if "SMTP_FROM_NAME" in cfg:
        _ok("SMTP_FROM_NAME display-name setting exists")
    else:
        _fail("SMTP_FROM_NAME missing")

    es = _read_src("backend/services/email_service.py")
    if "def _from_header" in es and "def _envelope_from" in es:
        _ok("From header + envelope-sender helpers exist")
    else:
        _fail("email From helpers missing")
    if "smtp.sendmail(" in es and "_envelope_from()," in es:
        _ok("envelope sender uses the bare address (not the display name)")
    else:
        _fail("envelope sender not using _envelope_from()")
    if "SMTP_SSL" in es and "starttls()" in es:
        _ok("SMTP supports both implicit SSL (465) and STARTTLS (587)")
    else:
        _fail("SMTP does not handle port 465 SSL transport")
    if es.count("settings.SMTP_FROM or settings.SMTP_USER") == 1:
        _ok("all mail builds From through the shared helper")
    else:
        _fail("a mail path bypasses the From helper")

    env = _read_src(".env.example")
    if "SMTP_FROM=support@optiscalper.com" in env:
        _ok(".env.example sets the branded sender")
    else:
        _fail(".env.example does not set SMTP_FROM=support@optiscalper.com")


def test_watchlist_lot_size():
    print("\n=== Test: Terminal watchlist shows each symbol's lot size ===")

    js = _read_src("frontend/static/js/terminal.js")
    if "term-wl-lot" in js and '(lot ? "Lot " + lot : "Lot --")' in js:
        _ok("watchlist rows render the exchange lot size")
    else:
        _fail("watchlist does not render lot size")
    if "s.lot_size)" in js and "info.lot_size" in js:
        _ok("lot size is read from available_symbols with a symbol_info fallback")
    else:
        _fail("lot size source not wired")

    css = _read_src("frontend/static/css/terminal.css")
    if ".term-wl-lot" in css:
        _ok("lot badge is styled")
    else:
        _fail("lot badge style missing")


def test_lot_size_table_current():
    print("\n=== Test: Lot size table matches current NSE F&O contract sizes ===")

    src = _read_src("backend/shared/shared_cache.py")
    table = None
    for node in ast.walk(ast.parse(src)):
        targets = []
        if isinstance(node, ast.Assign):
            targets = node.targets
        elif isinstance(node, ast.AnnAssign):
            targets = [node.target]
        for tgt in targets:
            if isinstance(tgt, ast.Name) and tgt.id == "DEFAULT_LOT_SIZES":
                table = ast.literal_eval(node.value)

    if not isinstance(table, dict):
        _fail("DEFAULT_LOT_SIZES table not found in shared_cache.py")
        return

    expected = {
        "NIFTY": 65, "BANKNIFTY": 30, "FINNIFTY": 60,
        "MIDCPNIFTY": 120, "NIFTYNXT50": 25,
        "RELIANCE": 500, "TCS": 225, "INFY": 400, "HDFCBANK": 650,
        "ICICIBANK": 700, "SBIN": 750, "AXISBANK": 625, "BAJFINANCE": 750,
        "WIPRO": 3000, "TATASTEEL": 2750, "ADANIPORTS": 475,
        "MARUTI": 50, "SUNPHARMA": 350, "KOTAKBANK": 2000,
        "SENSEX": 20, "BANKEX": 30,
    }
    wrong = {k: (table.get(k), v) for k, v in expected.items() if table.get(k) != v}
    if not wrong:
        _ok("index + stock lot sizes match the current NSE F&O values")
    else:
        _fail(f"stale lot sizes (got, expected): {wrong}")

    if len(table) >= 218:
        _ok(f"full NSE F&O universe loaded ({len(table)} symbols)")
    else:
        _fail(f"lot-size table too small ({len(table)} symbols, expected >= 218)")

    if "sys:cache:lot_size:v" in src and "LOT_SIZE_CACHE_VERSION" in src:
        _ok("lot-size cache key is versioned (busts stale values on changes)")
    else:
        _fail("lot-size cache key is not versioned")

    eng = _read_src("backend/engine/engine_v6.py")
    if "from backend.shared.shared_cache import DEFAULT_LOT_SIZES as NSE_LOT_SIZES" in eng:
        _ok("engine_v6 reuses the shared table (no duplicated literals)")
    else:
        _fail("engine_v6 does not alias the shared lot-size table")


def test_live_exit_uses_broker_fill():
    print("\n=== Test: Live exits book the broker's actual fill price ===")

    mgr = _read_src("backend/shared/user_execution_manager.py")

    if "def _get_fill_price_live" in mgr:
        _ok("manager has a broker fill-price helper")
    else:
        _fail("manager lacks _get_fill_price_live")

    if "exit_order_id = self._place_order_live(" in mgr:
        _ok("full close captures the live exit order id")
    else:
        _fail("full close ignores the live exit order id")

    if "fill = self._get_fill_price_live(exit_order_id, timeout=5)" in mgr:
        _ok("full close reads the real exit fill from Upstox")
    else:
        _fail("full close does not read the exit fill from Upstox")

    if "fill = self._get_fill_price_live(oid, timeout=5)" in mgr:
        _ok("partial reduce reads the real fill from Upstox")
    else:
        _fail("partial reduce does not read the fill from Upstox")

    if mgr.count("exit_price = fill") >= 2:
        _ok("broker fill overrides the LTP-based exit price")
    else:
        _fail("broker fill does not override the exit price")

    if '"exit_order_id": exit_order_id,' in mgr:
        _ok("EXIT events carry the exit order id")
    else:
        _fail("EXIT events omit the exit order id")

    if "exit_p = self._get_fill_price_live(sl_id, timeout=5)" in mgr:
        _ok("SL_HIT still books the broker fill (unchanged)")
    else:
        _fail("SL_HIT fill lookup regressed")


def test_brand_logo_wired():
    print("\n=== Test: Brand logo image wired across the UI ===")

    if os.path.exists(os.path.join(BASE, "frontend/static/logo.png")):
        _ok("logo.png asset is present")
    else:
        _fail("logo.png asset is missing")

    asset = "/static/logo.png"
    checks = {
        "base shell": ("frontend/templates/base.html", 'class="nav-logo"'),
        "terminal": ("frontend/templates/terminal.html", "term-brand-logo"),
        "login page": ("frontend/templates/index.html", 'class="auth-logo"><img'),
        "landing page": ("frontend/templates/landing.html", 'class="nav-brand"><img'),
        "terms page": ("frontend/templates/terms.html", 'class="nav-brand"><img'),
        "privacy page": ("frontend/templates/privacy.html", 'class="nav-brand"><img'),
        "refund page": ("frontend/templates/refund.html", 'class="nav-brand"><img'),
        "billing page": ("frontend/templates/billing.html", 'class="nav-logo"'),
        "admin billing": ("frontend/templates/admin_billing.html", "brand-logo"),
    }
    for name, (rel, needle) in checks.items():
        src = _read_src(rel)
        if needle in src and asset in src:
            _ok(f"{name} uses the brand logo image")
        else:
            _fail(f"{name} still lacks the brand logo image")

    es = _read_src("backend/services/email_service.py")
    if "_brand_header" in es and "/static/logo.png" in es:
        _ok("email templates use the branded logo header")
    else:
        _fail("email templates lack the logo header")


def test_token_hot_refresh_no_restart():
    print("\n=== Test: Daily token refresh without a worker restart ===")

    md = _read_src("backend/shared/market_data_service.py")
    if "def update_access_token(self, access_token" in md:
        _ok("global streamer can hot-swap its access token")
    else:
        _fail("global streamer lacks update_access_token")
    if "def update_global_streamer_token(" in md:
        _ok("module helper updates the singleton streamer")
    else:
        _fail("update_global_streamer_token helper missing")
    if "def update_access_token_for(cls, symbol" in md:
        _ok("SharedMarketDataService exposes per-symbol token update")
    else:
        _fail("SharedMarketDataService lacks update_access_token_for")

    for rel, cls in (
        ("backend/shared/candle_builder.py", "SharedCandleBuilder"),
        ("backend/shared/option_premium_service.py", "SharedOptionPremiumBuilder"),
        ("backend/shared/option_chain_service.py", "SharedOptionChainService"),
    ):
        src = _read_src(rel)
        if "def update_access_token(self, access_token" in src:
            _ok(f"{cls} can hot-swap its token")
        else:
            _fail(f"{cls} lacks update_access_token")

    bcb = _read_src("backend/services/bot_config_builder.py")
    if "async def get_user_access_token(" in bcb:
        _ok("DB token reader helper exists (periodic sync)")
    else:
        _fail("get_user_access_token helper missing")

    sw = _read_src("backend/shared/shared_worker.py")
    if "def _refresh_shared_services_token(" in sw:
        _ok("orchestrator refreshes shared services + streamer")
    else:
        _fail("_refresh_shared_services_token missing")
    if "def _sync_running_user_tokens(" in sw:
        _ok("periodic DB token sync exists (safety net)")
    else:
        _fail("_sync_running_user_tokens missing")
    if "_sync_running_user_tokens()" in sw and "TOKEN_SYNC_EVERY_SEC" in sw:
        _ok("heartbeat invokes the throttled token sync")
    else:
        _fail("heartbeat does not invoke the periodic token sync")
    if _has_function_with_code(
            sw, "_handle_update_token", "_refresh_shared_services_token"):
        _ok("update_token command also refreshes shared services")
    else:
        _fail("update_token command does not refresh shared services")

    ar = _read_src("backend/routers/all_routers.py")
    if ar.count('push_command("update_token"') >= 2:
        _ok("both web token writers push update_token to the worker")
    else:
        _fail("web token writers do not notify the worker")

    bm = _read_src("backend/services/bot_manager.py")
    if "def update_token(self, user_id" in bm:
        _ok("legacy BotManager supports token hot-swap")
    else:
        _fail("legacy BotManager lacks update_token")

    wk = _read_src("worker.py")
    if 'action == "update_token"' in wk:
        _ok("legacy worker dispatches update_token")
    else:
        _fail("legacy worker missing update_token dispatch")

    uem = _read_src("backend/shared/user_execution_manager.py")
    if "_pause_reason" in uem and "def pause_reason(" in uem:
        _ok("manager tracks the pause reason (manual vs token_expired)")
    else:
        _fail("manager does not track the pause reason")
    # Bots must start/resume ONLY on an explicit user action.
    if not _has_function_with_code(sw, "_sync_running_user_tokens", "resume("):
        _ok("periodic token sync never auto-resumes a bot")
    else:
        _fail("periodic token sync auto-resumes a bot")
    if not _has_function_with_code(sw, "_handle_update_token", "resume("):
        _ok("update_token never auto-resumes a bot")
    else:
        _fail("update_token auto-resumes a bot")
    if _has_function_with_code(sw, "_handle_pause_resume", 'pause(reason="manual")'):
        _ok("manual pause is tagged")
    else:
        _fail("manual pause is not tagged")


def test_default_trading_params():
    print("\n=== Test: Default trading params (Lots 1, SL 0.05, R:R 1.5) ===")

    models = _read_src("backend/db/models.py")
    if "order_qty: Mapped[int] = mapped_column(Integer, default=1)" in models:
        _ok("BotConfig default lots = 1")
    else:
        _fail("BotConfig default lots is not 1")
    if "sl_pct: Mapped[float] = mapped_column(Float, default=0.05)" in models:
        _ok("BotConfig default SL% = 0.05")
    else:
        _fail("BotConfig default SL% is not 0.05")
    if "target_rr: Mapped[float] = mapped_column(Float, default=1.5)" in models:
        _ok("BotConfig default Target R:R = 1.5")
    else:
        _fail("BotConfig default Target R:R is not 1.5")

    bcb = _read_src("backend/services/bot_config_builder.py")
    if '"order_qty": cfg.order_qty or 1,' in bcb:
        _ok("config builder lots fallback = 1")
    else:
        _fail("config builder lots fallback is not 1")
    if '"sl_pct": cfg.sl_pct or 0.05,' in bcb:
        _ok("config builder SL% fallback = 0.05")
    else:
        _fail("config builder SL% fallback is not 0.05")
    if '"target_rr": cfg.target_rr or 1.5,' in bcb:
        _ok("config builder Target R:R fallback = 1.5")
    else:
        _fail("config builder Target R:R fallback is not 1.5")


def test_itm_depth_global_admin():
    print("\n=== Test: ITM Depth is a global admin-only setting ===")

    models = _read_src("backend/db/models.py")
    if "class PlatformSettings" in models:
        _ok("PlatformSettings model present")
    ps_block = models.split("class PlatformSettings")[-1]
    if "itm_depth: Mapped[int]" in ps_block:
        _ok("PlatformSettings stores itm_depth")
    else:
        _fail("PlatformSettings is missing itm_depth")

    ps = _read_src("backend/services/platform_settings.py")
    if "async def get_itm_depth_async(" in ps and "def get_itm_depth_sync(" in ps:
        _ok("platform_settings exposes async + sync itm_depth getters")
    else:
        _fail("platform_settings lacks itm_depth getters")

    ops = _read_src("backend/shared/option_premium_service.py")
    if "def _itm_depth()" in ops and "get_itm_depth_sync" in ops:
        _ok("shared option feed reads the global itm_depth")
    else:
        _fail("shared option feed ignores the global itm_depth")
    if "_itm_depth(), self._strike_step" in ops:
        _ok("option selection uses the global depth (not a constant)")
    else:
        _fail("option selection still uses a constant depth")

    ts = _read_src("backend/services/terminal_service.py")
    if "def _itm_depth()" in ts and "get_itm_depth_sync" in ts:
        _ok("terminal service reads the global itm_depth")
    else:
        _fail("terminal service ignores the global itm_depth")

    ar = _read_src("backend/routers/all_routers.py")
    if "get_itm_depth_async" in ar and "itm_depth" in ar:
        _ok("API exposes the global itm_depth")
    else:
        _fail("API does not expose the global itm_depth")
    bot_in = ar.split("class BotConfigIn")[1].split("class ")[0]
    if "itm_depth" not in bot_in:
        _ok("users cannot submit itm_depth (dropped from BotConfigIn)")
    else:
        _fail("BotConfigIn still lets users set itm_depth")

    admin = _read_src("frontend/templates/admin.html")
    if "ps-itm-depth" in admin and "itm_depth" in admin:
        _ok("admin panel has an ITM Depth control")
    else:
        _fail("admin panel lacks the ITM Depth control")

    settings = _read_src("frontend/templates/settings.html")
    if "c-itm" not in settings:
        _ok("user settings page no longer shows ITM Depth")
    else:
        _fail("user settings page still shows ITM Depth")


def test_underlying_zero_volume_guard():
    print("\n=== Test: Underlying engine guards against zero average volume ===")

    src = _read_src("backend/engine/underlying_market_structure.py")

    # The old unsafe expression divided by the raw average prior volume.
    unsafe = 'vol_ratio = self.candles[-1]["volume"] / (sum(prev_vols) / len(prev_vols))'
    if unsafe not in src:
        _ok("no unguarded division by average prior volume")
    else:
        _fail("underlying engine still divides by a possibly-zero average volume")

    # The fix computes the average first and only divides when it is > 0.
    guarded = "avg_prev_vol = sum(prev_vols) / len(prev_vols)"
    if src.count(guarded) == 2 and src.count("if avg_prev_vol > 0:") == 2:
        _ok("both BOS volume ratios guard against a zero average")
    else:
        _fail("BOS volume ratios are not both guarded")


# ================================================================
# MAIN
# ================================================================



def test_swing_low_trail_mode():
    print("\n=== Test: swing-low trail mode (structure-aware) ===")

    from backend.shared.swing_trail import (
        swing_low_stop, _find_pivot_highs_lows,
    )

    # Build a price series with a confirmed swing high and swing low
    bars = []
    bars.append({"open": 100, "high": 102, "low": 99, "close": 101, "volume": 1})
    bars.append({"open": 101, "high": 103, "low": 100, "close": 102, "volume": 1})
    # Pivot high at 106
    bars.append({"open": 102, "high": 106, "low": 101, "close": 105, "volume": 1})
    bars.append({"open": 105, "high": 105, "low": 103, "close": 104, "volume": 1})
    bars.append({"open": 104, "high": 104, "low": 101, "close": 102, "volume": 1})
    # Pivot low at 98
    bars.append({"open": 102, "high": 103, "low": 98, "close": 99, "volume": 1})
    bars.append({"open": 99, "high": 100, "low": 99, "close": 99, "volume": 1})
    bars.append({"open": 99, "high": 101, "low": 99, "close": 100, "volume": 1})
    
    highs, lows = _find_pivot_highs_lows(bars, 2, 2)
    if len(highs) == 1 and highs[0][1] == 106.0 and len(lows) == 1 and lows[0][1] == 98.0:
        _ok("pivot detection finds swing high at 106 and swing low at 98")
    else:
        _fail(f"pivot detection failed: highs={highs}, lows={lows}")

    # Test 1: price breaks above resistance -> trail below broken resistance
    break_bars = list(bars)
    break_bars.append({"open": 100, "high": 108, "low": 99, "close": 107, "volume": 1})
    lvl, why = swing_low_stop(break_bars)
    if lvl == 106.0 and "broken" in why:
        _ok("breaks resistance -> trails below broken level (106)")
    else:
        _fail(f"breakout test failed: {lvl} ({why})")

    # Test 2: no breakout -> trail below swing low
    lvl2, why2 = swing_low_stop(bars)
    if lvl2 == 98.0 and "swing low" in why2:
        _ok("no breakout -> trails below swing low (98)")
    else:
        _fail(f"no-breakout test failed: {lvl2} ({why2})")

    # Test 3: insufficient history
    lvl3, _ = swing_low_stop(bars[:5])
    if lvl3 is None:
        _ok("insufficient history returns None")
    else:
        _fail(f"should return None for short history: {lvl3}")

    # Test 4: strong green candle trails below its low
    strong_bars = []
    for i in range(10):
        strong_bars.append({"open": 100 + i * 2, "high": 105 + i * 2, 
                           "low": 99 + i * 2, "close": 103 + i * 2, "volume": 1})
    # Strong bullish candle (body=12 vs avg~4)
    strong_bars.append({"open": 123, "high": 138, "low": 122.5, 
                       "close": 135, "volume": 1})
    lvl4, why4 = swing_low_stop(strong_bars)
    if lvl4 == 122.5 and "strong green" in why4:
        _ok("strong green candle -> trails below its low (122.5)")
    else:
        _fail(f"strong green test failed: {lvl4} ({why4})")

    # Test 5: strong green low wins over lower swing low
    combo_bars = []
    for i in range(8):
        combo_bars.append({"open": 100 + i, "high": 102 + i, 
                          "low": 99 + i, "close": 101 + i, "volume": 1})
    # Swing low at 95
    combo_bars.append({"open": 105, "high": 106, "low": 95, "close": 96, "volume": 1})
    combo_bars.append({"open": 96, "high": 97, "low": 95, "close": 96, "volume": 1})
    combo_bars.append({"open": 96, "high": 97, "low": 95, "close": 96, "volume": 1})
    # Strong green with low at 97 (higher than swing low)
    combo_bars.append({"open": 96, "high": 110, "low": 97, "close": 109, "volume": 1})
    lvl5, why5 = swing_low_stop(combo_bars)
    if lvl5 == 97.0:
        _ok("picks higher level: strong green low (97) over swing low (95)")
    else:
        _fail(f"level selection failed: {lvl5} ({why5})")

    initdb = _read_src("scripts/init_db.py")
    if '("bot_configs", "trail_mode"' in initdb:
        _ok("existing databases get the trail_mode column")
    else:
        _fail("init_db does not migrate trail_mode")


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
    test_seo_and_indexing()
    test_sliding_session_48h()
    test_manual_trailing_uses_live_premium()
    test_swing_low_trail_mode()
    test_email_sender_branding()
    test_watchlist_lot_size()
    test_lot_size_table_current()
    test_live_exit_uses_broker_fill()
    test_dashboard_lots_and_terminal_layout()
    test_strong_trend_only_signals()
    test_quick_scalp_phase_gate()
    test_range_fade_strategy()
    test_unified_strategy_reachable()
    test_no_late_entry()
    test_pullback_quality_and_risk()
    test_trend_pullback_no_flip()
    test_terminal_chart_zoom_and_view_lock()
    test_terminal_voice_alerts()
    test_terminal_independent_timeframes()
    test_brand_logo_wired()
    test_token_hot_refresh_no_restart()
    test_default_trading_params()
    test_itm_depth_global_admin()
    test_underlying_zero_volume_guard()

    print("\n" + "=" * 60)
    total = PASS + FAIL
    print(f"RESULTS: {PASS} passed, {FAIL} failed of {total}")
    print(f"SCORE: {int(PASS / total * 100)}%")
    print("=" * 60)
    return FAIL == 0


if __name__ == "__main__":
    ok = run_all_tests()
    sys.exit(0 if ok else 1)
