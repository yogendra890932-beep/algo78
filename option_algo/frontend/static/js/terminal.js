/* ════════════════════════════════════════════════════════════════
   Advanced Trading Terminal — client application
   Pure presentation layer: reads live data over /ws/terminal and
   issues orders through /api/terminal/order (same Redis command
   queue the existing platform uses). No engine logic here.
   ════════════════════════════════════════════════════════════════ */
(function () {
"use strict";

/* ─────────────── Auth helpers (mirrors base.html) ─────────────── */
function getToken() { return localStorage.getItem("access_token"); }
function getUserId() {
  const cached = localStorage.getItem("user_id");
  if (cached) return cached;
  const token = getToken();
  if (!token) return null;
  try {
    const p = JSON.parse(atob(token.split(".")[1].replace(/-/g, "+").replace(/_/g, "/")));
    return p.sub || null;
  } catch (e) { return null; }
}
async function apiFetch(url, options) {
  options = options || {};
  const token = getToken();
  options.headers = Object.assign({
    "Content-Type": "application/json"
  }, (token ? { "Authorization": "Bearer " + token } : {}), (options.headers || {}));
  try {
    const resp = await fetch(url, options);
    if (resp.status === 401) { logout(); return null; }
    return resp;
  } catch (e) {
    console.error("API error:", e);
    return null;
  }
}
function logout() { localStorage.clear(); window.location.href = "/login"; }

/* ─────────────── Global state ─────────────── */
const state = {
  bootstrap: null,
  selectedSymbol: null,
  tf: "1m",
  series: "premium",
  overlays: { ema: true, vwap: false, bb: false, vol: true },
  candles: [],
  candles5m: [],
  ema9: [], ema15: [], ema21: [], vwap: [], bbU: [], bbM: [], bbL: [],
  current: null,
  premiumState: null,
  underlyingLTP: null,
  positions: [],
  tickLTP: {},
  signals: [],
  events: [],
  ws: null,
  wsHealthy: false,
  orderLine: null,      // drag state {kind:'sl'|'target'}
  orderLineValue: null,
  dragEl: null,
  chart: null,          // chart engine instance
  pnlThrottle: 0,
};

const $ = (id) => document.getElementById(id);
const num = (v) => { const n = Number(v); return isFinite(n) ? n : null; };
const fmt = (v, d) => (v == null ? "--" : Number(v).toFixed(d == null ? 2 : d));
const fmtINR = (v) => (v == null ? "₹ 0.00" : "₹ " + Number(v).toLocaleString("en-IN", { minimumFractionDigits: 2, maximumFractionDigits: 2 }));
const clsChg = (v) => (v > 0 ? "up" : (v < 0 ? "down" : ""));
const sign = (v) => (v > 0 ? "+" : "");

/* ─────────────── Bootstrap ─────────────── */
async function loadBootstrap() {
  const r = await apiFetch("/api/terminal/bootstrap");
  if (!r) return;
  const data = await r.json();
  state.bootstrap = data;
  renderUser(data.user);
  renderWatchlist(data.symbols, data.available_symbols);
  renderBotStatus(data.bot_running, data.bot_status);
  renderPositions(data.positions || []);
  renderTradeStatus();

  const first = Object.keys(data.symbols || {})[0] || "NIFTY";
  await selectSymbol(first);

  loadTrades();
  connectWS();
}

/* ─────────────── Watchlist & symbol selection ─────────────── */
function renderWatchlist(symbols, available) {
  const el = $("watchlist");
  const count = $("watchlist-count");
  const list = document.createElement("div");
  const syms = (available && available.length ? available : Object.keys(symbols || {}));
  count.textContent = syms.length;
  syms.forEach((s) => {
    const key = typeof s === "string" ? s : (s.symbol || "");
    const lot = typeof s === "string" ? null : s.lot_size;
    const info = (symbols || {})[key];
    const row = document.createElement("div");
    row.className = "term-wl-item";
    row.dataset.symbol = key;
    const price = info && info.underlying ? info.underlying.ltp : null;
    const chg = info && info.underlying ? info.underlying.change_pct : null;
    const active = info && info.active_option ? info.active_option : "";
    row.innerHTML =
      '<span class="term-wl-sym">' + key + "</span>" +
      '<span class="term-wl-price">' + fmt(price) + "</span>" +
      '<span class="term-wl-chg ' + clsChg(chg) + '">' + (chg == null ? "--" : sign(chg) + chg.toFixed(2) + "%") + "</span>" +
      (active ? '<span class="term-wl-active-opt">' + active + "</span>" : "");
    row.addEventListener("click", () => { selectSymbol(key); });
    list.appendChild(row);
  });
  el.innerHTML = "";
  el.appendChild(list);
  renderWatchlistActive();
}

function renderWatchlistActive() {
  document.querySelectorAll(".term-wl-item").forEach((r) => {
    r.classList.toggle("active", r.dataset.symbol === state.selectedSymbol);
  });
}

async function selectSymbol(sym) {
  if (!sym) return;
  state.selectedSymbol = sym;
  renderWatchlistActive();
  const info = (state.bootstrap && state.bootstrap.symbols || {})[sym];
  renderSelectedSymbol(info);
  renderAtm(info);

  if (state.ws && state.ws.readyState === WebSocket.OPEN) {
    state.ws.send(JSON.stringify({ action: "subscribe", symbol: sym }));
  } else {
    fetchSnapshot(sym);
  }
}

async function fetchSnapshot(sym) {
  const r = await apiFetch("/api/terminal/snapshot?symbol=" + encodeURIComponent(sym));
  if (!r) return;
  const data = await r.json();
  handleSnapshot(data);
}

/* ─────────────── Snapshot handling ─────────────── */
function aggregate5m(candles) {
  const out = [];
  const buckets = {};
  candles.forEach((b) => {
    const t = String(b.time != null ? b.time : b.minute);
    const m = t.match(/(\d{2}):(\d{2})/);
    if (!m) return;
    const mins = (+m[1]) * 60 + (+m[2]);
    const bucketEnd = Math.floor(mins / 5) * 5 + 5;
    const key = String(Math.floor(bucketEnd / 60)).padStart(2, "0") + ":" + String(bucketEnd % 60).padStart(2, "0");
    const prev = buckets[key];
    if (prev) {
      prev.high = Math.max(prev.high, b.high);
      prev.low = Math.min(prev.low, b.low);
      prev.close = b.close;
      prev.volume = (prev.volume || 0) + (b.volume || 0);
    } else {
      buckets[key] = { time: key, open: b.open, high: b.high, low: b.low, close: b.close, volume: b.volume || 0 };
    }
  });
  Object.keys(buckets).sort().forEach((k) => out.push(buckets[k]));
  return out;
}

function seriesCandles(snap) {
  if (state.series === "premium") {
    const pc = snap.premium_candles || [];
    if (pc.length) return state.tf === "5m" ? aggregate5m(pc) : pc;
  }
  return state.tf === "5m" ? (snap.candles_5m || []) : (snap.candles || []);
}

function handleSnapshot(m) {
  if (m.symbol !== state.selectedSymbol) return;
  state.candles = seriesCandles(m);
  state.premiumState = m.premium_state || null;
  state.current = m.premium_current || null;
  state.underlyingLTP = m.underlying ? m.underlying.ltp : null;
  prepareOverlays(state.candles);

  const info = (state.bootstrap && state.bootstrap.symbols || {})[m.symbol];
  renderSelectedSymbol(Object.assign({}, info, { underlying: m.underlying, premium_state: m.premium_state }));
  renderAtm(Object.assign({}, info, { underlying: m.underlying, itm: m.itm, premium_state: m.premium_state }));
  renderActiveOption(m.premium_state);
  renderStrategyStatus(m);
  renderPositions(m.positions || state.positions);
  drawChart();
}

/* ─────────────── WS ─────────────── */
function connectWS() {
  const uid = getUserId();
  const token = getToken();
  if (!uid || !token) { redirectLogin(); return; }
  if (tokenExpired()) { redirectLogin(); return; }
  const proto = location.protocol === "https:" ? "wss://" : "ws://";
  const ws = new WebSocket(proto + location.host + "/terminal/ws?user_id=" + encodeURIComponent(uid) + "&token=" + encodeURIComponent(token));
  state.ws = ws;
  ws.onopen = () => {
    state.wsHealthy = true;
    $("term-conn-state").textContent = "Feed: LIVE";
    $("term-conn-state").className = "term-pill term-pill-live";
    if (state.selectedSymbol) {
      ws.send(JSON.stringify({ action: "subscribe", symbol: state.selectedSymbol }));
    }
  };
  ws.onmessage = (ev) => { try { handleWSMessage(JSON.parse(ev.data)); } catch (e) { } };
  ws.onclose = (ev) => {
    state.wsHealthy = false;
    if (ev && ev.code === 4001) {
      $("term-conn-state").textContent = "Feed: UNAUTHORIZED";
      $("term-conn-state").className = "term-pill term-pill-closed";
      redirectLogin();
      return;
    }
    $("term-conn-state").textContent = "Feed: RECONNECTING";
    $("term-conn-state").className = "term-pill term-pill-idle";
    setTimeout(connectWS, 3000);
  };
  ws.onerror = () => { try { ws.close(); } catch (e) { } };
}

function wsSend(obj) {
  if (state.ws && state.ws.readyState === WebSocket.OPEN) {
    try { state.ws.send(JSON.stringify(obj)); } catch (e) { }
  }
}

function handleWSMessage(m) {
  if (!m || m._ping) return;
  switch (m.type) {
    case "snapshot":
      handleSnapshot(m);
      break;
    case "state":
      if (m.symbol === state.selectedSymbol) {
        state.premiumState = m.state || null;
        renderActiveOption(state.premiumState);
        const info = (state.bootstrap && state.bootstrap.symbols || {})[m.symbol] || {};
        renderAtm(Object.assign({}, info, { premium_state: state.premiumState }));
      }
      break;
    case "premium_bar":
      if (m.symbol === state.selectedSymbol && state.series === "premium") {
        pushBar(m.candle);
      }
      break;
    case "current":
      if (m.symbol === state.selectedSymbol) {
        state.current = m.candle || null;
        if (state.series === "premium") updateLastBar(m.candle);
        renderPnlThrottled();
      }
      break;
    case "tick":
      handleTick(m);
      break;
    case "positions":
      state.positions = m.positions || [];
      renderPositions(state.positions);
      renderTradeStatus();
      renderPnl();
      break;
    case "event":
      handleEvent(m);
      break;
    case "pong":
      break;
  }
}

function handleTick(m) {
  if (m.ltp != null) state.tickLTP[String(m.token)] = m.ltp;
  if (m.symbol !== state.selectedSymbol) return;
  const symInfo = (state.bootstrap && state.bootstrap.symbols || {})[state.selectedSymbol];
  const underTok = symInfo && symInfo.underlying ? String(symInfo.underlying.token || "") : "";
  if (underTok && underTok === String(m.token)) {
    state.underlyingLTP = m.ltp;
    const info = symInfo || {};
    renderSelectedSymbol(Object.assign({}, info, { underlying: Object.assign({}, info.underlying, { ltp: m.ltp }) }));
    if (state.series === "underlying") updateLastUnderlying(m.ltp);
  }
  renderPnlThrottled();
}

function updateLastUnderlying(ltp) {
  const candles = state.candles;
  if (!candles.length) return;
  const last = candles[candles.length - 1];
  if (ltp != null) {
    last.close = ltp;
    last.high = Math.max(last.high || 0, ltp);
    last.low = Math.min(last.low == null ? ltp : last.low, ltp);
    prepareOverlays(candles);
    state.chart.setData();
  }
}

/* ─────────────── Events / Signals / Logs ─────────────── */
function handleEvent(m) {
  const evt = String(m.event || "").toUpperCase();
  const msg = m.message || m.msg || m.reason || "";
  const symbol = m.symbol || m.trading_symbol || state.selectedSymbol || "";
  const ts = m.ts || m.timestamp || new Date().toISOString();

  addEventRow({ ts, evt, msg, symbol });
  addOrderInfo({ ts, evt, msg, symbol });

  if (evt === "SIGNAL") {
    addSignalRow(Object.assign({ ts, event: evt }, m.signal || m));
  }
  if (["ENTRY", "EXIT", "SL_TRAIL", "DIRECTION_FLIP", "PENDING_TRADE"].includes(evt)) {
    setTimeout(loadTrades, 900);
  }
  if (evt === "BOT_STATUS") {
    renderBotStatus(true, m);
  }
  renderPnlThrottled();
}

const logCap = (arr, n) => { if (arr.length > n) arr.splice(n); };

function addEventRow(m) {
  const body = $("event-body");
  if (body.querySelector(".term-empty")) body.innerHTML = "";
  const time = fmtTime(m.ts);
  const tag = String(m.evt || m.event || "");
  const tr = document.createElement("tr");
  tr.innerHTML = "<td>" + time + "</td><td><b>" + tag + "</b></td><td>" + esc(m.msg || m.symbol || "") + "</td>";
  body.insertBefore(tr, body.firstChild);
  while (body.children.length > 60) body.removeChild(body.lastChild);
}

function addSignalRow(m) {
  const body = $("signal-body");
  if (body.querySelector(".term-empty")) body.innerHTML = "";
  const strategy = m.strategy || "";
  const opt = m.trading_symbol || "";
  const dir = (m.opt_type || (m.state && m.state.opt_type) || "").toUpperCase();
  const entry = m.entry_price;
  const sl = m.stop_loss;
  const regime = m.regime || "";
  const tr = document.createElement("tr");
  const dirCls = dir === "CE" ? "up" : (dir === "PE" ? "down" : "");
  tr.innerHTML = "<td>" + fmtTime(m.ts) + "</td><td>" + esc(strategy) + "</td><td>" + esc(opt) + "</td>" +
    '<td class="' + dirCls + '"><b>' + esc(dir || "--") + "</b></td><td>" + fmt(entry) + "</td><td>" + fmt(sl) + "</td><td>" + esc(regime || "--") + "</td>";
  body.insertBefore(tr, body.firstChild);
  while (body.children.length > 80) body.removeChild(body.lastChild);
}

function addOrderInfo(m) {
  const el = $("order-info");
  if (el.querySelector(".term-empty")) el.innerHTML = "";
  const div = document.createElement("div");
  div.className = "term-order-ev";
  const tagCls = (String(m.evt).toUpperCase().indexOf("EXIT") >= 0 || String(m.evt).toUpperCase().indexOf("SL") >= 0) ? "down" : "up";
  div.innerHTML =
    '<span class="ev-time">' + fmtTime(m.ts) + "</span>" +
    '<span class="ev-tag ' + tagCls + '">' + esc(m.evt) + "</span>" +
    '<span class="ev-msg">' + esc(m.msg || m.symbol || "") + "</span>";
  el.insertBefore(div, el.firstChild);
  while (el.children.length > 40) el.removeChild(el.lastChild);
}

async function loadTrades() {
  const r = await apiFetch("/api/trades/?today=true&limit=50");
  if (!r) return;
  const trades = await r.json();
  renderTradeLog(trades || []);
  renderRealized(trades || []);
}

function renderTradeLog(trades) {
  const body = $("trade-log-body");
  if (!trades.length) {
    body.innerHTML = '<tr><td colspan="9" class="term-empty">No trades yet</td></tr>';
    return;
  }
  body.innerHTML = "";
  trades.forEach((t) => {
    const pnl = t.pnl;
    const tr = document.createElement("tr");
    tr.innerHTML =
      "<td>" + fmtTime(t.entry_ts) + "</td>" +
      "<td>" + esc(t.trading_symbol || t.strategy || "") + "</td>" +
      "<td>" + esc((t.opt_type || "") + " " + (t.strike || "")) + "</td>" +
      "<td>" + esc(t.strategy || "") + "</td>" +
      "<td>" + fmt(t.qty) + "</td>" +
      "<td>" + fmt(t.entry_price) + "</td>" +
      "<td>" + fmt(t.exit_price) + "</td>" +
      '<td class="' + clsChg(pnl) + '"><b>' + (pnl == null ? "--" : fmtINR(pnl)) + "</b></td>" +
      "<td>" + esc(t.status || "") + "</td>";
    body.appendChild(tr);
  });
}

/* ─────────────── Rendering helpers ─────────────── */
function renderUser(user) {
  $("term-user-label").textContent = user.name + " (" + (user.role || "user") + ")";
}

function renderSelectedSymbol(info) {
  if (!info) return;
  const under = info.underlying || {};
  $("sym-name").textContent = info.symbol || "--";
  $("sym-lot").textContent = "Lot: " + (info.lot_size || "--");
  $("sym-ltp").textContent = fmt(under.ltp);
  $("sym-ltp").className = "term-price " + clsChg(under.change);
  const chgTxt = under.change_pct == null ? "--" : sign(under.change_pct) + under.change_pct.toFixed(2) + "%";
  $("sym-change").textContent = chgTxt;
  $("sym-change").className = "term-change " + clsChg(under.change_pct);
  renderActiveOption(info.premium_state);
}

function renderActiveOption(premiumState) {
  const el = $("sym-active-option");
  if (premiumState && premiumState.trading_symbol) {
    el.textContent = "Active option: " + premiumState.trading_symbol;
  } else {
    el.textContent = "Active option: --";
  }
}

function renderAtm(info) {
  const el = $("atm-strike");
  if (!info || !info.itm) {
    el.innerHTML = '<div class="term-strike-muted">Waiting for underlying…</div>';
    return;
  }
  const itm = info.itm;
  const st = info.premium_state || state.premiumState || {};
  const cur = state.current || {};
  const active = String(st.trading_symbol || "");
  const activePremium = (cur && cur.close != null) ? cur.close : null;
  const ceTxt = active.indexOf("CE") >= 0 ? fmt(activePremium) : intrinsicHint(itm.underlying, itm.itm_ce);
  const peTxt = active.indexOf("PE") >= 0 ? fmt(activePremium) : intrinsicHint(itm.underlying, itm.itm_pe);
  el.innerHTML =
    '<div class="term-strike-row"><span class="term-strike-tag atm">ATM</span>' +
    '<span class="term-strike-name">' + itm.atm + '</span>' +
    '<span class="term-strike-val term-muted">step ' + itm.strike_step + "</span></div>" +
    '<div class="term-strike-row"><span class="term-strike-tag ce">ITM CE</span>' +
    '<span class="term-strike-name">' + itm.itm_ce + " CE</span>" +
    '<span class="term-strike-val up">' + ceTxt + "</span></div>" +
    '<div class="term-strike-row"><span class="term-strike-tag pe">ITM PE</span>' +
    '<span class="term-strike-name">' + itm.itm_pe + " PE</span>" +
    '<span class="term-strike-val down">' + peTxt + "</span></div>";
}

function intrinsicHint(underlying, strike) {
  if (underlying == null || strike == null) return "--";
  const v = Math.max(0, underlying - strike);
  return "~" + v.toFixed(0);
}

function renderStrategyStatus(snap) {
  const el = $("strategy-status");
  const cfg = (state.bootstrap && state.bootstrap.config) || {};
  const ind = snap ? snap.indicators_1m : {};
  const dir = (ind.ema9 != null && ind.ema15 != null)
    ? (ind.ema9 >= ind.ema15 ? "BULL" : "BEAR")
    : "--";
  const emaCross = (ind.ema9 != null && ind.ema15 != null)
    ? fmt(ind.ema9) + " vs " + fmt(ind.ema15) : "--";
  const html =
    '<div class="term-stat-grid">' +
    '<div class="term-stat"><div class="term-stat-label">Strategy</div><div class="term-stat-value">' + esc(cfg.strategy || "--") + "</div></div>" +
    '<div class="term-stat"><div class="term-stat-label">Mode</div><div class="term-stat-value">' + esc(cfg.execution_mode || "--") + "</div></div>" +
    '<div class="term-stat"><div class="term-stat-label">Direction</div><div class="term-stat-value ' + (dir === "BULL" ? "up" : (dir === "BEAR" ? "down" : "")) + '">' + dir + "</div></div>" +
    '<div class="term-stat"><div class="term-stat-label">EMA 9 / 15</div><div class="term-stat-value">' + emaCross + "</div></div>" +
    '<div class="term-stat"><div class="term-stat-label">RSI 7</div><div class="term-stat-value">' + fmt(ind.rsi_7) + "</div></div>" +
    '<div class="term-stat"><div class="term-stat-label">ATR 14</div><div class="term-stat-value">' + fmt(ind.atr_14) + "</div></div>" +
    "</div>";
  el.innerHTML = html;
}

function renderBotStatus(running, status) {
  const el = $("trade-status");
  status = status || {};
  const paused = (status.paused === true || String(status.status || "").toLowerCase() === "paused") ? true : false;
  const runningTxt = running ? (paused ? "Paused" : "Running") : "Stopped";
  const runCls = running ? (paused ? "term-pill-idle" : "term-pill-live") : "term-pill-closed";
  $("term-market-state").textContent = "Market: " + (running ? "OPEN" : "--");
  $("term-market-state").className = "term-pill " + (running ? "term-pill-open" : "term-pill-idle");
  const posCount = state.positions.length;
  el.innerHTML =
    '<div class="term-stat-grid">' +
    '<div class="term-stat"><div class="term-stat-label">Bot</div><div class="term-stat-value ' + (running ? "up" : "down") + '">' + runningTxt + "</div></div>" +
    '<div class="term-stat"><div class="term-stat-label">Open positions</div><div class="term-stat-value">' + posCount + "</div></div>" +
    '<div class="term-stat"><div class="term-stat-label">Paper mode</div><div class="term-stat-value">' + esc((state.bootstrap && state.bootstrap.config && state.bootstrap.config.paper_mode) ? "Yes" : (state.bootstrap && state.bootstrap.config ? "No" : "--")) + "</div></div>" +
    '<div class="term-stat"><div class="term-stat-label">Qty</div><div class="term-stat-value">' + fmt((state.bootstrap && state.bootstrap.config && state.bootstrap.config.order_qty), 0) + "</div></div>" +
    "</div>";
}

function renderTradeStatus() {
  const running = state.bootstrap && state.bootstrap.bot_running;
  renderBotStatus(running, state.bootstrap && state.bootstrap.bot_status);
}

/* ─────────────── Positions ─────────────── */
function positionData(p) { return p.position || p; }

function livePrice(pos) {
  const key = String(pos.instrument_key || "");
  if (state.tickLTP[key] != null) return state.tickLTP[key];
  if (String(pos.symbol || "").toUpperCase() === String(state.selectedSymbol || "").toUpperCase()) {
    if (state.current && state.current.close != null) return state.current.close;
    if (state.premiumState && state.premiumState.ltp != null) return state.premiumState.ltp;
  }
  return num(pos.entry_price);
}

function positionPnl(pos) {
  const entry = num(pos.entry_price);
  const qty = num(pos.qty);
  if (entry == null || !qty) return null;
  const live = livePrice(pos);
  const side = String(pos.side || "BUY").toUpperCase();
  return side === "SELL" ? (entry - live) * qty : (live - entry) * qty;
}

function renderPositions(positions) {
  state.positions = positions || [];
  const el = $("position-list");
  if (!state.positions.length) {
    el.innerHTML = '<div class="term-empty">No open positions</div>';
    renderPnl();
    return;
  }
  el.innerHTML = "";
  state.positions.forEach((p) => {
    const pos = positionData(p);
    const pnl = positionPnl(pos);
    const card = document.createElement("div");
    card.className = "term-pos-card";
    const opt = pos.trading_symbol || (pos.symbol || "");
    const side = String(pos.side || "BUY").toUpperCase();
    const strategy = pos.strategy || "";
    const meta =
      '<div class="term-pos-meta">' +
      "<span>Qty</span><b>" + fmt(pos.qty, 0) + "</b>" +
      "<span>Entry</span><b>" + fmt(pos.entry_price) + "</b>" +
      "<span>SL</span><b>" + fmt(pos.sl_trigger) + "</b>" +
      "<span>Target</span><b>" + fmt(pos.target) + "</b>" +
      "<span>LTP</span><b>" + fmt(livePrice(pos)) + "</b>" +
      "<span>Near Tgt</span><b>" + fmt(pos.near_target) + "</b>" +
      "</div>";
    const actions =
      '<div class="term-pos-actions">' +
      '<input type="number" step="0.05" class="pos-sl" value="' + fmt(pos.sl_trigger) + '" placeholder="SL"/>' +
      '<input type="number" step="0.05" class="pos-tgt" value="' + fmt(pos.target) + '" placeholder="Target"/>' +
      '<button class="term-btn term-btn-xs term-btn-danger pos-sq">Square Off</button>' +
      "</div>";
    card.innerHTML =
      '<div class="term-pos-top">' +
      '<span class="term-pos-opt">' + esc(opt) + "</span>" +
      '<span class="term-pos-side ' + side + '">' + side + "</span>" +
      "</div>" +
      '<div class="term-pos-meta"><span>Strategy</span><b>' + esc(strategy) + "</b></div>" +
      meta +
      '<div class="term-pos-actions">' + actions + "</div>" +
      '<div class="term-pos-pnl ' + clsChg(pnl) + '">PnL: ' + fmtINR(pnl) + "</div>";
    el.appendChild(card);
  });
  bindPositionActions(el);
  renderPnl();
}

function bindPositionActions(el) {
  el.querySelectorAll(".pos-sl").forEach((inp) => {
    inp.addEventListener("change", () => sendOrder("modify_sl", inp.value));
  });
  el.querySelectorAll(".pos-tgt").forEach((inp) => {
    inp.addEventListener("change", () => sendOrder("modify_target", inp.value));
  });
  el.querySelectorAll(".pos-sq").forEach((btn) => {
    btn.addEventListener("click", () => {
      const card = btn.closest(".term-pos-card");
      const optTxt = card.querySelector(".term-pos-opt").textContent;
      const sym = positionSymbolFromOption(optTxt);
      sendOrder("squareoff", null, sym);
    });
  });
}

function positionSymbolFromOption(optTxt) {
  const sym = state.positions.find((p) => {
    const pos = positionData(p);
    return (pos.trading_symbol || "") === optTxt;
  });
  return (sym && positionData(sym).symbol) || state.selectedSymbol;
}

/* ─────────────── PnL ─────────────── */
function computePnl() {
  let unreal = 0;
  state.positions.forEach((p) => {
    const pnl = positionPnl(positionData(p));
    if (pnl != null) unreal += pnl;
  });
  return unreal;
}

function renderRealized(trades) {
  let realized = 0;
  (trades || []).forEach((t) => { if (t.pnl != null) realized += Number(t.pnl); });
  state.realizedToday = realized;
  renderPnl();
}

function renderPnl() {
  const unreal = computePnl();
  const realized = state.realizedToday || 0;
  const net = unreal + realized;
  $("pnl-net").textContent = fmtINR(net);
  $("pnl-net").className = "term-pnl-net " + clsChg(net);
  $("pnl-unreal").textContent = fmtINR(unreal);
  $("pnl-unreal").className = clsChg(unreal);
  $("pnl-real").textContent = fmtINR(realized);
  $("pnl-real").className = clsChg(realized);
  $("pnl-pos-count").textContent = state.positions.length;
}

function renderPnlThrottled() {
  const now = Date.now();
  if (now - state.pnlThrottle < 500) return;
  state.pnlThrottle = now;
  renderPnl();
}

/* ─────────────── Orders ─────────────── */
async function sendOrder(action, value, symbol) {
  const body = { action: action, symbol: symbol || state.selectedSymbol };
  if (value != null) body.value = Number(value);
  const r = await apiFetch("/api/terminal/order", { method: "POST", body: JSON.stringify(body) });
  if (!r) return;
  const data = await r.json();
  if (!r.ok) {
    addOrderInfo({ ts: new Date().toISOString(), evt: "ERROR", msg: data.detail || action + " failed" });
    return;
  }
  addOrderInfo({ ts: new Date().toISOString(), evt: action.toUpperCase(), msg: (symbol || state.selectedSymbol) + " → queued" });
}

/* ─────────────── Overlay math (presentation only) ─────────────── */
function prepareOverlays(candles) {
  const n = candles.length;
  state.ema9 = []; state.ema15 = []; state.ema21 = [];
  state.vwap = []; state.bbU = []; state.bbM = []; state.bbL = [];
  if (!n) return;
  const closes = candles.map((c) => num(c.close) || 0);
  const ema = (span) => {
    const out = []; let prev = closes[0];
    const k = 2 / (span + 1);
    for (let i = 0; i < n; i++) {
      prev = i === 0 ? closes[0] : closes[i] * k + prev * (1 - k);
      out.push(prev);
    }
    return out;
  };
  state.ema9 = ema(9); state.ema15 = ema(15); state.ema21 = ema(21);

  let cumPV = 0, cumV = 0;
  for (let i = 0; i < n; i++) {
    const tp = (num(candles[i].high) || 0) + (num(candles[i].low) || 0) + (num(candles[i].close) || 0);
    const v = num(candles[i].volume) || 0;
    cumPV += (tp / 3) * v; cumV += v;
    state.vwap.push(cumV ? cumPV / cumV : 0);
  }
  for (let i = 0; i < n; i++) {
    if (i < 19) { state.bbU.push(null); state.bbM.push(null); state.bbL.push(null); continue; }
    const win = closes.slice(i - 19, i + 1);
    const mean = win.reduce((a, b) => a + b, 0) / 20;
    const varr = win.reduce((a, b) => a + (b - mean) * (b - mean), 0) / 20;
    const sd = Math.sqrt(varr);
    state.bbM.push(mean);
    state.bbU.push(mean + 2 * sd);
    state.bbL.push(mean - 2 * sd);
  }
}

/* ─────────────── Chart engine ─────────────── */
class CandlestickChart {
  constructor(canvas, wrap) {
    this.canvas = canvas;
    this.ctx = canvas.getContext("2d");
    this.wrap = wrap;
    this.dpr = window.devicePixelRatio || 1;
    this.w = 0; this.h = 0;
    this.layout = null;
    this.pxPerBar = 5;
    this.offset = 0;
    this.mouse = null;
    this.crossIndex = -1;
    this.drag = null;

    this.resizeObserver = new ResizeObserver(() => this.resize());
    this.resizeObserver.observe(wrap);

    canvas.addEventListener("mousemove", (e) => this.onMouseMove(e));
    canvas.addEventListener("mousedown", (e) => this.onMouseDown(e));
    canvas.addEventListener("mouseup", (e) => this.onMouseUp(e));
    canvas.addEventListener("mouseleave", () => this.onMouseLeave());
    canvas.addEventListener("wheel", (e) => this.onWheel(e), { passive: false });
    canvas.addEventListener("touchstart", (e) => this.onTouchStart(e), { passive: false });
    canvas.addEventListener("touchmove", (e) => this.onTouchMove(e), { passive: false });
    canvas.addEventListener("touchend", (e) => this.onTouchEnd(e), { passive: false });
  }

  onTouchStart(e) {
    if (e.touches.length) {
      this.onMouseDown({ clientX: e.touches[0].clientX, clientY: e.touches[0].clientY });
      if (this.drag) e.preventDefault();
    }
  }
  onTouchMove(e) {
    if (e.touches.length) {
      this.onMouseMove({ clientX: e.touches[0].clientX, clientY: e.touches[0].clientY });
      if (this.drag) e.preventDefault();
    }
  }
  onTouchEnd(e) {
    this.onMouseUp({});
  }

  resize() {
    const rect = this.wrap.getBoundingClientRect();
    this.w = rect.width; this.h = rect.height;
    this.canvas.width = Math.floor(rect.width * this.dpr);
    this.canvas.height = Math.floor(rect.height * this.dpr);
    this.canvas.style.width = rect.width + "px";
    this.canvas.style.height = rect.height + "px";
    this.ctx.setTransform(this.dpr, 0, 0, this.dpr, 0, 0);
    requestAnimationFrame(() => drawChart());
  }

  setData() { requestAnimationFrame(() => drawChart()); }

  /* layout computation happens in drawChart for shared state access */
  getPlotRect() {
    const layout = this.layout || { left: 8, right: 72, top: 12, bottom: 24, volH: 0 };
    return {
      left: layout.left, right: this.w - layout.right,
      top: layout.top, bottom: this.h - layout.bottom,
      volH: layout.volH
    };
  }

  priceToY(price) { return this.mapY(price); }
  mapY(price) {
    const r = this.getPlotRect();
    const min = this.ymin, max = this.ymax;
    if (max === min) return (r.top + r.bottom - r.volH) / 2;
    return r.top + ((max - price) / (max - min)) * (r.bottom - r.volH - r.top);
  }
  yToPrice(y) {
    const r = this.getPlotRect();
    const min = this.ymin, max = this.ymax;
    if (max === min) return (max + min) / 2;
    const ratio = (y - r.top) / (r.bottom - r.volH - r.top);
    return max - ratio * (max - min);
  }

  xForIndex(i) {
    const r = this.getPlotRect();
    const total = state.candles.length;
    const w = r.right - r.left;
    const step = total > 1 ? w / total : w;
    return r.left + step * (i - this.offset + 0.5);
  }
  indexForX(x) {
    const r = this.getPlotRect();
    const w = r.right - r.left;
    const total = state.candles.length;
    const step = total > 1 ? w / total : w;
    return Math.floor((x - r.left) / step) + this.offset;
  }

  onMouseMove(e) {
    const rect = this.canvas.getBoundingClientRect();
    const x = e.clientX - rect.left, y = e.clientY - rect.top;
    if (this.drag) {
      this.drag.y = y;
      this.drag.value = this.yToPrice(y);
      state.orderLineValue = this.drag.value;
      this.renderDragHint();
      requestAnimationFrame(() => drawChart());
      return;
    }
    this.mouse = { x: x, y: y };
    this.crossIndex = this.indexForX(x);
    requestAnimationFrame(() => drawChart());
  }

  onMouseDown(e) {
    const rect = this.canvas.getBoundingClientRect();
    const y = e.clientY - rect.top;
    const x = e.clientX - rect.left;
    const pos = selectedPosition();
    if (!pos) return;
    if (Math.abs(y - this.mapY(num(pos.sl_trigger))) <= 7) {
      this.drag = { kind: "sl", y: y, value: num(pos.sl_trigger) };
      this.canvas.style.cursor = "row-resize";
      this.canvas.style.touchAction = "none";
      e.preventDefault();
    } else if (Math.abs(y - this.mapY(num(pos.target))) <= 7) {
      this.drag = { kind: "target", y: y, value: num(pos.target) };
      this.canvas.style.cursor = "row-resize";
      this.canvas.style.touchAction = "none";
      e.preventDefault();
    }
  }

  onMouseUp(e) {
    if (!this.drag) return;
    const kind = this.drag.kind;
    const value = this.drag.value;
    this.drag = null;
    this.canvas.style.cursor = "crosshair";
    hideDragHint();
    if (value != null && isFinite(value)) {
      sendOrder(kind === "sl" ? "modify_sl" : "modify_target", value);
    }
    requestAnimationFrame(() => drawChart());
  }

  onMouseLeave() {
    this.mouse = null;
    this.crossIndex = -1;
    $("term-crosshair").style.display = "none";
    requestAnimationFrame(() => drawChart());
  }

  onWheel(e) {
    e.preventDefault();
    const rect = this.canvas.getBoundingClientRect();
    const x = e.clientX - rect.left;
    const anchor = this.indexForX(x);
    const factor = e.deltaY < 0 ? 0.85 : 1.18;
    const total = state.candles.length;
    this.pxPerBar = Math.min(30, Math.max(1.2, this.pxPerBar * factor));
    const r = this.getPlotRect();
    const w = r.right - r.left;
    this.offset = Math.max(0, anchor - (x - r.left) / this.pxPerBar);
    this.offset = Math.max(0, Math.min(total - w / this.pxPerBar, this.offset));
    requestAnimationFrame(() => drawChart());
  }

  renderDragHint() {
    const el = $("drag-hint");
    el.classList.add("show");
    const kindTxt = this.drag.kind === "sl" ? "SL" : "TARGET";
    el.textContent = kindTxt + ": " + fmt(this.drag.value) + "  (release to set)";
  }
}

function selectedPosition() {
  const sym = state.selectedSymbol;
  const p = state.positions.find((q) => {
    const pos = positionData(q);
    return String(pos.symbol || "").toUpperCase() === String(sym || "").toUpperCase();
  });
  return p ? positionData(p) : null;
}

function hideDragHint() {
  const el = $("drag-hint");
  el.classList.remove("show");
  setTimeout(() => { el.textContent = ""; }, 300);
}

function drawChart() {
  const canvas = $("term-chart-canvas");
  if (!state.chart) return;
  const ctx = canvas.getContext("2d");
  const c = state.chart;
  const W = c.w, H = c.h;
  ctx.clearRect(0, 0, W, H);

  const candles = state.candles || [];
  if (!candles.length) {
    ctx.fillStyle = "#6b7691";
    ctx.font = "13px sans-serif";
    ctx.textAlign = "center";
    ctx.fillText("No data — waiting for live feed…", W / 2, H / 2);
    return;
  }

  const layout = {
    left: 8, right: 72, top: 12, bottom: 22,
    volH: state.overlays.vol ? 60 : 0
  };
  c.layout = layout;
  const r = { left: layout.left, right: W - layout.right, top: layout.top, bottom: H - layout.bottom };

  /* visible window */
  const total = candles.length;
  const plotW = r.right - r.left;
  c.pxPerBar = Math.max(c.pxPerBar, plotW / total);
  const visible = Math.floor(plotW / c.pxPerBar);
  if (c.offset > total - visible) c.offset = Math.max(0, total - visible);

  const from = Math.floor(c.offset);
  const to = Math.min(total, from + visible);

  /* price range over visible bars + overlays + sl/target */
  let min = Infinity, max = -Infinity;
  for (let i = from; i < to; i++) {
    const b = candles[i];
    if (b.low < min) min = b.low; if (b.high > max) max = b.high;
    const e9 = state.ema9[i], e15 = state.ema15[i], e21 = state.ema21[i];
    if (e9 != null) { if (e9 < min) min = e9; if (e9 > max) max = e9; }
    if (e15 != null) { if (e15 < min) min = e15; if (e15 > max) max = e15; }
    if (e21 != null) { if (e21 < min) min = e21; if (e21 > max) max = e21; }
    if (state.vwap[i] != null) { if (state.vwap[i] < min) min = state.vwap[i]; if (state.vwap[i] > max) max = state.vwap[i]; }
    if (state.bbU[i] != null) { if (state.bbU[i] < min) min = state.bbU[i]; if (state.bbU[i] > max) max = state.bbU[i]; }
    if (state.bbL[i] != null) { if (state.bbL[i] < min) min = state.bbL[i]; if (state.bbL[i] > max) max = state.bbL[i]; }
  }
  const pos = selectedPosition();
  if (pos) {
    if (num(pos.sl_trigger) != null) { if (pos.sl_trigger < min) min = pos.sl_trigger; if (pos.sl_trigger > max) max = pos.sl_trigger; }
    if (num(pos.target) != null) { if (pos.target < min) min = pos.target; if (pos.target > max) max = pos.target; }
  }
  const pad = (max - min) * 0.08 || 1;
  c.ymin = min - pad; c.ymax = max + pad;

  /* grid + axes */
  const gridColor = "rgba(46,58,88,0.45)";
  const priceTicks = niceTicks(min - pad, max + pad, 6);
  ctx.strokeStyle = gridColor; ctx.lineWidth = 1;
  ctx.font = "10px sans-serif";
  ctx.textAlign = "left";
  priceTicks.forEach((p) => {
    const y = c.mapY(p);
    ctx.beginPath(); ctx.moveTo(r.left, y); ctx.lineTo(r.right, y); ctx.stroke();
    ctx.fillStyle = "#6b7691";
    ctx.fillText(p.toFixed(1), r.right + 6, y + 3);
  });

  /* time labels */
  const stepT = Math.max(1, Math.ceil(visible / 8));
  ctx.textAlign = "center";
  ctx.fillStyle = "#6b7691";
  for (let i = from; i < to; i += stepT) {
    const x = c.xForIndex(i);
    ctx.fillText(fmtTime(candles[i].time), x, H - 6);
    ctx.beginPath(); ctx.moveTo(x, r.top); ctx.lineTo(x, r.bottom - layout.volH); ctx.strokeStyle = "rgba(46,58,88,0.22)"; ctx.stroke();
  }

  /* volume */
  if (state.overlays.vol) {
    let vmax = 1;
    for (let i = from; i < to; i++) if (num(candles[i].volume) > vmax) vmax = num(candles[i].volume);
    const vTop = r.bottom - layout.volH;
    for (let i = from; i < to; i++) {
      const b = candles[i];
      const x = c.xForIndex(i);
      const bw = Math.max(1, c.pxPerBar * 0.6);
      const vh = (num(b.volume) || 0) / vmax * (layout.volH - 6);
      ctx.fillStyle = (num(b.close) >= num(b.open)) ? "rgba(34,197,94,0.35)" : "rgba(239,68,68,0.35)";
      ctx.fillRect(x - bw / 2, vTop + (layout.volH - 6) - vh, bw, vh);
    }
    ctx.strokeStyle = "rgba(46,58,88,0.6)";
    ctx.beginPath(); ctx.moveTo(r.left, vTop); ctx.lineTo(r.right, vTop); ctx.stroke();
  }

  /* candles */
  const cw = Math.max(1, c.pxPerBar * 0.62);
  for (let i = from; i < to; i++) {
    const b = candles[i];
    const x = c.xForIndex(i);
    const o = c.mapY(b.open), hi = c.mapY(b.high), lo = c.mapY(b.low), cl = c.mapY(b.close);
    const up = num(b.close) >= num(b.open);
    const col = up ? "#22c55e" : "#ef4444";
    ctx.strokeStyle = col; ctx.fillStyle = col; ctx.lineWidth = 1;
    ctx.beginPath(); ctx.moveTo(x, hi); ctx.lineTo(x, lo); ctx.stroke();
    const bodyTop = Math.min(o, cl), bodyH = Math.max(1, Math.abs(o - cl));
    ctx.fillRect(x - cw / 2, bodyTop, cw, bodyH);
  }

  /* overlays */
  if (state.overlays.ema) {
    drawLine(state.ema9, "#f59e0b");
    drawLine(state.ema15, "#3b82f6");
    drawLine(state.ema21, "#a855f7");
  }
  if (state.overlays.vwap) drawLine(state.vwap, "#22d3ee");
  if (state.overlays.bb) {
    drawLine(state.bbU, "#64748b", true);
    drawLine(state.bbM, "#64748b", true);
    drawLine(state.bbL, "#64748b", true);
  }

  /* SL / Target lines */
  if (pos) {
    const sl = num(pos.sl_trigger), tg = num(pos.target);
    if (sl != null) drawHL(sl, "#ef4444", "SL " + sl.toFixed(2));
    if (tg != null) drawHL(tg, "#22c55e", "TGT " + tg.toFixed(2));
    if (c.drag) {
      drawHL(c.drag.value, c.drag.kind === "sl" ? "#ef4444" : "#22c55e", (c.drag.kind === "sl" ? "SL " : "TGT ") + fmt(c.drag.value), true);
    }
  }

  /* last price marker */
  const lastC = num(candles[to - 1].close);
  if (lastC != null) {
    const y = c.mapY(lastC);
    ctx.fillStyle = (num(candles[to - 1].close) >= num(candles[to - 1].open)) ? "#22c55e" : "#ef4444";
    ctx.fillRect(r.right, y - 1, 4, 2);
    ctx.beginPath(); ctx.moveTo(r.left, y); ctx.lineTo(r.right, y); ctx.strokeStyle = "rgba(59,130,246,0.35)"; ctx.stroke();
  }

  /* crosshair */
  if (c.mouse && c.crossIndex >= from && c.crossIndex < to) {
    const ci = c.crossIndex;
    const x = c.xForIndex(ci), y = c.mouse.y;
    ctx.strokeStyle = "rgba(139,153,190,0.5)"; ctx.lineWidth = 1;
    ctx.setLineDash([4, 4]);
    ctx.beginPath(); ctx.moveTo(x, r.top); ctx.lineTo(x, r.bottom); ctx.stroke();
    ctx.beginPath(); ctx.moveTo(r.left, y); ctx.lineTo(r.right, y); ctx.stroke();
    ctx.setLineDash([]);
    const b = candles[ci];
    const crossEl = $("term-crosshair");
    crossEl.style.display = "flex";
    $("ch-time").textContent = fmtTime(b.time);
    $("ch-ohlc").textContent = "O " + fmt(b.open) + " H " + fmt(b.high) + " L " + fmt(b.low) + " C " + fmt(b.close);
  }

  /* legend */
  renderLegend(lastC);
}

function drawLine(arr, color, dashed) {
  const ctx = state.chart.ctx;
  const c = state.chart;
  const from = Math.floor(c.offset);
  const to = Math.min(state.candles.length, from + Math.floor((c.getPlotRect().right - c.getPlotRect().left) / c.pxPerBar));
  ctx.strokeStyle = color; ctx.lineWidth = 1.2;
  ctx.setLineDash(dashed ? [4, 3] : []);
  ctx.beginPath();
  let started = false;
  for (let i = from; i < to; i++) {
    const v = arr[i];
    if (v == null || !isFinite(v)) { started = false; continue; }
    const x = c.xForIndex(i), y = c.mapY(v);
    if (!started) { ctx.moveTo(x, y); started = true; }
    else ctx.lineTo(x, y);
  }
  ctx.stroke();
  ctx.setLineDash([]);
}

function drawHL(price, color, label, isDrag) {
  const ctx = state.chart.ctx;
  const c = state.chart;
  const r = c.getPlotRect();
  const y = c.mapY(price);
  ctx.strokeStyle = color;
  ctx.lineWidth = isDrag ? 1.6 : 1.2;
  ctx.setLineDash([6, 4]);
  ctx.beginPath(); ctx.moveTo(r.left, y); ctx.lineTo(r.right, y); ctx.stroke();
  ctx.setLineDash([]);
  ctx.font = "10px sans-serif";
  ctx.textAlign = "right";
  ctx.fillStyle = "#0b0e17";
  const tw = ctx.measureText(label).width + 10;
  ctx.fillStyle = color;
  ctx.fillRect(r.right - tw, y - 8, tw, 14);
  ctx.fillStyle = "#fff";
  ctx.fillText(label, r.right - 4, y + 3);
}

function niceTicks(min, max, count) {
  const span = max - min;
  if (!(span > 0)) return [min];
  const step = Math.pow(10, Math.floor(Math.log10(span / count)));
  const err = span / count / step;
  let nice = step * (err >= 7.5 ? 10 : err >= 3.5 ? 5 : err >= 1.5 ? 2 : 1);
  const out = [];
  for (let v = Math.ceil(min / nice) * nice; v <= max + 1e-9; v += nice) out.push(v);
  return out.slice(0, count + 1);
}

function renderLegend(lastC) {
  const el = $("chart-legend");
  const last = state.candles[state.candles.length - 1];
  const ltp = lastC != null ? lastC : (last ? last.close : null);
  const parts = [
    '<span>LTP <b style="color:#dbe3f0">' + fmt(ltp) + "</b></span>",
    '<span><span style="color:#f59e0b">EMA9</span> <b>' + fmt(state.ema9[state.ema9.length - 1]) + "</b></span>",
    '<span><span style="color:#3b82f6">EMA15</span> <b>' + fmt(state.ema15[state.ema15.length - 1]) + "</b></span>",
    '<span><span style="color:#a855f7">EMA21</span> <b>' + fmt(state.ema21[state.ema21.length - 1]) + "</b></span>"
  ];
  if (state.overlays.vwap) parts.push('<span><span style="color:#22d3ee">VWAP</span> <b>' + fmt(state.vwap[state.vwap.length - 1]) + "</b></span>");
  el.innerHTML = parts.join("");
}

/* ─────────────── Live updates ─────────────── */
function pushBar(candle) {
  if (!candle) return;
  const candles = state.candles;
  const key = barKey(candle);
  const last = candles[candles.length - 1];
  if (last && barKey(last) === key) {
    Object.assign(last, normalizeBar(candle));
  } else {
    candles.push(normalizeBar(candle));
  }
  prepareOverlays(candles);
  state.chart.setData();
  renderPnlThrottled();
}

function updateLastBar(candle) {
  if (!candle) return;
  const candles = state.candles;
  if (!candles.length) return;
  const last = candles[candles.length - 1];
  Object.assign(last, normalizeBar(candle));
  prepareOverlays(candles);
  state.chart.setData();
  renderPnlThrottled();
}

function barKey(b) {
  return String(b.t != null ? b.t : (b.time != null ? b.time : b.minute));
}

function normalizeBar(b) {
  return {
    time: b.time != null ? b.time : b.minute,
    open: num(b.open) || 0,
    high: num(b.high) || 0,
    low: num(b.low) || 0,
    close: num(b.close) || 0,
    volume: num(b.volume) || 0
  };
}

/* ─────────────── Utilities ─────────────── */
function fmtTime(v) {
  if (!v) return "--";
  const s = String(v);
  const m = s.match(/(\d{2}):(\d{2})(?::\d{2})?/);
  if (m) return m[1] + ":" + m[2];
  const d = new Date(v);
  if (!isNaN(d)) {
    const hh = String(d.getHours()).padStart(2, "0");
    const mm = String(d.getMinutes()).padStart(2, "0");
    return hh + ":" + mm;
  }
  return s.slice(11, 16) || s;
}

function esc(s) {
  return String(s == null ? "" : s)
    .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;").replace(/'/g, "&#39;");
}

function redirectLogin() {
  localStorage.removeItem("access_token");
  localStorage.removeItem("user_id");
  window.location.href = "/login";
}

function tokenExpired() {
  const t = getToken();
  if (!t) return true;
  try {
    const p = JSON.parse(atob(t.split(".")[1]));
    return !p.exp || p.exp * 1000 < Date.now();
  } catch (e) { return true; }
}

/* ─────────────── Toolbar events ─────────────── */
function bindToolbar() {
  document.querySelectorAll(".term-tf").forEach((btn) => {
    btn.addEventListener("click", () => {
      document.querySelectorAll(".term-tf").forEach((b) => b.classList.remove("term-tf-active"));
      btn.classList.add("term-tf-active");
      state.tf = btn.dataset.tf;
      fetchSnapshot(state.selectedSymbol);
    });
  });

  const toggleOverlay = (key) => (ev) => {
    state.overlays[key] = ev.target.checked;
    drawChart();
  };
  $("ov-ema").addEventListener("change", toggleOverlay("ema"));
  $("ov-vwap").addEventListener("change", toggleOverlay("vwap"));
  $("ov-bb").addEventListener("change", toggleOverlay("bb"));
  $("ov-vol").addEventListener("change", toggleOverlay("vol"));

  $("chart-src").addEventListener("change", (e) => {
    state.series = e.target.value;
    fetchSnapshot(state.selectedSymbol);
  });

  $("sq-off-all").addEventListener("click", () => {
    if (state.positions.length && confirm("Square off all open positions?")) {
      sendOrder("squareoff", null, null);
    }
  });

  document.querySelectorAll(".term-tab").forEach((tab) => {
    tab.addEventListener("click", () => {
      document.querySelectorAll(".term-tab").forEach((t) => t.classList.remove("term-tab-active"));
      document.querySelectorAll(".term-tabpane").forEach((p) => p.classList.remove("term-tabpane-active"));
      tab.classList.add("term-tab-active");
      $("pane-" + tab.dataset.tab).classList.add("term-tabpane-active");
    });
  });

  $("term-logout").addEventListener("click", logout);

  const toggleLeft = $("term-toggle-left");
  const syncLeftToggle = (collapsed) => {
    toggleLeft.classList.toggle("term-btn-accent", collapsed);
    toggleLeft.title = collapsed ? "Show left panel (watchlist)" : "Hide left panel (watchlist)";
  };
  toggleLeft.addEventListener("click", () => {
    const collapsed = document.body.classList.toggle("left-collapsed");
    localStorage.setItem("term.leftCollapsed", collapsed ? "1" : "0");
    syncLeftToggle(collapsed);
    state.chart && state.chart.resize();
  });
  syncLeftToggle(localStorage.getItem("term.leftCollapsed") === "1");
  if (localStorage.getItem("term.leftCollapsed") === "1") {
    document.body.classList.add("left-collapsed");
  }
}

/* ─────────────── Init ─────────────── */
document.addEventListener("DOMContentLoaded", () => {
  if (!getToken()) { redirectLogin(); return; }
  bindToolbar();
  state.chart = new CandlestickChart($("term-chart-canvas"), document.querySelector(".term-chart-wrap"));
  window.addEventListener("resize", () => state.chart && state.chart.resize());
  loadBootstrap();
});

})();
