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
    const m = t.match(/(\d{4}-\d{2}-\d{2})[ T]?(\d{2}):(\d{2})/);
    let hmStr;
    if (m) {
      hmStr = m[2] + ":" + m[3];
    } else {
      const m2 = t.match(/(\d{2}):(\d{2})/);
      hmStr = m2 ? m2[1] + ":" + m2[2] : t;
    }
    const mins = (+hmStr.slice(0, 2)) * 60 + (+hmStr.slice(3, 5));
    const bucketEnd = Math.floor(mins / 5) * 5 + 5;
    const key = String(Math.floor(bucketEnd / 60)).padStart(2, "0") + ":" + String(bucketEnd % 60).padStart(2, "0");
    const fullKey = (m ? m[1] + " " : "") + key;
    const prev = buckets[fullKey];
    if (prev) {
      prev.high = Math.max(prev.high, b.high);
      prev.low = Math.min(prev.low, b.low);
      prev.close = b.close;
      prev.volume = (prev.volume || 0) + (b.volume || 0);
    } else {
      buckets[fullKey] = { time: fullKey, open: b.open, high: b.high, low: b.low, close: b.close, volume: b.volume || 0 };
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

/* ─────────────── Chart engine (TradingView Lightweight Charts) ─────────────── */
function toLwcTime(t) {
  const s = String(t == null ? "" : t).trim();
  if (!s) return null;
  let m = s.match(/^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2})(?::(\d{2}))?/);
  let y, mo, d, hh, mm, ss = 0;
  if (m) {
    y = +m[1]; mo = +m[2]; d = +m[3]; hh = +m[4]; mm = +m[5]; ss = m[6] ? +m[6] : 0;
  } else {
    m = s.match(/^(\d{2}):(\d{2})(?::(\d{2}))?/);
    if (m) {
      const nowIst = new Date(Date.now() + 330 * 60 * 1000);
      y = nowIst.getUTCFullYear(); mo = nowIst.getUTCMonth() + 1; d = nowIst.getUTCDate();
      hh = +m[1]; mm = +m[2]; ss = m[3] ? +m[3] : 0;
    } else {
      const p = Date.parse(s);
      if (isNaN(p)) return null;
      return Math.floor(p / 1000);
    }
  }
  return Math.floor((Date.UTC(y, mo - 1, d, hh, mm, ss) - 330 * 60 * 1000) / 1000);
}

function barTime(b) {
  return b && (b.time != null ? b.time : b.minute);
}

function barToLwc(b) {
  const time = toLwcTime(barTime(b));
  if (time == null) return null;
  return { time: time, open: num(b.open) || 0, high: num(b.high) || 0, low: num(b.low) || 0, close: num(b.close) || 0 };
}

function volToLwc(b) {
  const time = toLwcTime(barTime(b));
  if (time == null) return null;
  const up = num(b.close) >= num(b.open);
  return { time: time, value: num(b.volume) || 0, color: up ? "rgba(34,197,94,0.35)" : "rgba(239,68,68,0.35)" };
}

function lineToLwc(arr, candles) {
  const out = [];
  for (let i = 0; i < candles.length; i++) {
    const v = arr && arr[i];
    if (v == null || !isFinite(v)) continue;
    const time = toLwcTime(barTime(candles[i]));
    if (time == null) continue;
    out.push({ time: time, value: v });
  }
  return out;
}

function closeLineToLwc(candles) {
  const out = [];
  for (let i = 0; i < candles.length; i++) {
    const time = toLwcTime(barTime(candles[i]));
    if (time == null) continue;
    out.push({ time: time, value: num(candles[i].close) || 0 });
  }
  return out;
}

function lwcTimeToStr(t) {
  if (t == null) return "--";
  const n = Number(t);
  if (isNaN(n)) return String(t);
  const d = new Date(n * 1000 + 330 * 60 * 1000);
  return String(d.getUTCHours()).padStart(2, "0") + ":" + String(d.getUTCMinutes()).padStart(2, "0");
}

class LwcChart {
  constructor(container, wrap) {
    this.el = container;
    this.wrap = wrap;
    this.chart = null;
    this.series = {};
    this.priceLines = { sl: null, tgt: null, drag: null };
    this.drag = null;
    this._fitted = false;
    this._lastFirst = null;
    this._scrollLocked = false;
    this.init();
  }

  init() {
    const C = LightweightCharts;
    this.chart = C.createChart(this.el, {
      autoSize: true,
      layout: {
        background: { type: "solid", color: "#0b0e17" },
        textColor: "#8b99be",
        fontSize: 11
      },
      grid: {
        vertLines: { color: "rgba(46,58,88,0.35)" },
        horzLines: { color: "rgba(46,58,88,0.35)" }
      },
      rightPriceScale: { borderColor: "rgba(46,58,88,0.6)" },
      timeScale: {
        borderColor: "rgba(46,58,88,0.6)",
        timeVisible: true,
        secondsVisible: false,
        rightOffset: 2,
        barSpacing: 7
      },
      crosshair: {
        mode: C.CrosshairMode.Normal,
        vertLine: { color: "rgba(139,153,190,0.4)", labelBackgroundColor: "#1e2537" },
        horzLine: { color: "rgba(139,153,190,0.4)", labelBackgroundColor: "#1e2537" }
      },
      localization: { locale: "en-IN" }
    });
    this.rebuildMain();
    this.ensureOverlaySeries();
    this.chart.subscribeCrosshairMove((p) => this.onCrosshair(p));
    this.bindPointerEvents();
  }

  rebuildMain() {
    const old = this.series.main;
    if (old) { try { this.chart.removeSeries(old); } catch (e) { } this.series.main = null; }
    if (state.series === "underlying") {
      this.series.main = this.chart.addLineSeries({
        color: "#3b82f6", lineWidth: 2,
        priceLineVisible: true, lastValueVisible: true, crosshairMarkerVisible: true
      });
    } else {
      this.series.main = this.chart.addCandlestickSeries({
        upColor: "#22c55e", downColor: "#ef4444",
        borderVisible: false, wickUpColor: "#22c55e", wickDownColor: "#ef4444",
        priceLineVisible: true, lastValueVisible: true
      });
    }
    this._fitted = false;
    this.renderPositionLines();
  }

  ensureOverlaySeries() {
    const mk = (key, color, opts) => {
      if (this.series[key]) return;
      this.series[key] = this.chart.addLineSeries(Object.assign({
        color: color, lineWidth: 1,
        priceLineVisible: false, lastValueVisible: false, crosshairMarkerVisible: false
      }, opts || {}));
    };
    mk("ema9", "#f59e0b");
    mk("ema15", "#3b82f6");
    mk("ema21", "#a855f7");
    mk("vwap", "#22d3ee");
    mk("bbU", "#64748b", { lineStyle: 2 });
    mk("bbM", "#64748b", { lineStyle: 2 });
    mk("bbL", "#64748b", { lineStyle: 2 });
    if (!this.series.vol) {
      this.series.vol = this.chart.addHistogramSeries({
        priceFormat: { type: "volume" },
        priceScaleId: "vol",
        lastValueVisible: false, priceLineVisible: false
      });
      try { this.chart.priceScale("vol").applyOptions({ scaleMargins: { top: 0.85, bottom: 0 } }); } catch (e) { }
    }
  }

  fullRender() {
    if (!this.chart) return;
    this.ensureOverlaySeries();
    const candles = state.candles || [];
    const main = this.series.main;
    if (state.series === "underlying") {
      main.setData(closeLineToLwc(candles));
    } else {
      main.setData(candles.map(barToLwc).filter(Boolean));
    }
    const ov = state.overlays;
    this.series.vol.setData((ov.vol && state.series === "premium") ? candles.map(volToLwc).filter(Boolean) : []);
    this.series.ema9.setData(ov.ema ? lineToLwc(state.ema9, candles) : []);
    this.series.ema15.setData(ov.ema ? lineToLwc(state.ema15, candles) : []);
    this.series.ema21.setData(ov.ema ? lineToLwc(state.ema21, candles) : []);
    this.series.vwap.setData(ov.vwap ? lineToLwc(state.vwap, candles) : []);
    this.series.bbU.setData(ov.bb ? lineToLwc(state.bbU, candles) : []);
    this.series.bbM.setData(ov.bb ? lineToLwc(state.bbM, candles) : []);
    this.series.bbL.setData(ov.bb ? lineToLwc(state.bbL, candles) : []);
    this.renderPositionLines();
    const first = candles.length ? barTime(candles[0]) : null;
    if (!this._fitted || first !== this._lastFirst) {
      this._fitted = true;
      this._lastFirst = first;
      try { this.chart.timeScale().fitContent(); } catch (e) { }
    }
    renderLegend(candles.length ? num(candles[candles.length - 1].close) : null);
  }

  updateLive() {
    if (!this.chart) return;
    const candles = state.candles || [];
    const last = candles[candles.length - 1];
    if (!last) return;
    const time = toLwcTime(barTime(last));
    if (time == null) return;
    const i = candles.length - 1;
    if (state.series === "underlying") {
      this.series.main.update({ time: time, value: num(last.close) || 0 });
    } else {
      const b = barToLwc(last);
      if (b) this.series.main.update(b);
    }
    if (state.overlays.vol && state.series === "premium") {
      const v = volToLwc(last);
      if (v) this.series.vol.update(v);
    }
    const ov = state.overlays;
    if (ov.ema) {
      const u = (key, arr) => {
        const v = arr[i];
        if (v != null && isFinite(v)) this.series[key].update({ time: time, value: v });
      };
      u("ema9", state.ema9); u("ema15", state.ema15); u("ema21", state.ema21);
    }
    if (ov.vwap && state.vwap[i] != null) this.series.vwap.update({ time: time, value: state.vwap[i] });
    if (ov.bb) {
      if (state.bbU[i] != null) this.series.bbU.update({ time: time, value: state.bbU[i] });
      if (state.bbM[i] != null) this.series.bbM.update({ time: time, value: state.bbM[i] });
      if (state.bbL[i] != null) this.series.bbL.update({ time: time, value: state.bbL[i] });
    }
  }

  setData() { this.updateLive(); }

  resize() {
    try { this.chart.applyOptions({ width: this.el.clientWidth, height: this.el.clientHeight }); } catch (e) { }
  }

  renderPositionLines() {
    for (const k of ["sl", "tgt"]) {
      if (this.priceLines[k]) {
        try { if (this.series.main) this.series.main.removePriceLine(this.priceLines[k]); } catch (e) { }
        this.priceLines[k] = null;
      }
    }
    const pos = selectedPosition();
    if (!pos || !this.series.main) return;
    const sl = num(pos.sl_trigger), tg = num(pos.target);
    if (sl != null) {
      this.priceLines.sl = this.series.main.createPriceLine({
        price: sl, color: "#ef4444", lineWidth: 1, lineStyle: 2,
        axisLabelVisible: true, title: "SL"
      });
    }
    if (tg != null) {
      this.priceLines.tgt = this.series.main.createPriceLine({
        price: tg, color: "#22c55e", lineWidth: 1, lineStyle: 2,
        axisLabelVisible: true, title: "TGT"
      });
    }
  }

  setDragLine(price, kind) {
    if (!this.series.main) return;
    if (!this.priceLines.drag) {
      this.priceLines.drag = this.series.main.createPriceLine({
        price: price, color: kind === "sl" ? "#ef4444" : "#22c55e",
        lineWidth: 2, lineStyle: 0, axisLabelVisible: true,
        title: kind === "sl" ? "SL" : "TGT"
      });
    } else {
      this.priceLines.drag.applyOptions({ price: price });
    }
  }

  clearDragLine() {
    if (this.priceLines.drag) {
      try { if (this.series.main) this.series.main.removePriceLine(this.priceLines.drag); } catch (e) { }
      this.priceLines.drag = null;
    }
  }

  renderDragHint() {
    const el = $("drag-hint");
    el.classList.add("show");
    const kindTxt = this.drag.kind === "sl" ? "SL" : "TARGET";
    el.textContent = kindTxt + ": " + fmt(this.drag.value) + "  (release to set)";
  }

  setScrollLocked(locked) {
    if (this._scrollLocked === locked) return;
    this._scrollLocked = locked;
    try { this.chart.applyOptions({ handleScroll: !locked, handleScale: !locked }); } catch (e) { }
  }

  bindPointerEvents() {
    const el = this.el;
    const startDrag = (clientY) => {
      const pos = selectedPosition();
      if (!pos) return;
      const rect = el.getBoundingClientRect();
      const y = clientY - rect.top;
      const sl = num(pos.sl_trigger), tg = num(pos.target);
      const near = (p) => (p != null && this.chart.priceToCoordinate(p) != null && Math.abs(this.chart.priceToCoordinate(p) - y) <= 8);
      let kind = null;
      if (near(sl)) kind = "sl";
      else if (near(tg)) kind = "target";
      if (!kind) return;
      this.drag = { kind: kind, value: kind === "sl" ? sl : tg };
      el.style.cursor = "row-resize";
      this.setScrollLocked(true);
    };
    const moveDrag = (clientY) => {
      if (!this.drag) return;
      const rect = el.getBoundingClientRect();
      const price = this.chart.coordinateToPrice(clientY - rect.top);
      if (price == null) return;
      this.drag.value = price;
      this.setDragLine(price, this.drag.kind);
      this.renderDragHint();
    };
    const endDrag = () => {
      if (!this.drag) return;
      const kind = this.drag.kind;
      const value = this.drag.value;
      this.drag = null;
      this.clearDragLine();
      el.style.cursor = "crosshair";
      this.setScrollLocked(false);
      hideDragHint();
      if (value != null && isFinite(value)) {
        sendOrder(kind === "sl" ? "modify_sl" : "modify_target", value);
      }
      this.renderPositionLines();
    };
    el.addEventListener("pointerdown", (e) => startDrag(e.clientY));
    el.addEventListener("pointermove", (e) => moveDrag(e.clientY));
    window.addEventListener("pointerup", endDrag);
    el.addEventListener("touchstart", (e) => {
      if (e.touches.length) startDrag(e.touches[0].clientY);
    }, { passive: true });
    el.addEventListener("touchmove", (e) => {
      if (e.touches.length) moveDrag(e.touches[0].clientY);
    }, { passive: true });
    el.addEventListener("touchend", endDrag);
  }

  onCrosshair(param) {
    const crossEl = $("term-crosshair");
    if (!param || param.time == null) {
      crossEl.style.display = "none";
      return;
    }
    const d = param.seriesData && param.seriesData.get(this.series.main);
    crossEl.style.display = "flex";
    $("ch-time").textContent = lwcTimeToStr(param.time);
    if (d && d.close != null) {
      $("ch-ohlc").textContent = "O " + fmt(d.open) + " H " + fmt(d.high) + " L " + fmt(d.low) + " C " + fmt(d.close);
    } else if (d && d.value != null) {
      $("ch-ohlc").textContent = "P " + fmt(d.value);
    } else {
      $("ch-ohlc").textContent = "O -- H -- L -- C --";
    }
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
  if (state.chart) state.chart.fullRender();
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
    if (state.chart) state.chart.rebuildMain();
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
  state.chart = new LwcChart($("term-chart"), document.querySelector(".term-chart-wrap"));
  window.addEventListener("resize", () => state.chart && state.chart.resize());
  loadBootstrap();
});

})();
