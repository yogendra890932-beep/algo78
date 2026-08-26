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
const TICK_SIZE = 0.05; // NSE/BSE equity-option premium tick size

const state = {
  bootstrap: null,
  selectedSymbol: null,
  tf: "1m",
  chartMode: "12",         // "1"=underlying only, "2"=premium only, "12"=both
  overlays: { ema: true, vwap: false, bb: false, vol: true },
  candles: [],
  candles5m: [],
  premiumCandles: [],
  underlyingCandles: [],
  underlying5m: [],
  underlyingLive: null, // developing 1m underlying bar (not yet closed)
  ema9: [], ema15: [], ema21: [], vwap: [], bbU: [], bbM: [], bbL: [],
  current: null,
  premiumState: null,
  chartOption: null,    // trading_symbol the chart currently references
  underlyingLTP: null,
  positions: [],
  pendingTrades: [],
  tickLTP: {},
  signals: [],
  events: [],
  ws: null,
  wsHealthy: false,
  chart: null,          // premium (option) chart engine instance
  underlyingChart: null,// underlying chart engine instance
  pnlThrottle: 0,
  // ── Chart overlay (Entry/SL/Target) state ──
  overlayKey: null,     // trading_symbol the overlay currently reflects
  overlayBaseline: null,// first-seen backend/strategy SL+target (for Reset)
  overlayPnl: null,     // authoritative backend unrealized PnL when available
  overlayPnlCalc: 0,    // fallback locally-computed PnL
  modifying: null,      // {kind:'sl'|'target'} — concurrency lock while a modify is in flight
  pendingModify: null,  // staged modification awaiting Confirm {kind,value,symbol} | {kind:'reset',sl,tgt,symbol}
  pendingExpireTimer: 0,
  toastTimer: 0,
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
  renderPendingTrades(data.pending_trades || []);
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
  clearStrikeRollRefresh();
  renderWatchlistActive();
  const info = (state.bootstrap && state.bootstrap.symbols || {})[sym];
  renderSelectedSymbol(info);
  renderAtm(info);
  state.premiumCandles = [];
  state.underlyingCandles = [];
  state.underlying5m = [];
  state.underlyingLive = null;
  state.candles = [];
  state.chartOption = null;

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

function premiumShownCandles() {
  return state.tf === "5m" ? aggregate5m(state.premiumCandles) : state.premiumCandles;
}

function handleSnapshot(m) {
  if (m.symbol !== state.selectedSymbol) return;
  state.premiumCandles = (m.premium_candles || []).map(normalizeBar);
  if (state.premiumCandles.length > 0) clearStrikeRollRefresh();
  state.underlyingCandles = (m.candles || []).map(normalizeBar);
  state.underlying5m = (m.candles_5m || []).map(normalizeBar);
  state.underlyingLive = null;
  state.premiumState = m.premium_state || null;
  state.chartOption = (m.premium_state && m.premium_state.trading_symbol) || null;
  state.current = m.premium_current || null;
  state.underlyingLTP = m.underlying ? m.underlying.ltp : null;
  state.candles = premiumShownCandles();
  prepareOverlays(state.candles);

  const info = (state.bootstrap && state.bootstrap.symbols || {})[m.symbol];
  renderSelectedSymbol(Object.assign({}, info, { underlying: m.underlying, premium_state: m.premium_state }));
  renderAtm(Object.assign({}, info, { underlying: m.underlying, itm: m.itm, premium_state: m.premium_state }));
  renderActiveOption(m.premium_state);
  renderStrategyStatus(m);
  renderPositions(m.positions || state.positions);
  renderPendingTrades(m.pending_trades || []);
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
        const prevOpt = state.chartOption;
        state.premiumState = m.state || null;
        const newOpt = (state.premiumState && state.premiumState.trading_symbol) || null;
        state.chartOption = newOpt;
        if (prevOpt && newOpt && newOpt !== prevOpt) {
          state.premiumCandles = [];
          state.current = null;
          state.candles = premiumShownCandles();
          prepareOverlays([]);
          if (state.chart) state.chart.fullRender();
          scheduleStrikeRollRefresh(m.symbol);
        }
        renderActiveOption(state.premiumState);
        const info = (state.bootstrap && state.bootstrap.symbols || {})[m.symbol] || {};
        renderAtm(Object.assign({}, info, { premium_state: state.premiumState }));
      }
      break;
    case "premium_bar":
      if (m.symbol === state.selectedSymbol) {
        pushPremiumBar(m.candle);
      }
      break;
    case "underlying_bar":
      if (m.symbol === state.selectedSymbol) {
        pushUnderlyingBar(m.interval, m.candle);
      }
      break;
    case "current":
      if (m.symbol === state.selectedSymbol) {
        state.current = m.candle || null;
        updatePremiumLast(m.candle);
        schedulePendingLineRefresh();
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
    updateLastUnderlying(m.ltp);
  }
  schedulePendingLineRefresh();
  renderPnlThrottled();
}

let _pendingLineTimer = null;
function schedulePendingLineRefresh() {
  if (_pendingLineTimer) return;
  _pendingLineTimer = setTimeout(() => {
    _pendingLineTimer = null;
    if (state.chart) state.chart.renderPendingMarkers();
  }, 250);
}

/* ─────────────── Strike-roll chart refresh ───────────────
   When the active option rolls to a new strike the worker wipes the
   premium candles and re-warms them over a few seconds, so the snapshot
   pushed alongside the "state" change can be empty. Keep pulling fresh
   snapshots until the new strike's bars actually arrive, otherwise the
   premium chart stays blank/stale until the next minute closes. */
let _strikeRollTimer = null;
let _strikeRollTries = 0;
function scheduleStrikeRollRefresh(symbol) {
  clearStrikeRollRefresh();
  _strikeRollTimer = setTimeout(() => {
    _strikeRollTimer = null;
    _strikeRollTries++;
    if (state.premiumCandles.length > 0 || _strikeRollTries > 45) {
      _strikeRollTries = 0;
      return;
    }
    wsSend({ action: "snapshot", symbol: symbol });
    scheduleStrikeRollRefresh(symbol);
  }, 2000);
}
function clearStrikeRollRefresh() {
  if (_strikeRollTimer) { clearTimeout(_strikeRollTimer); _strikeRollTimer = null; }
  _strikeRollTries = 0;
}

function updateLastUnderlying(ltp) {
  if (ltp == null) return;
  // Maintain a proper developing 1m bar instead of mutating the last
  // closed bar — otherwise the underlying chart never rolls new candles.
  const nowMin = currentIstMinuteStr();
  const live = state.underlyingLive;
  if (!live || live.time !== nowMin) {
    state.underlyingLive = { time: nowMin, open: ltp, high: ltp, low: ltp, close: ltp, volume: 0 };
  } else {
    if (!live.open) live.open = ltp;
    live.close = ltp;
    live.high = Math.max(live.high || 0, ltp);
    live.low = Math.min(live.low == null ? ltp : live.low, ltp);
  }
  if (state.underlyingChart) state.underlyingChart.updateLive(underlyingShownCandles());
}

function pushUnderlyingBar(interval, candle) {
  if (!candle) return;
  const nb = normalizeBar(candle);
  if (interval === "5m") {
    const arr = state.underlying5m;
    const last = arr[arr.length - 1];
    if (last && barKey(last) === barKey(nb)) arr[arr.length - 1] = nb;
    else arr.push(nb);
  } else {
    const arr = state.underlyingCandles;
    const last = arr[arr.length - 1];
    if (last && barKey(last) === barKey(nb)) arr[arr.length - 1] = nb;
    else arr.push(nb);
    // The minute that just closed is now covered by a closed bar — drop
    // any matching developing bar so it doesn't duplicate.
    if (state.underlyingLive && String(state.underlyingLive.time).slice(0, 16) === String(nb.time).slice(0, 16)) {
      state.underlyingLive = null;
    }
  }
  if (state.underlyingChart) state.underlyingChart.setData(underlyingShownCandles());
}

function currentIstMinuteStr() {
  const d = new Date(Date.now() + 330 * 60 * 1000);
  const p = (n) => String(n).padStart(2, "0");
  return d.getUTCFullYear() + "-" + p(d.getUTCMonth() + 1) + "-" + p(d.getUTCDate()) +
         " " + p(d.getUTCHours()) + ":" + p(d.getUTCMinutes());
}

function current5mEndKey() {
  const s = currentIstMinuteStr();
  const hm = s.slice(11, 16);
  const mins = (+hm.slice(0, 2)) * 60 + (+hm.slice(3, 5));
  const end = Math.floor(mins / 5) * 5 + 5;
  return s.slice(0, 11) + String(Math.floor(end / 60)).padStart(2, "0") + ":" + String(end % 60).padStart(2, "0");
}

function buildUnderlyingLive5m() {
  const endKey = current5mEndKey();
  const bucket = endKey.slice(11, 16);
  const bars = [];
  const arr = state.underlyingCandles;
  for (let i = Math.max(0, arr.length - 6); i < arr.length; i++) {
    const b = arr[i];
    const t = String(b.time != null ? b.time : b.minute);
    const m = t.match(/(\d{4}-\d{2}-\d{2})[ T]?(\d{2}):(\d{2})/);
    if (!m) continue;
    const hm = m[2] + ":" + m[3];
    const mins = (+hm.slice(0, 2)) * 60 + (+hm.slice(3, 5));
    const e = Math.floor(mins / 5) * 5 + 5;
    const k = String(Math.floor(e / 60)).padStart(2, "0") + ":" + String(e % 60).padStart(2, "0");
    if (k === bucket) bars.push(b);
  }
  if (state.underlyingLive && state.underlyingLive.close > 0) bars.push(state.underlyingLive);
  if (!bars.length) return null;
  return {
    time: endKey,
    open: bars[0].open,
    high: Math.max.apply(null, bars.map((b) => b.high)),
    low: Math.min.apply(null, bars.map((b) => b.low)),
    close: bars[bars.length - 1].close,
    volume: bars.reduce((a, b) => a + (b.volume || 0), 0)
  };
}

function underlyingShownCandles() {
  if (state.tf === "5m") {
    const arr = state.underlying5m.slice();
    const live5 = buildUnderlyingLive5m();
    if (live5) {
      const last = arr[arr.length - 1];
      const lk = String(last ? (last.time != null ? last.time : last.minute) : "").slice(0, 16);
      if (lk === live5.time) arr[arr.length - 1] = live5;
      else if (!last || lk < live5.time) arr.push(live5);
    }
    return arr;
  }
  const arr = state.underlyingCandles.slice();
  const live = state.underlyingLive;
  if (live && live.close > 0) {
    const last = arr[arr.length - 1];
    const lk = String(last ? (last.time != null ? last.time : last.minute) : "").slice(0, 16);
    if (lk === live.time) arr[arr.length - 1] = live;
    else if (!last || lk < live.time) arr.push(live);
  }
  return arr;
}

/* ─────────────── Events / Signals / Logs ─────────────── */
function handleEvent(m) {
  const evt = String(m.event || "").toUpperCase();
  let msg = m.message || m.msg || m.reason || "";
  const symbol = m.symbol || m.trading_symbol || state.selectedSymbol || "";
  const ts = m.ts || m.timestamp || new Date().toISOString();

  if (evt === "SL_TRAIL") {
    msg = (msg || symbol) + " SL → ₹" + fmt(m.new_sl) + " | LTP ₹" + fmt(m.ltp);
  }

  addEventRow({ ts, evt, msg, symbol });
  addOrderInfo({ ts, evt, msg, symbol });

  if (evt === "SIGNAL") {
    addSignalRow(Object.assign({ ts, event: evt }, m.signal || m));
  }
  if (["ENTRY", "EXIT", "SL_TRAIL", "DIRECTION_FLIP", "PENDING_TRADE"].includes(evt)) {
    setTimeout(loadTrades, 900);
  }
  if (evt === "PENDING_TRADE") {
    refreshPendingTrades();
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
  const name = (premiumState && premiumState.trading_symbol) ? premiumState.trading_symbol : null;
  if (name) {
    el.textContent = "Active option: " + name;
  } else {
    el.textContent = "Active option: --";
  }
  const cl = $("chart-symbol-label");
  if (cl) cl.textContent = name || "--";
  const ul = $("underlying-symbol-label");
  if (ul) ul.textContent = state.selectedSymbol || "--";
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

function pendingPrice() {
  if (state.current && state.current.close != null) return num(state.current.close);
  if (state.premiumState && state.premiumState.ltp != null) return num(state.premiumState.ltp);
  return null;
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
    if (state.chart) state.chart.renderPositionLines();
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
  if (state.chart) state.chart.renderPositionLines();
}

function bindPositionActions(el) {
  const cardSymbol = (node) => {
    const card = node.closest(".term-pos-card");
    const optTxt = card ? card.querySelector(".term-pos-opt").textContent : "";
    return positionSymbolFromOption(optTxt);
  };
  el.querySelectorAll(".pos-sl").forEach((inp) => {
    inp.addEventListener("change", () => sendOrder("modify_sl", inp.value, cardSymbol(inp)));
  });
  el.querySelectorAll(".pos-tgt").forEach((inp) => {
    inp.addEventListener("change", () => sendOrder("modify_target", inp.value, cardSymbol(inp)));
  });
  el.querySelectorAll(".pos-sq").forEach((btn) => {
    btn.addEventListener("click", () => sendOrder("squareoff", null, cardSymbol(btn)));
  });
}

function positionSymbolFromOption(optTxt) {
  const sym = state.positions.find((p) => {
    const pos = positionData(p);
    return (pos.trading_symbol || "") === optTxt;
  });
  return (sym && positionData(sym).symbol) || state.selectedSymbol;
}

/* ─────────────── Pending trades (semi-auto approval) ─────────────── */
async function refreshPendingTrades() {
  const r = await apiFetch("/api/users/pending-trades");
  if (!r) return;
  try {
    const data = await r.json();
    renderPendingTrades(Array.isArray(data) ? data : []);
  } catch (e) { }
}

function pendingOptionLabel(p) {
  const sym = String(p.symbol || "").toUpperCase();
  const hasStrike = /\b\d{4,6}\b/.test(sym) && /\b(CE|PE)\b/.test(sym);
  if (hasStrike) return sym;
  const opt = p.opt_type || "";
  return sym + (opt ? " " + opt : "");
}

function renderPendingTrades(pending) {
  state.pendingTrades = pending || [];
  if (state.pendingExpireTimer) { clearTimeout(state.pendingExpireTimer); state.pendingExpireTimer = null; }
  const el = $("pending-list");
  const count = $("pending-count");
  count.textContent = state.pendingTrades.length;
  if (!state.pendingTrades.length) {
    if (_pendingCountdownTimer) { clearInterval(_pendingCountdownTimer); _pendingCountdownTimer = null; }
    el.innerHTML = '<div class="term-empty">No pending trades</div>';
    if (state.chart) state.chart.renderPendingMarkers();
    return;
  }
  el.innerHTML = "";
  state.pendingTrades.forEach((p) => {
    const card = document.createElement("div");
    card.className = "term-pending-card";
    card.dataset.tradeId = p.id;
    const label = pendingOptionLabel(p);
    const sideCls = String(p.opt_type || "CE").toUpperCase() === "PE" ? "down" : "up";
    const expMs = p.expires_at ? (new Date(p.expires_at).getTime() - Date.now()) : null;
    const expSecs = expMs != null && expMs > 0 ? Math.max(1, Math.ceil(expMs / 1000)) : null;
    card.innerHTML =
      '<div class="term-pos-top">' +
      '<span class="term-pos-opt">' + esc(label) + "</span>" +
      '<span class="term-pos-side ' + sideCls + '">' + esc(String(p.opt_type || "").toUpperCase() || "BUY") + "</span>" +
      "</div>" +
      '<div class="term-pos-meta">' +
      "<span>Strategy</span><b>" + esc(p.strategy || "--") + "</b>" +
      "<span>Qty</span><b>" + fmt(p.quantity, 0) + "</b>" +
      "</div>" +
      (expSecs != null ? '<div class="term-pending-exp">Auto-expires in <span class="pending-timer">' + expSecs + 's</span></div>' : "") +
      '<div class="term-pending-actions">' +
      '<button class="term-btn term-btn-xs term-btn-primary p-approve">Approve</button>' +
      '<button class="term-btn term-btn-xs term-btn-danger p-reject">Reject</button>' +
      "</div>";
    el.appendChild(card);
  });
  bindPendingActions(el);
  if (state.chart) state.chart.renderPendingMarkers();

  const first = state.pendingTrades[0];
  if (first && first.expires_at) {
    const until = new Date(first.expires_at).getTime() - Date.now();
    if (until > 0) {
      state.pendingExpireTimer = setTimeout(() => {
        state.pendingExpireTimer = null;
        refreshPendingTrades();
      }, until + 300);
      startPendingCountdown(until);
    } else {
      setTimeout(refreshPendingTrades, 400);
    }
  }
}

let _pendingCountdownTimer = null;
function startPendingCountdown(ms) {
  if (_pendingCountdownTimer) { clearInterval(_pendingCountdownTimer); _pendingCountdownTimer = null; }
  const el = document.querySelector(".pending-timer");
  const t0 = Date.now();
  _pendingCountdownTimer = setInterval(() => {
    const remain = Math.max(0, Math.ceil((ms - (Date.now() - t0)) / 1000));
    if (el) el.textContent = remain + "s";
    if (remain <= 0 && _pendingCountdownTimer) {
      clearInterval(_pendingCountdownTimer);
      _pendingCountdownTimer = null;
    }
  }, 250);
}

function bindPendingActions(el) {
  el.querySelectorAll(".p-approve").forEach((btn) => {
    btn.addEventListener("click", () => {
      const id = btn.closest(".term-pending-card").dataset.tradeId;
      approvePendingTrade(id);
    });
  });
  el.querySelectorAll(".p-reject").forEach((btn) => {
    btn.addEventListener("click", () => {
      const id = btn.closest(".term-pending-card").dataset.tradeId;
      rejectPendingTrade(id);
    });
  });
}

async function approvePendingTrade(id) {
  if (!id) return;
  toast("Approving trade…", "processing");
  const r = await apiFetch("/api/users/pending-trades/" + encodeURIComponent(id) + "/approve", {
    method: "POST", headers: { "Content-Type": "application/json" }, body: "{}"
  });
  if (!r) return;
  let data = null;
  try { data = await r.json(); } catch (e) { }
  if (!r.ok) {
    const err = (data && (data.detail || (data.errors && data.errors[0]))) || "Approval failed";
    toast(String(err), "err");
    refreshPendingTrades();
    return;
  }
  toast("Trade approved", "ok");
  refreshPendingTrades();
  wsSend({ action: "positions" });
  setTimeout(loadTrades, 900);
}

async function rejectPendingTrade(id) {
  if (!id) return;
  toast("Rejecting trade…", "processing");
  const r = await apiFetch("/api/users/pending-trades/" + encodeURIComponent(id) + "/reject", {
    method: "POST", headers: { "Content-Type": "application/json" }, body: "{}"
  });
  if (!r) return;
  let data = null;
  try { data = await r.json(); } catch (e) { }
  if (!r.ok) {
    const err = (data && (data.detail || (data.errors && data.errors[0]))) || "Rejection failed";
    toast(String(err), "err");
    refreshPendingTrades();
    return;
  }
  toast("Trade rejected", "ok");
  refreshPendingTrades();
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
  updatePositionChipLive(selectedPosition());
}

/* ─────────────── Orders ─────────────── */
async function sendOrder(action, value, symbol, opts) {
  const sym = symbol || state.selectedSymbol;
  const isModify = action === "modify_sl" || action === "modify_target";
  if (isModify) {
    if (state.modifying) { toast("A SL/Target modification is already in progress", "err"); return; }
    state.modifying = { kind: action === "modify_sl" ? "sl" : "target" };
    updateChipProcessing(state.modifying.kind);
  }
  const body = { action: action, symbol: sym };
  if (value != null) body.value = Number(value);
  try {
    const r = await apiFetch("/api/terminal/order", { method: "POST", body: JSON.stringify(body) });
    if (!r) return;
    const data = await r.json();
    const ts = new Date().toISOString();
    if (!r.ok) {
      const err = (data && (data.detail || (data.errors && data.errors[0]))) || action + " failed";
      addOrderInfo({ ts: ts, evt: "ERROR", msg: sym + " → " + err });
      toast(err, "err");
      return;
    }
    if (data.queued) {
      addOrderInfo({ ts: ts, evt: action.toUpperCase(), msg: sym + " → queued" });
      toast("Request queued — it will apply shortly", "processing");
      return;
    }
    if (data.changed && data.changed.length) {
      const names = data.changed
        .map((c) => (typeof c === "object" && c ? (c.symbol || c.trading_symbol || "") : c))
        .filter(Boolean);
      addOrderInfo({ ts: ts, evt: action.toUpperCase(), msg: sym + " → applied (" + (names.length ? names.join(", ") : "ok") + ")" });
      toast(isModify ? (action === "modify_sl" ? "SL updated" : "Target updated") : "Applied", "ok");
    } else if (data.errors && data.errors.length) {
      addOrderInfo({ ts: ts, evt: "ERROR", msg: sym + " → " + data.errors[0] });
      toast(data.errors[0], "err");
    } else {
      addOrderInfo({ ts: ts, evt: action.toUpperCase(), msg: sym + " → ok" });
      toast("Applied", "ok");
    }
  } finally {
    if (isModify) {
      state.modifying = null;
      updateChipProcessing(null);
    }
    if (state.chart) state.chart.renderPositionLines();
  }
}

/* ─────────────── Overlay math (presentation only) ─────────────── */
function computeOverlays(candles) {
  const n = candles.length;
  const out = { ema9: [], ema15: [], ema21: [], vwap: [], bbU: [], bbM: [], bbL: [] };
  if (!n) return out;
  const closes = candles.map((c) => num(c.close) || 0);
  const ema = (span) => {
    const r = []; let prev = closes[0];
    const k = 2 / (span + 1);
    for (let i = 0; i < n; i++) {
      prev = i === 0 ? closes[0] : closes[i] * k + prev * (1 - k);
      r.push(prev);
    }
    return r;
  };
  out.ema9 = ema(9); out.ema15 = ema(15); out.ema21 = ema(21);

  let cumPV = 0, cumV = 0;
  for (let i = 0; i < n; i++) {
    const tp = (num(candles[i].high) || 0) + (num(candles[i].low) || 0) + (num(candles[i].close) || 0);
    const v = num(candles[i].volume) || 0;
    cumPV += (tp / 3) * v; cumV += v;
    out.vwap.push(cumV ? cumPV / cumV : 0);
  }
  for (let i = 0; i < n; i++) {
    if (i < 19) { out.bbU.push(null); out.bbM.push(null); out.bbL.push(null); continue; }
    const win = closes.slice(i - 19, i + 1);
    const mean = win.reduce((a, b) => a + b, 0) / 20;
    const varr = win.reduce((a, b) => a + (b - mean) * (b - mean), 0) / 20;
    const sd = Math.sqrt(varr);
    out.bbM.push(mean);
    out.bbU.push(mean + 2 * sd);
    out.bbL.push(mean - 2 * sd);
  }
  return out;
}

function prepareOverlays(candles) {
  const o = computeOverlays(candles);
  state.ema9 = o.ema9; state.ema15 = o.ema15; state.ema21 = o.ema21;
  state.vwap = o.vwap; state.bbU = o.bbU; state.bbM = o.bbM; state.bbL = o.bbL;
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

function lwcTimeToStr(t) {
  if (t == null) return "--";
  const n = Number(t);
  if (isNaN(n)) return String(t);
  const d = new Date(n * 1000 + 330 * 60 * 1000);
  return String(d.getUTCHours()).padStart(2, "0") + ":" + String(d.getUTCMinutes()).padStart(2, "0");
}

function lwcTickMarkFormatter(t, tickMarkType) {
  if (t == null) return "";
  const n = Number(t);
  if (isNaN(n)) return String(t);
  const d = new Date(n * 1000 + 330 * 60 * 1000);
  const hh = String(d.getUTCHours()).padStart(2, "0");
  const mm = String(d.getUTCMinutes()).padStart(2, "0");
  const ss = String(d.getUTCSeconds()).padStart(2, "0");
  const mon = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"][d.getUTCMonth()];
  if (tickMarkType === 0) return String(d.getUTCFullYear());
  if (tickMarkType === 1) return mon;
  if (tickMarkType === 2) return d.getUTCDate() + " " + mon;
  if (tickMarkType === 4) return hh + ":" + mm + ":" + ss;
  return hh + ":" + mm;
}

function lwcPoints(points) {
  // Lightweight Charts requires ascending, unique times. Stale bars written
  // out-of-order (e.g. previous-session close after a worker restart) would
  // otherwise throw on setData — so sort ascending and dedupe keeping last.
  const seen = {};
  for (const p of points) {
    if (p == null || p.time == null) continue;
    seen[p.time] = p;
  }
  return Object.keys(seen).sort((a, b) => Number(a) - Number(b)).map((t) => seen[t]);
}

/* ─────────────── Risk / reward zone primitive ───────────────
   Draws semi-transparent bands between Entry↔SL (red) and
   Entry↔Target (green) directly on the main series pane. Native
   series primitives stay aligned during pan/zoom/resize. */
class ZoneBandPrimitive {
  constructor(series) {
    this._series = series;
    this._levels = [];
  }
  setLevels(levels) { this._levels = levels || []; }
  paneViews() { return [new ZoneBandView(this)]; }
}

class ZoneBandView {
  constructor(band) { this._band = band; }
  renderer() { return new ZoneBandRenderer(this._band); }
}

class ZoneBandRenderer {
  constructor(band) { this._band = band; }
  draw(target) {
    target.useBitmapCoordinateSpace((scope) => {
      const series = this._band._series;
      const ctx = scope.context;
      const w = scope.mediaSize.width * scope.horizontalPixelRatio;
      const h = scope.mediaSize.height * scope.verticalPixelRatio;
      const levels = this._band._levels || [];
      for (const z of levels) {
        if (z.from == null || z.to == null) continue;
        let y1, y2;
        try { y1 = series.priceToCoordinate(z.from); y2 = series.priceToCoordinate(z.to); } catch (e) { continue; }
        if (y1 == null || y2 == null) continue;
        const top = Math.min(y1, y2);
        const bh = Math.abs(y1 - y2);
        if (bh < 0.5) continue;
        ctx.fillStyle = z.color;
        ctx.fillRect(0, Math.max(0, top * scope.verticalPixelRatio), w, Math.min(bh, h) * scope.verticalPixelRatio);
      }
    });
  }
}

class LwcChart {
  constructor(container, wrap) {
    this.el = container;
    this.wrap = wrap;
    this.chart = null;
    this.series = {};
    this.priceLines = { entry: null, sl: null, tgt: null, ltp: null, drag: null };
    this.pendingLines = [];
    this.overlay = { entry: null, sl: null, tgt: null, ltp: null, side: "BUY", qty: null, symbol: null, tradingSymbol: null };
    this.zoneState = { zones: [] };
    this.zonePrimitive = null;
    this._zoneKey = "";
    this.drag = null;         // active drag {kind, value, base, symbol, side}
    this._fitted = false;
    this._lastFirst = null;
    this._scrollLocked = false;
    this._lastDragClientX = 0;
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
        barSpacing: 7,
        tickMarkFormatter: (t, tm) => lwcTickMarkFormatter(t, tm)
      },
      crosshair: {
        mode: C.CrosshairMode.Normal,
        vertLine: { color: "rgba(139,153,190,0.4)", labelBackgroundColor: "#1e2537" },
        horzLine: { color: "rgba(139,153,190,0.4)", labelBackgroundColor: "#1e2537" }
      },
      localization: {
        locale: "en-IN",
        timeFormatter: (t) => lwcTimeToStr(t)
      }
    });
    this.rebuildMain();
    this.ensureOverlaySeries();
    this.chart.subscribeCrosshairMove((p) => this.onCrosshair(p));
    this.bindPointerEvents();
  }

  rebuildMain() {
    if (this.drag) {
      this.drag = null;
      this.hideDragPreview();
      this.setScrollLocked(false);
    }
    const old = this.series.main;
    if (old) {
      if (this.zonePrimitive) { try { old.detachPrimitive(this.zonePrimitive); } catch (e) { } }
      try { this.chart.removeSeries(old); } catch (e) { }
      this.series.main = null;
    }
    this.series.main = this.chart.addCandlestickSeries({
      upColor: "#22c55e", downColor: "#ef4444",
      borderVisible: false, wickUpColor: "#22c55e", wickDownColor: "#ef4444",
      priceLineVisible: true, lastValueVisible: true
    });
    // price lines / primitive are attached to the (new) main series
    for (const k of Object.keys(this.priceLines)) this.priceLines[k] = null;
    this.pendingLines = [];
    for (const k of Object.keys(this.overlay)) this.overlay[k] = null;
    this.zonePrimitive = null;
    this._zoneKey = "";
    this._fitted = false;
    this.ensureZonePrimitive();
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
    main.setData(lwcPoints(candles.map(barToLwc).filter(Boolean)));
    const ov = state.overlays;
    this.series.vol.setData(lwcPoints(ov.vol ? candles.map(volToLwc).filter(Boolean) : []));
    this.series.ema9.setData(lwcPoints(ov.ema ? lineToLwc(state.ema9, candles) : []));
    this.series.ema15.setData(lwcPoints(ov.ema ? lineToLwc(state.ema15, candles) : []));
    this.series.ema21.setData(lwcPoints(ov.ema ? lineToLwc(state.ema21, candles) : []));
    this.series.vwap.setData(lwcPoints(ov.vwap ? lineToLwc(state.vwap, candles) : []));
    this.series.bbU.setData(lwcPoints(ov.bb ? lineToLwc(state.bbU, candles) : []));
    this.series.bbM.setData(lwcPoints(ov.bb ? lineToLwc(state.bbM, candles) : []));
    this.series.bbL.setData(lwcPoints(ov.bb ? lineToLwc(state.bbL, candles) : []));
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
    const b = barToLwc(last);
    if (b) this.series.main.update(b);
    if (state.overlays.vol) {
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
    this.renderPositionLines();
  }

  setData() { this.updateLive(); }

  resize() {
    try { this.chart.applyOptions({ width: this.el.clientWidth, height: this.el.clientHeight }); } catch (e) { }
  }

  renderPositionLines() {
    const main = this.series.main;
    if (!main) return;
    const pos = selectedPosition();
    const ltp = pos ? livePrice(pos) : null;
    this.overlay.entry = pos ? num(pos.entry_price) : null;
    this.overlay.sl = pos ? num(pos.sl_trigger) : null;
    this.overlay.tgt = pos ? num(pos.target) : null;
    this.overlay.ltp = ltp != null ? num(ltp) : null;
    this.overlay.side = pos ? String(pos.side || "BUY").toUpperCase() : "BUY";
    this.overlay.qty = pos ? num(pos.qty) : null;
    this.overlay.symbol = pos ? (pos.symbol || state.selectedSymbol) : null;
    this.overlay.tradingSymbol = pos ? (pos.trading_symbol || "") : "";

    // Entry / SL / Target / LTP as native price lines — they stay anchored
    // to the price scale while the chart pans/zooms.
    this._syncPriceLine("entry", this.overlay.entry, "#38bdf8", 1, 1, "Entry");
    this._syncPriceLine("sl", this.overlay.sl, "#ef4444", 1, 2, "SL");
    this._syncPriceLine("tgt", this.overlay.tgt, "#22c55e", 1, 2, "TGT");
    this._syncPriceLine("ltp", this.overlay.ltp, "#facc15", 1, 0, "LTP");

    this.renderPendingMarkers();

    this.updateZones();
    renderPositionChip();
  }

  _syncPriceLine(key, price, color, lineWidth, lineStyle, title) {
    const main = this.series.main;
    if (!main) return;
    const existing = this.priceLines[key];
    if (price == null) {
      if (existing) {
        try { main.removePriceLine(existing); } catch (e) { }
        this.priceLines[key] = null;
      }
      return;
    }
    const cfg = { price: price, color: color, lineWidth: lineWidth, lineStyle: lineStyle, axisLabelVisible: true, title: title };
    if (existing) {
      try { existing.applyOptions(cfg); } catch (e) { }
    } else {
      try { this.priceLines[key] = main.createPriceLine(cfg); } catch (e) { }
    }
  }

  // Pending semi-auto trade for the selected symbol → dynamic dashed
  // amber line tracking the current LTP. Drawn only while the trade is
  // still WAITING — it is removed automatically once the user approves
  // (becomes a position overlay) or the approval window expires.
  renderPendingMarkers() {
    const main = this.series.main;
    if (!main) return;
    const sym = String(state.selectedSymbol || "").toUpperCase();
    const pending = (state.pendingTrades || []).filter((p) => {
      if (String(p.status || "").toUpperCase() !== "WAITING") return false;
      const ps = String(p.symbol || "").toUpperCase();
      return ps === sym || ps.startsWith(sym + " ");
    });
    while (this.pendingLines.length > pending.length) {
      const pl = this.pendingLines.pop();
      try { main.removePriceLine(pl); } catch (e) { }
    }
    pending.forEach((p, i) => {
      const price = pendingPrice();
      if (price == null) return;
      const cfg = {
        price: price, color: "#f59e0b", lineWidth: 1, lineStyle: 3,
        axisLabelVisible: true,
        title: "PENDING " + (p.strategy || "").toUpperCase()
      };
      const existing = this.pendingLines[i];
      if (existing) {
        try { existing.applyOptions(cfg); } catch (e) { }
      } else {
        try { this.pendingLines[i] = main.createPriceLine(cfg); } catch (e) { }
      }
    });
    this.pendingLines.length = pending.length;
  }

  ensureZonePrimitive() {
    const main = this.series.main;
    if (!main || this.zonePrimitive) return;
    try {
      this.zonePrimitive = new ZoneBandPrimitive(main);
      main.attachPrimitive(this.zonePrimitive);
    } catch (e) {
      this.zonePrimitive = null;
    }
  }

  updateZones() {
    const o = this.overlay;
    const levels = [];
    if (o.sl != null && o.entry != null && o.sl !== o.entry) {
      levels.push({ from: o.sl, to: o.entry, color: "rgba(239,68,68,0.10)" });
    }
    if (o.entry != null && o.tgt != null && o.entry !== o.tgt) {
      levels.push({ from: o.entry, to: o.tgt, color: "rgba(34,197,94,0.10)" });
    }
    this.zoneState.zones = levels;
    if (this.zonePrimitive) this.zonePrimitive.setLevels(levels);
    const key = JSON.stringify(levels.map((z) => [z.from, z.to]));
    if (key !== this._zoneKey) {
      this._zoneKey = key;
      this._touch();
    }
  }

  // Force a repaint so the zone primitive re-renders even when no live
  // tick flows (e.g. bot paused right after a SL/Target change).
  _touch() {
    const main = this.series.main;
    if (!main) return;
    try {
      const p = this.overlay.ltp != null ? this.overlay.ltp : (this.overlay.entry != null ? this.overlay.entry : 0);
      const pl = main.createPriceLine({ price: p, color: "#000000", lineWidth: 0, axisLabelVisible: false });
      main.removePriceLine(pl);
    } catch (e) { }
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
    el.textContent = kindTxt + ": " + fmt(this.drag.value) + "  (release to preview)";
  }

  showDragPreview(price, clientX, clientY) {
    const el = $("term-drag-preview");
    if (!el || !this.drag) return;
    const check = validateLevel(this.drag.kind, price, this.drag);
    const kindTxt = this.drag.kind === "sl" ? "SL" : "TARGET";
    el.innerHTML =
      '<div class="tp-row"><span>' + kindTxt + "</span><span class='tp-price'>" + fmt(price) + "</span></div>" +
      '<div class="tp-row"><span>From</span><span>' + fmt(this.drag.base) + "</span></div>" +
      (check.rr != null ? '<div class="tp-row"><span>RR</span><span class="' + (check.valid ? "tp-valid" : "tp-invalid") + '">1 : ' + check.rr.toFixed(2) + "</span></div>" : "") +
      '<div class="tp-row"><span class="' + (check.valid ? "tp-valid" : "tp-invalid") + '">' + (check.valid ? "Valid — release to modify" : check.reason) + "</span></div>";
    el.hidden = false;
    const wrap = this.wrap;
    if (wrap) {
      const wr = wrap.getBoundingClientRect();
      const left = clientX - wr.left;
      const top = clientY - wr.top;
      el.style.left = Math.max(8, Math.min(left, wr.width - 40)) + "px";
      el.style.top = Math.max(24, Math.min(top, wr.height - 60)) + "px";
    }
  }

  hideDragPreview() {
    const el = $("term-drag-preview");
    if (el) { el.hidden = true; el.innerHTML = ""; }
  }

  setScrollLocked(locked) {
    if (this._scrollLocked === locked) return;
    this._scrollLocked = locked;
    try { this.chart.applyOptions({ handleScroll: !locked, handleScale: !locked }); } catch (e) { }
  }

  bindPointerEvents() {
    const el = this.el;
    const startDrag = (clientX, clientY) => {
      const pos = selectedPosition();
      if (!pos) return;
      if (!this.series.main) return;
      if (state.modifying) { toast("A SL/Target modification is already in progress", "err"); return; }
      const rect = el.getBoundingClientRect();
      const y = clientY - rect.top;
      const sl = num(pos.sl_trigger), tg = num(pos.target);
      const near = (p) => (p != null && this.series.main.priceToCoordinate(p) != null && Math.abs(this.series.main.priceToCoordinate(p) - y) <= 12);
      let kind = null;
      if (near(sl)) kind = "sl";
      else if (near(tg)) kind = "target";
      if (!kind) return;
      this.drag = {
        kind: kind,
        value: kind === "sl" ? sl : tg,
        base: kind === "sl" ? sl : tg,
        symbol: pos.symbol || state.selectedSymbol,
        side: String(pos.side || "BUY").toUpperCase()
      };
      this._lastDragClientX = clientX;
      el.style.cursor = "row-resize";
      this.setScrollLocked(true);
      this.renderDragHint();
      this.showDragPreview(this.drag.value, clientX, clientY);
    };
    const moveDrag = (clientX, clientY) => {
      if (!this.drag) return;
      const rect = el.getBoundingClientRect();
      const price = this.series.main.coordinateToPrice(clientY - rect.top);
      if (price == null) return;
      const snapped = snapTick(price);
      this.drag.value = snapped;
      this._lastDragClientX = clientX;
      this.setDragLine(snapped, this.drag.kind);
      this.renderDragHint();
      this.showDragPreview(snapped, clientX, clientY);
    };
    const endDrag = () => {
      if (!this.drag) return;
      const d = this.drag;
      this.drag = null;
      this.clearDragLine();
      el.style.cursor = "crosshair";
      this.setScrollLocked(false);
      hideDragHint();
      this.hideDragPreview();
      if (d.value == null || !isFinite(d.value)) { this.renderPositionLines(); return; }
      const v = snapTick(d.value);
      if (v === d.base) { this.renderPositionLines(); return; }
      const check = validateLevel(d.kind, v, d);
      if (!check.valid) {
        toast(check.reason, "err");
        this.renderPositionLines();
        return;
      }
      // Instant modify on release — no confirmation modal.
      const action = d.kind === "sl" ? "modify_sl" : "modify_target";
      const pos = selectedPosition();
      if (pos) {
        if (d.kind === "sl") pos.sl_trigger = v;
        else pos.target = v;
      }
      this.renderPositionLines();
      sendOrder(action, v, d.symbol, { source: "overlay" });
    };
    el.addEventListener("pointerdown", (e) => startDrag(e.clientX, e.clientY));
    el.addEventListener("pointermove", (e) => moveDrag(e.clientX, e.clientY));
    window.addEventListener("pointerup", endDrag);
    el.addEventListener("touchstart", (e) => {
      if (e.touches.length) startDrag(e.touches[0].clientX, e.touches[0].clientY);
    }, { passive: true });
    el.addEventListener("touchmove", (e) => {
      if (e.touches.length) moveDrag(e.touches[0].clientX, e.touches[0].clientY);
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

/* ─────────────── Overlay: validation & PnL math ─────────────── */
function snapTick(v) {
  const n = num(v);
  if (n == null) return null;
  return Number((Math.round(n / TICK_SIZE) * TICK_SIZE).toFixed(2));
}

function computeRR(side, entry, sl, tgt) {
  if (entry == null) return null;
  const risk = side === "SELL"
    ? (sl != null ? sl - entry : null)
    : (sl != null ? entry - sl : null);
  const reward = side === "SELL"
    ? (tgt != null ? entry - tgt : null)
    : (tgt != null ? tgt - entry : null);
  if (risk == null || reward == null || risk <= 0) return null;
  return reward / risk;
}

function validateLevel(kind, value, d) {
  const pos = selectedPosition();
  const entry = pos ? num(pos.entry_price) : null;
  const sl = pos ? num(pos.sl_trigger) : null;
  const tgt = pos ? num(pos.target) : null;
  if (entry == null) return { valid: false, reason: "No position entry available" };
  const side = String((d && d.side) || (pos && pos.side) || "BUY").toUpperCase();
  let effSl = sl, effTgt = tgt;
  if (kind === "sl") {
    effSl = value;
  } else {
    effTgt = value;
  }
  if (kind === "sl") {
    if (side === "SELL") {
      if (value <= entry) return { valid: false, reason: "SELL SL must stay above entry " + fmt(entry) };
      if (tgt != null && value <= tgt) return { valid: false, reason: "SELL SL must stay above target " + fmt(tgt) };
    } else {
      if (value >= entry) return { valid: false, reason: "SL must stay below entry " + fmt(entry) };
      if (tgt != null && value >= tgt) return { valid: false, reason: "SL must stay below target " + fmt(tgt) };
    }
  } else {
    if (side === "SELL") {
      if (value >= entry) return { valid: false, reason: "SELL target must stay below entry " + fmt(entry) };
      if (sl != null && value >= sl) return { valid: false, reason: "SELL target must stay below SL " + fmt(sl) };
    } else {
      if (value <= entry) return { valid: false, reason: "Target must stay above entry " + fmt(entry) };
      if (sl != null && value <= sl) return { valid: false, reason: "Target must stay above SL " + fmt(sl) };
    }
  }
  const rr = computeRR(side, entry, effSl, effTgt);
  return { valid: true, reason: "", rr: rr };
}

/* ─────────────── Underlying chart engine (left panel) ─────────────── */
class UnderlyingChart {
  constructor(container, wrap) {
    this.el = container;
    this.wrap = wrap;
    this.chart = null;
    this.series = {};
    this.ltpLine = null;
    this._fitted = false;
    this._lastFirst = null;
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
        barSpacing: 7,
        tickMarkFormatter: (t, tm) => lwcTickMarkFormatter(t, tm)
      },
      crosshair: {
        mode: C.CrosshairMode.Normal,
        vertLine: { color: "rgba(139,153,190,0.4)", labelBackgroundColor: "#1e2537" },
        horzLine: { color: "rgba(139,153,190,0.4)", labelBackgroundColor: "#1e2537" }
      },
      localization: {
        locale: "en-IN",
        timeFormatter: (t) => lwcTimeToStr(t)
      }
    });

    this.series.main = this.chart.addCandlestickSeries({
      upColor: "#22c55e", downColor: "#ef4444",
      borderVisible: false, wickUpColor: "#22c55e", wickDownColor: "#ef4444",
      priceLineVisible: true, lastValueVisible: true
    });
    if (!this.series.vol) {
      this.series.vol = this.chart.addHistogramSeries({
        priceFormat: { type: "volume" },
        priceScaleId: "vol",
        lastValueVisible: false, priceLineVisible: false
      });
      try { this.chart.priceScale("vol").applyOptions({ scaleMargins: { top: 0.85, bottom: 0 } }); } catch (e) { }
    }
    const mk = (key, color, opts) => {
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

    this.chart.subscribeCrosshairMove((p) => this.onCrosshair(p));
  }

  setData(candles) {
    if (!this.chart) return;
    const list = candles || [];
    this.series.main.setData(lwcPoints(list.map(barToLwc).filter(Boolean)));
    this.applyOverlays(list, false);
    this.syncLtpLine();
    const first = list.length ? barTime(list[0]) : null;
    if (!this._fitted || first !== this._lastFirst) {
      this._fitted = true;
      this._lastFirst = first;
      try { this.chart.timeScale().fitContent(); } catch (e) { }
    }
  }

  updateLive(candles) {
    if (!this.chart) return;
    const list = candles || [];
    const last = list[list.length - 1];
    const time = last ? toLwcTime(barTime(last)) : null;
    if (last && time != null) {
      const i = list.length - 1;
      const b = barToLwc(last);
      if (b) this.series.main.update(b);
      if (state.overlays.vol) {
        const v = volToLwc(last);
        if (v) this.series.vol.update(v);
      }
      const O = computeOverlays(list);
      if (state.overlays.ema) {
        const u = (key, arr) => {
          const v = arr[i];
          if (v != null && isFinite(v)) this.series[key].update({ time: time, value: v });
        };
        u("ema9", O.ema9); u("ema15", O.ema15); u("ema21", O.ema21);
      }
      if (state.overlays.vwap && O.vwap[i] != null) this.series.vwap.update({ time: time, value: O.vwap[i] });
      if (state.overlays.bb) {
        if (O.bbU[i] != null) this.series.bbU.update({ time: time, value: O.bbU[i] });
        if (O.bbM[i] != null) this.series.bbM.update({ time: time, value: O.bbM[i] });
        if (O.bbL[i] != null) this.series.bbL.update({ time: time, value: O.bbL[i] });
      }
    }
    this.syncLtpLine();
  }

  applyOverlays(list, live) {
    const O = computeOverlays(list);
    const pt = (arr) => lwcPoints(lineToLwc(arr, list));
    this.series.vol.setData(lwcPoints(state.overlays.vol ? list.map(volToLwc).filter(Boolean) : []));
    this.series.ema9.setData(lwcPoints(state.overlays.ema ? pt(O.ema9) : []));
    this.series.ema15.setData(lwcPoints(state.overlays.ema ? pt(O.ema15) : []));
    this.series.ema21.setData(lwcPoints(state.overlays.ema ? pt(O.ema21) : []));
    this.series.vwap.setData(lwcPoints(state.overlays.vwap ? pt(O.vwap) : []));
    this.series.bbU.setData(lwcPoints(state.overlays.bb ? pt(O.bbU) : []));
    this.series.bbM.setData(lwcPoints(state.overlays.bb ? pt(O.bbM) : []));
    this.series.bbL.setData(lwcPoints(state.overlays.bb ? pt(O.bbL) : []));
  }

  syncLtpLine() {
    const ltp = state.underlyingLTP;
    if (ltp == null) {
      if (this.ltpLine) {
        try { this.series.main.removePriceLine(this.ltpLine); } catch (e) { }
        this.ltpLine = null;
      }
      return;
    }
    if (!this.ltpLine) {
      try {
        this.ltpLine = this.series.main.createPriceLine({
          price: ltp, color: "#facc15", lineWidth: 1, lineStyle: 0,
          axisLabelVisible: true, title: "LTP"
        });
      } catch (e) { }
    } else {
      try { this.ltpLine.applyOptions({ price: ltp }); } catch (e) { }
    }
  }

  onCrosshair(p) {
    const time = p.time != null ? lwcTimeToStr(p.time) : "--";
    const tEl = $("u-ch-time");
    if (tEl) tEl.textContent = time;
    let bar = null;
    if (p.seriesData) bar = p.seriesData.get(this.series.main) || null;
    const oEl = $("u-ch-ohlc");
    if (oEl) {
      oEl.textContent = bar
        ? "O " + fmt(bar.open) + " H " + fmt(bar.high) + " L " + fmt(bar.low) + " C " + fmt(bar.close)
        : "O -- H -- L -- C --";
    }
  }

  resize() {
    try { this.chart.applyOptions({ width: this.el.clientWidth, height: this.el.clientHeight }); } catch (e) { }
  }
}

/* ─────────────── Overlay: position chip on the chart ─────────────── */
function renderPositionChip() {
  const el = $("term-position-chip");
  const pos = selectedPosition();
  if (!pos) {
    if (el) el.hidden = true;
    if (el) el.innerHTML = "";
    state.overlayKey = null;
    state.overlayBaseline = null;
    state.overlayPnl = null;
    state.overlayPnlCalc = 0;
    return;
  }
  const key = (pos.trading_symbol || "") + "|" + fmt(pos.entry_price) + "|" + num(pos.qty);
  if (state.overlayKey !== key) {
    state.overlayKey = key;
    state.overlayBaseline = { sl: num(pos.sl_trigger), tgt: num(pos.target) };
    state.overlayPnl = num(pos.unrealized_pnl);
    el.hidden = false;
    el.innerHTML = chipHtml(pos);
    bindChipActions(el, pos);
  } else {
    el.hidden = false;
  }
  updatePositionChipLive(pos);
}

function chipHtml(pos) {
  const side = String(pos.side || "BUY").toUpperCase();
  const opt = pos.trading_symbol || (pos.symbol || "");
  return (
    '<div class="term-chip-row">' +
    '<span class="term-chip-entry">' + esc(opt) + ' <span class="term-pos-side ' + side + '">' + side + "</span></span>" +
    '<span class="term-chip-pnl" id="chip-pnl"></span>' +
    "</div>" +
    '<div class="term-chip-meta">' +
    '<span>Qty <b id="chip-qty">--</b></span>' +
    '<span>Entry <b id="chip-entry">--</b></span>' +
    '<span>LTP <b id="chip-ltp">--</b></span>' +
    '<span>SL <b id="chip-sl">--</b></span>' +
    '<span>TGT <b id="chip-tgt">--</b></span>' +
    '<span>Near <b id="chip-near">--</b></span>' +
    "</div>" +
    '<div class="term-chip-row">' +
    '<span class="term-chip-rr">RR <b id="chip-rr">--</b></span>' +
    '<span class="term-chip-processing" id="chip-processing"></span>' +
    "</div>" +
    '<div class="term-chip-actions">' +
    '<button class="term-btn term-btn-xs' + (pos.trail_enabled !== false ? "" : " term-btn-danger") + '" id="chip-trail">Trail: ' + (pos.trail_enabled !== false ? "ON" : "OFF") + '</button>' +
    '<button class="term-btn term-btn-xs" id="chip-reset">Reset SL / Target</button>' +
    '<button class="term-btn term-btn-xs term-btn-danger" id="chip-square">Square Off</button>' +
    "</div>"
  );
}

function toggleTrail(pos) {
  const cur = pos.trail_enabled !== false;
  const next = !cur;
  pos.trail_enabled = next;
  const btn = $("chip-trail");
  if (btn) {
    btn.textContent = next ? "Trail: ON" : "Trail: OFF";
    btn.classList.toggle("term-btn-danger", !next);
  }
  sendOrder("trail_toggle", next ? 1 : 0, pos.symbol || state.selectedSymbol)
    .then(() => wsSend({ action: "positions" }));
}

function bindChipActions(el, pos) {
  const reset = el.querySelector("#chip-reset");
  const sq = el.querySelector("#chip-square");
  const trail = el.querySelector("#chip-trail");
  if (reset) reset.addEventListener("click", confirmResetLevels);
  if (sq) sq.addEventListener("click", () => sendOrder("squareoff", null, pos.symbol || state.selectedSymbol));
  if (trail) trail.addEventListener("click", () => toggleTrail(pos));
}

function updatePositionChipLive(pos) {
  const el = $("term-position-chip");
  if (!el || el.hidden || !pos) return;
  const set = (id, v, f) => {
    const n = $(id);
    if (n) n.textContent = f ? f(v) : (v == null ? "--" : String(v));
  };
  set("chip-qty", pos ? pos.qty : null, (v) => fmt(v, 0));
  set("chip-entry", pos ? pos.entry_price : null, fmt);
  set("chip-ltp", pos ? livePrice(pos) : null, fmt);
  set("chip-sl", pos ? pos.sl_trigger : null, fmt);
  set("chip-tgt", pos ? pos.target : null, fmt);
  set("chip-near", pos ? pos.near_target : null, fmt);
  const auth = num(pos && pos.unrealized_pnl);
  const pnl = auth != null ? auth : positionPnl(pos);
  state.overlayPnl = auth != null ? auth : null;
  state.overlayPnlCalc = auth != null ? positionPnl(pos) : (pnl || 0);
  const pnlEl = $("chip-pnl");
  if (pnlEl) {
    pnlEl.textContent = fmtINR(pnl);
    pnlEl.className = "term-chip-pnl " + clsChg(pnl);
  }
  const rr = computeRR(String(pos.side || "BUY").toUpperCase(), num(pos.entry_price), num(pos.sl_trigger), num(pos.target));
  set("chip-rr", rr, (v) => (v != null ? "1 : " + v.toFixed(2) : "--"));
}

function updateChipProcessing(kind) {
  const el = $("chip-processing");
  if (!el) return;
  el.textContent = kind ? "Modifying " + kind.toUpperCase() + "…" : "";
  const actions = $("term-position-chip") ? $("term-position-chip").querySelectorAll("button") : [];
  actions.forEach((b) => { b.disabled = !!kind; });
}

/* ─────────────── Overlay: toast ─────────────── */
function toast(msg, kind) {
  const el = $("term-toast");
  if (!el) return;
  el.textContent = msg;
  el.className = "term-toast " + (kind || "");
  el.hidden = false;
  clearTimeout(state.toastTimer);
  state.toastTimer = setTimeout(() => { el.hidden = true; }, 3400);
}

/* ─────────────── Overlay: confirmation modal ─────────────── */
function showModifyModal(kind, oldV, newV, symbol, check) {
  const modal = $("term-modal");
  if (!modal) return;
  const kindTxt = kind === "sl" ? "SL" : "TARGET";
  $("term-modal-title").textContent = "Modify " + kindTxt;
  $("term-modal-msg").innerHTML =
    "<div>Move " + kindTxt + " from <b>" + fmt(oldV) + "</b> to <b>" + fmt(newV) + "</b>?</div>" +
    (check.rr != null ? '<div style="margin-top:4px">Resulting RR <b>1 : ' + check.rr.toFixed(2) + "</b></div>" : "");
  modal.hidden = false;
  state.pendingModify = { kind: kind, value: newV, symbol: symbol };
}

function hideModal() {
  const modal = $("term-modal");
  if (modal) modal.hidden = true;
  state.pendingModify = null;
}

function restoreOverlayAfterCancel() {
  hideModal();
  if (state.chart) state.chart.renderPositionLines();
}

async function confirmPending() {
  const pm = state.pendingModify;
  hideModal();
  if (!pm) return;
  if (pm.kind === "reset") {
    await applyReset(pm);
    return;
  }
  const action = pm.kind === "sl" ? "modify_sl" : "modify_target";
  await sendOrder(action, pm.value, pm.symbol, { source: "overlay" });
}

async function applyReset(pm) {
  if (pm.sl != null) await sendOrder("modify_sl", pm.sl, pm.symbol, { source: "reset" });
  if (pm.tgt != null) await sendOrder("modify_target", pm.tgt, pm.symbol, { source: "reset" });
}

function confirmResetLevels() {
  if (state.modifying) { toast("A SL/Target modification is already in progress", "err"); return; }
  const pos = selectedPosition();
  if (!pos) return;
  const base = state.overlayBaseline || { sl: num(pos.sl_trigger), tgt: num(pos.target) };
  const curSl = num(pos.sl_trigger), curTg = num(pos.target);
  const changed = [];
  if (base.sl != null && (curSl == null || Math.abs(base.sl - curSl) > 1e-9)) changed.push("SL");
  if (base.tgt != null && (curTg == null || Math.abs(base.tgt - curTg) > 1e-9)) changed.push("Target");
  if (!changed.length) { toast("SL / Target already at original levels", "ok"); return; }
  const modal = $("term-modal");
  if (!modal) return;
  $("term-modal-title").textContent = "Reset SL / Target";
  $("term-modal-msg").innerHTML =
    "Restore the strategy's original levels?<br/>SL <b>" + fmt(base.sl) + "</b> | Target <b>" + fmt(base.tgt) + "</b>";
  modal.hidden = false;
  state.pendingModify = { kind: "reset", sl: base.sl, tgt: base.tgt, symbol: pos.symbol || state.selectedSymbol };
}

function bindOverlayUI() {
  const cancel = $("term-modal-cancel");
  const confirm = $("term-modal-confirm");
  const modal = $("term-modal");
  if (cancel) cancel.addEventListener("click", restoreOverlayAfterCancel);
  if (confirm) confirm.addEventListener("click", confirmPending);
  if (modal) {
    modal.addEventListener("click", (e) => {
      if (e.target === modal) restoreOverlayAfterCancel();
    });
  }
  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape" && modal && !modal.hidden) restoreOverlayAfterCancel();
  });
}

function drawChart() {
  if (state.chart) state.chart.fullRender();
  if (state.underlyingChart) state.underlyingChart.setData(underlyingShownCandles());
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
function refreshShownLive() {
  state.candles = premiumShownCandles();
  prepareOverlays(state.candles);
  if (state.chart) state.chart.setData();
}

function switchTimeframe() {
  if (!state.premiumCandles.length && !state.underlyingCandles.length && state.selectedSymbol) {
    fetchSnapshot(state.selectedSymbol);
    return;
  }
  state.candles = premiumShownCandles();
  prepareOverlays(state.candles);
  if (state.chart) state.chart.rebuildMain();
  if (state.underlyingChart) state.underlyingChart.setData(underlyingShownCandles());
  drawChart();
}

function setChartMode(mode) {
  state.chartMode = mode || "12";
  const wrap = document.querySelector(".term-chart-wrap");
  if (wrap) wrap.classList.remove("mode-1", "mode-2");
  if (mode === "1") wrap && wrap.classList.add("mode-1");
  else if (mode === "2") wrap && wrap.classList.add("mode-2");
  setTimeout(resizeCharts, 0);
}

function resizeCharts() {
  if (state.chart) state.chart.resize();
  if (state.underlyingChart) state.underlyingChart.resize();
}

function pushPremiumBar(candle) {
  if (!candle) return;
  const candles = state.premiumCandles;
  const key = barKey(candle);
  const last = candles[candles.length - 1];
  if (last && barKey(last) === key) {
    Object.assign(last, normalizeBar(candle));
  } else {
    candles.push(normalizeBar(candle));
  }
  refreshShownLive();
  renderPnlThrottled();
}

function updatePremiumLast(candle) {
  if (!candle) return;
  if (!state.premiumCandles.length) {
    // Strike just rolled and history is still warming up — seed the
    // developing candle as a partial bar so the chart is never blank.
    if (num(candle.close) > 0 || num(candle.open) > 0) {
      state.premiumCandles.push(normalizeBar(candle));
      refreshShownLive();
    }
    return;
  }
  Object.assign(state.premiumCandles[state.premiumCandles.length - 1], normalizeBar(candle));
  refreshShownLive();
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
  const s = String(v).trim();
  const bare = s.match(/^(\d{2}):(\d{2})(?::\d{2})?$/);
  if (bare) return bare[1] + ":" + bare[2];
  let ms;
  if (/[zZ]$/.test(s) || /[+-]\d{2}:?\d{2}$/.test(s)) {
    ms = Date.parse(s);
  } else {
    const dm = s.match(/(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2})(?::(\d{2}))?/);
    if (dm) ms = Date.UTC(+dm[1], +dm[2] - 1, +dm[3], +dm[4], +dm[5], +(dm[6] || 0));
  }
  if (ms == null || isNaN(ms)) {
    const fb = s.match(/(\d{2}):(\d{2})/);
    return fb ? fb[1] + ":" + fb[2] : (s.slice(11, 16) || s);
  }
  const d = new Date(ms + 330 * 60 * 1000);
  return String(d.getUTCHours()).padStart(2, "0") + ":" + String(d.getUTCMinutes()).padStart(2, "0");
}

function istParts() {
  const d = new Date(Date.now() + 330 * 60 * 1000);
  return { h: d.getUTCHours(), m: d.getUTCMinutes(), s: d.getUTCSeconds() };
}

function updateCandleTimer() {
  const el = $("candle-timer");
  if (!el) return;
  const { m, s } = istParts();
  const tfMin = state.tf === "5m" ? 5 : 1;
  const secondsInto = (m % tfMin) * 60 + s;
  const remaining = (secondsInto === 0 ? tfMin * 60 : tfMin * 60 - secondsInto);
  const mm = String(Math.floor(remaining / 60)).padStart(2, "0");
  const ss = String(remaining % 60).padStart(2, "0");
  el.textContent = "Candle " + tfMin + "m: " + mm + ":" + ss;
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
  bindOverlayUI();
  document.querySelectorAll(".term-tf").forEach((btn) => {
    btn.addEventListener("click", () => {
      document.querySelectorAll(".term-tf").forEach((b) => b.classList.remove("term-tf-active"));
      btn.classList.add("term-tf-active");
      state.tf = btn.dataset.tf;
      switchTimeframe();
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

  document.querySelectorAll(".term-mode").forEach((btn) => {
    btn.addEventListener("click", () => {
      document.querySelectorAll(".term-mode").forEach((b) => b.classList.remove("term-mode-active"));
      btn.classList.add("term-mode-active");
      setChartMode(btn.dataset.mode);
    });
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
    resizeCharts();
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
  state.chart = new LwcChart($("term-chart"), $("premium-col"));
  state.underlyingChart = new UnderlyingChart($("term-chart-underlying"), $("underlying-col"));
  window.addEventListener("resize", () => resizeCharts());
  updateCandleTimer();
  setInterval(updateCandleTimer, 1000);
  loadBootstrap();
});

})();
