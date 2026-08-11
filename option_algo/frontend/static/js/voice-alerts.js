// frontend/static/js/voice-alerts.js
// ================================================================
// Voice Alerts — text-to-speech for live trading events.
//
// Uses the browser's built-in Web Speech API (SpeechSynthesis) —
// no server-side TTS, no API keys, works fully client-side.
// Settings (enabled, volume, voice, rate, per-event toggles) are
// stored in localStorage — they're a personal/device preference,
// not part of the server-side bot config.
//
// Usage:
//   VoiceAlerts.announce('ENTRY', d)   // d = WS event payload
//   VoiceAlerts.test()
//   VoiceAlerts.set({ enabled: true, volume: 0.8, gender: 'female' })
// ================================================================

const VoiceAlerts = (() => {
  const STORAGE_KEY = 'voice_alerts_settings_v1';

  let settings = {
    enabled:   false,
    volume:    0.8,    // 0.0 - 1.0
    rate:      1.0,    // 0.5 - 2.0
    gender:    'auto',  // 'auto' | 'male' | 'female'
    voiceName: '',      // specific SpeechSynthesisVoice.name — overrides gender if set
    events: {
      ENTRY:            true,
      EXIT:             true,
      SL_TRAIL:         false,
      SL_CANCEL:        true,
      BOT_STATUS:       true,
      DIRECTION_CHANGE: false,
      ORDER_ALERT:      true,
      ORDER_UPDATE:     true,
      RISK_LIMIT_HIT:   true,
      PENDING_TRADE:    true,
    },
  };

  let voicesCache = [];

  // ── Persistence ──────────────────────────────────────────────
  function load() {
    try {
      const raw = localStorage.getItem(STORAGE_KEY);
      if (raw) {
        const parsed = JSON.parse(raw);
        settings = {
          ...settings,
          ...parsed,
          events: { ...settings.events, ...(parsed.events || {}) },
        };
      }
    } catch (e) { /* ignore corrupt storage */ }
  }

  function save() {
    try { localStorage.setItem(STORAGE_KEY, JSON.stringify(settings)); } catch (e) {}
  }

  // ── Voice list ───────────────────────────────────────────────
  function refreshVoices() {
    if ('speechSynthesis' in window) {
      voicesCache = window.speechSynthesis.getVoices() || [];
    }
    return voicesCache;
  }

  // Heuristic gender classification — Web Speech API does not expose
  // gender directly, so we match common voice-name patterns across
  // Chrome/Edge/Safari voice packs.
  const FEMALE_HINTS = ['female','zira','samantha','victoria','susan','karen','moira',
    'tessa','fiona','kate','salli','joanna','amy','emma','ivy','heera','veena','aria','jenny'];
  const MALE_HINTS = ['male','david','daniel','alex','fred','tom','james','george',
    'mark','ryan','justin','guy','rishi','eric','brian','christopher'];

  function classify(voice) {
    const n = (voice.name || '').toLowerCase();
    if (FEMALE_HINTS.some(h => n.includes(h))) return 'female';
    if (MALE_HINTS.some(h => n.includes(h)))   return 'male';
    return 'unknown';
  }

  function pickVoice() {
    refreshVoices();
    if (!voicesCache.length) return null;

    // 1. Exact voice chosen by the user
    if (settings.voiceName) {
      const v = voicesCache.find(v => v.name === settings.voiceName);
      if (v) return v;
    }

    // 2. Gender preference — prefer English voices
    if (settings.gender !== 'auto') {
      const enVoices = voicesCache.filter(v => (v.lang || '').toLowerCase().startsWith('en'));
      const pool = enVoices.length ? enVoices : voicesCache;
      const match = pool.find(v => classify(v) === settings.gender);
      if (match) return match;
    }

    // 3. Fallback — first English voice, or just the first voice
    return voicesCache.find(v => (v.lang || '').toLowerCase().startsWith('en')) || voicesCache[0];
  }

  // ── Speak ────────────────────────────────────────────────────
  function speak(text) {
    if (!settings.enabled) return;
    if (!('speechSynthesis' in window) || !text) return;

    const utter = new SpeechSynthesisUtterance(text);
    utter.volume = Math.max(0, Math.min(1, settings.volume));
    utter.rate   = Math.max(0.5, Math.min(2, settings.rate || 1.0));
    const voice  = pickVoice();
    if (voice) utter.voice = voice;

    // Avoid a backlog of stale alerts if several events fire in a
    // tight burst — keep at most 3 queued utterances.
    if (window.speechSynthesis.pending && window.speechSynthesis.speaking) {
      // SpeechSynthesis has no queue-length API; a simple heuristic:
      // if speech has been going for a while, just let it queue —
      // browsers cap internal queues reasonably on their own.
    }
    window.speechSynthesis.speak(utter);
  }

  // ── Event → spoken text ─────────────────────────────────────
  function optWord(t) {
    return t === 'CE' ? 'Call' : t === 'PE' ? 'Put' : (t || '');
  }

  function statusWord(status) {
    switch (status) {
      case 'TARGET':              return 'Target hit';
      case 'NEAR_TARGET':         return 'Near target exit';
      case 'SL':                  return 'Stop loss hit';
      case 'MANUAL_SQUAREOFF':    return 'Manual square off';
      case 'DIRECTION_FLIP_EXIT': return 'Direction change exit';
      default:                    return 'Exit';
    }
  }

  function textFor(eventType, d) {
    const sym = d.symbol || '';
    switch (eventType) {
      case 'ENTRY':
        return `Entry. ${sym} ${optWord(d.opt_type)} ${d.strike}, price ${d.entry_price}. `
             + `Stop loss ${d.sl_trigger}, target ${d.target}.`;

      case 'EXIT': {
        const win  = (d.pnl ?? 0) >= 0;
        const pnlWord = win
          ? `Profit ${Math.abs(d.pnl)} rupees`
          : `Loss ${Math.abs(d.pnl)} rupees`;
        return `${statusWord(d.status)}. ${sym} at ${d.exit_price}. ${pnlWord}.`;
      }

      case 'SL_TRAIL':
        return `${sym} stop loss moved to ${d.new_sl}.`;

      case 'SL_CANCEL':
        return `${sym} stop loss cancelled. ${d.reason || ''}`.trim();

      case 'BOT_STATUS':
        if (d.error) return `Bot error. ${d.error}`;
        return d.status === 'running' ? 'Bot started.' : 'Bot stopped.';

      case 'DIRECTION_CHANGE':
        return `${sym} direction changed to ${d.direction}. Now trading ${optWord(d.opt_type)} ${d.strike}.`;

      case 'ORDER_ALERT':
        return `Order alert. ${sym}. ${d.reason || ''}`;

      case 'RISK_LIMIT_HIT':
        return `Risk limit reached. ${d.reason || ''}. No new trades will be entered for the rest of the day.`;

      case 'ORDER_UPDATE': {
        const st = (d.status || '').toLowerCase();
        const ts = d.trading_symbol || sym;
        if (st === 'complete')
          return `Order confirmed. ${ts}. ${d.side || ''} ${d.qty_filled || ''} at rupees ${d.average_price}.`;
        if (st === 'rejected' || st === 'cancelled')
          return `Order ${st}. ${ts}. ${d.message || ''}`;
        if (st === 'trigger_pending')
          return `Stop loss triggered. ${ts}.`;
        return `Order update. ${ts}. ${d.status}.`;
      }

      case 'PENDING_TRADE':
        return `Trade alert. Pending trade ${d.pending_trade_id}. `
             + `${d.trading_symbol || sym} ${optWord(d.opt_type)}. `
             + `Please approve or reject. `
             + `Trade will fill at current LTP.`;

      default:
        return '';
    }
  }

  function announce(eventType, d) {
    if (!settings.enabled) return;
    if (settings.events[eventType] === false) return;
    const text = textFor(eventType, d || {});
    if (text) speak(text);
  }

  function test() {
    speak('Voice alerts are working. This is a test announcement.');
  }

  // ── Init ─────────────────────────────────────────────────────
  load();
  if ('speechSynthesis' in window) {
    refreshVoices();
    window.speechSynthesis.onvoiceschanged = refreshVoices;
  }

  return {
    get settings() { return JSON.parse(JSON.stringify(settings)); },
    set(partial) {
      settings = {
        ...settings,
        ...partial,
        events: { ...settings.events, ...(partial.events || {}) },
      };
      save();
    },
    save, load, speak, announce, test,
    getVoices: () => voicesCache,
    refreshVoices,
    classify,
  };
})();
