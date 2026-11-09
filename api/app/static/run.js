// Live run page.
// Invariant: we apply events strictly in seq order (seq == applied + 1).
// Anything else is either a repeat (ignored, counted) or a gap (counted, reconnect from `applied`).
const runId = location.pathname.split('/').pop();
const state = {
  applied: 0,            // last seq applied to `points`
  serverSeq: null,       // last_seq stored in DB, from `status` events
  conflicts: 0,
  reconnects: 0,
  gaps: 0,
  dups: 0,
  conn: 'connecting',
  points: new Map(),     // key -> Map(step -> value)
};
// Read by the e2e test to compare with the database.
window.__runState = () => ({
  applied: state.applied,
  serverSeq: state.serverSeq,
  conflicts: state.conflicts,
  reconnects: state.reconnects,
  gaps: state.gaps,
  dups: state.dups,
  conn: state.conn,
  points: [...state.points].flatMap(([k, m]) => [...m].map(([s, v]) => [k, s, v])),
});

const WATCHDOG_MS = 35000;   // server sends a heartbeat after 15 s of silence
let controller = null;
let lastActivity = Date.now();
let stopped = false;

function apply(ev) {
  if (ev.seq <= state.applied) { state.dups++; return true; }
  if (ev.seq !== state.applied + 1) { state.gaps++; return false; }
  let m = state.points.get(ev.key);
  if (!m) { m = new Map(); state.points.set(ev.key, m); }
  m.set(ev.step, ev.value);
  state.applied = ev.seq;
  return true;
}

function handle(type, data) {
  if (type === 'metric') {
    if (!apply(JSON.parse(data))) {                     // gap: drop this connection,
      controller.abort();                               // resume from state.applied
      throw new Error('gap');
    }
  } else if (type === 'status') {
    const s = JSON.parse(data);
    state.serverSeq = s.last_seq;
    state.conflicts = s.conflicts;
  } else if (type === 'revoked') {
    stopped = true;
    state.conn = 'key revoked';
  }
}

async function connectOnce() {
  controller = new AbortController();
  const res = await fetch(`/api/runs/${runId}/stream`, {
    headers: { Authorization: 'Bearer ' + getKey(), 'Last-Event-ID': String(state.applied) },
    cache: 'no-store',
    signal: controller.signal,
  });
  if (res.status === 401 || res.status === 404) {
    stopped = true;
    state.conn = res.status === 401 ? 'unauthorized' : 'not found';
    return;
  }
  if (!res.ok) throw new Error('HTTP ' + res.status);
  state.conn = 'live';
  delay = 250;   // backoff restarts after every successful connect
  const reader = res.body.pipeThrough(new TextDecoderStream()).getReader();
  let buf = '';
  for (;;) {
    const { value, done } = await reader.read();
    if (done) break;
    lastActivity = Date.now();
    buf += value;
    let i;
    while ((i = buf.indexOf('\n\n')) >= 0) {
      const block = buf.slice(0, i);
      buf = buf.slice(i + 2);
      let type = 'message', data = '';
      for (const line of block.split('\n')) {
        if (line.startsWith('event: ')) type = line.slice(7);
        else if (line.startsWith('data: ')) data += line.slice(6);
      }
      if (data) handle(type, data);
      if (stopped) { controller.abort(); return; }
    }
    schedule();
  }
}

let delay = 250;
async function run() {
  while (!stopped) {
    try {
      await connectOnce();
    } catch (e) {
      // network error, abort by watchdog or gap: fall through to reconnect
    }
    if (stopped) break;
    state.reconnects++;
    state.conn = 'reconnecting';
    schedule();
    await new Promise(r => setTimeout(r, delay + Math.random() * delay));
    delay = Math.min(delay * 2, 5000);
  }
  schedule();
}

setInterval(() => {
  if (controller && state.conn === 'live' && Date.now() - lastActivity > WATCHDOG_MS) controller.abort();
}, 1000);

// --- rendering (throttled to one frame) -----------------------------------
let pending = false;
function schedule() {
  if (pending) return;
  pending = true;
  requestAnimationFrame(() => { pending = false; render(); });
}

function setText(id, v) {
  const el = document.querySelector(`[data-testid="${id}"]`);
  el.textContent = String(v);
  return el;
}

function render() {
  setText('conn', state.conn);
  setText('applied-seq', state.applied);
  setText('server-seq', state.serverSeq ?? '?');
  setText('conflicts', state.conflicts).classList.toggle('bad', state.conflicts > 0);
  setText('reconnects', state.reconnects);
  setText('gaps', state.gaps).classList.toggle('bad', state.gaps > 0);
  setText('dups', state.dups);

  const keys = [...state.points.keys()].sort();
  const rows = [];
  const charts = [];
  for (const k of keys) {
    const m = state.points.get(k);
    const steps = [...m.keys()].sort((a, b) => a - b);   // order by step, not arrival
    const maxStep = steps[steps.length - 1];
    let sum = 0;
    for (const s of steps) sum += m.get(s);
    const tr = document.createElement('tr');
    tr.dataset.testid = 'metric-row';
    tr.dataset.key = k;
    for (const [cls, v] of [['key', k], ['count', steps.length], ['max-step', maxStep], ['last-value', m.get(maxStep)], ['sum', sum]]) {
      const td = document.createElement('td');
      td.dataset.col = cls;
      td.textContent = String(v);
      tr.append(td);
    }
    rows.push(tr);
    charts.push(chart(k, steps, m));
  }
  document.getElementById('metrics').replaceChildren(...rows);
  document.getElementById('charts').replaceChildren(...charts);
}

function chart(k, steps, m) {
  const box = document.createElement('div');
  box.className = 'chart';
  const title = document.createElement('div');
  title.textContent = k;
  const ns = 'http://www.w3.org/2000/svg';
  const svg = document.createElementNS(ns, 'svg');
  svg.setAttribute('viewBox', '0 0 100 100');
  svg.setAttribute('preserveAspectRatio', 'none');
  const vals = steps.map(s => m.get(s));
  const lo = Math.min(...vals), hi = Math.max(...vals);
  const x0 = steps[0], x1 = steps[steps.length - 1];
  const pts = steps.map((s, i) => {
    const x = x1 === x0 ? 50 : ((s - x0) / (x1 - x0)) * 100;
    const y = hi === lo ? 50 : 100 - ((vals[i] - lo) / (hi - lo)) * 100;
    return x.toFixed(2) + ',' + y.toFixed(2);
  });
  const line = document.createElementNS(ns, 'polyline');
  line.setAttribute('points', pts.join(' '));
  svg.append(line);
  box.append(title, svg);
  return box;
}

(async () => {
  const res = await api('/api/runs/' + runId);
  if (res.ok) {
    const r = await res.json();
    document.getElementById('title').textContent = r.name;
    document.title = r.name;
  }
  run();
})();
