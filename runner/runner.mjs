import { mkdirSync, writeFileSync, existsSync } from "node:fs";
import { execFile } from "node:child_process";
import { AsyncLocalStorage } from "node:async_hooks";
import { format } from "node:util";
import { tmpdir } from "node:os";
import { join, dirname, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const HUBS = (process.env.HUB_URL || "").split(/[\s,]+/).map((s) => s.trim().replace(/\/+$/, "")).filter(Boolean);
let HUB = HUBS[0] || "";
const TOKEN = process.env.RUNNER_TOKEN || "";
const TAGS = (process.env.RUNNER_TAGS || "").split(",").map((s) => s.trim().toLowerCase()).filter(Boolean);
const SLOTS = Math.max(1, Math.min(32, Number(process.env.RUNNER_SLOTS) || 3));
const SLOT = Math.max(0, Math.min(99, Number(process.env.RUNNER_SLOT) || 0));
const FLEET = SLOT > 0;
const KIND = process.env.RUNNER_KIND || "vps";
const GHA = KIND === "gha";
const MAX_MS = (Number(process.env.RUNNER_MAX_MINUTES) || (GHA ? 330 : 0)) * 60000;
const EVENT = process.env.GITHUB_EVENT_NAME || "";
const ID = KIND + (FLEET ? "-s" + SLOT : "") + "-" + Math.random().toString(36).slice(2, 8);
const STARTED_FILE = "/tmp/mia_started";
const NORESPAWN_FILE = "/tmp/mia_norespawn";
const flag = (f) => { try { writeFileSync(f, String(Date.now())); } catch {} };
let SHUTDOWN = false;
let staleSeen = false;
const active = new Map();
function abortAll() { SHUTDOWN = true; for (const a of active.values()) a.abort(); }

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const backoff = (n) => Math.round(Math.min(300000, 5000 * 2 ** n) * (0.5 + Math.random() / 2));
let BEAT_MS = 20000;
const als = new AsyncLocalStorage();
const orig = { log: console.log.bind(console), info: console.info.bind(console), warn: console.warn.bind(console), error: console.error.bind(console), debug: console.debug.bind(console) };
const origOut = process.stdout.write.bind(process.stdout);
const origErr = process.stderr.write.bind(process.stderr);
const log = (...a) => als.exit(() => orig.log(new Date().toISOString(), ...a));
let QUIET = false;

// ---- console routing: while a job runs, console.* and raw stdout/stderr writes go to that job's log only ----
export function routeConsole(store, o, hookStreams) {
  for (const k of ["log", "info", "warn", "error", "debug"]) console[k] = (...a) => { const s = store.getStore(); if (s) s.push(format(...a)); else o[k](...a); };
  if (hookStreams) {
    const w = (real) => function (chunk, enc, cb) { const s = store.getStore(); if (!s) return real(chunk, enc, cb); s.push(String(chunk).replace(/\n$/, "")); if (typeof enc === "function") enc(); else if (typeof cb === "function") cb(); return true; };
    process.stdout.write = w(origOut);
    process.stderr.write = w(origErr);
  }
}

// ---- signal database over the libsql HTTP pipeline (no dependencies) ----
export const enc = (v) => (v === null || v === undefined ? { type: "null" } : typeof v === "number" ? (Number.isInteger(v) ? { type: "integer", value: String(v) } : { type: "float", value: v }) : { type: "text", value: String(v) });
const cell = (c) => (!c || c.type === "null" ? null : c.type === "integer" ? Number(c.value) : c.type === "float" ? Number(c.value) : c.value);

export function parsePipeline(d, n, stats) {
  const out = [];
  const res = Array.isArray(d && d.results) ? d.results : [];
  for (let i = 0; i < n; i++) {
    const r = res[i];
    if (!r || r.type !== "ok") throw new Error((r && r.error && r.error.message) || "signal statement failed");
    const x = (r.response && r.response.result) || {};
    const cols = (x.cols || []).map((c) => c.name);
    out.push({ rows: (x.rows || []).map((row) => Object.fromEntries(cols.map((k, j) => [k, cell(row[j])]))) });
    if (stats) {
      stats.statements++;
      if (typeof x.rows_read === "number") { stats.rowsRead += x.rows_read; stats.statsSeen = true; }
      if (typeof x.rows_written === "number") { stats.rowsWritten += x.rows_written; stats.statsSeen = true; }
    }
  }
  return out;
}

export const jitter = (ms, rnd = Math.random) => Math.round(ms * (0.8 + rnd() * 0.4));

export function makeBreaker(o = {}) {
  const th = o.threshold || 3, base = o.base || 60000, max = o.max || 600000;
  let fails = 0, opens = 0, until = 0;
  const trip = (now) => { opens++; until = now + Math.min(max, base * 2 ** (opens - 1)); fails = 0; };
  return {
    ok() { fails = 0; opens = 0; until = 0; },
    fail(now = Date.now()) { fails++; if (fails >= th) { trip(now); return true; } return false; },
    trip(now = Date.now()) { trip(now); },
    open(now = Date.now()) { return now < until; },
    until: () => until,
  };
}

export function makeCap(limit, clock = Date.now) {
  let hour = -1, n = 0;
  const roll = () => { const h = Math.floor(clock() / 3600000); if (h !== hour) { hour = h; n = 0; } };
  return { add(k = 1) { roll(); n += k; }, over() { roll(); return n >= limit; }, used() { roll(); return n; } };
}

export function makeSignal(cfg, o = {}) {
  const f = o.fetch || globalThis.fetch;
  const base = String(cfg.url).replace(/^libsql:\/\//i, "https://").replace(/\/+$/, "");
  const stats = { statements: 0, rowsRead: 0, rowsWritten: 0, statsSeen: false };
  async function pipe(stmts, ms = 8000) {
    const ac = new AbortController();
    const t = setTimeout(() => ac.abort(), ms);
    try {
      const r = await f(base + "/v2/pipeline", { method: "POST", headers: { Authorization: "Bearer " + cfg.token, "Content-Type": "application/json" }, body: JSON.stringify({ requests: [...stmts.map((s) => ({ type: "execute", stmt: { sql: s.sql, args: (s.args || []).map(enc) } })), { type: "close" }] }), signal: ac.signal });
      if (!r.ok) { const e = new Error("signal HTTP " + r.status); e.status = r.status; throw e; }
      return parsePipeline(await r.json(), stmts.length, stats);
    } finally { clearTimeout(t); }
  }
  return {
    stats,
    pipe,
    async peek(tags) {
      const t = tags.filter(Boolean);
      const r = await pipe([
        { sql: "SELECT COALESCE(SUM(pending),0) AS n FROM sig WHERE tag=''" + (t.length ? " OR tag IN (" + t.map(() => "?").join(",") + ")" : ""), args: t },
        { sql: "SELECT k, v FROM meta WHERE k IN ('next_due','fleet','slow')" },
      ]);
      const meta = Object.fromEntries(r[1].rows.map((x) => [x.k, x.v]));
      return { pending: Number((r[0].rows[0] || {}).n || 0), nextDue: Number(meta.next_due) || 0, fleet: meta.fleet === undefined ? null : String(meta.fleet), slow: meta.slow === "1" };
    },
    async presence(p) {
      const info = String(p.info || "").slice(0, 30);
      await pipe([{ sql: "INSERT INTO presence(id,tags,kind,slots,busy,slot,epoch,deadline,seen,info) VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET tags=excluded.tags, slots=excluded.slots, busy=excluded.busy, slot=excluded.slot, epoch=excluded.epoch, deadline=excluded.deadline, seen=excluded.seen, info=excluded.info", args: [p.id, p.tags, p.kind, p.slots, p.busy, p.slot, p.epoch, p.deadline, Date.now(), info] }]);
    },
    async goodbye(id) { await pipe([{ sql: "DELETE FROM presence WHERE id=?", args: [id] }], 3000); },
    async tryTick(gapMs, key = "tick") {
      const now = Date.now();
      const r = await pipe([{ sql: "INSERT INTO meta(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v WHERE CAST(meta.v AS INTEGER) <= ? RETURNING k", args: [key, String(now), now - gapMs] }]);
      return r[0].rows.length > 0;
    },
    async beat(job, runner) {
      const r = await pipe([
        { sql: "INSERT INTO beats(job,runner,at,cancel,watch) VALUES(?,?,?,0,0) ON CONFLICT(job) DO UPDATE SET runner=excluded.runner, at=excluded.at", args: [job, runner, Date.now()] },
        { sql: "SELECT cancel, watch FROM beats WHERE job=?", args: [job] },
      ]);
      const x = r[1].rows[0] || {};
      return { cancel: Number(x.cancel) === 1, watch: Number(x.watch) || 0 };
    },
  };
}


async function hub(op, body = {}, ms = 30000) {
  const ac = new AbortController();
  const t = setTimeout(() => ac.abort(), ms);
  try {
    const r = await fetch(HUB + "/api/runner/" + op, { method: "POST", headers: { "Content-Type": "application/json", Authorization: "Bearer " + TOKEN }, body: JSON.stringify({ id: ID, ...body }), signal: ac.signal });
    const d = await r.json().catch(() => ({}));
    if (!r.ok) { const e = new Error(d.error || "HTTP " + r.status); e.status = r.status; throw e; }
    return d;
  } finally { clearTimeout(t); }
}

function makeMia(base, key) {
  const call = async (cap, op, args = {}) => {
    const r = await fetch(base + "/api/platform/call", { method: "POST", headers: { "Content-Type": "application/json", Authorization: "Bearer " + key }, body: JSON.stringify({ cap, op, args }) });
    const d = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(d.error || "Mia call failed (" + r.status + ")");
    return d.result;
  };
  return {
    call,
    db: { query: (sql, args = []) => call("db", "query", { sql, args }), exec: (sql, args = []) => call("db", "exec", { sql, args }), batch: (stmts) => call("db", "batch", { stmts }) },
    files: { put: (path, data, type = "") => call("files", "put", { path, data, type }), get: (path) => call("files", "get", { path }), list: (prefix = "") => call("files", "list", { prefix }), del: (path) => call("files", "del", { path }) },
    jobs: {
      run: (name, payload = {}, opts = {}) => call("jobs", "run", { name, payload, ...opts }),
      status: (id) => call("jobs", "status", { id }),
      cancel: (id) => call("jobs", "cancel", { id }),
      list: (opts = {}) => call("jobs", "list", opts),
    },
    secrets: { get: (name) => call("secrets", "get", { name }) },
    notify: (text, title = "") => call("notify", "send", { text, title }),
    llm: { chat: (messages, opts = {}) => call("llm", "chat", { messages, ...opts }), ask: (prompt, opts = {}) => call("llm", "ask", { prompt, ...opts }) },
  };
}

const caches = new Map();
const ctl = { sig: null, bad: () => {}, good: () => {}, beatMs: 30000, logMs: 300000, watchMs: 10000 };

function prepare(c) {
  const dir = join(tmpdir(), "mia-bundles", c.version || "none");
  if (caches.has(dir)) return caches.get(dir);
  const p = (async () => {
    if (existsSync(join(dir, ".ok"))) return dir;
    const b = await hub("bundle", { job: c.job.id });
    mkdirSync(dir, { recursive: true });
    for (const [path, text] of Object.entries(b.files || {})) {
      if (path.includes("..")) continue;
      const f = join(dir, path);
      mkdirSync(dirname(f), { recursive: true });
      writeFileSync(f, text);
    }
    if (existsSync(join(dir, "jobs", "package.json"))) {
      await new Promise((res, rej) => execFile("npm", ["install", "--omit=dev", "--no-audit", "--no-fund", "--prefix", join(dir, "jobs")], { timeout: 300000 }, (e, so, se) => (e ? rej(new Error("npm install failed: " + String(se || e.message).slice(-400))) : res())));
    }
    writeFileSync(join(dir, ".ok"), "1");
    return dir;
  })();
  caches.set(dir, p);
  p.catch(() => caches.delete(dir));
  return p;
}

async function runJob(c) {
  const job = c.job;
  const ac = new AbortController();
  active.set(job.id, ac);
  let buf = "";
  const push = (line) => {
    buf = (buf + line + "\n").slice(-7000);
    if (!QUIET) orig.log("[" + job.name + "]", line);
  };
  const out = (...a) => push(a.map((x) => (typeof x === "string" ? x : JSON.stringify(x))).join(" "));
  let sent = null, lastSent = Date.now(), watched = false;
  const hubBeat = async (withLog) => {
    const snap = buf;
    try {
      const r = await hub("beat", withLog && snap !== sent ? { job: job.id, log: snap } : { job: job.id }, 15000);
      if (withLog && snap !== sent) { sent = snap; lastSent = Date.now(); }
      if (r.stale) staleSeen = true;
      if (r.cancel) ac.abort();
    } catch (e) { if (e.status === 401) ac.abort(); }
  };
  let beat, logTimer;
  if (c.hb && ctl.sig) {
    const sigBeat = async () => {
      if (!ctl.sig) return;
      try {
        const r = await ctl.sig.beat(job.id, ID);
        ctl.good();
        watched = r.watch > Date.now();
        if (r.cancel) ac.abort();
      } catch (e) { ctl.bad(e, "a job heartbeat"); }
    };
    sigBeat();
    beat = setInterval(sigBeat, ctl.beatMs);
    logTimer = setInterval(() => { if (buf !== sent && Date.now() - lastSent >= (watched ? ctl.watchMs : ctl.logMs)) hubBeat(true); }, 5000);
  } else {
    beat = setInterval(() => hubBeat(true), BEAT_MS);
  }
  let res;
  try {
    res = await als.run({ push }, async () => {
      const dir = await prepare(c);
      const file = join(dir, "jobs", job.name + ".mjs");
      if (!existsSync(file)) throw new Error("jobs/" + job.name + ".mjs was not found in the deployed zip.");
      const mod = await import(pathToFileURL(file).href);
      const fn = mod.default || mod.run;
      if (typeof fn !== "function") throw new Error("jobs/" + job.name + ".mjs must export a default function.");
      const mia = c.gw ? null : makeMia(HUB, c.key);
      const hubCall = (act, body = {}, ms = 120000) => hub("blobs", { job: job.id, act, ...body }, ms);
      const work = Promise.resolve(fn(job.payload, { mia, hub: hubCall, log: out, signal: ac.signal, ...(c.gw ? { secrets: c.secrets || {}, site: c.gw.site } : {}), job: { id: job.id, name: job.name, attempt: job.attempts, project: c.project } }));
      const timer = new Promise((_, rej) => setTimeout(() => { rej(new Error("Timed out after " + job.timeout_s + "s.")); ac.abort(); }, job.timeout_s * 1000));
      const stop = new Promise((_, rej) => { if (ac.signal.aborted) rej(new Error("Canceled.")); else ac.signal.addEventListener("abort", () => rej(new Error("Canceled.")), { once: true }); });
      const result = await Promise.race([work, timer, stop]);
      return { ok: true, result: result === undefined ? null : result };
    });
  } catch (e) {
    res = { ok: false, error: String(e && e.stack ? e.stack : e).slice(0, 4000) };
  } finally { clearInterval(beat); clearInterval(logTimer); active.delete(job.id); }
  if (SHUTDOWN && !res.ok) {
    for (let i = 0; i < 3; i++) { try { await hub("release", { job: job.id }, 8000); break; } catch { await sleep(800); } }
    return;
  }
  for (let i = 0; i < 4; i++) {
    try { await hub("done", { job: job.id, log: buf, ...res }); break; } catch (e) { if (i === 3) log("could not report", job.id, e.message); else await sleep(3000 * (i + 1)); }
  }
}

async function main() {
  if (!HUB || !TOKEN) { console.error("HUB_URL and RUNNER_TOKEN are required."); process.exit(2); }
  delete process.env.RUNNER_TOKEN;
  const DIRECT_OFF = process.env.RUNNER_DIRECT === "0";
  const helloBody = () => ({ tags: TAGS, kind: KIND, slots: SLOTS, event: EVENT, slot: SLOT, epoch: Number(process.env.RUNNER_EPOCH) || 0, v: 7 });

  let cfg = null;
  let lastErr = "";
  const bootEnd = Date.now() + 20 * 60000;
  for (let round = 0; !cfg && (FLEET ? Date.now() < bootEnd : round < 4); round++) {
    let rejected = 0;
    for (const h of HUBS) {
      HUB = h;
      try { cfg = await hub("hello", helloBody(), 25000); break; } catch (e) {
        lastErr = h + " " + e.message;
        if (e.status === 401) rejected++; else log("hub", h, "is not answering:", e.message);
      }
    }
    if (!cfg && rejected === HUBS.length) { log("The runner token was rejected."); if (FLEET) flag(NORESPAWN_FILE); process.exit(3); }
    if (!cfg && (FLEET || round < 3)) await sleep(FLEET ? Math.min(60000, 5000 * (round + 1)) : 4000 * (round + 1));
  }
  if (!cfg) { log("Hub unreachable:", lastErr); process.exit(1); }
  if (cfg.stale) { log("This start was replaced by a newer one, exiting."); flag(NORESPAWN_FILE); process.exit(0); }
  if (cfg.reconcile) { log("Reconcile run, the hub restarted any missing slot."); process.exit(0); }
  if (HUBS.length > 1) log("using hub", HUB);

  QUIET = process.env.RUNNER_QUIET === "1" || (GHA && cfg.quiet === true && process.env.RUNNER_QUIET !== "0");
  routeConsole(als, orig, QUIET);

  // ---- direct signal polling ----
  const hubKnowsDirect = Object.prototype.hasOwnProperty.call(cfg, "direct");
  let SIG = null, sigCfg = null;
  let cap = makeCap(4000);
  let needRefresh = false;
  let lastRefresh = Date.now();
  let fails = 3, sigFails = 0, lastOk = Date.now(), refreshFails = 0, nextRefresh = 0, dying = false;
  const adopt = (c2) => {
    if (DIRECT_OFF || !c2 || !c2.direct || !c2.sig || !c2.sig.url || !c2.sig.token) { SIG = null; sigCfg = null; ctl.sig = null; return; }
    orig.log("::add-mask::" + c2.sig.token);
    sigCfg = c2.sig;
    SIG = makeSignal(c2.sig);
    cap = makeCap(Number(c2.sig.hourCap) || 4000);
    ctl.sig = SIG;
    ctl.beatMs = Math.max(10000, Number(c2.sig.beatMs) || 30000);
    ctl.logMs = Math.max(30000, Number(c2.sig.logMs) || 300000);
    ctl.watchMs = Math.max(5000, Number(c2.sig.watchLogMs) || 10000);
    fails = Math.max(2, Number(c2.sig.failBeats) || 3);
  };
  adopt(cfg);
  if (!SIG && !DIRECT_OFF && hubKnowsDirect && cfg.sigError) {
    const cause = "Direct polling is not available: " + String(cfg.sigError).slice(0, 160);
    log(cause);
    flag(NORESPAWN_FILE);
    try { await hub("fault", { cause }, 10000); } catch {}
    process.exit(4);
  }
  const pollBase = (active, slow) => Math.max(2000, jitter(((active ? Number(sigCfg.pollMs) : Number(sigCfg.idleMs)) || (active ? 3000 : 10000)) * (slow ? 3 : 1)));
  const sigGood = () => { sigFails = 0; lastOk = Date.now(); };
  function sigBad(e, what) {
    if (e && (e.status === 401 || e.status === 403)) { needRefresh = true; log("signal token rejected, asking the hub for a new one"); return; }
    sigFails++;
    if (sigFails >= fails && Date.now() - lastOk >= (fails - 1) * ctl.beatMs) fatal("The signal database stopped answering during " + what + ": " + String((e && e.message) || e).slice(0, 120));
  }
  ctl.good = sigGood;
  ctl.bad = sigBad;
  async function refresh() {
    lastRefresh = Date.now();
    try {
      const c2 = await hub("hello", helloBody(), 25000);
      if (c2.stale) { staleSeen = true; return; }
      adopt(c2);
      if (!SIG && needRefresh) { fatal("The signal token could not be renewed: " + String(c2.sigError || "direct polling was switched off")); return; }
      needRefresh = false; refreshFails = 0; sigGood();
    } catch (e) {
      nextRefresh = Date.now() + 5000;
      if (needRefresh && ++refreshFails >= 3) fatal("The signal token could not be renewed: " + String(e.message).slice(0, 120));
    }
  }

  const startedAt = Date.now();
  const DEADLINE = FLEET ? Number(cfg.deadline) || startedAt + 300 * 60000 : 0;
  const HARD = FLEET ? Math.min(DEADLINE + 15 * 60000, startedAt + 345 * 60000) : 0;
  flag(STARTED_FILE);
  log("runner", ID, "online", KIND, "slots", SLOTS, "tags", TAGS.join(",") || "none", "keepAlive", cfg.keepAlive, SIG ? "direct" : "hub-poll", QUIET ? "quiet" : "", FLEET ? "slot " + SLOT + " until " + new Date(DEADLINE).toISOString() : "");

  let running = 0;
  let stopping = false;
  let lastBusy = Date.now();
  let lastTick = 0;
  let tickEvery = SIG ? Math.max(60000, Number(sigCfg.tickMs) || 300000) : FLEET ? 60000 : 240000;
  let claimErrs = 0;
  let nextPollAt = 0, emptyStreak = 0, lastWork = Date.now(), nextPresence = 0, hubSlow = false;
  if (cfg.beatMs) BEAT_MS = Math.max(10000, Number(cfg.beatMs) || 20000);

  async function goodbye() { if (SIG) { try { await SIG.goodbye(ID); } catch {} } }

  async function shutdown(why) {
    if (SHUTDOWN) return;
    stopping = true;
    log(why);
    abortAll();
    const t = Date.now();
    while (running > 0 && Date.now() - t < 6000) await sleep(200);
    await goodbye();
    process.exit(0);
  }
  async function fatal(cause) {
    if (dying) return;
    dying = true;
    stopping = true;
    log("stopping:", cause);
    flag(NORESPAWN_FILE);
    abortAll();
    const t = Date.now();
    while (running > 0 && Date.now() - t < 15000) await sleep(200);
    try { await hub("fault", { cause }, 10000); } catch (e) { log("could not report the cause:", e.message); }
    process.exit(4);
  }
  process.on("SIGTERM", () => shutdown("SIGTERM received, releasing jobs"));
  process.on("SIGINT", () => shutdown("SIGINT received, releasing jobs"));

  let lastLease = 0;
  while (true) {
    if (dying) { await sleep(500); continue; }
    const now = Date.now();
    if (staleSeen && !SHUTDOWN) { flag(NORESPAWN_FILE); await shutdown("This start was replaced by a newer one, releasing jobs."); }
    if (!stopping && FLEET && now >= DEADLINE) { stopping = true; log("lifetime reached, draining"); }
    else if (!stopping && !FLEET && MAX_MS && now - startedAt > MAX_MS) { stopping = true; log("lifetime reached, draining"); }
    if (stopping && running === 0) break;
    if (stopping && FLEET && !SHUTDOWN && now >= HARD) { log("drain limit reached, releasing the rest"); abortAll(); }
    if (!FLEET && GHA && !cfg.keepAlive && running === 0 && now - lastBusy > cfg.idleSeconds * 1000) { log("idle, exiting"); break; }

    if (SIG && now >= nextPresence) {
      nextPresence = now + jitter(Number(sigCfg.presenceMs) || 120000);
      try { await SIG.presence({ id: ID, tags: TAGS.join(","), kind: KIND, slots: SLOTS, busy: running, slot: FLEET ? SLOT : 0, epoch: FLEET ? Number(process.env.RUNNER_EPOCH) || 0 : 0, deadline: FLEET ? DEADLINE : 0, info: EVENT + "|" + (SIG.stats.statsSeen ? "s" : "n") }); cap.add(1); sigGood(); } catch (e) { sigBad(e, "a presence update"); }
    }
    if (!SIG && hubKnowsDirect && !DIRECT_OFF && now - lastRefresh > 3600000) refresh().catch(() => null);
    if (SIG && needRefresh && now >= nextRefresh) await refresh();

    if (now - lastTick > tickEvery) {
      lastTick = now;
      (async () => {
        if (SIG) { try { const won = await SIG.tryTick(Math.round(tickEvery * 0.8)); cap.add(1); sigGood(); if (!won) return; } catch (e) { sigBad(e, "the tick election"); return; } }
        const r = await hub("tick", {}, 320000);
        if (r && r.stale) staleSeen = true;
        if (r && Number(r.next) > 0) tickEvery = Math.min(300000, Math.max(60000, Number(r.next)));
      })().catch(() => {});
    }
    if (SIG && now - lastLease > 60000) {
      lastLease = now;
      (async () => {
        let won;
        try { won = await SIG.tryTick(48000, "leasetick"); sigGood(); } catch (e) { sigBad(e, "the lease election"); return; }
        if (!won) return;
        let open = 1;
        try { open = Number(((await SIG.pipe([{ sql: "SELECT COUNT(*) AS n FROM beats" }]))[0].rows[0] || {}).n || 0); cap.add(1); sigGood(); } catch (e) { sigBad(e, "the lease check"); return; }
        if (!open) return;
        const r = await hub("leases", {}, 60000);
        if (r && r.stale) staleSeen = true;
      })().catch((e) => log("lease check:", e.message));
    }
    if (stopping || running >= SLOTS) { await sleep(1000); continue; }

    const direct = !!SIG;
    if (direct) {
      if (now < nextPollAt) { await sleep(Math.min(nextPollAt - now, 1000)); continue; }
      let p;
      try { p = await SIG.peek(TAGS); cap.add(2); sigGood(); } catch (e) { nextPollAt = Date.now() + 5000; sigBad(e, "polling"); continue; }
      hubSlow = p.slow;
      if (FLEET && p.fleet !== null && p.fleet !== String(Number(process.env.RUNNER_EPOCH) || 0)) { staleSeen = true; continue; }
      const due = p.nextDue > 0 && p.nextDue <= Date.now();
      if (p.pending <= 0 && !due) { nextPollAt = Date.now() + (cap.over() ? 30000 : pollBase(Date.now() - lastWork < 60000, hubSlow)); continue; }
    }
    let c;
    try { c = await hub("claim", { tags: TAGS, wait: direct ? 0 : 40000, busy: running, v: 7, sb: direct ? 1 : 0 }, direct ? 30000 : 58000); claimErrs = 0; } catch (e) {
      if (e.status === 401) { if (FLEET) { log("The runner token was rejected, ending this start."); stopping = true; continue; } log("The runner token was rejected."); await goodbye(); process.exit(3); }
      log("claim error:", e.message);
      await sleep(backoff(claimErrs++));
      continue;
    }
    if (c.stale) { staleSeen = true; continue; }
    if (!c.job) {
      if (direct) { emptyStreak++; nextPollAt = Date.now() + Math.min(30000, 3000 * 2 ** Math.min(emptyStreak, 4)); continue; }
      for (let w = Number(c.again) || 0; w > 0 && !stopping && !SHUTDOWN && !staleSeen; w -= 1000) await sleep(Math.min(w, 1000));
      continue;
    }
    emptyStreak = 0; nextPollAt = 0; lastWork = Date.now();
    lastBusy = Date.now();
    running++;
    runJob(c).finally(() => { running--; lastBusy = Date.now(); lastWork = Date.now(); });
  }
  await goodbye();
  process.exit(0);
}

const entry = process.argv[1] ? resolve(process.argv[1]) : "";
if (entry && entry === fileURLToPath(import.meta.url)) main();
