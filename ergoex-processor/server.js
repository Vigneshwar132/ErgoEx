// server.js (DB-backed)
// MQTT -> TimescaleDB (telemetry_raw, worker_status, events) + WS + HTTP (static UI + APIs)

const mqtt = require("mqtt");
const WebSocket = require("ws");
const http = require("http");
const url = require("url");
const fs = require("fs");
const pathMod = require("path");
const { Pool } = require("pg");

/* ===================== CONFIG ===================== */
const MQTT_URL = process.env.MQTT_URL || "mqtt://127.0.0.1:1883";
const PG_HOST = process.env.PG_HOST || "127.0.0.1";
const PG_PORT = Number(process.env.PG_PORT || 5432);
const PG_DB = process.env.PG_DB || "ergoex_db";
const PG_USER = process.env.PG_USER || "ergoex";
const PG_PASS = process.env.PG_PASS || "ergoex_password";
const HTTP_PORT = Number(process.env.HTTP_PORT || 8090);
const WS_PORT = Number(process.env.WS_PORT || 8080);

const ONLINE_MS = 12_000; // device online if we got packet within this window
/* ================================================ */

const pool = new Pool({
  host: PG_HOST,
  port: PG_PORT,
  database: PG_DB,
  user: PG_USER,
  password: PG_PASS,
});

/* ===================== IN-MEM META ===================== */
const deviceMeta = new Map(); // device_id -> { last_rx_ms, worker_id, role, org_id, site_id }
const workerMeta = new Map(); // worker_id -> { org_id, site_id, roles: {trunk:{device_id,last_rx_ms}, knee:{...}} }
let lastPacket = null;

/* ===================== SETTINGS (MVP) ===================== */
const settings = {
  // Posture risk zones
  trunk_neutral_deg: 20,
  trunk_moderate_deg: 45,

  // Event detection thresholds (threshold-crossing)
  bend_start_deg: 30,
  bend_reset_deg: 20,
  lift_start_deg: 40,
  lift_reset_deg: 25,
  twist_start_dps: 120,
  twist_reset_dps: 60,

  // Prevent rapid double-counting
  event_cooldown_ms: 1500,

  // Risk labels
  caution_deg: 30,
  bend_deg: 60,

  timezone: "Asia/Kolkata",
};

/* ===================== MOTION STATE ===================== */
const workerMotion = new Map(); // worker_id -> state

function getMotion(worker_id) {
  if (!workerMotion.has(worker_id)) {
    workerMotion.set(worker_id, {
      trunk_pitch: null,
      knee_pitch: null,
      last_trunk_ms: 0,
      last_knee_ms: 0,
      in_bend: false,
      in_lift: false,
      in_twist: false,
      last_event_ms: { bend: 0, lift: 0, twist: 0 },
    });
  }
  return workerMotion.get(worker_id);
}

function canFire(state, type, tms) {
  const last = state.last_event_ms[type] || 0;
  return tms - last >= settings.event_cooldown_ms;
}

function severityFor(type, valueAbs) {
  if (type === "bend") {
    if (valueAbs >= settings.trunk_moderate_deg) return "high";
    if (valueAbs >= settings.bend_start_deg) return "medium";
    return "low";
  }
  if (type === "lift") {
    if (valueAbs >= 60) return "high";
    if (valueAbs >= settings.lift_start_deg) return "medium";
    return "low";
  }
  if (type === "twist") {
    if (valueAbs >= settings.twist_start_dps * 1.5) return "high";
    if (valueAbs >= settings.twist_start_dps) return "medium";
    return "low";
  }
  return "low";
}

/* ===================== ERGONOMICS (summary) ===================== */
function trunkZone(angle) {
  const a = Math.abs(Number(angle || 0));
  if (a <= 20) return "neutral";
  if (a <= 45) return "moderate";
  return "high";
}

function classifyStrategy(trunkPitch, kneePitch) {
  const t = Math.abs(Number(trunkPitch || 0));
  const k = Math.abs(Number(kneePitch || 0));
  if (t > 45 && k < 20) return "back_dominant";
  if (t < 30 && k > 40) return "squat_dominant";
  if (t > 30 && k > 30) return "mixed";
  return "neutral";
}

function rulaTrunkScore(trunkPitch) {
  const a = Math.abs(Number(trunkPitch || 0));
  if (a <= 5) return 1;
  if (a <= 20) return 2;
  if (a <= 60) return 3;
  return 4;
}

function rebaLegScore(kneePitch) {
  const a = Math.abs(Number(kneePitch || 0));
  if (a < 20) return 1;
  if (a <= 60) return 2;
  return 3;
}

function computeRulaReba(trunkPitch, kneePitch) {
  const rula = rulaTrunkScore(trunkPitch);
  const reba = rebaLegScore(kneePitch);
  return { rula, reba, combined: rula + reba };
}

function fatigueIncrement(strategy, trunkZoneName) {
  // This is your "Load Index" base: strategy + exposure = fatigue accumulation
  let inc = 0.1;
  if (strategy === "back_dominant") inc += 0.6;
  if (strategy === "mixed") inc += 0.4;
  if (strategy === "squat_dominant") inc += 0.2;
  if (trunkZoneName === "high") inc += 0.5;
  if (trunkZoneName === "moderate") inc += 0.2;
  return inc;
}

/* ===================== TIME/ONLINE ===================== */
function now() {
  return Date.now();
}

function isOnline(lastRx) {
  if (!lastRx) return false;
  return now() - lastRx <= ONLINE_MS;
}

function computeWorkerStatus(worker_id) {
  const w = workerMeta.get(worker_id);
  if (!w) return null;

  const trunk = w.roles.trunk || null;
  const knee = w.roles.knee || null;

  const trunk_online = trunk ? isOnline(trunk.last_rx_ms) : false;
  const knee_online = knee ? isOnline(knee.last_rx_ms) : false;

  const missing_roles = [];
  if (!trunk) missing_roles.push("trunk");
  if (!knee) missing_roles.push("knee");

  let alignment_ok = true;
  let ts_diff_ms = 0;
  if (trunk && knee) {
    ts_diff_ms = Math.abs((trunk.last_rx_ms || 0) - (knee.last_rx_ms || 0));
    alignment_ok = ts_diff_ms <= 2500;
  }

  let suit_state = "offline";
  if (trunk_online || knee_online) suit_state = "partial";
  if (trunk_online && knee_online) suit_state = "online";

  let worker_risk = "ok";
  if (lastPacket && lastPacket.worker_id === worker_id) {
    const p = Number(lastPacket.pitch || 0);
    if (Math.abs(p) > settings.bend_deg) worker_risk = "high";
    else if (Math.abs(p) > settings.caution_deg) worker_risk = "caution";
  }

  return {
    worker_id,
    org_id: w.org_id,
    site_id: w.site_id,
    trunk_device_id: trunk ? trunk.device_id : null,
    knee_device_id: knee ? knee.device_id : null,
    trunk_online,
    knee_online,
    missing_roles,
    alignment_ok,
    ts_diff_ms,
    suit_state,
    worker_risk,
    last_seen_ms: Math.max(trunk?.last_rx_ms || 0, knee?.last_rx_ms || 0),
  };
}

/* ===================== HR HELPERS ===================== */
// demo conversion: ADC raw -> bpm (temporary)
function hrRawToBpm(hr_raw) {
  if (hr_raw == null) return null;
  const v = Math.max(0, Math.min(4095, Number(hr_raw)));
  return Math.round(50 + (v / 4095) * 90);
}

// Merge HR from trunk + knee: if both recent -> avg; else most-recent
function mergeBpm(trunkBpm, trunkTs, kneeBpm, kneeTs) {
  const RECENT_MS = 10_000;
  const tOk = trunkBpm != null && trunkTs != null && now() - trunkTs <= RECENT_MS;
  const kOk = kneeBpm != null && kneeTs != null && now() - kneeTs <= RECENT_MS;

  if (tOk && kOk) return Math.round((trunkBpm + kneeBpm) / 2);
  if (tOk) return trunkBpm;
  if (kOk) return kneeBpm;

  // fallback: if older, still return most-recent
  if (trunkBpm != null && kneeBpm != null) return trunkTs >= kneeTs ? trunkBpm : kneeBpm;
  return trunkBpm ?? kneeBpm ?? null;
}

/* ===================== MQTT INGEST ===================== */
const client = mqtt.connect(MQTT_URL);

client.on("connect", () => {
  console.log("✅ MQTT CONNECTED", MQTT_URL);
  client.subscribe("ergoex/telemetry/#");
});

client.on("message", async (topic, buf) => {
  let msg;
  try {
    msg = JSON.parse(buf.toString("utf8"));
  } catch (e) {
    console.error("Bad JSON:", e.message);
    return;
  }

  const org_id = msg.org_id || "org_demo";
  const site_id = msg.site_id || "site_demo";
  const worker_id = msg.worker_id;
  const role = msg.role;
  const device_id = msg.device_id || msg.deviceId;
  if (!worker_id || !role || !device_id) return;

  const rx_ms = now();
  lastPacket = { ...msg, org_id, site_id, worker_id, role, device_id, rx_ms };

  // in-mem tracking
  deviceMeta.set(device_id, { last_rx_ms: rx_ms, worker_id, role, org_id, site_id });
  if (!workerMeta.has(worker_id)) workerMeta.set(worker_id, { org_id, site_id, roles: {} });
  const w = workerMeta.get(worker_id);
  w.org_id = org_id;
  w.site_id = site_id;
  w.roles[role] = { device_id, last_rx_ms: rx_ms };

  // time for DB
  const tsEpoch = Number(msg.ts || 0);
  const useTime = tsEpoch > 0 ? new Date(tsEpoch) : new Date();

  // normalize fields
  const pitch = msg.pitch != null ? Number(msg.pitch) : null;
  const roll = msg.roll != null ? Number(msg.roll) : null;
  const wz = msg.wz != null ? Number(msg.wz) : null;
  const amag = msg.amag != null ? Number(msg.amag) : null;
  const temp_c = msg.temp_c != null ? Number(msg.temp_c) : null;
  const hum = msg.hum != null ? Number(msg.hum) : null;
  const hr_raw = msg.hr_raw != null ? Number(msg.hr_raw) : null;
  const ts_sec_ms = msg.ts_sec != null ? Number(msg.ts_sec) : null;
  const time_ok = msg.time_ok ? true : false;

  // --- Threshold-crossing event detection (bend/lift/twist) ---
  const msNow = rx_ms;
  const stMotion = getMotion(worker_id);

  if (role === "trunk" && pitch != null) {
    stMotion.trunk_pitch = pitch;
    stMotion.last_trunk_ms = msNow;

    const a = Math.abs(pitch);
    // Bend: when crosses above start, and later resets below reset
    if (!stMotion.in_bend && a >= settings.bend_start_deg && canFire(stMotion, "bend", msNow)) {
      stMotion.in_bend = true;
      stMotion.last_event_ms.bend = msNow;
      try {
        await pool.query(
          `INSERT INTO events (time, org_id, site_id, worker_id, severity, event_type, details)
           VALUES ($1,$2,$3,$4,$5,'bend',$6)`,
          [useTime, org_id, site_id, worker_id, severityFor("bend", a), JSON.stringify({ trunk_pitch: pitch })]
        );
      } catch (e) {
        console.error("DB insert bend event failed:", e.message);
      }
    }
    if (stMotion.in_bend && a <= settings.bend_reset_deg) stMotion.in_bend = false;
  }

  if (role === "knee" && pitch != null) {
    stMotion.knee_pitch = pitch;
    stMotion.last_knee_ms = msNow;

    const a = Math.abs(pitch);
    // Lift proxy: knee flexion crosses above lift_start and resets below lift_reset
    if (!stMotion.in_lift && a >= settings.lift_start_deg && canFire(stMotion, "lift", msNow)) {
      stMotion.in_lift = true;
      stMotion.last_event_ms.lift = msNow;
      try {
        await pool.query(
          `INSERT INTO events (time, org_id, site_id, worker_id, severity, event_type, details)
           VALUES ($1,$2,$3,$4,$5,'lift',$6)`,
          [useTime, org_id, site_id, worker_id, severityFor("lift", a), JSON.stringify({ knee_pitch: pitch })]
        );
      } catch (e) {
        console.error("DB insert lift event failed:", e.message);
      }
    }
    if (stMotion.in_lift && a <= settings.lift_reset_deg) stMotion.in_lift = false;
  }

  if (role === "trunk" && wz != null) {
    const a = Math.abs(wz);
    // Twist: gyro z crosses threshold and resets
    if (!stMotion.in_twist && a >= settings.twist_start_dps && canFire(stMotion, "twist", msNow)) {
      stMotion.in_twist = true;
      stMotion.last_event_ms.twist = msNow;
      try {
        await pool.query(
          `INSERT INTO events (time, org_id, site_id, worker_id, severity, event_type, details)
           VALUES ($1,$2,$3,$4,$5,'twist',$6)`,
          [useTime, org_id, site_id, worker_id, severityFor("twist", a), JSON.stringify({ wz })]
        );
      } catch (e) {
        console.error("DB insert twist event failed:", e.message);
      }
    }
    if (stMotion.in_twist && a <= settings.twist_reset_dps) stMotion.in_twist = false;
  }

  // --- Write telemetry_raw ---
  try {
    await pool.query(
      `INSERT INTO telemetry_raw
       (time, org_id, site_id, worker_id, role, device_id, ts_sec_ms, time_ok,
        pitch, roll, wz, amag, temp_c, hum, hr_raw, rx_ms)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)`,
      [
        useTime,
        org_id,
        site_id,
        worker_id,
        role,
        device_id,
        ts_sec_ms,
        time_ok,
        pitch,
        roll,
        wz,
        amag,
        temp_c,
        hum,
        hr_raw,
        rx_ms,
      ]
    );
  } catch (e) {
    console.error("DB insert telemetry_raw failed:", e.message);
  }

  // --- worker_status snapshot ---
  const st = computeWorkerStatus(worker_id);
  if (st) {
    try {
      await pool.query(
        `INSERT INTO worker_status
         (time, org_id, site_id, worker_id, suit_state, worker_risk,
          trunk_online, knee_online, alignment_ok, ts_diff_ms, missing_roles, last_seen_ms)
         VALUES (NOW(),$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
        [
          st.org_id,
          st.site_id,
          st.worker_id,
          st.suit_state,
          st.worker_risk,
          st.trunk_online,
          st.knee_online,
          st.alignment_ok,
          st.ts_diff_ms,
          JSON.stringify(st.missing_roles || []),
          st.last_seen_ms,
        ]
      );
    } catch (e) {
      console.error("DB insert worker_status failed:", e.message);
    }
  }

  // --- WS broadcast (filtered by subscription) ---
  broadcastWS({
    type: "telemetry",
    data: {
      ts: useTime.getTime(),
      rx_ms,
      org_id,
      site_id,
      worker_id,
      role,
      device_id,
      pitch,
      roll,
      wz,
      temp_c,
      hum,
      hr_raw,
      bpm: hrRawToBpm(hr_raw),
    },
  });
});

client.on("error", (e) => console.error("MQTT error:", e.message));

/* ===================== WS (with subscribe) ===================== */
const wss = new WebSocket.Server({ port: WS_PORT });
const wsClients = new Set(); // {ws, sub:{worker_id?}}

wss.on("connection", (ws) => {
  const entry = { ws, sub: { worker_id: null } };
  wsClients.add(entry);

  ws.send(JSON.stringify({ type: "hello", ws_port: WS_PORT, http_port: HTTP_PORT }));

  ws.on("message", (buf) => {
    try {
      const msg = JSON.parse(buf.toString("utf8"));
      if (msg.type === "subscribe") {
        entry.sub.worker_id = msg.worker_id || null;
        ws.send(JSON.stringify({ type: "subscribed", worker_id: entry.sub.worker_id }));
      }
    } catch (_) {}
  });

  ws.on("close", () => wsClients.delete(entry));
  ws.on("error", () => wsClients.delete(entry));
});

function broadcastWS(obj) {
  const s = JSON.stringify(obj);
  const wid = obj?.data?.worker_id;

  for (const entry of wsClients) {
    try {
      // If client subscribed to a worker, only forward that worker
      if (entry.sub.worker_id && wid && entry.sub.worker_id !== wid) continue;
      entry.ws.send(s);
    } catch (_) {}
  }
}

console.log(`WS listening on ws://localhost:${WS_PORT}`);

/* ===================== HTTP HELPERS ===================== */
function parseQuery(req) {
  const u = url.parse(req.url, true);
  return { path: u.pathname || "/", q: u.query || {} };
}

function corsHeaders() {
  return {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
  };
}

function sendJson(res, code, obj) {
  res.writeHead(code, { "Content-Type": "application/json", ...corsHeaders() });
  res.end(JSON.stringify(obj));
}

function sendFile(res, filepath, contentType) {
  try {
    const data = fs.readFileSync(filepath);
    res.writeHead(200, {
      "Content-Type": contentType || "text/html; charset=utf-8",
      "Cache-Control": "no-store",
      ...corsHeaders(),
    });
    return res.end(data);
  } catch (e) {
    return sendJson(res, 500, { error: "sendFile failed", filepath, message: e.message });
  }
}

function sendCorsPreflight(res) {
  res.writeHead(204, corsHeaders());
  res.end();
}

/* ===================== API HELPERS ===================== */
async function latestPerRole(worker_id) {
  // latest row per role
  const r = await pool.query(
    `SELECT DISTINCT ON (role) role, time, pitch, temp_c, hr_raw
     FROM telemetry_raw
     WHERE worker_id=$1
     ORDER BY role, time DESC`,
    [worker_id]
  );

  const out = { trunk: null, knee: null };
  for (const row of r.rows) {
    out[row.role] = {
      time_ms: row.time ? new Date(row.time).getTime() : null,
      pitch: row.pitch != null ? Number(row.pitch) : null,
      temp_c: row.temp_c != null ? Number(row.temp_c) : null,
      hr_raw: row.hr_raw != null ? Number(row.hr_raw) : null,
      bpm: row.hr_raw != null ? hrRawToBpm(row.hr_raw) : null,
    };
  }
  return out;
}

/* ===================== HTTP ROUTES ===================== */
const server = http.createServer(async (req, res) => {
  if (req.method === "OPTIONS") return sendCorsPreflight(res);

  const { path, q } = parseQuery(req);

  // Static UI
  if (path === "/" || path === "/index.html") {
    return sendFile(res, pathMod.join(__dirname, "public", "index.html"), "text/html; charset=utf-8");
  }
  if (path === "/worker.html") {
    return sendFile(res, pathMod.join(__dirname, "public", "worker.html"), "text/html; charset=utf-8");
  }
  if (path === "/app.css") {
    return sendFile(res, pathMod.join(__dirname, "public", "app.css"), "text/css; charset=utf-8");
  }

  // Health
  if (path === "/health") {
    let onlineDevices = 0;
    for (const d of deviceMeta.values()) if (isOnline(d.last_rx_ms)) onlineDevices++;

    let onlineWorkers = 0;
    for (const wid of workerMeta.keys()) {
      const s = computeWorkerStatus(wid);
      if (s && s.suit_state === "online") onlineWorkers++;
    }

    return sendJson(res, 200, {
      ok: true,
      now_ms: now(),
      mqtt: { url: MQTT_URL },
      ws: { port: WS_PORT },
      http: { port: HTTP_PORT },
      devices_seen: deviceMeta.size,
      devices_online: onlineDevices,
      workers_seen: workerMeta.size,
      workers_online: onlineWorkers,
    });
  }

  // Workers list (DB)
  if (path === "/workers") {
    try {
      const r = await pool.query(`
        WITH latest_status AS (
          SELECT DISTINCT ON (worker_id)
            worker_id, org_id, site_id, suit_state, worker_risk,
            trunk_online, knee_online, alignment_ok, ts_diff_ms, missing_roles, last_seen_ms, time
          FROM worker_status
          ORDER BY worker_id, time DESC
        ),
        latest_role AS (
          SELECT DISTINCT ON (worker_id, role)
            worker_id, role, device_id, pitch, temp_c, hr_raw, time
          FROM telemetry_raw
          ORDER BY worker_id, role, time DESC
        ),
        trunk AS (
          SELECT worker_id,
            device_id AS trunk_device_id,
            pitch AS trunk_pitch,
            temp_c AS trunk_temp_c,
            hr_raw AS trunk_hr_raw,
            time AS trunk_time
          FROM latest_role WHERE role='trunk'
        ),
        knee AS (
          SELECT worker_id,
            device_id AS knee_device_id,
            pitch AS knee_pitch,
            temp_c AS knee_temp_c,
            hr_raw AS knee_hr_raw,
            time AS knee_time
          FROM latest_role WHERE role='knee'
        )
        SELECT
          s.worker_id, s.org_id, s.site_id,
          s.suit_state,
          CASE WHEN s.worker_risk='high-risk' THEN 'high' ELSE s.worker_risk END AS worker_risk,
          s.trunk_online, s.knee_online, s.alignment_ok, s.ts_diff_ms, s.missing_roles, s.last_seen_ms,
          t.trunk_device_id, k.knee_device_id,
          t.trunk_pitch, k.knee_pitch,
          COALESCE(t.trunk_temp_c, k.knee_temp_c) AS temp_c,
          COALESCE(t.trunk_hr_raw, k.knee_hr_raw) AS hr_raw
        FROM latest_status s
        LEFT JOIN trunk t ON t.worker_id = s.worker_id
        LEFT JOIN knee  k ON k.worker_id = s.worker_id
        ORDER BY s.worker_id;
      `);

      return sendJson(res, 200, { count: r.rows.length, data: r.rows });
    } catch (e) {
      return sendJson(res, 500, { error: e.message });
    }
  }

  // Events feed
  if (path === "/events") {
    const hours = Number(q.hours || 1);
    const limit = Number(q.limit || 50);
    try {
      const r = await pool.query(
        `SELECT time, worker_id, event_type, severity, details
         FROM events
         WHERE time > NOW() - ($1 || ' hours')::interval
         ORDER BY time DESC
         LIMIT $2`,
        [hours, limit]
      );

      const events = r.rows.map((x, i) => ({
        id: `${x.worker_id}-${x.time.getTime()}-${i}`,
        workerId: x.worker_id,
        type: x.event_type,
        severity: x.severity,
        timestamp: x.time,
        metrics: x.details || {},
      }));

      return sendJson(res, 200, { count: events.length, data: events });
    } catch (e) {
      return sendJson(res, 500, { error: e.message });
    }
  }

  // Worker live series (for your OWN graphs) — last N seconds at 1s bucket
  if (path === "/worker/live") {
    const worker_id = String(q.worker_id || "");
    const secs = Math.max(10, Math.min(Number(q.secs || 300), 3600));
    if (!worker_id) return sendJson(res, 400, { error: "worker_id required" });

    try {
      const rows = await pool.query(
        `SELECT time_bucket('1 second', time) AS t,
                MAX(pitch) FILTER (WHERE role='trunk') AS trunk_pitch,
                MAX(pitch) FILTER (WHERE role='knee')  AS knee_pitch,
                AVG(hr_raw) FILTER (WHERE role IN ('trunk','knee')) AS hr_raw_avg
         FROM telemetry_raw
         WHERE worker_id=$1 AND time > NOW() - ($2 || ' seconds')::interval
         GROUP BY t
         ORDER BY t ASC`,
        [worker_id, String(secs)]
      );

      const data = rows.rows.map((r) => ({
        ts: r.t ? new Date(r.t).getTime() : null,
        trunk_pitch: r.trunk_pitch != null ? Number(r.trunk_pitch) : null,
        knee_pitch: r.knee_pitch != null ? Number(r.knee_pitch) : null,
        bpm: r.hr_raw_avg != null ? hrRawToBpm(Number(r.hr_raw_avg)) : null,
      }));

      return sendJson(res, 200, { worker_id, secs, data });
    } catch (e) {
      return sendJson(res, 500, { error: e.message });
    }
  }

  // Worker detail (cards)
  if (path === "/worker/detail") {
    const worker_id = String(q.worker_id || "");
    if (!worker_id) return sendJson(res, 400, { error: "worker_id required" });

    const st = computeWorkerStatus(worker_id);
    if (!st) return sendJson(res, 404, { error: "unknown worker_id" });

    try {
      const roleLatest = await latestPerRole(worker_id);

      const trunk = roleLatest.trunk;
      const knee = roleLatest.knee;

      const mergedBpm = mergeBpm(
        trunk?.bpm ?? null,
        trunk?.time_ms ?? null,
        knee?.bpm ?? null,
        knee?.time_ms ?? null
      );

      const temperature =
        (trunk?.temp_c != null ? trunk.temp_c : null) ??
        (knee?.temp_c != null ? knee.temp_c : null);

      // quick load-index snapshot from last 60 minutes using summary math
      // (lighter DB cost: 1 sec buckets, then compute)
      const mins = 60;
      const rows = await pool.query(
        `SELECT time_bucket('1 second', time) AS t,
                MAX(pitch) FILTER (WHERE role='trunk') AS trunk_pitch,
                MAX(pitch) FILTER (WHERE role='knee')  AS knee_pitch
         FROM telemetry_raw
         WHERE worker_id=$1 AND time > NOW() - ($2 || ' minutes')::interval
         GROUP BY t
         ORDER BY t ASC`,
        [worker_id, String(mins)]
      );

      const good = rows.rows.filter((r) => r.trunk_pitch !== null && r.knee_pitch !== null);

      let fatigue = 0;
      let rulaSum = 0;
      let rebaSum = 0;

      for (const r of good) {
        const tp = Number(r.trunk_pitch);
        const kp = Number(r.knee_pitch);
        const strat = classifyStrategy(tp, kp);
        const tz = trunkZone(tp);
        const rr = computeRulaReba(tp, kp);
        rulaSum += rr.rula;
        rebaSum += rr.reba;
        fatigue += fatigueIncrement(strat, tz);
      }

      const denom = good.length || 1;
      const avgRula = +(rulaSum / denom).toFixed(2);
      const avgReba = +(rebaSum / denom).toFixed(2);

      // "Load Index" (simple scale): fatigue per analyzed second * 100
      const loadIndex = +((fatigue / denom) * 100).toFixed(1);

      return sendJson(res, 200, {
        worker_id,
        org_id: st.org_id,
        site_id: st.site_id,
        suit_state: st.suit_state,
        worker_risk: st.worker_risk,
        trunk_pitch: trunk?.pitch ?? null,
        knee_pitch: knee?.pitch ?? null,
        heart_rate: mergedBpm,
        temperature,
        load_index: loadIndex,
        avg_rula: avgRula,
        avg_reba: avgReba,
        micro_breaks_taken: 2,
        micro_breaks_recommended: 4,
      });
    } catch (e) {
      return sendJson(res, 500, { error: e.message });
    }
  }

  // Shift summary (counts)
  if (path === "/worker/shift-summary") {
    const worker_id = String(q.worker_id || "");
    const hours = Number(q.hours || 8);
    if (!worker_id) return sendJson(res, 400, { error: "worker_id required" });

    try {
      const r = await pool.query(
        `SELECT event_type, COUNT(*)::int AS c
         FROM events
         WHERE worker_id=$1 AND time > NOW() - ($2 || ' hours')::interval
         GROUP BY event_type`,
        [worker_id, hours]
      );

      const map = {};
      for (const row of r.rows) map[row.event_type] = row.c;

      return sendJson(res, 200, {
        worker_id,
        hours,
        lift: map["lift"] || 0,
        bend: map["bend"] || 0,
        twist: map["twist"] || 0,
      });
    } catch (e) {
      return sendJson(res, 500, { error: e.message });
    }
  }

  // fallback
  return sendJson(res, 404, { error: "not found" });
});

server.listen(HTTP_PORT, async () => {
  console.log(`HTTP running on http://localhost:${HTTP_PORT}`);
  try {
    await pool.query("SELECT 1");
    console.log("Postgres connected ✅");
  } catch (e) {
    console.error("Postgres NOT connected ❌", e.message);
  }
});
