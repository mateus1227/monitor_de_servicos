/************************************************************
 * NOC MONITOR - BACKEND COMPLETO (1 ARQUIVO)
 *
 * CORREÇÕES APLICADAS:
 * - OFFLINE consolida com 1 falha
 * - ALERTA OFFLINE envia 5s após virar OFFLINE
 * - ONLINE consolida com 1 sucesso
 * - Recuperação envia assim que voltar
 * - Timer real de alerta (não depende do próximo loop)
 * - shouldCheck respeita current_interval_ms
 * - tipos com intervalos menores
 * - força regras no banco já existente
 *
 * RELATÓRIOS ADICIONADOS:
 * - Disponibilidade por item
 * - Disponibilidade por tipo
 * - Disponibilidade geral
 * - Fórmula: Tempo disponível / Tempo total × 100
 *
 * NOVO:
 * - suporte a VPN_JUMP / TCP_JUMP
 *   -> faz a checagem TCP através de um Jump Server HTTP
 *
 * ATUALIZAÇÃO:
 * - relatórios agora retornam/exportam:
 *   segundos, minutos, horas e HH:mm:ss
 ************************************************************/

process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0";

const express = require("express");
const axios = require("axios");
const sqlite3 = require("sqlite3").verbose();
const session = require("express-session");
const fs = require("fs");
const path = require("path");
const https = require("https");
const net = require("net");
const ping = require("ping");

const { Client, LocalAuth } = require("whatsapp-web.js");
const qrcode = require("qrcode-terminal");

// XLSX export (instale: npm i exceljs)
let ExcelJS = null;
try {
  ExcelJS = require("exceljs");
} catch {
  // se não instalar, o endpoint xlsx dá erro amigável
}

// ================= CONFIG =================
const configPath = path.join(__dirname, "config.json");
if (!fs.existsSync(configPath)) {
  console.error("❌ config.json não encontrado.");
  process.exit(1);
}
const config = JSON.parse(fs.readFileSync(configPath, "utf-8"));

// ================= EXPRESS =================
const app = express();
app.use(express.json({ limit: "2mb" }));
app.use(express.static(path.join(__dirname, "public")));

app.use(
  session({
    secret: config.sessionSecret || "monitor-secret",
    resave: false,
    saveUninitialized: true,
  })
);

function auth(req, res, next) {
  if (!req.session.logado) return res.sendStatus(403);
  next();
}

// ================= WHATSAPP =================
let zapPronto = false;

const zap = new Client({
  authStrategy: new LocalAuth(),
  puppeteer: {
    headless: true,
    args: ["--no-sandbox", "--disable-setuid-sandbox"],
  },
});

zap.on("qr", (qr) => {
  console.log("==================================");
  console.log(" ESCANEIE O QR DO WHATSAPP");
  console.log("==================================");
  qrcode.generate(qr, { small: true });
});

zap.on("ready", () => {
  console.log("✅ WHATSAPP CONECTADO");
  zapPronto = true;
});

zap.on("authenticated", () => console.log("✅ AUTH OK"));
zap.on("auth_failure", (err) => {
  console.log("❌ FALHA AUTH:", err);
  zapPronto = false;
});
zap.on("disconnected", (reason) => {
  console.log("❌ WHATSAPP DESCONECTADO:", reason);
  zapPronto = false;
});

zap.initialize();

// ======== ENVIO WHATSAPP (GRUPO) COM DEBUG ========
async function enviarWhatsApp(msg, rulesRow) {
  const enabled =
    Number(rulesRow?.notify_whatsapp ?? (config.whatsapp?.enabled ? 1 : 0)) === 1;
  const grupo = String(rulesRow?.whatsapp_group || config.whatsapp?.grupo || "").trim();

  console.log("🔍 [WHATSAPP DEBUG]");
  console.log("  → notify_whatsapp (rules):", rulesRow?.notify_whatsapp);
  console.log("  → whatsapp.enabled (config):", config.whatsapp?.enabled);
  console.log("  → enabled calculado:", enabled);
  console.log("  → whatsapp_group (rules):", rulesRow?.whatsapp_group);
  console.log("  → whatsapp.grupo (config):", config.whatsapp?.grupo);
  console.log("  → grupo final:", grupo || "(VAZIO)");
  console.log("  → zapPronto:", zapPronto);
  console.log("  → msg preview:", msg ? msg.substring(0, 160) + "..." : "(null)");
  console.log("  ---------------------------");

  if (!enabled) {
    console.log("⚠️ WhatsApp DISABLED nas configurações - notificação ignorada");
    return false;
  }
  if (!grupo) {
    console.log("⚠️ Grupo do WhatsApp NÃO CONFIGURADO - notificação ignorada");
    return false;
  }
  if (!zapPronto) {
    console.log("⚠️ WhatsApp NÃO PRONTO (zapPronto=false) - notificação ignorada");
    return false;
  }

  try {
    console.log(`📤 Enviando WhatsApp para ${grupo}...`);
    await zap.sendMessage(grupo, msg);
    console.log("✅ WHATSAPP ENVIADO COM SUCESSO");
    return true;
  } catch (e) {
    console.log("❌ Erro ao enviar WhatsApp:", e?.message || e);
    const emsg = String(e?.message || "");
    if (emsg.includes("detached Frame")) {
      zapPronto = false;
      console.log("⚠️ Frame detach detectado. Reinicie o WhatsApp/servidor se persistir.");
    }
    return false;
  }
}

// ================= BANCO =================
const db = new sqlite3.Database(path.join(__dirname, "monitor.db"));

db.serialize(() => {
  db.run(`
    CREATE TABLE IF NOT EXISTS apis(
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      nome TEXT NOT NULL,
      url TEXT NOT NULL,
      metodo TEXT DEFAULT 'GET',
      headers_json TEXT,
      xml TEXT,
      porta INTEGER,
      ativo INTEGER DEFAULT 1,
      timeout_ms INTEGER DEFAULT 5000,
      mute_until DATETIME,
      tipo TEXT DEFAULT 'Outros',
      interval_ms INTEGER,
      last_check_at DATETIME,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      adaptive_enabled INTEGER DEFAULT 1,
      current_interval_ms INTEGER,
      ok_streak INTEGER DEFAULT 0,
      fail_streak INTEGER DEFAULT 0,
      hys_ok_streak INTEGER DEFAULT 0,
      hys_fail_streak INTEGER DEFAULT 0,
      last_status TEXT DEFAULT 'UNKNOWN',
      last_tempo INTEGER,
      last_detalhe TEXT,
      last_data DATETIME
    )
  `);

  db.run(`ALTER TABLE apis ADD COLUMN porta INTEGER`, () => {});
  db.run(`ALTER TABLE apis ADD COLUMN mute_until DATETIME`, () => {});
  db.run(`ALTER TABLE apis ADD COLUMN type_id INTEGER`, () => {});
  db.run(`ALTER TABLE apis ADD COLUMN interval_ms INTEGER`, () => {});
  db.run(`ALTER TABLE apis ADD COLUMN last_check_at DATETIME`, () => {});
  db.run(`ALTER TABLE apis ADD COLUMN adaptive_enabled INTEGER DEFAULT 1`, (err) => {
    if (err && !String(err.message).includes("duplicate column name")) console.error("ALTER adaptive_enabled:", err.message);
  });
  db.run(`ALTER TABLE apis ADD COLUMN current_interval_ms INTEGER`, (err) => {
    if (err && !String(err.message).includes("duplicate column name")) console.error("ALTER current_interval_ms:", err.message);
  });
  db.run(`ALTER TABLE apis ADD COLUMN ok_streak INTEGER DEFAULT 0`, (err) => {
    if (err && !String(err.message).includes("duplicate column name")) console.error("ALTER ok_streak:", err.message);
  });
  db.run(`ALTER TABLE apis ADD COLUMN fail_streak INTEGER DEFAULT 0`, (err) => {
    if (err && !String(err.message).includes("duplicate column name")) console.error("ALTER fail_streak:", err.message);
  });
  db.run(`ALTER TABLE apis ADD COLUMN hys_ok_streak INTEGER DEFAULT 0`, (err) => {
    if (err && !String(err.message).includes("duplicate column name")) console.error("ALTER hys_ok_streak:", err.message);
  });
  db.run(`ALTER TABLE apis ADD COLUMN hys_fail_streak INTEGER DEFAULT 0`, (err) => {
    if (err && !String(err.message).includes("duplicate column name")) console.error("ALTER hys_fail_streak:", err.message);
  });
  db.run(`ALTER TABLE apis ADD COLUMN last_status TEXT DEFAULT 'UNKNOWN'`, (err) => {
    if (err && !String(err.message).includes("duplicate column name")) console.error("ALTER last_status:", err.message);
  });
  db.run(`ALTER TABLE apis ADD COLUMN last_tempo INTEGER`, (err) => {
    if (err && !String(err.message).includes("duplicate column name")) console.error("ALTER last_tempo:", err.message);
  });
  db.run(`ALTER TABLE apis ADD COLUMN last_detalhe TEXT`, (err) => {
    if (err && !String(err.message).includes("duplicate column name")) console.error("ALTER last_detalhe:", err.message);
  });
  db.run(`ALTER TABLE apis ADD COLUMN last_data DATETIME`, (err) => {
    if (err && !String(err.message).includes("duplicate column name")) console.error("ALTER last_data:", err.message);
  });
  db.run(`ALTER TABLE apis ADD COLUMN tipo TEXT`, (err) => {
    if (err && !String(err.message).includes("duplicate column name")) {
      console.error("ALTER tipo:", err.message);
    }
  });

  // tipos de equipamento
  db.run(`
    CREATE TABLE IF NOT EXISTS monitor_types(
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL UNIQUE,
      default_interval_ms INTEGER NOT NULL DEFAULT 3000,
      default_timeout_ms INTEGER NOT NULL DEFAULT 5000,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
  `);

  // seed com intervalos menores
  db.run(
    `INSERT OR IGNORE INTO monitor_types(name, default_interval_ms, default_timeout_ms)
     VALUES
      ('SERVIDOR', 1000, 5000),
      ('FIREWALL', 1000, 4000),
      ('SWITCH', 3000, 3000),
      ('LINK', 3000, 3000)`
  );

  // força ajuste dos tipos já existentes
  db.run(`UPDATE monitor_types SET default_interval_ms=1000, default_timeout_ms=5000 WHERE name='SERVIDOR'`);
  db.run(`UPDATE monitor_types SET default_interval_ms=1000, default_timeout_ms=4000 WHERE name='FIREWALL'`);
  db.run(`UPDATE monitor_types SET default_interval_ms=3000, default_timeout_ms=3000 WHERE name='SWITCH'`);
  db.run(`UPDATE monitor_types SET default_interval_ms=3000, default_timeout_ms=3000 WHERE name='LINK'`);

  db.run(`
    CREATE TABLE IF NOT EXISTS logs(
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      api_id INTEGER NOT NULL,
      status TEXT NOT NULL,
      tempo INTEGER NOT NULL,
      detalhe TEXT,
      data DATETIME DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY(api_id) REFERENCES apis(id)
    )
  `);

  db.run(`
    CREATE TABLE IF NOT EXISTS api_state(
      api_id INTEGER PRIMARY KEY,
      ultimo_status TEXT DEFAULT 'UNKNOWN',
      updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY(api_id) REFERENCES apis(id)
    )
  `);

  db.run(`
    CREATE TABLE IF NOT EXISTS alert_rules(
      id INTEGER PRIMARY KEY CHECK (id=1),
      offline_after_sec INTEGER DEFAULT 5,
      slow_ms INTEGER DEFAULT 1200,
      slow_after_sec INTEGER DEFAULT 180,
      flap_window_sec INTEGER DEFAULT 1800,
      flap_count INTEGER DEFAULT 4,
      fail_n INTEGER DEFAULT 1,
      ok_m INTEGER DEFAULT 1,
      still_down_every_sec INTEGER DEFAULT 600,
      notify_whatsapp INTEGER DEFAULT 1,
      whatsapp_group TEXT DEFAULT '',
      updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
  `);

  const addCol = (col, typeAndDefault) =>
    db.run(`ALTER TABLE alert_rules ADD COLUMN ${col} ${typeAndDefault}`, () => {});

  addCol("offline_after_sec", "INTEGER DEFAULT 5");
  addCol("slow_ms", "INTEGER DEFAULT 1200");
  addCol("slow_after_sec", "INTEGER DEFAULT 180");
  addCol("flap_window_sec", "INTEGER DEFAULT 1800");
  addCol("flap_count", "INTEGER DEFAULT 4");
  addCol("fail_n", "INTEGER DEFAULT 1");
  addCol("ok_m", "INTEGER DEFAULT 1");
  addCol("still_down_every_sec", "INTEGER DEFAULT 600");
  addCol("notify_whatsapp", "INTEGER DEFAULT 1");
  addCol("whatsapp_group", "TEXT DEFAULT ''");
  addCol("updated_at", "DATETIME DEFAULT CURRENT_TIMESTAMP");

  db.run(
    `INSERT OR IGNORE INTO alert_rules(
       id, offline_after_sec, fail_n, ok_m, whatsapp_group, notify_whatsapp
     )
     VALUES(1, 5, 1, 1, ?, ?)`,
    [config.whatsapp?.grupo || "", config.whatsapp?.enabled ? 1 : 0]
  );

  // força o banco existente a usar os valores corretos
  db.run(`
    UPDATE alert_rules
    SET offline_after_sec = 5,
        fail_n = 1,
        ok_m = 1,
        updated_at = CURRENT_TIMESTAMP
    WHERE id = 1
  `);

  db.run(`
    CREATE TABLE IF NOT EXISTS incidents(
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      api_id INTEGER NOT NULL,
      type TEXT NOT NULL,
      opened_at DATETIME NOT NULL,
      closed_at DATETIME,
      duration_sec INTEGER DEFAULT 0,
      start_detail TEXT,
      end_detail TEXT,
      start_tempo INTEGER,
      end_tempo INTEGER,
      alerted INTEGER DEFAULT 0,
      alert_at DATETIME,
      FOREIGN KEY(api_id) REFERENCES apis(id)
    )
  `);

  db.run(`
    CREATE INDEX IF NOT EXISTS idx_incidents_api_time
    ON incidents(api_id, opened_at)
  `);

  db.run(`
    CREATE TABLE IF NOT EXISTS suppressions(
      api_id INTEGER PRIMARY KEY,
      until DATETIME NOT NULL,
      reason TEXT,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
  `);
});

// ============ DB HELPERS ============
function dbAll(sql, params = []) {
  return new Promise((resolve, reject) => {
    db.all(sql, params, (err, rows) => (err ? reject(err) : resolve(rows)));
  });
}
function dbGet(sql, params = []) {
  return new Promise((resolve, reject) => {
    db.get(sql, params, (err, row) => (err ? reject(err) : resolve(row)));
  });
}
function dbRun(sql, params = []) {
  return new Promise((resolve, reject) => {
    db.run(sql, params, function (err) {
      if (err) return reject(err);
      resolve({ changes: this.changes, lastID: this.lastID });
    });
  });
}

async function setUltimoStatus(api_id, status) {
  await dbRun(
    `
    INSERT INTO api_state(api_id, ultimo_status, updated_at)
    VALUES(?,?,CURRENT_TIMESTAMP)
    ON CONFLICT(api_id) DO UPDATE SET
      ultimo_status=excluded.ultimo_status,
      updated_at=CURRENT_TIMESTAMP
    `,
    [api_id, status]
  );
}

function nowSqlite() {
  return new Date().toISOString().replace("T", " ").slice(0, 19);
}

async function getRules() {
  const row = await dbGet(`SELECT * FROM alert_rules WHERE id=1`);
  return {
    offline_after_sec: Math.max(1, Number(row?.offline_after_sec ?? config.rules?.offline_after_sec ?? 5)),
    slow_ms: Math.max(50, Number(row?.slow_ms ?? config.rules?.slow_ms ?? 1200)),
    slow_after_sec: Math.max(10, Number(row?.slow_after_sec ?? config.rules?.slow_after_sec ?? 180)),
    flap_window_sec: Math.max(60, Number(row?.flap_window_sec ?? config.rules?.flap_window_sec ?? 1800)),
    flap_count: Math.max(2, Number(row?.flap_count ?? config.rules?.flap_count ?? 4)),
    fail_n: Math.max(1, Number(row?.fail_n ?? config.rules?.fail_n ?? 1)),
    ok_m: Math.max(1, Number(row?.ok_m ?? config.rules?.ok_m ?? 1)),
    still_down_every_sec: Math.max(60, Number(row?.still_down_every_sec ?? config.rules?.still_down_every_sec ?? 600)),
    notify_whatsapp: Number(row?.notify_whatsapp ?? (config.whatsapp?.enabled ? 1 : 0)) ? 1 : 0,
    whatsapp_group: String(row?.whatsapp_group ?? config.whatsapp?.grupo ?? ""),
  };
}

async function getOpenIncident(api_id, type) {
  return dbGet(
    `SELECT * FROM incidents
     WHERE api_id=? AND type=? AND closed_at IS NULL
     ORDER BY id DESC LIMIT 1`,
    [api_id, type]
  );
}

async function openIncident({ api_id, type, start_detail, start_tempo }) {
  const opened_at = nowSqlite();
  const r = await dbRun(
    `INSERT INTO incidents(api_id,type,opened_at,start_detail,start_tempo,alerted)
     VALUES(?,?,?,?,?,0)`,
    [api_id, type, opened_at, start_detail || "", start_tempo ?? null]
  );
  console.log(`🆕 Incidente aberto: id=${r.lastID}, api_id=${api_id}, type=${type}`);
  return r.lastID;
}

async function closeIncident({ id, end_detail, end_tempo }) {
  const closed_at = nowSqlite();
  await dbRun(
    `UPDATE incidents
     SET closed_at=?,
         end_detail=?,
         end_tempo=?,
         duration_sec = CAST((julianday(?) - julianday(opened_at)) * 86400 AS INTEGER)
     WHERE id=?`,
    [closed_at, end_detail || "", end_tempo ?? null, closed_at, id]
  );
  console.log(`✅ Incidente fechado: id=${id}`);
}

async function markIncidentAlerted(id) {
  const alert_at = nowSqlite();
  await dbRun(`UPDATE incidents SET alerted=1, alert_at=? WHERE id=?`, [alert_at, id]);
  console.log(`🔔 Incidente marcado como alertado: id=${id}`);
}

// ======= MUTE (silenciar serviço) =======
async function isMuted(api_id) {
  const row = await dbGet(`SELECT mute_until FROM apis WHERE id=?`, [api_id]);
  if (!row?.mute_until) return false;
  const untilMs = new Date(row.mute_until.replace(" ", "T") + "Z").getTime();
  return Date.now() < untilMs;
}

// ======= SUPPRESSION (anti-flapping) =======
async function isSuppressed(api_id) {
  const row = await dbGet(`SELECT until FROM suppressions WHERE api_id=?`, [api_id]);
  if (!row?.until) return false;
  const untilMs = new Date(row.until.replace(" ", "T") + "Z").getTime();
  return Date.now() < untilMs;
}

async function setSuppressed(api_id, seconds, reason) {
  const until = new Date(Date.now() + seconds * 1000)
    .toISOString()
    .replace("T", " ")
    .slice(0, 19);

  await dbRun(
    `INSERT INTO suppressions(api_id, until, reason)
     VALUES(?,?,?)
     ON CONFLICT(api_id) DO UPDATE SET until=excluded.until, reason=excluded.reason`,
    [api_id, until, reason || ""]
  );
}

async function countFlaps(api_id, windowSec) {
  const w = Math.max(60, Number(windowSec || 1800));
  const row = await dbGet(
    `SELECT COUNT(*) AS c
     FROM incidents
     WHERE api_id=? AND type='OFFLINE'
       AND opened_at >= datetime('now', ?)`,
    [api_id, `-${w} seconds`]
  );
  return Number(row?.c || 0);
}

// ======= Timers de alerta offline =======
const offlineAlertTimers = new Map(); // api_id -> timeout

function clearOfflineAlertTimer(api_id) {
  const t = offlineAlertTimers.get(api_id);
  if (t) {
    clearTimeout(t);
    offlineAlertTimers.delete(api_id);
  }
}

async function fireOfflineAlertIfStillOpen(api_id) {
  try {
    offlineAlertTimers.delete(api_id);

    const rules = await getRules();
    const api = await dbGet(
      `
      SELECT 
        a.*,
        t.name AS type_name,
        t.default_interval_ms,
        t.default_timeout_ms
      FROM apis a
      LEFT JOIN monitor_types t ON t.id = a.type_id
      WHERE a.id=?
      LIMIT 1
      `,
      [api_id]
    );
    if (!api) return;

    const offOpen = await getOpenIncident(api_id, "OFFLINE");
    if (!offOpen) {
      console.log(`ℹ️ Timer OFFLINE ignorado: incidente não está mais aberto api_id=${api_id}`);
      return;
    }
    if (Number(offOpen.alerted) === 1) {
      console.log(`ℹ️ Timer OFFLINE ignorado: incidente já alertado api_id=${api_id}`);
      return;
    }

    const curStateRow = await dbGet(`SELECT ultimo_status FROM api_state WHERE api_id=?`, [api_id]);
    const curState = String(curStateRow?.ultimo_status || api.last_status || "UNKNOWN").toUpperCase();
    if (curState !== "OFFLINE") {
      console.log(`ℹ️ Timer OFFLINE ignorado: estado atual não é OFFLINE api_id=${api_id}, state=${curState}`);
      return;
    }

    const muted = await isMuted(api_id);
    if (muted) {
      console.log(`🔕 Timer OFFLINE: serviço silenciado api_id=${api_id}`);
      return;
    }

    const suppressed = await isSuppressed(api_id);
    if (suppressed) {
      console.log(`🔕 Timer OFFLINE: serviço suprimido api_id=${api_id}`);
      return;
    }

    const flaps = await countFlaps(api_id, rules.flap_window_sec || 1800);
    const flapCount = Math.max(2, Number(rules.flap_count || 4));

    if (flaps >= flapCount) {
      console.log(`⚠️ FLAPPING detectado (${flaps} eventos). Suprimindo alertas por 20min.`);
      await enviarWhatsApp(msgFlap(api.nome, flaps, rules.flap_window_sec || 1800), rules);
      await setSuppressed(api_id, 20 * 60, "flapping");
      return;
    }

    const sent = await enviarWhatsApp(msgCaiuCritico(api.nome, api.last_detalhe || offOpen.start_detail || ""), rules);
    if (sent) {
      await markIncidentAlerted(offOpen.id);
    } else {
      console.log(`⚠️ Falha ao enviar OFFLINE api_id=${api_id}; tentando novamente em 5s`);
      scheduleOfflineAlert(api_id, 5000);
    }
  } catch (e) {
    console.log(`❌ Erro timer OFFLINE api_id=${api_id}:`, e.message);
    scheduleOfflineAlert(api_id, 5000);
  }
}

function scheduleOfflineAlert(api_id, delayMs) {
  clearOfflineAlertTimer(api_id);
  const ms = Math.max(1000, Number(delayMs || 5000));
  const timer = setTimeout(() => {
    fireOfflineAlertIfStillOpen(api_id).catch((err) => {
      console.log(`❌ Erro em fireOfflineAlertIfStillOpen api_id=${api_id}:`, err.message);
    });
  }, ms);
  offlineAlertTimers.set(api_id, timer);
  console.log(`⏰ Timer OFFLINE agendado api_id=${api_id} para ${ms}ms`);
}

async function bootstrapOpenOfflineAlertTimers() {
  try {
    const rules = await getRules();
    const openRows = await dbAll(
      `
      SELECT i.api_id, i.id, i.opened_at, i.alerted
      FROM incidents i
      WHERE i.type='OFFLINE' AND i.closed_at IS NULL
      `
    );

    for (const row of openRows) {
      if (Number(row.alerted) === 1) continue;

      const openedMs = new Date(String(row.opened_at).replace(" ", "T") + "Z").getTime();
      const elapsed = Date.now() - openedMs;
      const thresholdMs = Math.max(1000, Number(rules.offline_after_sec || 5) * 1000);
      const remaining = Math.max(1000, thresholdMs - elapsed);

      scheduleOfflineAlert(row.api_id, remaining);
    }

    if (openRows.length) {
      console.log(`🔁 Timers de OFFLINE restaurados: ${openRows.length}`);
    }
  } catch (e) {
    console.log("❌ Erro bootstrapOpenOfflineAlertTimers:", e.message);
  }
}

// ======= Scheduler por intervalo =======
function getEffectiveMonitorInterval(api) {
  const base = Number(api.interval_ms ?? api.default_interval_ms ?? 3000) || 3000;
  const adaptiveEnabled = Number(api.adaptive_enabled ?? 1) === 1;
  const current = Number(api.current_interval_ms ?? 0);

  if (adaptiveEnabled && current > 0) return current;
  return base;
}

function shouldCheck(api) {
  const interval = Math.max(500, getEffectiveMonitorInterval(api));
  if (!api.last_check_at) return true;
  const lastMs = new Date(String(api.last_check_at).replace(" ", "T") + "Z").getTime();
  return Date.now() - lastMs >= interval;
}

async function markChecked(api_id) {
  await dbRun(`UPDATE apis SET last_check_at=CURRENT_TIMESTAMP WHERE id=?`, [api_id]);
}

async function updateAdaptiveInterval(api, status) {
  const base = Number(api.interval_ms ?? api.default_interval_ms ?? 3000) || 3000;

  const minFast = Math.max(500, Math.floor(base * 0.25));
  const maxStable = Math.max(base, Math.min(base * 10, 10 * 60 * 1000));
  const maxFailure = Math.max(minFast, Math.min(base * 2, 2 * 60 * 1000));

  const adaptiveEnabled = Number(api.adaptive_enabled ?? 1) === 1;
  if (!adaptiveEnabled) {
    await dbRun(
      `UPDATE apis SET current_interval_ms=NULL, ok_streak=0, fail_streak=0 WHERE id=?`,
      [api.id]
    );
    return;
  }

  let ok = Number(api.ok_streak || 0);
  let fail = Number(api.fail_streak || 0);
  let cur = Number(api.current_interval_ms ?? base);

  if (status === "ONLINE") {
    ok += 1;
    fail = 0;

    if (cur < base) cur = base;

    if (ok >= 3 && ok % 3 === 0) {
      cur = Math.min(maxStable, Math.round(cur * 1.6));
    }
  } else if (status === "OFFLINE") {
    fail += 1;
    ok = 0;

    cur = minFast;

    const steps = Math.max(0, Math.floor((fail - 1) / 5));
    const factor = Math.pow(2, Math.min(6, steps));
    cur = Math.min(maxFailure, Math.round(minFast * factor));
  } else {
    cur = Math.max(minFast, cur);
  }

  await dbRun(
    `UPDATE apis SET current_interval_ms=?, ok_streak=?, fail_streak=? WHERE id=?`,
    [cur, ok, fail, api.id]
  );
}

// ================= UTIL =================
function agora() {
  const d = new Date();
  return {
    data: d.toLocaleDateString("pt-BR"),
    hora: d.toLocaleTimeString("pt-BR"),
  };
}

function msgCaiuCritico(nome, detalhe) {
  const t = agora();
  const det = detalhe ? `\n\n⚠️ ${detalhe}` : "";
  return `🚨 *ALERTA CRÍTICO!*\n\n🔴 Serviço: *${nome}*\n📅 Data: ${t.data}\n🕒 Hora: ${t.hora} (BRT)\n\n⚠️ O serviço parou de responder (timeout/erro de conexão).${det}`;
}

function msgVoltouCritico(nome) {
  const t = agora();
  return `✅ *SERVIÇO RESTABELECIDO!*\n\n🟢 Serviço: *${nome}*\n📅 Data: ${t.data}\n🕒 Hora: ${t.hora} (BRT)\n\n🚀 O serviço voltou a responder normalmente.`;
}

function msgAindaOffline(nome, minutos) {
  const t = agora();
  return `⏳ *AINDA OFFLINE*\n\n🔴 Serviço: *${nome}*\n📅 Data: ${t.data}\n🕒 Hora: ${t.hora} (BRT)\n\n⚠️ Continua indisponível há ~${minutos} min.`;
}

function msgFlap(nome, count, windowSec) {
  const t = agora();
  const min = Math.round((Number(windowSec || 1800)) / 60);
  return `⚠️ *INSTABILIDADE DETECTADA*\n\n🟡 Serviço: *${nome}*\n📅 Data: ${t.data}\n🕒 Hora: ${t.hora} (BRT)\n\n🔁 Ocorreram *${count}* quedas em ~${min} min.\n\n➡️ Vou reduzir alertas repetidos por um período para evitar spam.`;
}

function limparTexto(s) {
  if (s == null) return "";
  return String(s).replace(/\s+/g, " ").trim();
}

// ================= MONITOR =================
const httpsAgent = new https.Agent({ rejectUnauthorized: false });
const DEFAULT_BODY = "";
let rodando = false;

// Escalonamento (memória)
const lastStillDownAt = new Map(); // api_id -> timestamp

function parseHeaders(headers_json) {
  let headers = {};
  try {
    if (headers_json && headers_json.trim()) headers = JSON.parse(headers_json);
  } catch {
    return { headers: {}, err: "headers_json inválido" };
  }
  return { headers, err: "" };
}

async function checarPing(host, timeoutMs) {
  const inicio = Date.now();
  try {
    const res = await ping.promise.probe(host, {
      timeout: Math.max(1, Math.ceil(timeoutMs / 1000)),
      extra: ["-n", "1"],
    });
    const tempo = Date.now() - inicio;
    if (res.alive) return { status: "ONLINE", tempo, detalhe: "" };
    return { status: "OFFLINE", tempo, detalhe: "Sem resposta ao PING" };
  } catch (e) {
    const tempo = Date.now() - inicio;
    return { status: "OFFLINE", tempo, detalhe: e.message || "PING falhou" };
  }
}

function checarTcp(host, port, timeoutMs) {
  return new Promise((resolve) => {
    const inicio = Date.now();
    const socket = new net.Socket();
    let settled = false;

    const finish = (status, detalhe) => {
      if (settled) return;
      settled = true;
      const tempo = Date.now() - inicio;
      try { socket.destroy(); } catch {}
      resolve({ status, tempo, detalhe: detalhe || "" });
    };

    socket.setTimeout(timeoutMs);
    socket.once("connect", () => finish("ONLINE", ""));
    socket.once("timeout", () => finish("OFFLINE", "Timeout TCP"));
    socket.once("error", (err) => finish("OFFLINE", err?.message || "Erro TCP"));

    socket.connect(Number(port), host);
  });
}

// ===== NOVO: TCP via Jump Server =====
async function checarTcpViaJumpServer(host, port, timeoutMs) {
  const inicio = Date.now();

  try {
    const jumpEnabled = Boolean(config.jumpServer?.enabled);
    const baseUrl = String(config.jumpServer?.baseUrl || "").trim();
    const apiKey = String(config.jumpServer?.apiKey || "").trim();
    const routePath = String(config.jumpServer?.routePath || "/check").trim() || "/check";

    if (!jumpEnabled) {
      return {
        status: "OFFLINE",
        tempo: Date.now() - inicio,
        detalhe: "Jump Server desabilitado no config.json",
      };
    }

    if (!baseUrl) {
      return {
        status: "OFFLINE",
        tempo: Date.now() - inicio,
        detalhe: "jumpServer.baseUrl não configurado",
      };
    }

    const timeout = Math.max(1000, Number(timeoutMs || 5000));
    const url = `${baseUrl.replace(/\/+$/, "")}${routePath.startsWith("/") ? routePath : `/${routePath}`}`;

    const resp = await axios.get(url, {
      params: {
        host: String(host || "").trim(),
        port: Number(port),
        timeout,
      },
      timeout: timeout + 2000,
      headers: {
        "x-api-key": apiKey,
      },
      validateStatus: null,
    });

    const tempo = Date.now() - inicio;

    if (resp.status >= 400) {
      const msg = typeof resp.data === "object"
        ? resp.data?.error || `Jump HTTP ${resp.status}`
        : `Jump HTTP ${resp.status}`;

      return {
        status: "OFFLINE",
        tempo,
        detalhe: String(msg || `Jump HTTP ${resp.status}`),
      };
    }

    const data = resp.data || {};
    return {
      status: String(data.status || "OFFLINE").toUpperCase(),
      tempo: Number(data.tempo ?? tempo),
      detalhe: String(data.detalhe || ""),
    };
  } catch (e) {
    return {
      status: "OFFLINE",
      tempo: Date.now() - inicio,
      detalhe: `Erro Jump: ${e.message || "falha na chamada"}`,
    };
  }
}

async function checarHttp(api, effectiveTimeout) {
  let status = "OFFLINE";
  let tempo = 0;
  let detalhe = "";

  const inicio = Date.now();
  const timeout = Number(effectiveTimeout) || 5000;
  const metodo = String(api.metodo || "GET").toUpperCase();

  const { headers, err } = parseHeaders(api.headers_json);
  if (err) detalhe = err;

  const body = api.xml && api.xml.trim().length ? api.xml : DEFAULT_BODY;

  try {
    let resp;
    if (metodo === "GET") {
      resp = await axios.get(api.url, {
        headers,
        timeout,
        validateStatus: null,
        httpsAgent,
      });
    } else {
      resp = await axios.post(api.url, body, {
        headers,
        timeout,
        validateStatus: null,
        httpsAgent,
      });
    }

    tempo = Date.now() - inicio;
    status = "ONLINE";

    const raw = typeof resp.data === "string" ? resp.data : JSON.stringify(resp.data);
    if (!detalhe && raw && raw.length < 120) detalhe = raw.trim();
  } catch (e) {
    tempo = Date.now() - inicio;
    status = "OFFLINE";
    detalhe = e.message || "Erro";
  }

  return { status, tempo, detalhe };
}

async function verificarApi(api) {
  const metodo = String(api.metodo || "GET").toUpperCase();
  const effectiveTimeout = Number(api.timeout_ms ?? api.default_timeout_ms ?? 5000) || 5000;

  let result;

  if (metodo === "PING") {
    const host = String(api.url || "").replace(/^https?:\/\//i, "").split("/")[0];
    result = await checarPing(host, effectiveTimeout);
  } else if (metodo === "TCP" || metodo === "TELNET") {
    const host = String(api.url || "").replace(/^https?:\/\//i, "").split("/")[0];
    const port = Number(api.porta) || 80;
    result = await checarTcp(host, port, effectiveTimeout);
  } else if (metodo === "VPN_JUMP" || metodo === "TCP_JUMP") {
    const host = String(api.url || "").replace(/^https?:\/\//i, "").split("/")[0];
    const port = Number(api.porta) || 80;
    result = await checarTcpViaJumpServer(host, port, effectiveTimeout);
  } else {
    result = await checarHttp(api, effectiveTimeout);
  }

  await dbRun("INSERT INTO logs(api_id,status,tempo,detalhe) VALUES(?,?,?,?)", [
    api.id,
    result.status,
    result.tempo,
    result.detalhe,
  ]);

  const rules = await getRules();

  const failN = Math.max(1, Number(rules.fail_n ?? 1));
  const okM = Math.max(1, Number(rules.ok_m ?? 1));

  const curStateRow = await dbGet(`SELECT ultimo_status FROM api_state WHERE api_id=?`, [api.id]);
  const curState = String(curStateRow?.ultimo_status || api.last_status || "UNKNOWN").toUpperCase();

  let hOk = Number(api.hys_ok_streak || 0);
  let hFail = Number(api.hys_fail_streak || 0);

  const raw = String(result.status || "UNKNOWN").toUpperCase();

  if (raw === "OFFLINE") {
    hFail += 1;
    hOk = 0;
  } else if (raw === "ONLINE") {
    hOk += 1;
    hFail = 0;
  } else {
    hOk = 0;
    hFail = 0;
  }

  let newState = curState;
  if (curState !== "OFFLINE" && hFail >= failN) newState = "OFFLINE";
  if ((curState === "OFFLINE" || curState === "UNKNOWN") && hOk >= okM) newState = "ONLINE";

  let uiDetalhe = result.detalhe || "";
  if (raw === "OFFLINE" && newState !== "OFFLINE") {
    uiDetalhe = `Pré-falha (${hFail}/${failN}): ${uiDetalhe || "sem detalhe"}`;
  }
  if (raw === "ONLINE" && curState === "OFFLINE" && newState !== "ONLINE") {
    uiDetalhe = `Recuperando (${hOk}/${okM})`;
  }

  await dbRun(
    `UPDATE apis
     SET hys_ok_streak=?,
         hys_fail_streak=?,
         last_status=?,
         last_tempo=?,
         last_detalhe=?,
         last_data=CURRENT_TIMESTAMP
     WHERE id=?`,
    [hOk, hFail, newState, result.tempo, uiDetalhe, api.id]
  );

  const muted = await isMuted(api.id);
  if (muted) console.log(`🔕 ${api.nome}: silenciado (mute_until).`);

  const suppressed = await isSuppressed(api.id);
  if (suppressed) console.log(`🔕 ${api.nome}: suprimido por instabilidade (flapping).`);

  const offOpen = await getOpenIncident(api.id, "OFFLINE");

  // =================== OFFLINE ===================
  if (newState === "OFFLINE") {
    console.log(`🔴 [OFFLINE] ${api.nome} | detalhe: ${result.detalhe || "sem detalhe"}`);

    if (!offOpen) {
      console.log(`🆕 Abrindo incidente OFFLINE para ${api.nome}`);
      await openIncident({
        api_id: api.id,
        type: "OFFLINE",
        start_detail: result.detalhe,
        start_tempo: result.tempo,
      });

      const delayMs = Math.max(1000, Number(rules.offline_after_sec || 5) * 1000);
      scheduleOfflineAlert(api.id, delayMs);
    } else if (!Number(offOpen.alerted)) {
      const elapsedRow = await dbGet(
        `SELECT CAST((julianday(CURRENT_TIMESTAMP) - julianday(?)) * 86400 AS INTEGER) AS sec`,
        [offOpen.opened_at]
      );
      const sec = Number(elapsedRow?.sec || 0);
      const thrSec = Math.max(1, Number(rules.offline_after_sec || 5));

      console.log(`⏳ ${api.nome}: aguardando alerta | decorrido: ${sec}s | threshold: ${thrSec}s`);

      if (!muted && !suppressed && sec >= thrSec) {
        const flaps = await countFlaps(api.id, rules.flap_window_sec || 1800);
        const flapCount = Math.max(2, Number(rules.flap_count || 4));

        if (flaps >= flapCount) {
          console.log(`⚠️ FLAPPING detectado (${flaps} eventos). Suprimindo alertas por 20min.`);
          await enviarWhatsApp(msgFlap(api.nome, flaps, rules.flap_window_sec || 1800), rules);
          await setSuppressed(api.id, 20 * 60, "flapping");
        } else {
          console.log(`🚨 Threshold atingido! Enviando alerta WhatsApp para ${api.nome}`);
          const sent = await enviarWhatsApp(msgCaiuCritico(api.nome, result.detalhe), rules);
          if (sent) {
            await markIncidentAlerted(offOpen.id);
            clearOfflineAlertTimer(api.id);
          }
        }
      }
    } else {
      console.log(`ℹ️ ${api.nome}: incidente já alertado, aguardando recuperação`);

      const elapsedRow2 = await dbGet(
        `SELECT CAST((julianday(CURRENT_TIMESTAMP) - julianday(?)) * 86400 AS INTEGER) AS sec`,
        [offOpen.opened_at]
      );
      const sec2 = Number(elapsedRow2?.sec || 0);

      const escalateAfter = 5 * 60;
      const remindEvery = Math.max(60, Number(rules.still_down_every_sec || 600));

      if (sec2 >= escalateAfter) {
        const last = lastStillDownAt.get(api.id) || 0;
        if (Date.now() - last >= remindEvery * 1000) {
          if (!muted && !(await isSuppressed(api.id))) {
            const mins = Math.max(1, Math.round(sec2 / 60));
            await enviarWhatsApp(msgAindaOffline(api.nome, mins), rules);
          }
          lastStillDownAt.set(api.id, Date.now());
        }
      }
    }
  }

  // =================== ONLINE ===================
  if (newState === "ONLINE" && offOpen) {
    console.log(`🟢 [ONLINE] ${api.nome} - fechando incidente`);
    clearOfflineAlertTimer(api.id);

    const wasAlerted = Number(offOpen.alerted) === 1;

    await closeIncident({
      id: offOpen.id,
      end_detail: result.detalhe,
      end_tempo: result.tempo,
    });

    if (wasAlerted) {
      if (!muted) {
        console.log(`📤 Enviando notificação de recuperação para ${api.nome}`);
        await enviarWhatsApp(msgVoltouCritico(api.nome), rules);
      } else {
        console.log(`ℹ️ Recuperação sem envio (serviço está silenciado)`);
      }
    } else {
      console.log(`ℹ️ Recuperação sem envio (alerta OFFLINE não foi disparado)`);
    }

    lastStillDownAt.delete(api.id);
  }

  await updateAdaptiveInterval(api, newState);
  await setUltimoStatus(api.id, newState);
  console.log(`📊 ${api.nome} | raw=${result.status} → ${newState} | ${result.tempo}ms | ${result.detalhe || ""}`);
}

async function loopMonitor() {
  if (rodando) return;
  rodando = true;

  try {
    const apis = await dbAll(`
      SELECT 
        a.*,
        t.name AS type_name,
        t.default_interval_ms,
        t.default_timeout_ms
      FROM apis a
      LEFT JOIN monitor_types t ON t.id = a.type_id
      WHERE a.ativo=1
      ORDER BY a.id ASC
    `);

    const toCheck = apis.filter((a) => shouldCheck(a));
    const countWillCheck = toCheck.length;

    const CONCURRENCY = 8;

    async function worker(queue) {
      while (queue.length) {
        const api = queue.shift();
        try {
          await verificarApi(api);
          await markChecked(api.id);
        } catch (e) {
          console.log(`❌ Erro ao verificar ${api?.nome || api?.id}:`, e.message);
        }
      }
    }

    const queue = [...toCheck];
    const workers = Array.from(
      { length: Math.min(CONCURRENCY, queue.length) },
      () => worker(queue)
    );
    await Promise.all(workers);

    console.log(`🔄 Loop monitor: ${apis.length} ativos | checados agora: ${countWillCheck}`);
  } catch (e) {
    console.log("❌ Erro loopMonitor:", e.message);
  } finally {
    rodando = false;
  }
}

// roda o scheduler a cada 1s
setInterval(loopMonitor, 1000);
loopMonitor().catch(() => {});
bootstrapOpenOfflineAlertTimers().catch(() => {});

// =================== COMANDO VERIFICAR + SILENCIAR (GRUPO) ===================
let lastVerificarAt = 0;
const VERIFICAR_COOLDOWN_MS = 10000;

function linhaResumo(api) {
  const nome = limparTexto(api.nome || "SEM NOME");
  const st = limparTexto(api.last_status || "—");
  const tempo = api.last_tempo == null ? "—" : `${api.last_tempo}ms`;
  const detalhe = limparTexto(api.last_detalhe || "—");
  const ativo = Number(api.ativo) === 1 ? "✅" : "⏸️";
  const tipo = api.type_name ? ` (${api.type_name})` : "";
  return `• ${ativo} ${nome}${tipo} | ${st} | ${tempo} | ${detalhe}`;
}

async function resumoTodasApis() {
  const apis = await dbAll(`
    SELECT a.*, t.name AS type_name
    FROM apis a
    LEFT JOIN monitor_types t ON t.id = a.type_id
    ORDER BY a.id ASC
  `);

  let online = 0, offline = 0;

  const linhas = [];
  for (const api of apis) {
    const last = await dbGet(
      "SELECT status, tempo, detalhe, data FROM logs WHERE api_id=? ORDER BY id DESC LIMIT 1",
      [api.id]
    );

    const row = {
      id: api.id,
      nome: api.nome,
      ativo: api.ativo,
      type_name: api.type_name || null,
      last_status: api.last_status || last?.status || "—",
      last_tempo: api.last_tempo ?? last?.tempo ?? null,
      last_detalhe: api.last_detalhe || last?.detalhe || "—",
      last_data: api.last_data || last?.data || null,
    };

    if (row.last_status === "ONLINE") online++;
    if (row.last_status === "OFFLINE") offline++;

    linhas.push(row);
  }

  return { linhas, online, offline, total: linhas.length };
}

function montarMsgVerificar(res) {
  const t = agora();
  const header =
    `📡 *STATUS DO MONITOR*\n` +
    `🗓️ ${t.data} ⏰ ${t.hora} (BRT)\n\n` +
    `📌 Total: ${res.total} | ✅ Online: ${res.online} | ❌ Offline: ${res.offline}\n\n`;
  const corpo = res.linhas.map(linhaResumo).join("\n");
  return header + corpo;
}

zap.on("message", async (msg) => {
  try {
    if (msg.fromMe) return;
    if (!config.whatsapp?.enabled) return;
    const grupo = config.whatsapp?.grupo;
    if (!grupo) return;
    if (msg.from !== grupo) return;

    const textoRaw = (msg.body || "").trim();
    const texto = textoRaw.toUpperCase();

    if (texto === "VERIFICAR" || texto === "STATUS") {
      const now = Date.now();
      if (now - lastVerificarAt < VERIFICAR_COOLDOWN_MS) return;
      lastVerificarAt = now;

      if (!zapPronto) {
        await zap.sendMessage(grupo, "⚠️ Estou conectando no WhatsApp… tenta novamente em alguns segundos.");
        return;
      }

      const resu = await resumoTodasApis();
      await zap.sendMessage(grupo, montarMsgVerificar(resu));
      return;
    }

    if (texto.startsWith("SILENCIAR")) {
      const parts = textoRaw.trim().split(/\s+/);
      const id = Number(parts[1]);
      const dur = (parts[2] || "30M").toUpperCase();

      if (!id) {
        await zap.sendMessage(grupo, "Uso: SILENCIAR <id> <30M|2H>\nEx: SILENCIAR 3 30M");
        return;
      }

      let minutes = 30;
      const m = dur.match(/^(\d+)(M|H)$/);
      if (m) {
        const n = Number(m[1]);
        minutes = m[2] === "H" ? n * 60 : n;
      }

      const until = new Date(Date.now() + minutes * 60 * 1000)
        .toISOString()
        .replace("T", " ")
        .slice(0, 19);

      await dbRun(`UPDATE apis SET mute_until=? WHERE id=?`, [until, id]);
      await zap.sendMessage(grupo, `🔕 Serviço id=${id} silenciado por ${minutes} min.`);
      return;
    }

    if (texto.startsWith("ATIVAR")) {
      const parts = textoRaw.trim().split(/\s+/);
      const id = Number(parts[1]);
      if (!id) {
        await zap.sendMessage(grupo, "Uso: ATIVAR <id>\nEx: ATIVAR 3");
        return;
      }
      await dbRun(`UPDATE apis SET mute_until=NULL WHERE id=?`, [id]);
      await zap.sendMessage(grupo, `🔔 Serviço id=${id} reativado (alertas voltaram).`);
      return;
    }
  } catch (e) {
    console.log("❌ Erro comandos WhatsApp:", e.message);
  }
});

// ================= LOGIN =================
app.post("/login", (req, res) => {
  if (req.body.user === config.login?.user && req.body.pass === config.login?.pass) {
    req.session.logado = true;
    return res.json({ ok: true });
  }
  return res.json({ ok: false });
});

app.post("/logout", (req, res) => {
  req.session.destroy(() => res.json({ ok: true }));
});

// ================= TIPOS (LISTAR) =================
app.get("/api/types", auth, async (req, res) => {
  try {
    const rows = await dbAll("SELECT * FROM monitor_types ORDER BY name ASC");
    res.json(rows || []);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.post("/api/types", auth, async (req, res) => {
  try {
    const name = String(req.body?.name || "").trim().toUpperCase();
    const default_interval_ms = Math.max(500, Number(req.body?.default_interval_ms ?? 3000));
    const default_timeout_ms = Math.max(500, Number(req.body?.default_timeout_ms ?? 5000));
    if (!name) return res.status(400).json({ error: "name obrigatório" });

    const r = await dbRun(
      `INSERT INTO monitor_types(name, default_interval_ms, default_timeout_ms) VALUES(?,?,?)`,
      [name, default_interval_ms, default_timeout_ms]
    );
    res.json({ ok: true, id: r.lastID });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.put("/api/types/:id", auth, async (req, res) => {
  try {
    const id = Number(req.params.id);
    const name = String(req.body?.name || "").trim().toUpperCase();
    const default_interval_ms = Math.max(500, Number(req.body?.default_interval_ms ?? 3000));
    const default_timeout_ms = Math.max(500, Number(req.body?.default_timeout_ms ?? 5000));
    if (!id) return res.status(400).json({ error: "id inválido" });
    if (!name) return res.status(400).json({ error: "name obrigatório" });

    const r = await dbRun(
      `UPDATE monitor_types SET name=?, default_interval_ms=?, default_timeout_ms=? WHERE id=?`,
      [name, default_interval_ms, default_timeout_ms, id]
    );
    res.json({ ok: true, changes: r.changes });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ================= ALERT RULES =================
app.get("/api/alert-rules", auth, async (req, res) => {
  try {
    const rules = await getRules();
    res.json({
      offline_after_sec: Number(rules.offline_after_sec ?? 5),
      slow_ms: Number(rules.slow_ms ?? 1200),
      slow_after_sec: Number(rules.slow_after_sec ?? 180),
      flap_window_sec: Number(rules.flap_window_sec ?? 1800),
      flap_count: Number(rules.flap_count ?? 4),
      fail_n: Number(rules.fail_n ?? 1),
      ok_m: Number(rules.ok_m ?? 1),
      still_down_every_sec: Number(rules.still_down_every_sec ?? 600),
      notify_whatsapp: Number(rules.notify_whatsapp ?? 1),
      whatsapp_group: String(rules.whatsapp_group ?? ""),
      zapPronto,
      slow_disabled: true,
    });
  } catch (e) {
    res.status(500).json({ error: e.message, zapPronto });
  }
});

app.put("/api/alert-rules", auth, async (req, res) => {
  try {
    const body = req.body || {};
    const offline_after_sec = Math.max(1, Number(body.offline_after_sec ?? 5));
    const slow_ms = Math.max(50, Number(body.slow_ms ?? 1200));
    const slow_after_sec = Math.max(10, Number(body.slow_after_sec ?? 180));
    const flap_window_sec = Math.max(60, Number(body.flap_window_sec ?? 1800));
    const flap_count = Math.max(2, Number(body.flap_count ?? 4));
    const fail_n = Math.max(1, Number(body.fail_n ?? 1));
    const ok_m = Math.max(1, Number(body.ok_m ?? 1));
    const still_down_every_sec = Math.max(60, Number(body.still_down_every_sec ?? 600));
    const notify_whatsapp = Number(body.notify_whatsapp ?? 1) ? 1 : 0;
    const whatsapp_group = String(body.whatsapp_group ?? "").trim();

    await dbRun(
      `
      UPDATE alert_rules
      SET offline_after_sec=?,
          slow_ms=?,
          slow_after_sec=?,
          flap_window_sec=?,
          flap_count=?,
          fail_n=?,
          ok_m=?,
          still_down_every_sec=?,
          notify_whatsapp=?,
          whatsapp_group=?,
          updated_at=CURRENT_TIMESTAMP
      WHERE id=1
      `,
      [
        offline_after_sec,
        slow_ms,
        slow_after_sec,
        flap_window_sec,
        flap_count,
        fail_n,
        ok_m,
        still_down_every_sec,
        notify_whatsapp,
        whatsapp_group,
      ]
    );

    const rules = await getRules();
    res.json({ ok: true, rules });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ================= TESTE WHATSAPP =================
app.post("/api/test-whatsapp", auth, async (req, res) => {
  try {
    const rules = await getRules();
    const msg = (req.body?.msg || "").trim() || "🚀 TESTE MONITOR OK";
    const sent = await enviarWhatsApp(msg, rules);
    res.json({ ok: sent, zapPronto });
  } catch (e) {
    res.json({ ok: false, error: e.message, zapPronto });
  }
});

// ================= CRUD APIS =================
app.get("/api/apis", auth, async (req, res) => {
  try {
    const rows = await dbAll(`
      SELECT a.*, t.name AS type_name, t.default_interval_ms, t.default_timeout_ms
      FROM apis a
      LEFT JOIN monitor_types t ON t.id = a.type_id
      ORDER BY a.id DESC
    `);
    res.json(rows);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.post("/api/apis", auth, async (req, res) => {
  try {
    const {
      nome,
      url,
      metodo = "GET",
      headers_json = "",
      xml = "",
      porta = null,
      ativo = 1,
      timeout_ms = null,
      type_id = null,
      interval_ms = null,
      adaptive_enabled = 1,
      tipo = "",
      mute_until = null,
    } = req.body || {};

    if (!nome || !url) return res.status(400).json({ error: "nome e url são obrigatórios" });

    const r = await dbRun(
      `INSERT INTO apis(nome,url,metodo,headers_json,xml,porta,ativo,timeout_ms,type_id,interval_ms,adaptive_enabled,current_interval_ms,ok_streak,fail_streak,tipo,mute_until)
       VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
      [
        nome.trim(),
        url.trim(),
        String(metodo).toUpperCase(),
        headers_json || "",
        xml || "",
        porta != null ? Number(porta) : null,
        Number(ativo) ? 1 : 0,
        timeout_ms != null ? Number(timeout_ms) : null,
        type_id != null ? Number(type_id) : null,
        interval_ms != null ? Number(interval_ms) : null,
        Number(adaptive_enabled) ? 1 : 0,
        null,
        0,
        0,
        String(tipo || "Outros").trim(),
        mute_until ? String(mute_until) : null,
      ]
    );

    await setUltimoStatus(r.lastID, "UNKNOWN");
    res.json({ ok: true, id: r.lastID });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.put("/api/apis/:id", auth, async (req, res) => {
  try {
    const id = Number(req.params.id);
    const {
      nome,
      url,
      metodo = "GET",
      headers_json = "",
      xml = "",
      porta = null,
      ativo = 1,
      timeout_ms = null,
      type_id = null,
      interval_ms = null,
      adaptive_enabled = 1,
      mute_until = null,
      tipo = "Outros",
    } = req.body || {};

    if (!nome || !url) return res.status(400).json({ error: "nome e url são obrigatórios" });

    const r = await dbRun(
      `UPDATE apis
         SET nome=?,
             url=?,
             metodo=?,
             headers_json=?,
             xml=?,
             porta=?,
             ativo=?,
             timeout_ms=?,
             type_id=?,
             interval_ms=?,
             adaptive_enabled=?,
             current_interval_ms=?,
             ok_streak=?,
             fail_streak=?,
             mute_until=?,
             tipo=?
       WHERE id=?`,
      [
        nome.trim(),
        url.trim(),
        String(metodo).toUpperCase(),
        headers_json || "",
        xml || "",
        porta != null ? Number(porta) : null,
        Number(ativo) ? 1 : 0,
        timeout_ms != null ? Number(timeout_ms) : null,
        type_id != null ? Number(type_id) : null,
        interval_ms != null ? Number(interval_ms) : null,
        Number(adaptive_enabled) ? 1 : 0,
        interval_ms != null ? Number(interval_ms) : null,
        0,
        0,
        mute_until ? String(mute_until) : null,
        String(tipo || "Outros").trim(),
        id,
      ]
    );

    res.json({ ok: true, changes: r.changes });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.delete("/api/apis/:id", auth, async (req, res) => {
  try {
    const id = Number(req.params.id);
    clearOfflineAlertTimer(id);
    await dbRun("DELETE FROM apis WHERE id=?", [id]);
    await dbRun("DELETE FROM api_state WHERE api_id=?", [id]);
    await dbRun("DELETE FROM incidents WHERE api_id=? AND closed_at IS NULL", [id]);
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ================= LOGS (GRÁFICO) =================
app.get("/api/logs", auth, async (req, res) => {
  try {
    const api_id = req.query.api_id ? Number(req.query.api_id) : null;
    const limit = req.query.limit ? Math.min(Number(req.query.limit), 2000) : 500;
    if (!api_id) return res.status(400).json({ error: "api_id obrigatório" });

    const rows = await dbAll(
      "SELECT * FROM logs WHERE api_id=? ORDER BY id DESC LIMIT ?",
      [api_id, limit]
    );
    res.json(rows || []);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ================= SUMMARY =================
app.get("/api/summary", auth, async (req, res) => {
  try {
    const apis = await dbAll(`
      SELECT a.*, t.name AS type_name, t.default_interval_ms, t.default_timeout_ms
      FROM apis a
      LEFT JOIN monitor_types t ON t.id = a.type_id
      ORDER BY a.id DESC
    `);

    const result = [];
    for (const api of apis) {
      const last = await dbGet(
        "SELECT status, tempo, detalhe, data FROM logs WHERE api_id=? ORDER BY id DESC LIMIT 1",
        [api.id]
      );

      result.push({
        id: api.id,
        nome: api.nome,
        type_id: api.type_id,
        type_name: api.type_name || null,
        url: api.url,
        metodo: api.metodo,
        porta: api.porta,
        ativo: api.ativo,
        timeout_ms: api.timeout_ms,
        interval_ms: api.interval_ms,
        adaptive_enabled: api.adaptive_enabled ?? 1,
        current_interval_ms: api.current_interval_ms ?? null,
        ok_streak: api.ok_streak ?? 0,
        fail_streak: api.fail_streak ?? 0,
        effective_interval_ms: Number(getEffectiveMonitorInterval(api)),
        mute_until: api.mute_until,
        tipo: api.tipo || api.type_name || null,
        last_check_at: api.last_check_at,
        last_status: api.last_status || last?.status || "—",
        last_tempo: api.last_tempo ?? last?.tempo ?? null,
        last_detalhe: api.last_detalhe || last?.detalhe || "",
        last_data: api.last_data || last?.data || null,
      });
    }

    res.json(result);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ================= HELPERS RELATÓRIOS =================
function parseDateRange(from, to) {
  const start = new Date(`${from}T00:00:00-03:00`);
  const end = new Date(`${to}T23:59:59-03:00`);

  if (Number.isNaN(start.getTime()) || Number.isNaN(end.getTime())) {
    throw new Error("Datas inválidas");
  }

  if (end < start) {
    throw new Error("Período inválido: 'to' menor que 'from'");
  }

  return { start, end };
}

function overlapSeconds(startA, endA, startB, endB) {
  const ini = Math.max(startA.getTime(), startB.getTime());
  const fim = Math.min(endA.getTime(), endB.getTime());
  if (fim <= ini) return 0;
  return Math.floor((fim - ini) / 1000);
}

function pct(num, den) {
  if (!den || den <= 0) return 100;
  return Number(((num / den) * 100).toFixed(4));
}

function toMinutes(sec) {
  return Number((Number(sec || 0) / 60).toFixed(2));
}

function toHours(sec) {
  return Number((Number(sec || 0) / 3600).toFixed(2));
}

function formatDurationHMS(totalSeconds) {
  const sec = Math.max(0, Math.floor(Number(totalSeconds || 0)));
  const h = Math.floor(sec / 3600);
  const m = Math.floor((sec % 3600) / 60);
  const s = sec % 60;

  return [
    String(h).padStart(2, "0"),
    String(m).padStart(2, "0"),
    String(s).padStart(2, "0"),
  ].join(":");
}

function formatReportMetric(totalSec, unavailableSec) {
  const total = Math.max(0, Number(totalSec || 0));
  const indisponivel = Math.max(0, Math.min(total, Number(unavailableSec || 0)));
  const disponivel = Math.max(0, total - indisponivel);
  const disponibilidade = pct(disponivel, total);
  const lacuna = Number((100 - disponibilidade).toFixed(4));

  return {
    tempo_total_sec: total,
    tempo_total_min: toMinutes(total),
    tempo_total_h: toHours(total),
    tempo_total_hms: formatDurationHMS(total),

    tempo_disponivel_sec: disponivel,
    tempo_disponivel_min: toMinutes(disponivel),
    tempo_disponivel_h: toHours(disponivel),
    tempo_disponivel_hms: formatDurationHMS(disponivel),

    tempo_indisponivel_sec: indisponivel,
    tempo_indisponivel_min: toMinutes(indisponivel),
    tempo_indisponivel_h: toHours(indisponivel),
    tempo_indisponivel_hms: formatDurationHMS(indisponivel),

    disponibilidade_pct: disponibilidade,
    indisponibilidade_pct: lacuna,
    calculo: "Tempo disponível / Tempo total × 100",
    indicador: "Disponibilidade de sistemas Criticos",
    direcao: "Melhor para Cima",
    unidade: "%",
    calculo_lacuna: lacuna,
  };
}

async function gerarRelatorioDisponibilidade({ from, to, api_id = "", type_id = "", tipo = "" }) {
  const { start, end } = parseDateRange(from, to);
  const totalPeriodSec = Math.floor((end.getTime() - start.getTime()) / 1000);

  const apiWhere = ["a.ativo = 1"];
  const apiParams = [];

  if (api_id) {
    apiWhere.push("a.id = ?");
    apiParams.push(Number(api_id));
  }

  if (type_id) {
    apiWhere.push("a.type_id = ?");
    apiParams.push(Number(type_id));
  }

  if (tipo) {
    apiWhere.push("UPPER(COALESCE(a.tipo, t.name, 'OUTROS')) = UPPER(?)");
    apiParams.push(String(tipo).trim());
  }

  const apiWhereSql = apiWhere.length ? `WHERE ${apiWhere.join(" AND ")}` : "";

  const apis = await dbAll(
    `
    SELECT
      a.id,
      a.nome,
      a.tipo,
      a.type_id,
      a.ativo,
      t.name AS type_name
    FROM apis a
    LEFT JOIN monitor_types t ON t.id = a.type_id
    ${apiWhereSql}
    ORDER BY a.nome ASC
    `,
    apiParams
  );

  if (!apis.length) {
    return {
      period: {
        from,
        to,
        tempo_total_periodo_sec: totalPeriodSec,
        tempo_total_periodo_min: toMinutes(totalPeriodSec),
        tempo_total_periodo_h: toHours(totalPeriodSec),
        tempo_total_periodo_hms: formatDurationHMS(totalPeriodSec),
      },
      overall: {
        total_itens: 0,
        ...formatReportMetric(0, 0),
      },
      by_item: [],
      by_type: [],
    };
  }

  const apiIds = apis.map((a) => Number(a.id));

  const incidents = await dbAll(
    `
    SELECT
      i.id,
      i.api_id,
      i.type,
      i.opened_at,
      i.closed_at,
      i.duration_sec,
      i.start_detail,
      i.end_detail
    FROM incidents i
    WHERE i.api_id IN (${apiIds.map(() => "?").join(",")})
      AND i.type = 'OFFLINE'
      AND i.opened_at <= ?
      AND COALESCE(i.closed_at, CURRENT_TIMESTAMP) >= ?
    ORDER BY i.api_id, i.opened_at ASC
    `,
    [...apiIds, `${to} 23:59:59`, `${from} 00:00:00`]
  );

  const incidentsByApi = new Map();
  for (const inc of incidents) {
    const list = incidentsByApi.get(Number(inc.api_id)) || [];
    list.push(inc);
    incidentsByApi.set(Number(inc.api_id), list);
  }

  const byItem = [];
  const byTypeMap = new Map();
  let overallUnavailableSec = 0;

  for (const api of apis) {
    const tipoNome = api.type_name || api.tipo || "Outros";
    const list = incidentsByApi.get(Number(api.id)) || [];
    let unavailableSec = 0;

    for (const inc of list) {
      const incStart = new Date(String(inc.opened_at).replace(" ", "T") + "-03:00");
      const incEnd = inc.closed_at
        ? new Date(String(inc.closed_at).replace(" ", "T") + "-03:00")
        : end;

      unavailableSec += overlapSeconds(incStart, incEnd, start, end);
    }

    if (unavailableSec > totalPeriodSec) unavailableSec = totalPeriodSec;

    const metric = formatReportMetric(totalPeriodSec, unavailableSec);

    byItem.push({
      api_id: api.id,
      nome: api.nome,
      tipo: tipoNome,
      ativo: Number(api.ativo || 0),
      ...metric,
    });

    overallUnavailableSec += unavailableSec;

    if (!byTypeMap.has(tipoNome)) {
      byTypeMap.set(tipoNome, {
        tipo: tipoNome,
        qtd_itens: 0,
        tempo_total_sec: 0,
        tempo_indisponivel_sec: 0,
      });
    }

    const grp = byTypeMap.get(tipoNome);
    grp.qtd_itens += 1;
    grp.tempo_total_sec += totalPeriodSec;
    grp.tempo_indisponivel_sec += unavailableSec;
  }

  const byType = Array.from(byTypeMap.values())
    .map((grp) => ({
      tipo: grp.tipo,
      qtd_itens: grp.qtd_itens,
      ...formatReportMetric(grp.tempo_total_sec, grp.tempo_indisponivel_sec),
    }))
    .sort((a, b) => a.tipo.localeCompare(b.tipo, "pt-BR"));

  const overallTotalSec = totalPeriodSec * apis.length;

  return {
    period: {
      from,
      to,
      tempo_total_periodo_sec: totalPeriodSec,
      tempo_total_periodo_min: toMinutes(totalPeriodSec),
      tempo_total_periodo_h: toHours(totalPeriodSec),
      tempo_total_periodo_hms: formatDurationHMS(totalPeriodSec),
    },
    overall: {
      total_itens: apis.length,
      ...formatReportMetric(overallTotalSec, overallUnavailableSec),
    },
    by_item: byItem,
    by_type: byType,
  };
}

// ================= RELATÓRIOS =================
app.get("/api/reports/overview", auth, async (req, res) => {
  try {
    const { from, to, type = "", api_id = "" } = req.query;
    if (!from || !to) return res.status(400).json({ error: "from/to obrigatórios" });

    const where = [];
    const params = [];

    where.push(`opened_at BETWEEN ? AND ?`);
    params.push(`${from} 00:00:00`, `${to} 23:59:59`);

    if (type) { where.push(`type=?`); params.push(type); }
    if (api_id) { where.push(`api_id=?`); params.push(Number(api_id)); }
    const whereSql = `WHERE ${where.join(" AND ")}`;

    const row = await dbGet(
      `
      SELECT
        COUNT(*) AS total_incidents,
        COALESCE(SUM(CASE WHEN type='OFFLINE' THEN duration_sec ELSE 0 END), 0) AS total_downtime_sec,
        COALESCE(AVG(CASE WHEN type='OFFLINE' THEN duration_sec END), 0) AS mttr_sec
      FROM incidents
      ${whereSql}
      `,
      params
    );

    const topDown = await dbAll(
      `
      SELECT api_id, COALESCE(SUM(CASE WHEN type='OFFLINE' THEN duration_sec ELSE 0 END),0) AS downtime_sec
      FROM incidents
      ${whereSql}
      GROUP BY api_id
      ORDER BY downtime_sec DESC
      LIMIT 10
      `,
      params
    );

    const topInc = await dbAll(
      `
      SELECT api_id, COUNT(*) AS incidents
      FROM incidents
      ${whereSql}
      GROUP BY api_id
      ORDER BY incidents DESC
      LIMIT 10
      `,
      params
    );

    const ids = Array.from(new Set([...topDown.map((x) => x.api_id), ...topInc.map((x) => x.api_id)]));
    let nameMap = new Map();
    if (ids.length) {
      const names = await dbAll(`SELECT id, nome FROM apis WHERE id IN (${ids.map(() => "?").join(",")})`, ids);
      nameMap = new Map(names.map((n) => [Number(n.id), n.nome]));
    }

    const top_by_downtime = topDown.map((x) => ({
      ...x,
      service_name: nameMap.get(Number(x.api_id)) || null,
      downtime_min: toMinutes(x.downtime_sec),
      downtime_h: toHours(x.downtime_sec),
      downtime_hms: formatDurationHMS(x.downtime_sec),
    }));

    const top_by_incidents = topInc.map((x) => ({
      ...x,
      service_name: nameMap.get(Number(x.api_id)) || null,
    }));

    const worst_service_name = top_by_downtime[0]?.service_name || null;

    res.json({
      total_incidents: Number(row?.total_incidents || 0),
      total_downtime_sec: Number(row?.total_downtime_sec || 0),
      total_downtime_min: toMinutes(row?.total_downtime_sec || 0),
      total_downtime_h: toHours(row?.total_downtime_sec || 0),
      total_downtime_hms: formatDurationHMS(row?.total_downtime_sec || 0),
      mttr_sec: Math.round(Number(row?.mttr_sec || 0)),
      mttr_min: toMinutes(row?.mttr_sec || 0),
      mttr_h: toHours(row?.mttr_sec || 0),
      mttr_hms: formatDurationHMS(row?.mttr_sec || 0),
      worst_service_name,
      top_by_downtime,
      top_by_incidents,
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get("/api/reports/incidents", auth, async (req, res) => {
  try {
    const { from, to, type = "", api_id = "" } = req.query;
    if (!from || !to) return res.status(400).json({ error: "from/to obrigatórios" });

    const where = [];
    const params = [];

    where.push(`i.opened_at BETWEEN ? AND ?`);
    params.push(`${from} 00:00:00`, `${to} 23:59:59`);

    if (type) { where.push(`i.type=?`); params.push(type); }
    if (api_id) { where.push(`i.api_id=?`); params.push(Number(api_id)); }
    const whereSql = `WHERE ${where.join(" AND ")}`;

    const rows = await dbAll(
      `
      SELECT i.*,
             a.nome AS service_name,
             a.url AS service_url,
             a.metodo AS service_method
      FROM incidents i
      LEFT JOIN apis a ON a.id=i.api_id
      ${whereSql}
      ORDER BY i.opened_at DESC
      LIMIT 5000
      `,
      params
    );

    const enriched = (rows || []).map((r) => ({
      ...r,
      duration_min: toMinutes(r.duration_sec || 0),
      duration_h: toHours(r.duration_sec || 0),
      duration_hms: formatDurationHMS(r.duration_sec || 0),
    }));

    res.json(enriched);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get("/api/reports/timeline", auth, async (req, res) => {
  try {
    const { from, to, api_id = "" } = req.query;
    if (!from || !to) return res.status(400).json({ error: "from/to obrigatórios" });

    const where = [`opened_at BETWEEN ? AND ?`];
    const params = [`${from} 00:00:00`, `${to} 23:59:59`];

    if (api_id) { where.push(`api_id=?`); params.push(Number(api_id)); }
    const whereSql = `WHERE ${where.join(" AND ")}`;

    const rows = await dbAll(
      `
      SELECT
        substr(opened_at,1,10) AS day,
        COUNT(*) AS incidents,
        COALESCE(SUM(CASE WHEN type='OFFLINE' THEN duration_sec ELSE 0 END),0) AS downtime_sec
      FROM incidents
      ${whereSql}
      GROUP BY day
      ORDER BY day ASC
      `,
      params
    );

    const enriched = (rows || []).map((r) => ({
      ...r,
      downtime_min: toMinutes(r.downtime_sec || 0),
      downtime_h: toHours(r.downtime_sec || 0),
      downtime_hms: formatDurationHMS(r.downtime_sec || 0),
    }));

    res.json(enriched);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ===== NOVO RELATÓRIO DE DISPONIBILIDADE =====
app.get("/api/reports/availability", auth, async (req, res) => {
  try {
    const { from, to, api_id = "", type_id = "", tipo = "" } = req.query;
    if (!from || !to) {
      return res.status(400).json({ error: "from/to obrigatórios" });
    }

    const report = await gerarRelatorioDisponibilidade({
      from,
      to,
      api_id,
      type_id,
      tipo,
    });

    res.json(report);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ================= EXPORT CSV =================
app.get("/api/reports/export.csv", auth, async (req, res) => {
  try {
    const { from, to, api_id = "", type_id = "", tipo = "" } = req.query;
    if (!from || !to) return res.status(400).send("from/to obrigatórios");

    const report = await gerarRelatorioDisponibilidade({ from, to, api_id, type_id, tipo });

    let csv =
      "ID;Serviço;Tipo;Tempo Total (s);Tempo Total (min);Tempo Total (h);Tempo Total (HH:mm:ss);Tempo Disponível (s);Tempo Disponível (min);Tempo Disponível (h);Tempo Disponível (HH:mm:ss);Tempo Indisponível (s);Tempo Indisponível (min);Tempo Indisponível (h);Tempo Indisponível (HH:mm:ss);Disponibilidade (%);Indisponibilidade (%);Cálculo\n";

    for (const r of report.by_item) {
      csv += [
        r.api_id,
        (r.nome || "").replace(/;/g, ","),
        (r.tipo || "").replace(/;/g, ","),

        r.tempo_total_sec,
        String(r.tempo_total_min).replace(".", ","),
        String(r.tempo_total_h).replace(".", ","),
        r.tempo_total_hms,

        r.tempo_disponivel_sec,
        String(r.tempo_disponivel_min).replace(".", ","),
        String(r.tempo_disponivel_h).replace(".", ","),
        r.tempo_disponivel_hms,

        r.tempo_indisponivel_sec,
        String(r.tempo_indisponivel_min).replace(".", ","),
        String(r.tempo_indisponivel_h).replace(".", ","),
        r.tempo_indisponivel_hms,

        String(r.disponibilidade_pct).replace(".", ","),
        String(r.indisponibilidade_pct).replace(".", ","),
        r.calculo,
      ].join(";") + "\n";
    }

    res.setHeader("Content-Type", "text/csv; charset=utf-8");
    res.setHeader("Content-Disposition", "attachment; filename=relatorio_disponibilidade.csv");
    res.send(csv);
  } catch (e) {
    res.status(500).send(e.message);
  }
});

// ================= EXPORT XLSX =================
app.get("/api/reports/export.xlsx", auth, async (req, res) => {
  try {
    if (!ExcelJS) {
      return res.status(500).send("exceljs não instalado. Rode: npm i exceljs");
    }

    const { from, to, api_id = "", type_id = "", tipo = "" } = req.query;
    if (!from || !to) return res.status(400).send("from/to obrigatórios");

    const report = await gerarRelatorioDisponibilidade({ from, to, api_id, type_id, tipo });

    const wb = new ExcelJS.Workbook();

    const wsResumo = wb.addWorksheet("Resumo");
    wsResumo.columns = [
      { header: "Período De", key: "from", width: 15 },
      { header: "Período Até", key: "to", width: 15 },
      { header: "Total Itens", key: "total_itens", width: 12 },

      { header: "Tempo Total (s)", key: "tempo_total_sec", width: 18 },
      { header: "Tempo Total (min)", key: "tempo_total_min", width: 18 },
      { header: "Tempo Total (h)", key: "tempo_total_h", width: 18 },
      { header: "Tempo Total (HH:mm:ss)", key: "tempo_total_hms", width: 20 },

      { header: "Tempo Disponível (s)", key: "tempo_disponivel_sec", width: 22 },
      { header: "Tempo Disponível (min)", key: "tempo_disponivel_min", width: 22 },
      { header: "Tempo Disponível (h)", key: "tempo_disponivel_h", width: 22 },
      { header: "Tempo Disponível (HH:mm:ss)", key: "tempo_disponivel_hms", width: 24 },

      { header: "Tempo Indisponível (s)", key: "tempo_indisponivel_sec", width: 24 },
      { header: "Tempo Indisponível (min)", key: "tempo_indisponivel_min", width: 24 },
      { header: "Tempo Indisponível (h)", key: "tempo_indisponivel_h", width: 24 },
      { header: "Tempo Indisponível (HH:mm:ss)", key: "tempo_indisponivel_hms", width: 26 },

      { header: "Disponibilidade (%)", key: "disponibilidade_pct", width: 18 },
      { header: "Indisponibilidade (%)", key: "indisponibilidade_pct", width: 22 },
      { header: "Cálculo", key: "calculo", width: 35 },
      { header: "Indicador", key: "indicador", width: 34 },
      { header: "Direção", key: "direcao", width: 18 },
      { header: "Unidade", key: "unidade", width: 10 },
      { header: "Cálculo Lacuna", key: "calculo_lacuna", width: 18 },
    ];

    wsResumo.addRow({
      from: report.period.from,
      to: report.period.to,
      total_itens: report.overall.total_itens,

      tempo_total_sec: report.overall.tempo_total_sec,
      tempo_total_min: report.overall.tempo_total_min,
      tempo_total_h: report.overall.tempo_total_h,
      tempo_total_hms: report.overall.tempo_total_hms,

      tempo_disponivel_sec: report.overall.tempo_disponivel_sec,
      tempo_disponivel_min: report.overall.tempo_disponivel_min,
      tempo_disponivel_h: report.overall.tempo_disponivel_h,
      tempo_disponivel_hms: report.overall.tempo_disponivel_hms,

      tempo_indisponivel_sec: report.overall.tempo_indisponivel_sec,
      tempo_indisponivel_min: report.overall.tempo_indisponivel_min,
      tempo_indisponivel_h: report.overall.tempo_indisponivel_h,
      tempo_indisponivel_hms: report.overall.tempo_indisponivel_hms,

      disponibilidade_pct: report.overall.disponibilidade_pct,
      indisponibilidade_pct: report.overall.indisponibilidade_pct,
      calculo: report.overall.calculo,
      indicador: report.overall.indicador,
      direcao: report.overall.direcao,
      unidade: report.overall.unidade,
      calculo_lacuna: report.overall.calculo_lacuna,
    });
    wsResumo.getRow(1).font = { bold: true };

    const wsItens = wb.addWorksheet("Por Item");
    wsItens.columns = [
      { header: "ID", key: "api_id", width: 8 },
      { header: "Serviço", key: "nome", width: 30 },
      { header: "Tipo", key: "tipo", width: 18 },
      { header: "Ativo", key: "ativo", width: 10 },

      { header: "Tempo Total (s)", key: "tempo_total_sec", width: 18 },
      { header: "Tempo Total (min)", key: "tempo_total_min", width: 18 },
      { header: "Tempo Total (h)", key: "tempo_total_h", width: 18 },
      { header: "Tempo Total (HH:mm:ss)", key: "tempo_total_hms", width: 20 },

      { header: "Tempo Disponível (s)", key: "tempo_disponivel_sec", width: 22 },
      { header: "Tempo Disponível (min)", key: "tempo_disponivel_min", width: 22 },
      { header: "Tempo Disponível (h)", key: "tempo_disponivel_h", width: 22 },
      { header: "Tempo Disponível (HH:mm:ss)", key: "tempo_disponivel_hms", width: 24 },

      { header: "Tempo Indisponível (s)", key: "tempo_indisponivel_sec", width: 24 },
      { header: "Tempo Indisponível (min)", key: "tempo_indisponivel_min", width: 24 },
      { header: "Tempo Indisponível (h)", key: "tempo_indisponivel_h", width: 24 },
      { header: "Tempo Indisponível (HH:mm:ss)", key: "tempo_indisponivel_hms", width: 26 },

      { header: "Disponibilidade (%)", key: "disponibilidade_pct", width: 18 },
      { header: "Indisponibilidade (%)", key: "indisponibilidade_pct", width: 22 },
      { header: "Cálculo", key: "calculo", width: 35 },
      { header: "Indicador", key: "indicador", width: 34 },
      { header: "Direção", key: "direcao", width: 18 },
      { header: "Unidade", key: "unidade", width: 10 },
      { header: "Cálculo Lacuna", key: "calculo_lacuna", width: 18 },
    ];
    report.by_item.forEach((r) => wsItens.addRow(r));
    wsItens.getRow(1).font = { bold: true };

    const wsTipos = wb.addWorksheet("Por Tipo");
    wsTipos.columns = [
      { header: "Tipo", key: "tipo", width: 20 },
      { header: "Qtd Itens", key: "qtd_itens", width: 12 },

      { header: "Tempo Total (s)", key: "tempo_total_sec", width: 18 },
      { header: "Tempo Total (min)", key: "tempo_total_min", width: 18 },
      { header: "Tempo Total (h)", key: "tempo_total_h", width: 18 },
      { header: "Tempo Total (HH:mm:ss)", key: "tempo_total_hms", width: 20 },

      { header: "Tempo Disponível (s)", key: "tempo_disponivel_sec", width: 22 },
      { header: "Tempo Disponível (min)", key: "tempo_disponivel_min", width: 22 },
      { header: "Tempo Disponível (h)", key: "tempo_disponivel_h", width: 22 },
      { header: "Tempo Disponível (HH:mm:ss)", key: "tempo_disponivel_hms", width: 24 },

      { header: "Tempo Indisponível (s)", key: "tempo_indisponivel_sec", width: 24 },
      { header: "Tempo Indisponível (min)", key: "tempo_indisponivel_min", width: 24 },
      { header: "Tempo Indisponível (h)", key: "tempo_indisponivel_h", width: 24 },
      { header: "Tempo Indisponível (HH:mm:ss)", key: "tempo_indisponivel_hms", width: 26 },

      { header: "Disponibilidade (%)", key: "disponibilidade_pct", width: 18 },
      { header: "Indisponibilidade (%)", key: "indisponibilidade_pct", width: 22 },
      { header: "Cálculo", key: "calculo", width: 35 },
      { header: "Indicador", key: "indicador", width: 34 },
      { header: "Direção", key: "direcao", width: 18 },
      { header: "Unidade", key: "unidade", width: 10 },
      { header: "Cálculo Lacuna", key: "calculo_lacuna", width: 18 },
    ];
    report.by_type.forEach((r) => wsTipos.addRow(r));
    wsTipos.getRow(1).font = { bold: true };

    res.setHeader(
      "Content-Type",
      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    );
    res.setHeader(
      "Content-Disposition",
      "attachment; filename=relatorio_disponibilidade.xlsx"
    );

    await wb.xlsx.write(res);
    res.end();
  } catch (e) {
    res.status(500).send(e.message);
  }
});

// ================= START =================
const PORT = config.port || 3011;
app.listen(PORT, () => {
  console.log("==================================");
  console.log(" NOC MONITOR RODANDO");
  console.log(" Arquivo:", __filename);
  console.log(` http://localhost:${PORT}/login.html`);
  console.log("==================================");
});
