// Signaling + chat relay. Optional: DATABASE_URL (Postgres/Neon) and FIREBASE_SERVICE_ACCOUNT (JSON) env vars.
const { WebSocketServer } = require('ws');

let pool = null;
if (process.env.DATABASE_URL) {
  const { Pool } = require('pg');
  pool = new Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false } });
  pool.query(
    'CREATE TABLE IF NOT EXISTS messages (id SERIAL PRIMARY KEY, to_id TEXT, from_id TEXT, body TEXT, ts BIGINT);' +
    'CREATE TABLE IF NOT EXISTS tokens (id TEXT PRIMARY KEY, token TEXT);' +
    'CREATE TABLE IF NOT EXISTS keys (id TEXT PRIMARY KEY, pk TEXT)'
  ).catch((e) => console.error('DB init failed:', e.message));
}

let admin = null;
if (process.env.FIREBASE_SERVICE_ACCOUNT) {
  try {
    admin = require('firebase-admin');
    admin.initializeApp({ credential: admin.credential.cert(JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT)) });
  } catch (e) { console.error('Firebase init failed:', e.message); admin = null; }
}

let memMsgs = [];
const memTok = new Map();
const memKeys = new Map();
const pending = new Map(); // callee id -> {from, v, ts}

async function saveMsg(to, from, body) {
  const ts = Date.now();
  if (pool) await pool.query('INSERT INTO messages(to_id,from_id,body,ts) VALUES($1,$2,$3,$4)', [to, from, body, ts]);
  else memMsgs.push({ to_id: to, from_id: from, body, ts });
}
async function takeMsgs(to) {
  let rows;
  if (pool) rows = (await pool.query('DELETE FROM messages WHERE to_id=$1 RETURNING from_id, body, ts', [to])).rows;
  else { rows = memMsgs.filter((m) => m.to_id === to); memMsgs = memMsgs.filter((m) => m.to_id !== to); }
  return rows.sort((a, b) => Number(a.ts) - Number(b.ts));
}
async function setTok(id, token) {
  if (pool) await pool.query('INSERT INTO tokens(id,token) VALUES($1,$2) ON CONFLICT (id) DO UPDATE SET token=$2', [id, token]);
  else memTok.set(id, token);
}
async function getTok(id) {
  if (pool) { const r = await pool.query('SELECT token FROM tokens WHERE id=$1', [id]); return r.rows[0] && r.rows[0].token; }
  return memTok.get(id);
}
async function getKey(id) {
  if (pool) { const r = await pool.query('SELECT pk FROM keys WHERE id=$1', [id]); return r.rows[0] && r.rows[0].pk; }
  return memKeys.get(id);
}
async function setKey(id, pk) {
  if (pool) await pool.query('INSERT INTO keys(id,pk) VALUES($1,$2) ON CONFLICT (id) DO NOTHING', [id, pk]);
  else memKeys.set(id, pk);
}
async function push(to, title, body) {
  if (!admin) return false;
  try {
    const token = await getTok(to);
    if (!token) return false;
    await admin.messaging().send({ token, notification: { title, body }, android: { priority: 'high' } });
    return true;
  } catch (e) { console.error('push failed:', e.message); return false; }
}

const clients = new Map();
const wss = new WebSocketServer({ port: process.env.PORT || 8080 });
wss.on('connection', (ws) => {
  let id = null;
  ws.isAlive = true;
  ws.on('pong', () => (ws.isAlive = true));
  ws.on('message', async (raw) => {
    try {
      let m;
      try { m = JSON.parse(raw); } catch { return; }
      if (m.t === 'reg') {
        if (!/^\d{6,12}$/.test(m.id) || typeof m.pk !== 'string' || m.pk.length !== 44) { ws.send(JSON.stringify({ t: 'err', msg: 'Update the app (bad registration)' })); return; }
        const known = await getKey(m.id);
        if (known && known !== m.pk) { ws.send(JSON.stringify({ t: 'err', msg: 'This ID is already taken' })); return; }
        if (!known) await setKey(m.id, m.pk);
        id = m.id; clients.set(id, ws);
        for (const x of await takeMsgs(id)) ws.send(JSON.stringify({ t: 'msg', from: x.from_id, c: x.body }));
        const p = pending.get(id); pending.delete(id);
        if (p && Date.now() - p.ts < 45000) ws.send(JSON.stringify({ t: 'invite', from: p.from, v: p.v }));
        return;
      }
      if (!id) return;
      if (m.t === 'tok') { if (typeof m.token === 'string' && m.token.length < 400) await setTok(id, m.token); return; }
      if (m.t === 'getkey') { if (typeof m.to === 'string') ws.send(JSON.stringify({ t: 'key', from: m.to, pk: (await getKey(m.to)) || null })); return; }
      if (typeof m.to !== 'string') return;
      const target = clients.get(m.to);
      const online = target && target.readyState === 1;
      if (m.t === 'msg') {
        if (typeof m.c !== 'string' || !m.c || m.c.length > 8000) return;
        if (online) target.send(JSON.stringify({ t: 'msg', from: id, c: m.c }));
        else { await saveMsg(m.to, id, m.c); await push(m.to, 'New message', 'You have a new encrypted message'); }
        return;
      }
      if (m.t === 'invite' && !online) {
        if (await push(m.to, 'Incoming ' + (m.v ? 'video ' : '') + 'call', 'Call from ' + id)) {
          pending.set(m.to, { from: id, v: !!m.v, ts: Date.now() });
          ws.send(JSON.stringify({ t: 'ringing', from: m.to }));
        } else ws.send(JSON.stringify({ t: 'offline', from: m.to }));
        return;
      }
      if (m.t === 'bye' && !online) { const p = pending.get(m.to); if (p && p.from === id) pending.delete(m.to); return; }
      if (online) { m.from = id; target.send(JSON.stringify(m)); }
    } catch (e) { console.error('handler error:', e.message); }
  });
  ws.on('close', () => { if (id && clients.get(id) === ws) clients.delete(id); });
});
setInterval(() => wss.clients.forEach((w) => { if (!w.isAlive) return w.terminate(); w.isAlive = false; w.ping(); }), 30000);
console.log('Server running. DB:', !!pool, 'Push:', !!admin);
