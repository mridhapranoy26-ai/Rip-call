// Anonymous WebSocket signaling relay. Put behind TLS (wss) in production.
const { WebSocketServer } = require('ws');
const wss = new WebSocketServer({ port: process.env.PORT || 8080 });
const clients = new Map();
wss.on('connection', (ws) => {
  let id = null;
  ws.isAlive = true;
  ws.on('pong', () => (ws.isAlive = true));
  ws.on('message', (raw) => {
    let m;
    try { m = JSON.parse(raw); } catch { return; }
    if (m.t === 'reg' && /^\d{6,12}$/.test(m.id)) { id = m.id; clients.set(id, ws); return; }
    if (!id || typeof m.to !== 'string') return;
    const target = clients.get(m.to);
    m.from = id; // server-set, cannot be spoofed
    if (target && target.readyState === 1) target.send(JSON.stringify(m));
    else if (m.t === 'invite') ws.send(JSON.stringify({ t: 'offline', from: m.to })); // FCM push hook goes here
  });
  ws.on('close', () => { if (id && clients.get(id) === ws) clients.delete(id); });
});
setInterval(() => wss.clients.forEach((w) => { if (!w.isAlive) return w.terminate(); w.isAlive = false; w.ping(); }), 30000);
console.log('Signaling server running');
