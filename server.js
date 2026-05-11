/**
 * server.js — Entry point
 *
 * Pure Node.js HTTP server with WebSocket upgrade.
 * No Express, no Fastify, no framework shortcuts.
 *
 * Endpoints:
 *   GET  /health          — liveness probe
 *   GET  /rooms/:id/presence  — current presence snapshot (REST)
 *   WS   /ws              — WebSocket chat endpoint
 */

const http   = require('http');
const { WebSocketServer } = require('ws');
const url    = require('url');

const db   = require('./db');
const r    = require('./redis');
const chat = require('./chat');

const PORT = parseInt(process.env.PORT || '3001');

/* ------------------------------------------------------------------ */
/* HTTP request router                                                 */
/* ------------------------------------------------------------------ */

function respond(res, statusCode, body) {
  const payload = typeof body === 'string' ? body : JSON.stringify(body);
  res.writeHead(statusCode, {
    'Content-Type':                'application/json',
    'Access-Control-Allow-Origin': '*',
  });
  res.end(payload);
}

async function routeRequest(req, res) {
  const parsed   = url.parse(req.url, true);
  const pathname = parsed.pathname;

  /* CORS pre-flight */
  if (req.method === 'OPTIONS') {
    res.writeHead(204, {
      'Access-Control-Allow-Origin':  '*',
      'Access-Control-Allow-Methods': 'GET, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type',
    });
    return res.end();
  }

  /* GET /health */
  if (req.method === 'GET' && pathname === '/health') {
    return respond(res, 200, { status: 'ok', ts: new Date() });
  }

  /* GET /rooms/:id/presence */
  const presenceMatch = pathname.match(/^\/rooms\/([^/]+)\/presence$/);
  if (req.method === 'GET' && presenceMatch) {
    const roomId   = presenceMatch[1];
    const presence = await r.getPresence(roomId);
    return respond(res, 200, { roomId, presence });
  }

  /* GET /rooms/:id/history?limit=50 */
  const historyMatch = pathname.match(/^\/rooms\/([^/]+)\/history$/);
  if (req.method === 'GET' && historyMatch) {
    const roomId = historyMatch[1];
    const limit  = Math.min(parseInt(parsed.query.limit || '50'), 200);
    const rows   = await db.getHistory(roomId, limit);
    return respond(res, 200, { roomId, messages: rows });
  }

  respond(res, 404, { error: 'Not found' });
}

const server = http.createServer(async (req, res) => {
  try {
    await routeRequest(req, res);
  } catch (err) {
    console.error('[http] unhandled error', err);
    respond(res, 500, { error: 'Internal server error' });
  }
});

/* ------------------------------------------------------------------ */
/* WebSocket server — attached to the same HTTP server                 */
/* ------------------------------------------------------------------ */

const wss = new WebSocketServer({
  server,
  path: '/ws',
  // Verify the upgrade; add auth token checks here if needed
  verifyClient: ({ req }, done) => {
    // Accept all origins for now; in production verify Origin header
    done(true);
  },
});

wss.on('connection', (ws, req) => {
  chat.handleConnection(ws, req).catch((err) => {
    console.error('[wss] connection handler threw', err);
    ws.terminate();
  });
});

/* ------------------------------------------------------------------ */
/* Graceful shutdown                                                   */
/* ------------------------------------------------------------------ */

async function shutdown(signal) {
  console.log(`\n[server] ${signal} received — shutting down gracefully`);

  await new Promise((resolve) => {
    wss.close(resolve);
    server.close(resolve);
  });

  await r.pub.quit();
  await r.sub.quit();
  await r.data.quit();
  await db.pool.end();

  console.log('[server] clean exit');
  process.exit(0);
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT',  () => shutdown('SIGINT'));

/* ------------------------------------------------------------------ */
/* Boot                                                                */
/* ------------------------------------------------------------------ */

async function main() {
  console.log('[server] connecting to Redis & PostgreSQL...');
  await r.connectAll();
  await db.bootstrap();

  server.listen(PORT, () => {
    console.log(`[server] listening on http://localhost:${PORT}`);
    console.log(`[server] WebSocket endpoint: ws://localhost:${PORT}/ws`);
  });
}

main().catch((err) => {
  console.error('[server] startup failed', err);
  process.exit(1);
});
