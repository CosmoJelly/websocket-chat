/**
 * chat.js — WebSocket session handler
 *
 * One instance of handleConnection() runs per connected client.
 *
 * Client → Server messages  (JSON, `type` field required):
 *   { type: 'join',    roomId, username }
 *   { type: 'message', content }
 *   { type: 'typing' }                       — user is typing
 *   { type: 'stop_typing' }
 *   { type: 'heartbeat' }                    — keep presence alive
 *
 * Server → Client messages  (JSON, `type` field):
 *   { type: 'welcome',   userId, roomId, history: [...], presence: {...} }
 *   { type: 'message',   id, userId, username, content, createdAt }
 *   { type: 'presence',  userId, username, online: true|false }
 *   { type: 'typing',    userId, username, isTyping: true|false }
 *   { type: 'error',     message }
 *   { type: 'pong' }
 */

const { v4: uuidv4 } = require('uuid');
const db    = require('./db');
const r     = require('./redis');

/* ------------------------------------------------------------------ */
/* Local registry: wsId → WebSocket  (for targeted sends)             */
/* Each WS connection gets a unique wsId to avoid userId collisions    */
/* when one userId reconnects from two tabs.                           */
/* ------------------------------------------------------------------ */
const connections = new Map();   // wsId → { ws, userId, username, roomId }

/* ------------------------------------------------------------------ */
/* Broadcast to every local connection in a room                       */
/* This node handles only its own connections; Redis pub/sub carries   */
/* messages to other nodes.                                            */
/* ------------------------------------------------------------------ */
function broadcastLocal(roomId, payload, excludeWsId = null) {
  const raw = JSON.stringify(payload);
  for (const [wsId, ctx] of connections) {
    if (ctx.roomId === roomId && wsId !== excludeWsId) {
      safeSend(ctx.ws, raw);
    }
  }
}

function safeSend(ws, raw) {
  if (ws.readyState === 1 /* OPEN */) {
    try { ws.send(raw); } catch (_) {}
  }
}

/* ------------------------------------------------------------------ */
/* Redis subscription — one per room, shared across local connections  */
/* ------------------------------------------------------------------ */
const subscribedRooms = new Set();

async function ensureRoomSubscribed(roomId) {
  if (subscribedRooms.has(roomId)) return;
  subscribedRooms.add(roomId);

  await r.subscribe(roomId, (payload) => {
    // Relay every Redis message to all local connections in the room.
    broadcastLocal(roomId, payload);
  });
}

/* ------------------------------------------------------------------ */
/* Main connection handler — called by server.js                       */
/* ------------------------------------------------------------------ */
async function handleConnection(ws, req) {
  const wsId = uuidv4();
  let   ctx  = null;           // set after successful 'join'

  /* Heartbeat / presence refresh interval */
  let heartbeatTimer = null;

  /* ---- inbound message router ------------------------------------ */
  ws.on('message', async (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw);
    } catch {
      return safeSend(ws, JSON.stringify({ type: 'error', message: 'Invalid JSON' }));
    }

    try {
      switch (msg.type) {
        case 'join':       await onJoin(msg);       break;
        case 'message':    await onMessage(msg);    break;
        case 'typing':     await onTyping(msg);     break;
        case 'stop_typing':await onStopTyping(msg); break;
        case 'heartbeat':  await onHeartbeat();     break;
        default:
          safeSend(ws, JSON.stringify({ type: 'error', message: `Unknown type: ${msg.type}` }));
      }
    } catch (err) {
      console.error(`[chat] handler error (${msg.type}):`, err.message);
      safeSend(ws, JSON.stringify({ type: 'error', message: 'Internal error' }));
    }
  });

  /* ---- disconnect ------------------------------------------------ */
  ws.on('close', async () => {
    clearInterval(heartbeatTimer);
    connections.delete(wsId);
    if (!ctx) return;

    await r.removePresent(ctx.roomId, ctx.userId);
    await r.clearTyping(ctx.roomId, ctx.userId);

    /* Notify others — but only if no other local tab for same userId */
    const stillHere = [...connections.values()]
      .some((c) => c.userId === ctx.userId && c.roomId === ctx.roomId);

    if (!stillHere) {
      await r.publish(ctx.roomId, {
        type: 'presence',
        userId:   ctx.userId,
        username: ctx.username,
        online:   false,
      });
    }

    console.log(`[chat] ${ctx.username} (${ctx.userId}) left ${ctx.roomId}`);
  });

  ws.on('error', (err) => console.error('[chat] ws error', err.message));

  /* ---------------------------------------------------------------- */
  /* Event handlers                                                    */
  /* ---------------------------------------------------------------- */

  async function onJoin(msg) {
    if (ctx) {
      return safeSend(ws, JSON.stringify({ type: 'error', message: 'Already joined' }));
    }

    const username = (msg.username || '').trim().slice(0, 32);
    const roomId   = (msg.roomId   || 'general').trim();

    if (!username) {
      return safeSend(ws, JSON.stringify({ type: 'error', message: 'username required' }));
    }

    const userId = uuidv4();

    ctx = { ws, wsId, userId, username, roomId };
    connections.set(wsId, ctx);

    /* Persist / update user record */
    await db.upsertUser(userId, username);

    /* Register presence */
    await r.setPresent(roomId, userId, username);
    await r.refreshPresence(roomId, userId, username);

    /* Subscribe this node to the room channel */
    await ensureRoomSubscribed(roomId);

    /* Fetch history & current presence */
    const [history, presence] = await Promise.all([
      db.getHistory(roomId, 50),
      r.getPresence(roomId),
    ]);

    /* Send welcome packet directly to the joining client */
    safeSend(ws, JSON.stringify({
      type:     'welcome',
      userId,
      roomId,
      history:  history.map(normaliseRow),
      presence,
    }));

    /* Announce arrival to everyone else via Redis */
    await r.publish(roomId, {
      type:     'presence',
      userId,
      username,
      online:   true,
    });

    /* Start heartbeat — keep presence TTL alive */
    heartbeatTimer = setInterval(async () => {
      await r.refreshPresence(roomId, userId, username);
    }, (r.PRESENCE_TTL / 2) * 1000);

    console.log(`[chat] ${username} (${userId}) joined ${roomId}`);
  }

  async function onMessage(msg) {
    if (!ctx) return;
    const content = (msg.content || '').trim();
    if (!content) return;
    if (content.length > 4000) {
      return safeSend(ws, JSON.stringify({ type: 'error', message: 'Message too long' }));
    }

    /* Persist */
    const row = await db.saveMessage(ctx.roomId, ctx.userId, ctx.username, content);

    /* Clear typing indicator */
    await r.clearTyping(ctx.roomId, ctx.userId);

    /* Broadcast via Redis so all nodes in the cluster receive it */
    await r.publish(ctx.roomId, {
      type:      'message',
      id:        row.id.toString(),
      userId:    ctx.userId,
      username:  ctx.username,
      content:   row.content,
      createdAt: row.created_at,
    });
  }

  async function onTyping(msg) {
    if (!ctx) return;
    await r.setTyping(ctx.roomId, ctx.userId, ctx.username);
    await r.publish(ctx.roomId, {
      type:      'typing',
      userId:    ctx.userId,
      username:  ctx.username,
      isTyping:  true,
    });
  }

  async function onStopTyping(msg) {
    if (!ctx) return;
    await r.clearTyping(ctx.roomId, ctx.userId);
    await r.publish(ctx.roomId, {
      type:      'typing',
      userId:    ctx.userId,
      username:  ctx.username,
      isTyping:  false,
    });
  }

  async function onHeartbeat() {
    if (ctx) await r.refreshPresence(ctx.roomId, ctx.userId, ctx.username);
    safeSend(ws, JSON.stringify({ type: 'pong' }));
  }
}

/* ------------------------------------------------------------------ */
/* Normalise a PG row to camelCase for the wire format                 */
/* ------------------------------------------------------------------ */
function normaliseRow(row) {
  return {
    id:        row.id.toString(),
    userId:    row.user_id,
    username:  row.username,
    content:   row.content,
    createdAt: row.created_at,
  };
}

module.exports = { handleConnection };
