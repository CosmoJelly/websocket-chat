/**
 * redis.js — Redis client factory + presence helpers
 *
 * We need THREE separate Redis client instances:
 *   publisher  — sends messages / events
 *   subscriber — receives messages / events  (can't share with publisher)
 *   data       — regular get/set for presence, etc.
 *
 * Presence model:
 *   HSET  presence:<roomId>   <userId>  <username>
 *   The key has a TTL that each client refreshes via a heartbeat.
 *   On clean disconnect we HDEL immediately.
 *
 * Typing model:
 *   SETEX typing:<roomId>:<userId>  3  <username>
 *   Expires automatically — no explicit "stopped typing" needed.
 *
 * Pub/Sub channel:  chat:<roomId>
 *   Every payload is a JSON string with a `type` discriminant.
 */

const redis = require('redis');

const REDIS_URL = process.env.REDIS_URL || 'redis://localhost:6379';

function createClient(name) {
  const client = redis.createClient({ url: REDIS_URL });
  client.on('error',   (err) => console.error(`[redis:${name}]`, err.message));
  client.on('connect', ()    => console.log(`[redis:${name}] connected`));
  return client;
}

/* Three dedicated clients */
const pub  = createClient('pub');
const sub  = createClient('sub');
const data = createClient('data');

async function connectAll() {
  await Promise.all([pub.connect(), sub.connect(), data.connect()]);
}

/* ------------------------------------------------------------------ */
/* Channel helpers                                                     */
/* ------------------------------------------------------------------ */

function roomChannel(roomId) {
  return `chat:${roomId}`;
}

/**
 * Publish any structured event to a room channel.
 * Consumers receive raw JSON; they call JSON.parse themselves.
 */
async function publish(roomId, payload) {
  await pub.publish(roomChannel(roomId), JSON.stringify(payload));
}

/**
 * Subscribe to a room channel.
 * `handler` receives the parsed JS object.
 */
async function subscribe(roomId, handler) {
  await sub.subscribe(roomChannel(roomId), (raw) => {
    try { handler(JSON.parse(raw)); }
    catch (e) { console.error('[redis:sub] parse error', e); }
  });
}

async function unsubscribe(roomId) {
  await sub.unsubscribe(roomChannel(roomId));
}

/* ------------------------------------------------------------------ */
/* Presence                                                            */
/* ------------------------------------------------------------------ */

const PRESENCE_TTL = 30;   // seconds — clients must heartbeat within this window

/**
 * Mark a user as present in a room.
 * Also sets a TTL on the whole hash so stale rooms clean themselves up.
 */
async function setPresent(roomId, userId, username) {
  const key = `presence:${roomId}`;
  await data.hSet(key, userId, username);
  await data.expire(key, PRESENCE_TTL * 3);   // room-level TTL
}

/**
 * Refresh a user's presence TTL (called on heartbeat).
 * We store per-user expiry in a parallel sorted set so we can evict
 * individuals without blowing away the whole room hash.
 */
async function refreshPresence(roomId, userId, username) {
  const score = Date.now() + PRESENCE_TTL * 1000;
  await data.hSet(`presence:${roomId}`, userId, username);
  await data.zAdd(`presence_exp:${roomId}`, [{ score, value: userId }]);
  await data.expire(`presence_exp:${roomId}`, PRESENCE_TTL * 3);
}

/**
 * Remove a user from a room's presence set.
 */
async function removePresent(roomId, userId) {
  await data.hDel(`presence:${roomId}`, userId);
  await data.zRem(`presence_exp:${roomId}`, userId);
}

/**
 * Return { userId: username } for everyone currently in a room,
 * after pruning entries whose per-user TTL has elapsed.
 */
async function getPresence(roomId) {
  // Prune stale entries first
  const now = Date.now();
  const stale = await data.zRangeByScore(`presence_exp:${roomId}`, 0, now);
  if (stale.length) {
    await Promise.all(stale.map((uid) => data.hDel(`presence:${roomId}`, uid)));
    await data.zRemRangeByScore(`presence_exp:${roomId}`, 0, now);
  }

  return data.hGetAll(`presence:${roomId}`);   // {} if key missing
}

/* ------------------------------------------------------------------ */
/* Typing indicators                                                   */
/* ------------------------------------------------------------------ */

const TYPING_TTL = 4;   // seconds

async function setTyping(roomId, userId, username) {
  await data.setEx(`typing:${roomId}:${userId}`, TYPING_TTL, username);
}

async function clearTyping(roomId, userId) {
  await data.del(`typing:${roomId}:${userId}`);
}

/**
 * Return { userId: username } for everyone currently typing.
 * Uses a SCAN so it works correctly in cluster mode too.
 */
async function getTyping(roomId) {
  const pattern = `typing:${roomId}:*`;
  const result  = {};
  let cursor = 0;

  do {
    const reply = await data.scan(cursor, { MATCH: pattern, COUNT: 100 });
    cursor = reply.cursor;
    for (const key of reply.keys) {
      const uid      = key.split(':')[2];
      const username = await data.get(key);
      if (username) result[uid] = username;
    }
  } while (cursor !== 0);

  return result;
}

module.exports = {
  pub, sub, data,
  connectAll,
  publish, subscribe, unsubscribe,
  setPresent, refreshPresence, removePresent, getPresence,
  setTyping, clearTyping, getTyping,
  PRESENCE_TTL,
};
