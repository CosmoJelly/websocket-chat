/**
 * db.js — PostgreSQL connection + schema bootstrap
 *
 * Tables:
 *   users    — registered chat identities (ephemeral or persistent)
 *   rooms    — chat channels
 *   messages — full message history with soft-delete
 */

const { Pool } = require('pg');

const pool = new Pool({
  host:     process.env.PG_HOST     || 'localhost',
  port:     parseInt(process.env.PG_PORT || '5432'),
  database: process.env.PG_DB       || 'wschat',
  user:     process.env.PG_USER     || 'postgres',
  password: process.env.PG_PASSWORD || 'postgres',
  max: 20,
  idleTimeoutMillis: 30_000,
  connectionTimeoutMillis: 5_000,
});

pool.on('error', (err) => {
  console.error('[pg] idle client error', err.message);
});

/* ------------------------------------------------------------------ */
/* Schema bootstrap — idempotent, run on server start                  */
/* ------------------------------------------------------------------ */

async function bootstrap() {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    await client.query(`
      CREATE TABLE IF NOT EXISTS rooms (
        id          TEXT        PRIMARY KEY,
        name        TEXT        NOT NULL,
        created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
    `);

    /* Seed a default room */
    await client.query(`
      INSERT INTO rooms (id, name)
      VALUES ('general', 'General')
      ON CONFLICT (id) DO NOTHING;
    `);

    await client.query(`
      CREATE TABLE IF NOT EXISTS users (
        id          TEXT        PRIMARY KEY,          -- UUID assigned at connect
        username    TEXT        NOT NULL,
        created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        last_seen   TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
    `);

    await client.query(`
      CREATE TABLE IF NOT EXISTS messages (
        id          BIGSERIAL   PRIMARY KEY,
        room_id     TEXT        NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
        user_id     TEXT        NOT NULL,
        username    TEXT        NOT NULL,             -- denormalised for speed
        content     TEXT        NOT NULL,
        deleted_at  TIMESTAMPTZ,                      -- soft delete
        created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
    `);

    await client.query(`
      CREATE INDEX IF NOT EXISTS messages_room_created
        ON messages (room_id, created_at DESC);
    `);

    await client.query('COMMIT');
    console.log('[pg] schema ready');
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}

/* ------------------------------------------------------------------ */
/* Helpers                                                             */
/* ------------------------------------------------------------------ */

/**
 * Upsert a user row on connect; update last_seen on reconnect.
 */
async function upsertUser(id, username) {
  await pool.query(`
    INSERT INTO users (id, username)
    VALUES ($1, $2)
    ON CONFLICT (id) DO UPDATE
      SET username  = EXCLUDED.username,
          last_seen = NOW();
  `, [id, username]);
}

/**
 * Persist a message and return the full row.
 */
async function saveMessage(roomId, userId, username, content) {
  const { rows } = await pool.query(`
    INSERT INTO messages (room_id, user_id, username, content)
    VALUES ($1, $2, $3, $4)
    RETURNING id, room_id, user_id, username, content, created_at;
  `, [roomId, userId, username, content]);
  return rows[0];
}

/**
 * Fetch the N most-recent messages for a room (returned oldest-first).
 */
async function getHistory(roomId, limit = 50) {
  const { rows } = await pool.query(`
    SELECT id, room_id, user_id, username, content, created_at
    FROM   messages
    WHERE  room_id   = $1
      AND  deleted_at IS NULL
    ORDER  BY created_at DESC
    LIMIT  $2;
  `, [roomId, limit]);
  return rows.reverse();   // oldest → newest
}

module.exports = { pool, bootstrap, upsertUser, saveMessage, getHistory };
