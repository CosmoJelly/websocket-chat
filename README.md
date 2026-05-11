# ws-chat

A production-grade WebSocket chat server with real-time presence, typing indicators, and persisted message history. Zero framework shortcuts — pure Node.js, Redis, and PostgreSQL.

---

## Stack

| Layer | Tech | Role |
|---|---|---|
| Transport | Node.js `http` + `ws` | Raw HTTP server, WebSocket upgrade |
| Realtime fan-out | Redis Pub/Sub | Broadcast across multiple server nodes |
| Presence & typing | Redis hashes + sorted sets + SETEX | Per-user TTLs, auto-expiry |
| Persistence | PostgreSQL | Message history, user records |

---

## Quick Start

### 1. Start infrastructure

```bash
docker compose up -d
```

Starts PostgreSQL on `5432` and Redis on `6379`. Schema is bootstrapped automatically on first server start.

### 2. Configure environment

```bash
cp .env.example .env
# edit .env if your ports/passwords differ
```

### 3. Start the server

```bash
# production
npm start

# development (auto-restart on file changes, Node 18+)
npm run dev
```

Server listens on `http://localhost:3001`.

### 4. Open the test client

Open `index.html` directly in your browser (no build step needed). Enter a username and connect. Open multiple tabs to test presence and messaging.

---

## Architecture

```
Browser A                    Server Node 1              Browser B
   │                              │                         │
   │──── WS /ws ─────────────────►│                         │
   │  { type:'join', username }   │                         │
   │                              │── upsertUser() ──► PG   │
   │                              │── setPresent() ──► Redis │
   │                              │── subscribe('chat:general') ◄─ Redis
   │◄── { type:'welcome',         │                         │
   │      history, presence }     │                         │
   │                              │                         │──── WS /ws ──►│
   │                              │                         │  { type:'join' }
   │                              │◄────────────────────────│
   │                              │── publish('chat:general', presence:online)
   │◄── { type:'presence',        │                         │
   │      online:true }     ◄─────┤                         │
   │                              │                         │
   │──── { type:'message' } ─────►│── saveMessage() ──► PG  │
   │                              │── publish('chat:general', message)
   │                              │                         │
   │◄── { type:'message' }  ◄─────┤────────────────────────►│
```

### Multi-node scaling

Each server node subscribes to the Redis channel for every room it has live connections in. When any node publishes a message or event, Redis delivers it to all subscriber nodes, which each fan it out to their local WebSocket connections. No sticky sessions required.

---

## WebSocket Protocol

Connect to `ws://localhost:3001/ws`.

All messages are JSON with a `type` discriminant.

### Client → Server

#### `join` — must be the first message sent
```json
{ "type": "join", "roomId": "general", "username": "alice" }
```

#### `message` — send a chat message
```json
{ "type": "message", "content": "hello world" }
```
- Max 4000 characters
- Persisted to PostgreSQL immediately
- Broadcast to room via Redis

#### `typing` — user started typing
```json
{ "type": "typing" }
```
Sets a 4-second Redis TTL. Re-send while typing to keep it alive.

#### `stop_typing` — user stopped typing
```json
{ "type": "stop_typing" }
```

#### `heartbeat` — keep presence alive
```json
{ "type": "heartbeat" }
```
Must be sent at least every 30 seconds or the user's presence entry expires. The test client sends one every 10 seconds.

---

### Server → Client

#### `welcome` — sent immediately after a successful `join`
```json
{
  "type": "welcome",
  "userId": "uuid-...",
  "roomId": "general",
  "history": [
    {
      "id": "42",
      "userId": "uuid-...",
      "username": "bob",
      "content": "hey",
      "createdAt": "2025-01-01T12:00:00.000Z"
    }
  ],
  "presence": {
    "uuid-alice": "alice",
    "uuid-bob":   "bob"
  }
}
```
History is the 50 most recent messages, ordered oldest → newest.

#### `message` — a new chat message
```json
{
  "type":      "message",
  "id":        "43",
  "userId":    "uuid-...",
  "username":  "alice",
  "content":   "hello world",
  "createdAt": "2025-01-01T12:01:00.000Z"
}
```

#### `presence` — a user joined or left
```json
{ "type": "presence", "userId": "uuid-...", "username": "carol", "online": true }
{ "type": "presence", "userId": "uuid-...", "username": "carol", "online": false }
```

#### `typing` — typing indicator update
```json
{ "type": "typing", "userId": "uuid-...", "username": "dave", "isTyping": true }
{ "type": "typing", "userId": "uuid-...", "username": "dave", "isTyping": false }
```

#### `pong` — heartbeat reply
```json
{ "type": "pong" }
```

#### `error` — something went wrong
```json
{ "type": "error", "message": "username required" }
```

---

## REST Endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/health` | Liveness probe |
| `GET` | `/rooms/:id/presence` | Current presence snapshot |
| `GET` | `/rooms/:id/history?limit=50` | Message history (max 200) |

---

## Redis Key Schema

| Key | Type | TTL | Contents |
|---|---|---|---|
| `presence:<roomId>` | Hash | 90s (room-level) | `userId → username` |
| `presence_exp:<roomId>` | Sorted set | 90s | `userId` scored by expiry ms |
| `typing:<roomId>:<userId>` | String | 4s | `username` |
| `chat:<roomId>` | Pub/Sub channel | — | JSON event payloads |

---

## PostgreSQL Schema

```sql
rooms    (id TEXT PK, name TEXT, created_at TIMESTAMPTZ)
users    (id TEXT PK, username TEXT, created_at, last_seen TIMESTAMPTZ)
messages (id BIGSERIAL PK, room_id TEXT FK, user_id TEXT,
          username TEXT, content TEXT, deleted_at TIMESTAMPTZ, created_at TIMESTAMPTZ)
```

`messages` is indexed on `(room_id, created_at DESC)` for fast history queries.

---

## File Structure

```
ws-chat/
├── server.js          # HTTP server + WebSocket upgrade, REST routes
├── chat.js            # Per-connection handler, event dispatcher
├── db.js              # PostgreSQL pool, schema bootstrap, query helpers
├── redis.js           # Redis clients (pub/sub/data), presence & typing helpers
├── index.html         # Browser test client (no build step)
├── docker-compose.yml # PostgreSQL + Redis
├── .env.example       # Environment variable template
└── package.json
```
