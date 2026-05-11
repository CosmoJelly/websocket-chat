# ws-chat
### A WebSocket chat server that actually works

I wanted to build a real chat server - not the tutorial kind with Socket.io doing all the heavy lifting. So I did it properly. Raw WebSockets, Redis pub/sub, Postgres for history, presence tracking, typing indicators. The whole thing.

Built with Node.js, Redis, PostgreSQL, and a stubborn refusal to use a framework.

---

## What it does

- **Real-time messaging** - raw WebSocket connections, no polling, no fallbacks
- **User presence** - see who's online, updates instantly when people join or leave
- **Typing indicators** - auto-expire after 4 seconds so they never get stuck
- **Message history** - last 50 messages loaded on join, persisted in Postgres forever
- **Multi-room support** - join any room by name, rooms are created on the fly
- **Scales horizontally** - Redis pub/sub means multiple server nodes work out of the box
- **Invite anyone** - throw it behind ngrok and share the URL

---

## Tech Stack

| | |
|---|---|
| Runtime | Node.js (pure `http` + `ws`) |
| Realtime fan-out | Redis Pub/Sub |
| Presence & typing | Redis hashes + sorted sets |
| Persistence | PostgreSQL |
| Client | Vanilla JS, single HTML file |

---

## Getting it running

### Prerequisites

- Node.js 18+
- PostgreSQL
- Redis
- ngrok account (if you want to test it with the help of some friends)

### 1. Clone and install

```bash
git clone https://github.com/CosmoJelly/websocket-chat.git
cd websocket-chat
npm install
```

### 2. Environment variables

```bash
cp .env.example .env
```

```env
PORT=3001

PG_HOST=localhost
PG_PORT=5432
PG_DB=wschat
PG_USER=postgres
PG_PASSWORD=postgres

REDIS_URL=redis://localhost:6379
```

### 3. Set up the database

```bash
# Arch Linux
sudo pacman -S postgresql redis (this could vary for your OS so just check)
sudo -u postgres initdb --locale=en_US.UTF-8 -D /var/lib/postgres/data
sudo systemctl enable --now postgresql
sudo systemctl enable --now redis
sudo -u postgres createdb wschat
```

### 4. Run it

```bash
npm start
```

Open `index.html` in your browser and connect or use
```bash
xdg-open index.html
```

---

## Inviting others

Expose your local server with ngrok and send them the URL:

```bash
ngrok http 3001
```

Tell them to open `index.html` and set the server field to:

```
wss://your-ngrok-url.ngrok-free.app/ws
```

---

## Protocol

Every message is JSON with a `type` field.

**Client → Server**
```json
{ "type": "join",         "roomId": "general", "username": "alice" }
{ "type": "message",      "content": "hey"                         }
{ "type": "typing"                                                  }
{ "type": "stop_typing"                                             }
{ "type": "heartbeat"                                               }
```

**Server → Client**
```json
{ "type": "welcome",  "userId": "...", "history": [...], "presence": {...} }
{ "type": "message",  "username": "alice", "content": "hey", "createdAt": "..." }
{ "type": "presence", "username": "alice", "online": true }
{ "type": "typing",   "username": "alice", "isTyping": true }
```

---

## Project structure

```
websocket-chat/
├── server.js       
├── chat.js            
├── db.js           
├── redis.js         
├── index.html
├── docker-compose.yml
├── .env.example
└── package.json
```

---

## Database schema

```sql
rooms    (id, name, created_at)
users    (id, username, created_at, last_seen)
messages (id, room_id → rooms, user_id, username,
          content, deleted_at, created_at)
```

---

## 🎧 Built to these playlists

> *[in my own head](https://open.spotify.com/playlist/0auscJmVTHpAzPK1til3I2?si=d6a7b18205d543b8)*
> *[on the soul search](https://open.spotify.com/playlist/5EupibT67sLlGYBYZj1qUP?si=51e057d732c4472d)*
> *[top lane tunes](https://open.spotify.com/playlist/2NDdO4ZAQTUg8ae5LY8t5y?si=cde32e103ce644a6)*
> *[real world actors](https://open.spotify.com/playlist/7qc583xE29T4hbWq2BJPXQ?si=a1891cf8e5dc40c5)*

---

## License

Do whatever you want with it.
