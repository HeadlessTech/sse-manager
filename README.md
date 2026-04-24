# sse-io

A lightweight Server-Sent Events (SSE) library for Node.js, inspired by [socket.io](https://socket.io). Manages clients, namespaces, and rooms so you can push updates to browsers without the overhead of WebSockets.

SSE is a native browser protocol — no client library needed. `sse-io` gives you the server-side infrastructure to organize connections the same way socket.io does: namespaces to separate concerns, rooms to group clients, and a fluent emit API to target exactly who needs an update.

Scales horizontally with the built-in **Redis adapter**.

---

## Contents

- [Why sse-io](#why-sse-io)
- [Install](#install)
- [Quick start](#quick-start)
- [Core concepts](#core-concepts)
- [API reference](#api-reference)
  - [SSEServer](#sseserver)
  - [SSENamespace](#ssenamespace)
  - [Client](#client)
  - [ChainableEmitter](#chainableemitter)
- [Typed events](#typed-events)
- [Middleware](#middleware)
- [Horizontal scaling with Redis](#horizontal-scaling-with-redis)
- [Wire format](#wire-format)
- [Browser usage](#browser-usage)
- [Migrating from a hand-rolled SSE helper](#migrating-from-a-hand-rolled-sse-helper)

---

## Why sse-io

SSE is the right tool when you need reliable server-to-client push:

- **Built into every browser** — no polyfills, no client library
- **Auto-reconnect** — the browser retries dropped connections automatically
- **Works over HTTP/1.1 and HTTP/2** — no upgrade handshake
- **Proxies and load balancers understand it** — unlike WebSockets

The gap is on the server side. Raw `res.write()` calls scattered across your codebase don't scale. `sse-io` fills that gap with the namespace/room model you already know from socket.io, without pulling in the WebSocket transport.

---

## Install

```bash
npm install sse-io
```

For horizontal scaling, also install the Redis peer dependency:

```bash
npm install ioredis
```

---

## Quick start

```typescript
import express from 'express';
import { SSEServer } from 'sse-io';

const app = express();
const io = new SSEServer();

const orders = io.of('/orders');

// 1. Define your SSE route
app.get('/stream/orders/:orderId', (req, res) => {
  const client = orders.connect(req, res);
  client.join(req.params.orderId);
});

// 2. Push updates from anywhere in your app
orders.to(orderId).emit('update_order', { orderId, status: 'ready' });

app.listen(3000);
```

```javascript
// Browser — no library needed
const es = new EventSource('/stream/orders/abc123');

es.addEventListener('update_order', (e) => {
  const data = JSON.parse(e.data);
  console.log(data); // { orderId: 'abc123', status: 'ready' }
});
```

---

## Core concepts

### Namespace

A namespace is a logical channel, identified by a name (e.g. `'/orders'`, `'/notifications'`). Each namespace manages its own set of connected clients and rooms. You can have as many namespaces as you like — they share the same HTTP server but are completely isolated from each other.

```typescript
const orders = io.of('/orders');
const notifications = io.of('/notifications');
```

### Room

A room is a named group of clients within a namespace. Clients can be in multiple rooms simultaneously. Rooms are created automatically when the first client joins and removed when the last client leaves.

```typescript
// Server
client.join('order-123');
client.join(['order-123', 'store-5']); // join multiple at once

// Emit to everyone in the room
orders.to('order-123').emit('update', data);
```

### Client

Represents a single SSE connection. Created by `namespace.connect(req, res)`. Exposes methods for joining rooms, emitting directly to that connection, and listening for lifecycle events.

### Adapter

Adapters handle the actual delivery of broadcast messages. The default `MemoryAdapter` delivers within the same process. The `RedisAdapter` uses Redis pub/sub to deliver across multiple server instances — enabling horizontal scaling without sticky sessions.

---

## API reference

### SSEServer

Top-level class. Manages namespaces and the active adapter.

```typescript
import { SSEServer } from 'sse-io';

const io = new SSEServer(options?);
```

**Options**

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `heartbeatInterval` | `number` | `30000` | Interval in ms for keepalive pings. Set to `0` to disable. |
| `cors.origin` | `string` | — | Value for `Access-Control-Allow-Origin` header on SSE responses. |
| `cors.credentials` | `boolean` | — | Whether to send `Access-Control-Allow-Credentials`. |

**Methods**

```typescript
// Get or create a namespace. Subsequent calls with the same name return the same instance.
io.of(name: string): SSENamespace
io.of<Events>(name: string): SSENamespace<Events>  // typed variant

// Set the adapter for all namespaces
io.adapter(adapter: Adapter): this

// Shorthand emit on the default '/' namespace
io.to(rooms: string | string[]): ChainableEmitter

// Gracefully close all namespaces and the adapter
await io.close()
```

---

### SSENamespace

Manages all clients and rooms within a namespace.

**Registering a connection**

```typescript
namespace.connect(req: IncomingMessage, res: ServerResponse): Client
```

Call this inside your HTTP route handler. It writes SSE headers, sends a `connection` event with the assigned `clientId`, and returns the `Client` object. You then call `client.join(room)` to subscribe the client to updates.

```typescript
app.get('/stream/orders/:orderId', (req, res) => {
  const client = orders.connect(req, res);
  client.join(req.params.orderId);
});
```

**Connection handler**

```typescript
namespace.on('connection', (client: Client) => {
  console.log('connected', client.id);
  client.on('disconnect', () => console.log('disconnected', client.id));
});
```

**Emitting**

```typescript
// Target one room
namespace.to('room-id').emit('event', data)

// Target multiple rooms (deduplicated — clients in both rooms only receive once)
namespace.to(['room-a', 'room-b']).emit('event', data)

// Chain .to() calls
namespace.to('room-a').to('room-b').emit('event', data)

// Broadcast to all clients in namespace
namespace.emit('event', data)

// Broadcast to all except clients in specific rooms
namespace.except('vip-room').emit('event', data)
```

**Middleware**

```typescript
namespace.use((client, next) => { ... })
```

See [Middleware](#middleware) for details.

**Introspection**

```typescript
namespace.clientCount              // number of connected clients
namespace.getClients()             // ReadonlyMap<ClientId, Client>
namespace.getRooms()               // ReadonlyMap<Room, ReadonlySet<ClientId>>
namespace.getRoom('room-id')       // ReadonlySet<ClientId> | undefined
```

**Lifecycle**

```typescript
await namespace.close()  // disconnect all clients and stop heartbeat
```

---

### Client

Represents a single SSE connection.

**Properties**

```typescript
client.id            // unique client ID (20-char alphanumeric string)
client.handshake     // { headers, query, url } — snapshot at connect time
client.rooms         // Set<Room> — rooms currently joined
client.disconnected  // boolean
```

**Methods**

```typescript
// Join one or more rooms
client.join('room-id')
client.join(['room-a', 'room-b'])

// Leave one or more rooms
client.leave('room-id')
client.leave(['room-a', 'room-b'])

// Send an event directly to this client only
client.emit('event-name', data)

// Close this client's connection
client.disconnect()

// Listen for lifecycle events
client.on('disconnect', () => { ... })
```

**Accessing request data**

The `handshake` object contains a snapshot of request metadata available at connect time:

```typescript
app.get('/stream/orders/:orderId', (req, res) => {
  const client = orders.connect(req, res);

  // Query params: /stream/orders/abc?userId=99
  console.log(client.handshake.query.userId);  // '99'

  // Headers
  console.log(client.handshake.headers.authorization);
});
```

---

### ChainableEmitter

Returned by `namespace.to()` and `namespace.except()`. Accumulates room filters before dispatching.

```typescript
// All of these are equivalent
namespace.to('room-a').to('room-b').emit('event', data)
namespace.to(['room-a', 'room-b']).emit('event', data)

// Exclude rooms
namespace.except('room-x').emit('event', data)
namespace.to('room-a').except('room-b').emit('event', data)
```

`.emit()` is the terminal call — nothing is sent until you call it.

---

## Typed events

Pass an event map as a generic to get full type-checking on event names and data shapes.

```typescript
interface OrderEvents {
  update_order: { orderId: string; status: string; updatedAt: string };
  cancel_order: { orderId: string; reason: string };
}

const orders = io.of<OrderEvents>('/orders');

// TypeScript enforces event name and data shape
orders.to('order-123').emit('update_order', {
  orderId: '123',
  status: 'ready',
  updatedAt: new Date().toISOString(),
});

// Error: 'unknown_event' is not assignable to keyof OrderEvents
orders.emit('unknown_event', {});

// Error: missing 'reason' field
orders.to('order-123').emit('cancel_order', { orderId: '123' });
```

The `Client` object is also typed:

```typescript
orders.on('connection', (client: Client<OrderEvents>) => {
  client.emit('update_order', { orderId: '...', status: '...', updatedAt: '...' });
});
```

---

## Middleware

Middleware runs on each new connection, before the `connection` event fires. Use it for authentication, rate limiting, or attaching metadata.

```typescript
orders.use((client, next) => {
  const token = client.handshake.headers['authorization'];
  if (!isValidToken(token)) {
    return next(new Error('Unauthorized'));
  }
  next();
});
```

Calling `next(error)` closes the connection immediately — the `connection` event never fires and the client is not registered.

Multiple middleware functions run in the order they were registered:

```typescript
orders.use(authenticate);
orders.use(rateLimit);
orders.use(attachUserMetadata);
```

---

## Horizontal scaling with Redis

By default `sse-io` uses an in-memory adapter, which only works when all clients are connected to the same server process. For horizontal scaling, use the Redis adapter.

```typescript
import { SSEServer } from 'sse-io';
import { RedisAdapter } from 'sse-io/adapters/redis';

const io = new SSEServer();
io.adapter(new RedisAdapter({ url: 'redis://localhost:6379' }));

const orders = io.of('/orders');
```

Now when **Server A** emits `orders.to('order-123').emit('update_order', data)`, the payload is published to a Redis channel. Every server instance (A, B, C, …) subscribes to that channel and delivers the event to whichever clients are locally connected — no sticky sessions required.

**Options**

```typescript
new RedisAdapter({
  url: 'redis://localhost:6379',    // ioredis connection URL
})

// Or pass your own ioredis clients (useful if you already manage connections)
new RedisAdapter({
  pubClient: existingRedisClient,
  subClient: existingRedisClient.duplicate(),
})

// Custom channel prefix (default: 'sse-io')
new RedisAdapter({
  url: 'redis://localhost:6379',
  channelPrefix: 'myapp-sse',
})
```

**Set the adapter before calling `io.of()`** — namespaces created after `io.adapter()` pick up the new adapter automatically, but namespaces created before are also updated.

---

## Wire format

`sse-io` uses the SSE spec's native `event:` field for named events. This means browsers can use `addEventListener` directly without any client-side unpacking:

```
id: 1745497200000
event: update_order
data: {"orderId":"abc123","status":"ready","updatedAt":"2026-04-24T10:00:00Z"}

```

Each message:
- `id` — millisecond timestamp, used by the browser for `Last-Event-ID` on reconnect
- `event` — the event name passed to `.emit()`
- `data` — `JSON.stringify(data)`, split across multiple `data:` lines if it contains newlines

Keepalive heartbeats are sent as SSE comment lines and are invisible to application code:

```
: ping

```

---

## Browser usage

No library needed. Use the native `EventSource` API:

```javascript
const es = new EventSource('/stream/orders/abc123');

// Listen for named events
es.addEventListener('update_order', (event) => {
  const data = JSON.parse(event.data);
  console.log(data.status); // 'ready'
});

es.addEventListener('cancel_order', (event) => {
  const data = JSON.parse(event.data);
  console.log(data.reason);
});

// Connection lifecycle
es.onopen = () => console.log('connected');
es.onerror = () => console.log('disconnected, browser will retry');
```

The browser reconnects automatically when the connection drops, sending the `Last-Event-ID` header so the server knows where it left off.

**Sending credentials** (cookies, auth headers):

`EventSource` sends cookies automatically for same-origin requests. For cross-origin with credentials:

```javascript
const es = new EventSource('/stream/orders/abc123', { withCredentials: true });
```

Pair with `cors.credentials: true` in `SSEServer` options and an explicit `cors.origin`.

---

## Migrating from a hand-rolled SSE helper

If you have something like this:

```typescript
// Before
class SSENamespace {
  subscribe(req, res) { ... }
  join(client, room) { ... }
  publish(data, room) { ... }
}

const ns = new SSENamespace('/orders/order');

router.get('/orders/:orderId', async (req, res) => {
  const client = ns.subscribe(req, res);
  ns.join(client, req.params.orderId);
});

ns.publish('update_order', orderId);
```

The equivalent with `sse-io`:

```typescript
// After
import { SSEServer } from 'sse-io';

const io = new SSEServer();
const orders = io.of('/orders');

router.get('/orders/:orderId', async (req, res) => {
  const client = orders.connect(req, res);
  client.join(req.params.orderId);
});

orders.to(orderId).emit('update_order', { orderId });
```

Key differences:
- `subscribe` → `connect`
- `publish(data, room)` → `to(room).emit(event, data)` — events are now named
- `join(client, room)` → `client.join(room)` — the client manages its own room membership
- Data is always JSON — no raw string payloads

The browser listener also becomes simpler because the event name arrives in the `event:` field:

```javascript
// Before (parsing a type wrapper)
es.onmessage = (e) => {
  // had to check if data === 'update_order' as a string
};

// After (native named event)
es.addEventListener('update_order', (e) => {
  const data = JSON.parse(e.data);
});
```
