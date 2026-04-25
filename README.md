# sse-io

Server-Sent Events (SSE) is a well-supported, reliable protocol for pushing data from server to client in real time. What it lacks is any server-side model for managing connections — there is no built-in concept of which clients should receive which updates, no grouping, no targeting.

`sse-io` brings that structure to SSE, modelled on the API and concepts of [socket.io](https://socket.io): namespaces to partition your event streams, rooms to group clients, middleware for auth, and a fluent emit API to target exactly who needs a given update. If you already know socket.io, the patterns here will feel immediately familiar.

Scales horizontally with the built-in **Redis adapter**.

---

## Contents

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
import express from "express";
import { SSEServer } from "sse-io";

const app = express();
const sseServer = new SSEServer();

const ordersNamespace = sseServer.of("/orders");

// 1. Define your SSE route
app.get("/stream/orders/:orderId", (req, res) => {
  const client = ordersNamespace.connect(req, res);
  client.join(req.params.orderId);
});

// 2. Push updates from anywhere in your app
ordersNamespace.to(orderId).emit("update_order", { orderId, status: "ready" });

app.listen(3000);
```

```javascript
// Browser — no library needed
const es = new EventSource("/stream/orders/abc123");

es.addEventListener("update_order", (e) => {
  const data = JSON.parse(e.data);
  console.log(data); // { orderId: 'abc123', status: 'ready' }
});
```

---

## Core concepts

### Namespace

A namespace is a logical channel, identified by a name (e.g. `'/orders'`, `'/notifications'`). Each namespace manages its own set of connected clients and rooms. You can have as many namespaces as you like — they share the same HTTP server but are completely isolated from each other.

```typescript
const ordersNamespace = sseServer.of("/orders");
const notificationsNamespace = sseServer.of("/notifications");
```

### Room

A room is a named group of clients within a namespace. Clients can be in multiple rooms simultaneously. Rooms are created automatically when the first client joins and removed when the last client leaves.

```typescript
// Server
client.join("order-123");
client.join(["order-123", "store-5"]); // join multiple at once

// Emit to everyone in the room
ordersNamespace.to("order-123").emit("update", data);
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

const sseServer = new SSEServer(options?);
```

**Options**

| Option              | Type      | Default | Description                                                      |
| ------------------- | --------- | ------- | ---------------------------------------------------------------- |
| `heartbeatInterval` | `number`  | `30000` | Interval in ms for keepalive pings. Set to `0` to disable.       |
| `cors.origin`       | `string`  | —       | Value for `Access-Control-Allow-Origin` header on SSE responses. |
| `cors.credentials`  | `boolean` | —       | Whether to send `Access-Control-Allow-Credentials`.              |

**CORS example**

Required when your frontend and backend are on different origins — `EventSource` is a browser request and follows CORS rules.

```typescript
const sseServer = new SSEServer({
  cors: {
    origin: "https://app.example.com",
    credentials: true,
  },
});
```

For local development with different ports:

```typescript
const sseServer = new SSEServer({
  cors: { origin: "http://localhost:3000" },
});
```

**Methods**

```typescript
// Get or create a namespace. Subsequent calls with the same name return the same instance.
sseServer.of(name: string): SSENamespace
sseServer.of<Events>(name: string): SSENamespace<Events>  // typed variant

// Set the adapter for all namespaces
sseServer.adapter(adapter: Adapter): this

// Shorthand emit on the default '/' namespace
sseServer.to(rooms: string | string[]): ChainableEmitter

// Gracefully close all namespaces and the adapter
await sseServer.close()
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
app.get("/stream/orders/:orderId", (req, res) => {
  const client = ordersNamespace.connect(req, res);
  client.join(req.params.orderId);
});
```

**Connection handler**

```typescript
namespace.on("connection", (client: Client) => {
  console.log("connected", client.id);
  client.on("disconnect", () => console.log("disconnected", client.id));
});
```

**Emitting**

```typescript
// Target one room
namespace.to("room-id").emit("event", data);

// Target multiple rooms (deduplicated — clients in both rooms only receive once)
namespace.to(["room-a", "room-b"]).emit("event", data);

// Chain .to() calls
namespace.to("room-a").to("room-b").emit("event", data);

// Broadcast to all clients in namespace
namespace.emit("event", data);

// Broadcast to all except clients in specific rooms
namespace.except("vip-room").emit("event", data);
```

**Middleware**

```typescript
namespace.use((client, next) => { ... })
```

See [Middleware](#middleware) for details.

**Introspection**

```typescript
namespace.clientCount; // number of connected clients
namespace.getClients(); // ReadonlyMap<ClientId, Client>
namespace.getRooms(); // ReadonlyMap<Room, ReadonlySet<ClientId>>
namespace.getRoom("room-id"); // ReadonlySet<ClientId> | undefined
```

**Lifecycle**

```typescript
await namespace.close(); // disconnect all clients and stop heartbeat
```

---

### Client

Represents a single SSE connection.

**Properties**

```typescript
client.id; // unique client ID (20-char alphanumeric string)
client.handshake; // { headers, query, url } — snapshot at connect time
client.rooms; // Set<Room> — rooms currently joined
client.disconnected; // boolean
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
app.get("/stream/orders/:orderId", (req, res) => {
  const client = ordersNamespace.connect(req, res);

  // Query params: /stream/orders/abc?userId=99
  console.log(client.handshake.query.userId); // '99'

  // Headers
  console.log(client.handshake.headers.authorization);
});
```

---

### ChainableEmitter

Returned by `namespace.to()` and `namespace.except()`. Accumulates room filters before dispatching.

```typescript
// All of these are equivalent
namespace.to("room-a").to("room-b").emit("event", data);
namespace.to(["room-a", "room-b"]).emit("event", data);

// Exclude rooms
namespace.except("room-x").emit("event", data);
namespace.to("room-a").except("room-b").emit("event", data);
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

const ordersNamespace = sseServer.of<OrderEvents>("/orders");

// TypeScript enforces event name and data shape
ordersNamespace.to("order-123").emit("update_order", {
  orderId: "123",
  status: "ready",
  updatedAt: new Date().toISOString(),
});

// Error: 'unknown_event' is not assignable to keyof OrderEvents
ordersNamespace.emit("unknown_event", {});

// Error: missing 'reason' field
ordersNamespace.to("order-123").emit("cancel_order", { orderId: "123" });
```

The `Client` object is also typed:

```typescript
ordersNamespace.on("connection", (client: Client<OrderEvents>) => {
  client.emit("update_order", {
    orderId: "...",
    status: "...",
    updatedAt: "...",
  });
});
```

---

## Middleware

Middleware runs on each new connection, before the `connection` event fires. Use it for authentication, rate limiting, or attaching metadata.

```typescript
ordersNamespace.use((client, next) => {
  const token = client.handshake.headers["authorization"];
  if (!isValidToken(token)) {
    return next(new Error("Unauthorized"));
  }
  next();
});
```

Calling `next(error)` closes the connection immediately — the `connection` event never fires and the client is not registered.

Multiple middleware functions run in the order they were registered:

```typescript
ordersNamespace.use(authenticate);
ordersNamespace.use(rateLimit);
ordersNamespace.use(attachUserMetadata);
```

---

## Horizontal scaling with Redis

By default `sse-io` uses an in-memory adapter, which only works when all clients are connected to the same server process. For horizontal scaling, use the Redis adapter.

```typescript
import { SSEServer } from "sse-io";
import { RedisAdapter } from "sse-io/adapters/redis";

const sseServer = new SSEServer();
sseServer.adapter(new RedisAdapter({ url: "redis://localhost:6379" }));

const ordersNamespace = sseServer.of("/orders");
```

Now when **Server A** emits `ordersNamespace.to('order-123').emit('update_order', data)`, the payload is published to a Redis channel. Every server instance (A, B, C, …) subscribes to that channel and delivers the event to whichever clients are locally connected — no sticky sessions required.

**Options**

```typescript
new RedisAdapter({
  url: "redis://localhost:6379", // ioredis connection URL
});

// Or pass your own ioredis clients (useful if you already manage connections)
new RedisAdapter({
  pubClient: existingRedisClient,
  subClient: existingRedisClient.duplicate(),
});

// Custom channel prefix (default: 'sse-io')
new RedisAdapter({
  url: "redis://localhost:6379",
  channelPrefix: "myapp-sse",
});
```

**Set the adapter before calling `sseServer.of()`** — namespaces created after `sseServer.adapter()` pick up the new adapter automatically, but namespaces created before are also updated.

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
const es = new EventSource("/stream/orders/abc123");

// Listen for named events
es.addEventListener("update_order", (event) => {
  const data = JSON.parse(event.data);
  console.log(data.status); // 'ready'
});

es.addEventListener("cancel_order", (event) => {
  const data = JSON.parse(event.data);
  console.log(data.reason);
});

// Connection lifecycle
es.onopen = () => console.log("connected");
es.onerror = () => console.log("disconnected, browser will retry");
```

The browser reconnects automatically when the connection drops, sending the `Last-Event-ID` header so the server knows where it left off.

**Sending credentials** (cookies, auth headers):

`EventSource` sends cookies automatically for same-origin requests. For cross-origin with credentials:

```javascript
const es = new EventSource("/stream/orders/abc123", { withCredentials: true });
```

Pair with `cors.credentials: true` in `SSEServer` options and an explicit `cors.origin`.
