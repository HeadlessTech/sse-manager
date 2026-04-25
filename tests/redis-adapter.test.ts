import { describe, it, expect, afterEach, vi } from "vitest";

// ioredis-mock's channels and modifiedKeyEvents are global singletons shared
// across all instances — raise their limits once so tests don't trigger warnings.
{
  const probe = new RedisMock();
  const ctx = (probe as unknown as { context: { channels: { setMaxListeners(n: number): void }; modifiedKeyEvents: { setMaxListeners(n: number): void } } }).context;
  ctx.channels.setMaxListeners(100);
  ctx.modifiedKeyEvents.setMaxListeners(100);
}
import { createServer, request, type Server } from "node:http";
import RedisMock from "ioredis-mock";
import { RedisAdapter } from "../src/adapters/redis.js";
import { SSEServer } from "../src/server.js";
import type { SSENamespace } from "../src/namespace.js";
import type { EventMap } from "../src/types.js";

// ── helpers ───────────────────────────────────────────────────────────────────

function makeMockClients() {
  const shared = new RedisMock();
  shared.setMaxListeners(50);
  return { pub: shared.duplicate(), sub: shared.duplicate() };
}

interface SSEEvent { id?: string; event?: string; data?: string; }

function createSSEClient(url: string) {
  const buffered: SSEEvent[] = [];
  const waiters = new Map<string, Array<(e: SSEEvent) => void>>();
  const { hostname, port, pathname, search } = new URL(url);
  const req = request(
    { hostname, port, path: pathname + search, headers: { Accept: "text/event-stream" } },
    (res) => {
      let carry = "";
      res.on("data", (chunk: Buffer) => {
        carry += chunk.toString();
        const messages = carry.split("\n\n");
        carry = messages.pop() ?? "";
        for (const msg of messages) {
          if (!msg.trim() || msg.startsWith(":")) continue;
          const evt: SSEEvent = {};
          for (const line of msg.split("\n")) {
            if (line.startsWith("id: ")) evt.id = line.slice(4);
            else if (line.startsWith("event: ")) evt.event = line.slice(7);
            else if (line.startsWith("data: ")) evt.data = line.slice(6);
          }
          if (!evt.event) continue;
          const pending = waiters.get(evt.event);
          if (pending?.length) pending.shift()!(evt);
          else buffered.push(evt);
        }
      });
    }
  );
  req.end();
  return {
    nextEvent(eventName: string, timeout = 2000): Promise<SSEEvent> {
      const idx = buffered.findIndex((e) => e.event === eventName);
      if (idx >= 0) return Promise.resolve(buffered.splice(idx, 1)[0]);
      return new Promise((resolve, reject) => {
        const timer = setTimeout(
          () => reject(new Error(`Timeout waiting for "${eventName}"`)),
          timeout
        );
        const list = waiters.get(eventName) ?? [];
        list.push((e) => { clearTimeout(timer); resolve(e); });
        waiters.set(eventName, list);
      });
    },
    close() { req.destroy(); },
  };
}

interface ServerInstance {
  ns: SSENamespace<EventMap>;
  connect: (productId: string) => ReturnType<typeof createSSEClient>;
  close: () => Promise<void>;
}

async function makeServerInstance(adapter: RedisAdapter): Promise<ServerInstance> {
  const sseServer = new SSEServer({ heartbeatInterval: 0 });
  sseServer.adapter(adapter);
  const ns = sseServer.of("/products");
  const clients: ReturnType<typeof createSSEClient>[] = [];

  const httpServer: Server = createServer((req, res) => {
    const match = req.url?.match(/^\/stream\/products\/([^?]+)/);
    if (match) {
      const client = ns.connect(req, res);
      client.join(match[1]);
    } else {
      res.writeHead(404); res.end();
    }
  });

  const port = await new Promise<number>((resolve) => {
    httpServer.listen(0, () => resolve((httpServer.address() as { port: number }).port));
  });

  return {
    ns,
    connect(productId) {
      const c = createSSEClient(`http://localhost:${port}/stream/products/${productId}`);
      clients.push(c);
      return c;
    },
    async close() {
      clients.forEach((c) => c.close());
      clients.length = 0;
      await sseServer.close();
      await new Promise<void>((resolve) => httpServer.close(() => resolve()));
    },
  };
}

// ── unit tests ────────────────────────────────────────────────────────────────

describe("RedisAdapter — unit", () => {
  it("broadcast() publishes a JSON payload to the channel", async () => {
    const { pub, sub } = makeMockClients();
    const adapter = new RedisAdapter({ pubClient: pub, subClient: sub });
    const publishSpy = vi.spyOn(pub, "publish");

    await adapter.broadcast("/products", ["product-123"], [], "stock_update", { stock: 5 }, "id-1");

    expect(publishSpy).toHaveBeenCalledOnce();
    const [channel, raw] = publishSpy.mock.calls[0] as [string, string];
    expect(channel).toBe("sse-manager");
    expect(JSON.parse(raw)).toEqual({
      namespaceName: "/products",
      rooms: ["product-123"],
      excludeRooms: [],
      event: "stock_update",
      data: { stock: 5 },
      id: "id-1",
    });

    await adapter.close();
  });

  it("channelPrefix option changes the Redis channel name", async () => {
    const { pub, sub } = makeMockClients();
    const adapter = new RedisAdapter({ pubClient: pub, subClient: sub, channelPrefix: "myapp" });
    const publishSpy = vi.spyOn(pub, "publish");

    await adapter.broadcast("/ns", [], [], "evt", null, "id");

    const [channel] = publishSpy.mock.calls[0] as [string, string];
    expect(channel).toBe("myapp");

    await adapter.close();
  });

  it("init() wires the message handler — incoming messages call the handler", async () => {
    const { pub, sub } = makeMockClients();
    const adapter = new RedisAdapter({ pubClient: pub, subClient: sub });
    const handler = vi.fn();
    adapter.init(handler);
    // subscribe() is fire-and-forget in init(); wait a tick for it to complete
    await new Promise<void>((resolve) => setImmediate(resolve));

    await pub.publish("sse-manager", JSON.stringify({
      namespaceName: "/products",
      rooms: ["product-123"],
      excludeRooms: [],
      event: "stock_update",
      data: { stock: 10 },
      id: "id-2",
    }));
    await new Promise<void>((resolve) => setImmediate(resolve));

    expect(handler).toHaveBeenCalledOnce();
    expect(handler).toHaveBeenCalledWith(expect.objectContaining({
      namespaceName: "/products",
      event: "stock_update",
      data: { stock: 10 },
    }));

    await adapter.close();
  });

  it("malformed JSON messages are silently ignored", async () => {
    const { pub, sub } = makeMockClients();
    const adapter = new RedisAdapter({ pubClient: pub, subClient: sub });
    const handler = vi.fn();
    adapter.init(handler);
    await new Promise<void>((resolve) => setImmediate(resolve));

    await pub.publish("sse-manager", "not-valid-json{{");

    expect(handler).not.toHaveBeenCalled();
    await adapter.close();
  });

  it("close() stops the handler from being called", async () => {
    const { pub, sub } = makeMockClients();
    const adapter = new RedisAdapter({ pubClient: pub, subClient: sub });
    const handler = vi.fn();
    adapter.init(handler);

    await adapter.close();
    await pub.publish("sse-manager", JSON.stringify({
      namespaceName: "/ns", rooms: [], excludeRooms: [], event: "e", data: null, id: "x",
    }));

    expect(handler).not.toHaveBeenCalled();
  });
});

// ── multi-server integration tests ───────────────────────────────────────────

describe("RedisAdapter — multi-server", () => {
  let serverA: ServerInstance;
  let serverB: ServerInstance;

  afterEach(async () => {
    await Promise.all([serverA?.close(), serverB?.close()]);
  });

  function makeAdapterPair() {
    // Both adapters share the same ioredis-mock bus — simulates two processes
    // connected to the same Redis instance.
    const shared = new RedisMock();
    shared.setMaxListeners(50);
    const adapterA = new RedisAdapter({
      pubClient: shared.duplicate(),
      subClient: shared.duplicate(),
    });
    const adapterB = new RedisAdapter({
      pubClient: shared.duplicate(),
      subClient: shared.duplicate(),
    });
    return { adapterA, adapterB };
  }

  it("event emitted on server A reaches a client connected to server B", async () => {
    const { adapterA, adapterB } = makeAdapterPair();
    [serverA, serverB] = await Promise.all([
      makeServerInstance(adapterA),
      makeServerInstance(adapterB),
    ]);

    const clientOnB = serverB.connect("product-123");
    await clientOnB.nextEvent("connection");

    serverA.ns.to("product-123").emit("stock_update", { stock: 99 });

    const event = await clientOnB.nextEvent("stock_update");
    expect(JSON.parse(event.data!)).toEqual({ stock: 99 });
  });

  it("clients on both servers receive the event", async () => {
    const { adapterA, adapterB } = makeAdapterPair();
    [serverA, serverB] = await Promise.all([
      makeServerInstance(adapterA),
      makeServerInstance(adapterB),
    ]);

    const clientOnA = serverA.connect("product-123");
    const clientOnB = serverB.connect("product-123");
    await Promise.all([
      clientOnA.nextEvent("connection"),
      clientOnB.nextEvent("connection"),
    ]);

    serverA.ns.to("product-123").emit("stock_update", { stock: 7 });

    const [e1, e2] = await Promise.all([
      clientOnA.nextEvent("stock_update"),
      clientOnB.nextEvent("stock_update"),
    ]);
    expect(JSON.parse(e1.data!)).toEqual({ stock: 7 });
    expect(JSON.parse(e2.data!)).toEqual({ stock: 7 });
  });

  it("room isolation holds across servers — wrong room client does not receive event", async () => {
    const { adapterA, adapterB } = makeAdapterPair();
    [serverA, serverB] = await Promise.all([
      makeServerInstance(adapterA),
      makeServerInstance(adapterB),
    ]);

    const clientOnA = serverA.connect("product-123");
    const clientOnB = serverB.connect("product-456"); // different product
    await Promise.all([
      clientOnA.nextEvent("connection"),
      clientOnB.nextEvent("connection"),
    ]);

    serverA.ns.to("product-123").emit("stock_update", { stock: 3 });

    await clientOnA.nextEvent("stock_update"); // must arrive

    const result = await Promise.race([
      clientOnB.nextEvent("stock_update").then(() => "received"),
      new Promise<"silent">((resolve) => setTimeout(() => resolve("silent"), 300)),
    ]);
    expect(result).toBe("silent");
  });

  it("namespace broadcast from server A reaches clients on both servers", async () => {
    const { adapterA, adapterB } = makeAdapterPair();
    [serverA, serverB] = await Promise.all([
      makeServerInstance(adapterA),
      makeServerInstance(adapterB),
    ]);

    const clientOnA = serverA.connect("product-123");
    const clientOnB = serverB.connect("product-456");
    await Promise.all([
      clientOnA.nextEvent("connection"),
      clientOnB.nextEvent("connection"),
    ]);

    serverA.ns.emit("system_alert", { message: "going down for maintenance" });

    const [e1, e2] = await Promise.all([
      clientOnA.nextEvent("system_alert"),
      clientOnB.nextEvent("system_alert"),
    ]);
    expect(JSON.parse(e1.data!)).toEqual({ message: "going down for maintenance" });
    expect(JSON.parse(e2.data!)).toEqual({ message: "going down for maintenance" });
  });
});
