import { describe, it, expect, afterEach, vi } from "vitest";
import { createServer, type Server } from "node:http";
import { RedisAdapter } from "../src/adapters/redis.js";
import { SSEServer } from "../src/server.js";
import { createSSEClient } from "./helpers.js";
import type { SSENamespace } from "../src/namespace.js";
import type { EventMap } from "../src/types.js";

// ── in-memory Redis pub/sub mock ──────────────────────────────────────────────

function makeRedisBus() {
  const subs = new Map<string, Set<(msg: string) => void>>();

  function createClient() {
    const myHandlers = new Map<string, (msg: string) => void>();
    return {
      async connect() {},
      async quit() {
        for (const [ch, fn] of myHandlers) subs.get(ch)?.delete(fn);
        myHandlers.clear();
      },
      async publish(channel: string, message: string) {
        subs.get(channel)?.forEach((fn) => fn(message));
      },
      async subscribe(channel: string, listener: (msg: string, ch: string) => void) {
        const wrapped = (msg: string) => listener(msg, channel);
        if (!subs.has(channel)) subs.set(channel, new Set());
        subs.get(channel)!.add(wrapped);
        myHandlers.set(channel, wrapped);
      },
      async unsubscribe(channel: string) {
        const fn = myHandlers.get(channel);
        if (fn) {
          subs.get(channel)?.delete(fn);
          myHandlers.delete(channel);
        }
      },
    };
  }

  return { createClient };
}

function makeMockClients() {
  const bus = makeRedisBus();
  return { pub: bus.createClient(), sub: bus.createClient() };
}

// ── helpers ───────────────────────────────────────────────────────────────────

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
    expect(channel).toBe("sse-manager::products");
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
    expect(channel).toBe("myapp::ns");

    await adapter.close();
  });

  it("init() wires the message handler — incoming messages call the handler", async () => {
    const { pub, sub } = makeMockClients();
    const adapter = new RedisAdapter({ pubClient: pub, subClient: sub });
    const handler = vi.fn();
    adapter.init("/products", handler);
    await new Promise<void>((resolve) => setImmediate(resolve));

    await pub.publish("sse-manager::products", JSON.stringify({
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
    adapter.init("/products", handler);
    await new Promise<void>((resolve) => setImmediate(resolve));

    await pub.publish("sse-manager::products", "not-valid-json{{");

    expect(handler).not.toHaveBeenCalled();
    await adapter.close();
  });

  it("close() stops the handler from being called", async () => {
    const { pub, sub } = makeMockClients();
    const adapter = new RedisAdapter({ pubClient: pub, subClient: sub });
    const handler = vi.fn();
    adapter.init("/ns", handler);

    await adapter.close();
    await pub.publish("sse-manager::ns", JSON.stringify({
      namespaceName: "/ns", rooms: [], excludeRooms: [], event: "e", data: null, id: "x",
    }));

    expect(handler).not.toHaveBeenCalled();
  });

  it("close() calls quit() on both pub and sub clients", async () => {
    const { pub, sub } = makeMockClients();
    const adapter = new RedisAdapter({ pubClient: pub, subClient: sub });
    const pubQuit = vi.spyOn(pub, "quit");
    const subQuit = vi.spyOn(sub, "quit");

    await adapter.close();

    expect(pubQuit).toHaveBeenCalledOnce();
    expect(subQuit).toHaveBeenCalledOnce();
  });

  it("uninit() removes the handler and unsubscribes from the channel", async () => {
    const { pub, sub } = makeMockClients();
    const adapter = new RedisAdapter({ pubClient: pub, subClient: sub });
    const unsubSpy = vi.spyOn(sub, "unsubscribe");
    const handler = vi.fn();

    adapter.init("/products", handler);
    await new Promise<void>((resolve) => setImmediate(resolve));

    adapter.uninit("/products");
    await new Promise<void>((resolve) => setImmediate(resolve));

    await pub.publish("sse-manager::products", JSON.stringify({
      namespaceName: "/products", rooms: [], excludeRooms: [], event: "e", data: null, id: "x",
    }));

    expect(handler).not.toHaveBeenCalled();
    expect(unsubSpy).toHaveBeenCalledWith("sse-manager::products");

    await adapter.close();
  });

  it("calling init() twice for the same namespace subscribes only once", async () => {
    const { pub, sub } = makeMockClients();
    const adapter = new RedisAdapter({ pubClient: pub, subClient: sub });
    const subscribeSpy = vi.spyOn(sub, "subscribe");
    const handler = vi.fn();

    adapter.init("/products", handler);
    adapter.init("/products", handler);
    await new Promise<void>((resolve) => setImmediate(resolve));

    expect(subscribeSpy).toHaveBeenCalledOnce();

    await adapter.close();
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
    // Both adapters share the same bus — simulates two processes on the same Redis instance.
    const bus = makeRedisBus();
    const adapterA = new RedisAdapter({
      pubClient: bus.createClient(),
      subClient: bus.createClient(),
    });
    const adapterB = new RedisAdapter({
      pubClient: bus.createClient(),
      subClient: bus.createClient(),
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
