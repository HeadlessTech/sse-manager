import { describe, it, expect, afterEach } from "vitest";
import { createServer, request, type Server } from "node:http";
import { SSEServer } from "../src/server.js";
import type { SSENamespace } from "../src/namespace.js";
import type { EventMap } from "../src/types.js";

// ── SSE client ────────────────────────────────────────────────────────────────

interface SSEEvent {
  id?: string;
  event?: string;
  data?: string;
}

interface SSEClient {
  nextEvent: (eventName: string, timeout?: number) => Promise<SSEEvent>;
  close: () => void;
}

function createSSEClient(url: string): SSEClient {
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
          if (pending?.length) {
            pending.shift()!(evt);
          } else {
            buffered.push(evt);
          }
        }
      });
    }
  );
  req.end();

  return {
    nextEvent(eventName, timeout = 2000) {
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

// ── test setup ────────────────────────────────────────────────────────────────

interface Ctx {
  ns: SSENamespace<EventMap>;
  connect: (productId: string) => SSEClient;
  close: () => Promise<void>;
}

async function setup(): Promise<Ctx> {
  const sseServer = new SSEServer({ heartbeatInterval: 0 });
  const ns = sseServer.of("/products");
  const clients: SSEClient[] = [];

  const httpServer: Server = createServer((req, res) => {
    const match = req.url?.match(/^\/stream\/products\/([^?]+)/);
    if (match) {
      const client = ns.connect(req, res);
      client.join(match[1]);
    } else {
      res.writeHead(404);
      res.end();
    }
  });

  const port = await new Promise<number>((resolve) => {
    httpServer.listen(0, () => resolve((httpServer.address() as { port: number }).port));
  });

  return {
    ns,
    connect(productId) {
      const client = createSSEClient(`http://localhost:${port}/stream/products/${productId}`);
      clients.push(client);
      return client;
    },
    async close() {
      clients.forEach((c) => c.close());
      clients.length = 0;
      await sseServer.close();
      await new Promise<void>((resolve) => httpServer.close(() => resolve()));
    },
  };
}

// ── tests ─────────────────────────────────────────────────────────────────────

describe("Integration — client receives events", () => {
  let ctx: Ctx;

  afterEach(async () => {
    await ctx?.close();
  });

  it("receives stock_update after subscribing to a product room", async () => {
    ctx = await setup();
    const client = ctx.connect("product-123");
    await client.nextEvent("connection"); // server-side client is now registered

    ctx.ns.to("product-123").emit("stock_update", { productId: "product-123", stock: 42 });

    const event = await client.nextEvent("stock_update");
    expect(JSON.parse(event.data!)).toEqual({ productId: "product-123", stock: 42 });
  });

  it("does not receive events emitted to a different product room", async () => {
    ctx = await setup();
    const clientA = ctx.connect("product-123");
    const clientB = ctx.connect("product-456");
    await Promise.all([clientA.nextEvent("connection"), clientB.nextEvent("connection")]);

    ctx.ns.to("product-456").emit("stock_update", { productId: "product-456", stock: 5 });

    const event = await clientB.nextEvent("stock_update");
    expect(JSON.parse(event.data!)).toEqual({ productId: "product-456", stock: 5 });

    const result = await Promise.race([
      clientA.nextEvent("stock_update").then(() => "received"),
      new Promise<"silent">((resolve) => setTimeout(() => resolve("silent"), 300)),
    ]);
    expect(result).toBe("silent");
  });

  it("all subscribers to the same product room receive the event", async () => {
    ctx = await setup();
    const client1 = ctx.connect("product-123");
    const client2 = ctx.connect("product-123");
    await Promise.all([client1.nextEvent("connection"), client2.nextEvent("connection")]);

    ctx.ns.to("product-123").emit("stock_update", { productId: "product-123", stock: 0 });

    const [e1, e2] = await Promise.all([
      client1.nextEvent("stock_update"),
      client2.nextEvent("stock_update"),
    ]);
    expect(JSON.parse(e1.data!)).toEqual({ productId: "product-123", stock: 0 });
    expect(JSON.parse(e2.data!)).toEqual({ productId: "product-123", stock: 0 });
  });

  it("receives the connection event with a clientId on connect", async () => {
    ctx = await setup();
    const client = ctx.connect("product-123");
    const event = await client.nextEvent("connection");

    const data = JSON.parse(event.data!);
    expect(typeof data.clientId).toBe("string");
    expect(data.clientId.length).toBeGreaterThan(0);
  });

  it("namespace broadcast reaches all connected clients regardless of room", async () => {
    ctx = await setup();
    const client1 = ctx.connect("product-123");
    const client2 = ctx.connect("product-456");
    await Promise.all([client1.nextEvent("connection"), client2.nextEvent("connection")]);

    ctx.ns.emit("system_alert", { message: "maintenance in 5 minutes" });

    const [e1, e2] = await Promise.all([
      client1.nextEvent("system_alert"),
      client2.nextEvent("system_alert"),
    ]);
    expect(JSON.parse(e1.data!)).toEqual({ message: "maintenance in 5 minutes" });
    expect(JSON.parse(e2.data!)).toEqual({ message: "maintenance in 5 minutes" });
  });

  it("event data survives JSON round-trip across the wire", async () => {
    ctx = await setup();
    const client = ctx.connect("product-789");
    await client.nextEvent("connection");

    const payload = {
      productId: "product-789",
      stock: 100,
      updatedAt: "2026-04-24T10:00:00Z",
      tags: ["sale", "featured"],
    };
    ctx.ns.to("product-789").emit("stock_update", payload);

    const event = await client.nextEvent("stock_update");
    expect(JSON.parse(event.data!)).toEqual(payload);
  });
});
