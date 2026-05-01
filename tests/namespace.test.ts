import { describe, it, expect, vi, afterEach } from "vitest";
import { SSEServer } from "../src/server.js";
import { MemoryAdapter } from "../src/adapters/memory.js";
import { mockReq, mockRes, parseChunks } from "./helpers.js";

function setup() {
  const io = new SSEServer({ heartbeatInterval: 0 });
  const ns = io.of("/test");
  return { io, ns };
}

describe("SSENamespace — connection", () => {
  it("writes SSE headers on connect", () => {
    const { ns } = setup();
    const req = mockReq();
    const res = mockRes();

    ns.connect(req as never, res as never);

    expect(res.writeHead).toHaveBeenCalledWith(
      200,
      expect.objectContaining({ "Content-Type": "text/event-stream" })
    );
  });

  it("sends a connection event immediately on connect", () => {
    const { ns } = setup();
    const req = mockReq();
    const res = mockRes();

    ns.connect(req as never, res as never);

    const events = parseChunks(res.written);
    const connEvent = events.find((e) => e.event === "connection");
    expect(connEvent).toBeDefined();
  });

  it("returns a Client with a unique id", () => {
    const { ns } = setup();
    const c1 = ns.connect(mockReq() as never, mockRes() as never);
    const c2 = ns.connect(mockReq() as never, mockRes() as never);
    expect(c1.id).toBeTruthy();
    expect(c1.id).not.toBe(c2.id);
  });

  it("fires the connection event on the namespace", () => {
    const { ns } = setup();
    const handler = vi.fn();
    ns.on("connection", handler);

    ns.connect(mockReq() as never, mockRes() as never);

    expect(handler).toHaveBeenCalledOnce();
  });

  it("increments clientCount after connect", () => {
    const { ns } = setup();
    expect(ns.clientCount).toBe(0);
    ns.connect(mockReq() as never, mockRes() as never);
    expect(ns.clientCount).toBe(1);
    ns.connect(mockReq() as never, mockRes() as never);
    expect(ns.clientCount).toBe(2);
  });

  it("populates handshake.query from the URL", () => {
    const { ns } = setup();
    const client = ns.connect(
      mockReq("/stream?orderId=abc") as never,
      mockRes() as never
    );
    expect(client.handshake.query.orderId).toBe("abc");
  });

  it("populates handshake.headers", () => {
    const { ns } = setup();
    const client = ns.connect(
      mockReq("/", { authorization: "Bearer token" }) as never,
      mockRes() as never
    );
    expect(client.handshake.headers.authorization).toBe("Bearer token");
  });
});

describe("SSENamespace — CORS", () => {
  it("sets Access-Control-Allow-Origin when cors.origin is provided", () => {
    const io = new SSEServer({ heartbeatInterval: 0, cors: { origin: "https://example.com" } });
    const ns = io.of("/test");
    const res = mockRes();

    ns.connect(mockReq() as never, res as never);

    expect(res.writeHead).toHaveBeenCalledWith(
      200,
      expect.objectContaining({ "Access-Control-Allow-Origin": "https://example.com" })
    );
  });

  it("sets Access-Control-Allow-Credentials when cors.credentials is true", () => {
    const io = new SSEServer({ heartbeatInterval: 0, cors: { origin: "https://example.com", credentials: true } });
    const ns = io.of("/test");
    const res = mockRes();

    ns.connect(mockReq() as never, res as never);

    expect(res.writeHead).toHaveBeenCalledWith(
      200,
      expect.objectContaining({ "Access-Control-Allow-Credentials": "true" })
    );
  });

  it("does not set CORS headers when cors option is omitted", () => {
    const { ns } = setup();
    const res = mockRes();

    ns.connect(mockReq() as never, res as never);

    const headers = res.writeHead.mock.calls[0][1] as Record<string, string>;
    expect(headers["Access-Control-Allow-Origin"]).toBeUndefined();
    expect(headers["Access-Control-Allow-Credentials"]).toBeUndefined();
  });
});

describe("SSENamespace — rooms", () => {
  it("join adds client to the room", () => {
    const { ns } = setup();
    const client = ns.connect(mockReq() as never, mockRes() as never);
    client.join("room-1");

    expect(ns.getRoom("room-1")?.has(client.id)).toBe(true);
    expect(client.rooms.has("room-1")).toBe(true);
  });

  it("join accepts an array of rooms", () => {
    const { ns } = setup();
    const client = ns.connect(mockReq() as never, mockRes() as never);
    client.join(["room-a", "room-b"]);

    expect(ns.getRoom("room-a")?.has(client.id)).toBe(true);
    expect(ns.getRoom("room-b")?.has(client.id)).toBe(true);
  });

  it("leave removes client from the room", () => {
    const { ns } = setup();
    const client = ns.connect(mockReq() as never, mockRes() as never);
    client.join("room-1");
    client.leave("room-1");

    expect(ns.getRoom("room-1")).toBeUndefined();
    expect(client.rooms.has("room-1")).toBe(false);
  });

  it("leave cleans up empty rooms from the namespace map", () => {
    const { ns } = setup();
    const client = ns.connect(mockReq() as never, mockRes() as never);
    client.join("temp-room");
    client.leave("temp-room");

    expect(ns.getRooms().has("temp-room")).toBe(false);
  });

  it("multiple clients can be in the same room", () => {
    const { ns } = setup();
    const c1 = ns.connect(mockReq() as never, mockRes() as never);
    const c2 = ns.connect(mockReq() as never, mockRes() as never);
    c1.join("shared");
    c2.join("shared");

    expect(ns.getRoom("shared")?.size).toBe(2);
  });
});

describe("SSENamespace — emit", () => {
  it("emit() broadcasts to all connected clients", () => {
    const { ns } = setup();
    const res1 = mockRes();
    const res2 = mockRes();
    ns.connect(mockReq() as never, res1 as never);
    ns.connect(mockReq() as never, res2 as never);

    ns.emit("ping", { msg: "hello" });

    const e1 = parseChunks(res1.written).find((e) => e.event === "ping");
    const e2 = parseChunks(res2.written).find((e) => e.event === "ping");
    expect(e1).toBeDefined();
    expect(e2).toBeDefined();
  });

  it("to(room).emit() delivers only to clients in that room", () => {
    const { ns } = setup();
    const res1 = mockRes();
    const res2 = mockRes();
    const c1 = ns.connect(mockReq() as never, res1 as never);
    ns.connect(mockReq() as never, res2 as never);
    c1.join("room-1");

    ns.to("room-1").emit("update", { x: 1 });

    const e1 = parseChunks(res1.written).find((e) => e.event === "update");
    const e2 = parseChunks(res2.written).find((e) => e.event === "update");
    expect(e1).toBeDefined();
    expect(e2).toBeUndefined();
  });

  it("to(room).emit() does not duplicate delivery for clients in multiple target rooms", () => {
    const { ns } = setup();
    const res1 = mockRes();
    const c1 = ns.connect(mockReq() as never, res1 as never);
    c1.join("room-a");
    c1.join("room-b");

    ns.to(["room-a", "room-b"]).emit("notice", {});

    const events = parseChunks(res1.written).filter((e) => e.event === "notice");
    expect(events).toHaveLength(1);
  });

  it("except(room).emit() skips clients in the excluded room", () => {
    const { ns } = setup();
    const res1 = mockRes();
    const res2 = mockRes();
    const c1 = ns.connect(mockReq() as never, res1 as never);
    ns.connect(mockReq() as never, res2 as never);
    c1.join("vip");

    ns.except("vip").emit("general", {});

    const e1 = parseChunks(res1.written).find((e) => e.event === "general");
    const e2 = parseChunks(res2.written).find((e) => e.event === "general");
    expect(e1).toBeUndefined();
    expect(e2).toBeDefined();
  });

  it("client.emit() delivers only to that specific client", () => {
    const { ns } = setup();
    const res1 = mockRes();
    const res2 = mockRes();
    const c1 = ns.connect(mockReq() as never, res1 as never);
    ns.connect(mockReq() as never, res2 as never);

    c1.emit("personal", { msg: "just you" });

    const e1 = parseChunks(res1.written).find((e) => e.event === "personal");
    const e2 = parseChunks(res2.written).find((e) => e.event === "personal");
    expect(e1).toBeDefined();
    expect(e2).toBeUndefined();
  });

  it("emit data is JSON-serialized and parseable", () => {
    const { ns } = setup();
    const res = mockRes();
    ns.connect(mockReq() as never, res as never);

    ns.emit("data_event", { orderId: "abc", status: "ready" });

    const event = parseChunks(res.written).find((e) => e.event === "data_event");
    expect(event).toBeDefined();
    expect(JSON.parse(event!.data!)).toEqual({ orderId: "abc", status: "ready" });
  });

  it("to(nonexistent room).emit() sends nothing", () => {
    const { ns } = setup();
    const res = mockRes();
    ns.connect(mockReq() as never, res as never);
    const before = res.written.length;

    ns.to("ghost-room").emit("noop", {});

    expect(res.written.length).toBe(before);
  });

  it("chaining to().to() accumulates rooms and delivers to both", () => {
    const { ns } = setup();
    const res1 = mockRes();
    const res2 = mockRes();
    const res3 = mockRes();
    const c1 = ns.connect(mockReq() as never, res1 as never);
    const c2 = ns.connect(mockReq() as never, res2 as never);
    ns.connect(mockReq() as never, res3 as never);
    c1.join("room-a");
    c2.join("room-b");

    ns.to("room-a").to("room-b").emit("notice", { x: 1 });

    expect(parseChunks(res1.written).find((e) => e.event === "notice")).toBeDefined();
    expect(parseChunks(res2.written).find((e) => e.event === "notice")).toBeDefined();
    expect(parseChunks(res3.written).find((e) => e.event === "notice")).toBeUndefined();
  });

  it("to(room).except(room).emit() delivers to targeted room but skips excluded clients", () => {
    const { ns } = setup();
    const res1 = mockRes();
    const res2 = mockRes();
    const c1 = ns.connect(mockReq() as never, res1 as never);
    const c2 = ns.connect(mockReq() as never, res2 as never);
    c1.join("subscribers");
    c2.join("subscribers");
    c2.join("muted");

    ns.to("subscribers").except("muted").emit("flash", { msg: "hi" });

    expect(parseChunks(res1.written).find((e) => e.event === "flash")).toBeDefined();
    expect(parseChunks(res2.written).find((e) => e.event === "flash")).toBeUndefined();
  });
});

describe("SSENamespace — middleware", () => {
  it("calls next() and fires connection event when middleware passes", () => {
    const { ns } = setup();
    const handler = vi.fn();
    ns.use((_client, next) => next());
    ns.on("connection", handler);

    ns.connect(mockReq() as never, mockRes() as never);

    expect(handler).toHaveBeenCalledOnce();
  });

  it("calling next(error) prevents connection event and disconnects client", () => {
    const { ns } = setup();
    const handler = vi.fn();
    ns.use((_client, next) => next(new Error("Unauthorized")));
    ns.on("connection", handler);

    const res = mockRes();
    ns.connect(mockReq() as never, res as never);

    expect(handler).not.toHaveBeenCalled();
    expect(res.end).toHaveBeenCalled();
    expect(ns.clientCount).toBe(0);
  });

  it("runs middleware in registration order", () => {
    const { ns } = setup();
    const order: number[] = [];
    ns.use((_c, next) => { order.push(1); next(); });
    ns.use((_c, next) => { order.push(2); next(); });
    ns.use((_c, next) => { order.push(3); next(); });

    ns.connect(mockReq() as never, mockRes() as never);

    expect(order).toEqual([1, 2, 3]);
  });

  it("stops the chain when a middleware calls next(error)", () => {
    const { ns } = setup();
    const third = vi.fn();
    ns.use((_c, next) => next());
    ns.use((_c, next) => next(new Error("stop")));
    ns.use((_c, next) => { third(); next(); });

    ns.connect(mockReq() as never, mockRes() as never);

    expect(third).not.toHaveBeenCalled();
  });

  it("middleware that throws synchronously disconnects the client", () => {
    const { ns } = setup();
    const handler = vi.fn();
    ns.use(() => { throw new Error("boom"); });
    ns.on("connection", handler);

    const res = mockRes();
    ns.connect(mockReq() as never, res as never);

    expect(handler).not.toHaveBeenCalled();
    expect(res.end).toHaveBeenCalled();
    expect(ns.clientCount).toBe(0);
  });
});

describe("SSENamespace — disconnect", () => {
  it("fires the disconnect event on the client", () => {
    const { ns } = setup();
    const req = mockReq();
    const client = ns.connect(req as never, mockRes() as never);
    const handler = vi.fn();
    client.on("disconnect", handler);

    req.simulateClose();

    expect(handler).toHaveBeenCalledOnce();
  });

  it("decrements clientCount on disconnect", () => {
    const { ns } = setup();
    const req = mockReq();
    ns.connect(req as never, mockRes() as never);
    expect(ns.clientCount).toBe(1);

    req.simulateClose();

    expect(ns.clientCount).toBe(0);
  });

  it("removes client from rooms on disconnect", () => {
    const { ns } = setup();
    const req = mockReq();
    const client = ns.connect(req as never, mockRes() as never);
    client.join("room-1");

    req.simulateClose();

    expect(ns.getRoom("room-1")).toBeUndefined();
  });

  it("client.disconnect() ends the response", () => {
    const { ns } = setup();
    const res = mockRes();
    const client = ns.connect(mockReq() as never, res as never);

    client.disconnect();

    expect(res.end).toHaveBeenCalled();
    expect(client.disconnected).toBe(true);
  });

  it("emit to a disconnected client does nothing", () => {
    const { ns } = setup();
    const res = mockRes();
    const client = ns.connect(mockReq() as never, res as never);
    client.disconnect();
    const before = res.written.length;

    client.emit("event", {});

    expect(res.written.length).toBe(before);
  });

  it("calling disconnect() twice fires the disconnect event only once", () => {
    const { ns } = setup();
    const client = ns.connect(mockReq() as never, mockRes() as never);
    const handler = vi.fn();
    client.on("disconnect", handler);

    client.disconnect();
    client.disconnect();

    expect(handler).toHaveBeenCalledOnce();
  });

  it("removes client from all rooms on disconnect", () => {
    const { ns } = setup();
    const req = mockReq();
    const client = ns.connect(req as never, mockRes() as never);
    client.join(["room-1", "room-2", "room-3"]);

    req.simulateClose();

    expect(ns.getRoom("room-1")).toBeUndefined();
    expect(ns.getRoom("room-2")).toBeUndefined();
    expect(ns.getRoom("room-3")).toBeUndefined();
  });
});

describe("SSENamespace — introspection", () => {
  it("getClients returns all connected clients", () => {
    const { ns } = setup();
    const c1 = ns.connect(mockReq() as never, mockRes() as never);
    const c2 = ns.connect(mockReq() as never, mockRes() as never);

    expect(ns.getClients().has(c1.id)).toBe(true);
    expect(ns.getClients().has(c2.id)).toBe(true);
  });

  it("getRooms returns all active rooms", () => {
    const { ns } = setup();
    const client = ns.connect(mockReq() as never, mockRes() as never);
    client.join("room-x");
    client.join("room-y");

    expect(ns.getRooms().has("room-x")).toBe(true);
    expect(ns.getRooms().has("room-y")).toBe(true);
  });
});

describe("SSENamespace — async middleware race condition", () => {
  it("client disconnecting before async middleware completes is not leaked into _clients", () => {
    const { ns } = setup();
    const req = mockReq();
    let resolveMiddleware!: () => void;
    ns.use((_c, next) => {
      resolveMiddleware = next; // hold — don't call next yet
    });

    ns.connect(req as never, mockRes() as never);
    expect(ns.clientCount).toBe(0); // not registered yet

    req.simulateClose(); // disconnect fires during middleware
    resolveMiddleware(); // middleware resolves after disconnect

    expect(ns.clientCount).toBe(0); // must not be leaked
  });
});

describe("SSENamespace — heartbeat", () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it("sends a ping comment at each heartbeat interval", async () => {
    vi.useFakeTimers();
    const io = new SSEServer({ heartbeatInterval: 1000 });
    const ns = io.of("/hb");
    const res = mockRes();
    ns.connect(mockReq() as never, res as never);

    vi.advanceTimersByTime(2000);

    const pings = res.written.filter((c) => c === ": ping\n\n");
    expect(pings).toHaveLength(2);

    await ns.close();
  });

  it("no heartbeat is sent when heartbeatInterval is 0", () => {
    vi.useFakeTimers();
    const io = new SSEServer({ heartbeatInterval: 0 });
    const ns = io.of("/hb");
    const res = mockRes();
    ns.connect(mockReq() as never, res as never);

    vi.advanceTimersByTime(60_000);

    expect(res.written.filter((c) => c === ": ping\n\n")).toHaveLength(0);
  });

  it("heartbeat stops after namespace close", async () => {
    vi.useFakeTimers();
    const io = new SSEServer({ heartbeatInterval: 1000 });
    const ns = io.of("/hb");
    const res = mockRes();
    ns.connect(mockReq() as never, res as never);

    await ns.close();
    vi.advanceTimersByTime(5000);

    expect(res.written.filter((c) => c === ": ping\n\n")).toHaveLength(0);
  });
});

describe("SSENamespace — adapter swap", () => {
  it("emits still reach connected clients after swapping adapter", () => {
    const io = new SSEServer({ heartbeatInterval: 0 });
    const ns = io.of("/swap");
    const res = mockRes();
    const client = ns.connect(mockReq() as never, res as never);
    client.join("room-1");

    io.adapter(new MemoryAdapter());

    ns.to("room-1").emit("after-swap", { ok: true });

    expect(parseChunks(res.written).find((e) => e.event === "after-swap")).toBeDefined();
  });
});

describe("SSENamespace — emit after close", () => {
  it("emitting on a closed namespace is a no-op and does not throw", async () => {
    const io = new SSEServer({ heartbeatInterval: 0 });
    const ns = io.of("/ns");
    const res = mockRes();
    ns.connect(mockReq() as never, res as never);
    const before = res.written.length;

    await io.close();

    expect(() => ns.emit("event", {})).not.toThrow();
    expect(res.written.length).toBe(before);
  });
});

describe("SSENamespace — multiline data", () => {
  it("multiline string data survives the SSE encode/parse round-trip", () => {
    const { ns } = setup();
    const res = mockRes();
    ns.connect(mockReq() as never, res as never);

    ns.emit("msg", "line1\nline2\nline3");

    const event = parseChunks(res.written).find((e) => e.event === "msg");
    expect(event?.data).toBe("line1\nline2\nline3");
  });
});
