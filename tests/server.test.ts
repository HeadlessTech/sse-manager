import { describe, it, expect, vi } from "vitest";
import { SSEServer } from "../src/server.js";
import { SSENamespace } from "../src/namespace.js";
import { MemoryAdapter } from "../src/adapters/memory.js";
import { mockReq, mockRes } from "./helpers.js";

describe("SSEServer — namespace registry", () => {
  it("of() returns an SSENamespace instance", () => {
    const io = new SSEServer({ heartbeatInterval: 0 });
    expect(io.of("/orders")).toBeInstanceOf(SSENamespace);
  });

  it("of() returns the same instance for the same name", () => {
    const io = new SSEServer({ heartbeatInterval: 0 });
    const a = io.of("/orders");
    const b = io.of("/orders");
    expect(a).toBe(b);
  });

  it("of() returns distinct instances for different names", () => {
    const io = new SSEServer({ heartbeatInterval: 0 });
    const orders = io.of("/orders");
    const notifications = io.of("/notifications");
    expect(orders).not.toBe(notifications);
  });

  it("namespace name is set correctly", () => {
    const io = new SSEServer({ heartbeatInterval: 0 });
    expect(io.of("/orders").name).toBe("/orders");
  });
});

describe("SSEServer — adapter", () => {
  it("uses MemoryAdapter by default", () => {
    const io = new SSEServer({ heartbeatInterval: 0 });
    const ns = io.of("/ns");
    const spy = vi.fn();
    ns.on("connection", spy);
    // If the adapter is wired up correctly the connection event fires
    ns.connect(mockReq() as never, mockRes() as never);
    expect(spy).toHaveBeenCalledOnce();
  });

  it("swapping adapter re-inits existing namespaces", () => {
    const io = new SSEServer({ heartbeatInterval: 0 });
    const ns = io.of("/ns");

    const newAdapter = new MemoryAdapter();
    const initSpy = vi.spyOn(newAdapter, "init");

    io.adapter(newAdapter);

    expect(initSpy).toHaveBeenCalledOnce();
    // Namespace should still work after swap
    const spy = vi.fn();
    ns.on("connection", spy);
    ns.connect(mockReq() as never, mockRes() as never);
    expect(spy).toHaveBeenCalledOnce();
  });

  it("namespaces created after adapter() call use the new adapter", () => {
    const io = new SSEServer({ heartbeatInterval: 0 });
    const newAdapter = new MemoryAdapter();
    const initSpy = vi.spyOn(newAdapter, "init");

    io.adapter(newAdapter);
    io.of("/after");

    expect(initSpy).toHaveBeenCalledTimes(1);
  });
});

describe("SSEServer — close", () => {
  it("close() disconnects all clients in all namespaces", async () => {
    const io = new SSEServer({ heartbeatInterval: 0 });
    const ns1 = io.of("/a");
    const ns2 = io.of("/b");

    const res1 = mockRes();
    const res2 = mockRes();
    ns1.connect(mockReq() as never, res1 as never);
    ns2.connect(mockReq() as never, res2 as never);

    await io.close();

    expect(res1.end).toHaveBeenCalled();
    expect(res2.end).toHaveBeenCalled();
  });

  it("close() empties all client maps", async () => {
    const io = new SSEServer({ heartbeatInterval: 0 });
    const ns = io.of("/ns");
    ns.connect(mockReq() as never, mockRes() as never);

    await io.close();

    expect(ns.clientCount).toBe(0);
  });
});

describe("SSEServer — convenience emit", () => {
  it("io.to(room) targets the default '/' namespace", () => {
    const io = new SSEServer({ heartbeatInterval: 0 });
    const defaultNs = io.of("/");
    const res = mockRes();
    const client = defaultNs.connect(mockReq() as never, res as never);
    client.join("room-1");

    io.to("room-1").emit("hello", { msg: "world" });

    const events = res.written.join("");
    expect(events).toContain("event: hello");
  });
});
