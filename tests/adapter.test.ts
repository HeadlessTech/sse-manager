import { describe, it, expect, vi } from "vitest";
import { MemoryAdapter } from "../src/adapters/memory.js";

describe("MemoryAdapter", () => {
  it("calls the handler synchronously when broadcast is called", () => {
    const adapter = new MemoryAdapter();
    const handler = vi.fn();
    adapter.init(handler);

    adapter.broadcast("/orders", ["room-1"], [], "update_order", { id: 1 }, "ts-1");

    expect(handler).toHaveBeenCalledOnce();
    expect(handler).toHaveBeenCalledWith({
      namespaceName: "/orders",
      rooms: ["room-1"],
      excludeRooms: [],
      event: "update_order",
      data: { id: 1 },
      id: "ts-1",
    });
  });

  it("passes empty rooms array for a namespace-wide broadcast", () => {
    const adapter = new MemoryAdapter();
    const handler = vi.fn();
    adapter.init(handler);

    adapter.broadcast("/ns", [], [], "ping", null, "ts-2");

    expect(handler).toHaveBeenCalledWith(
      expect.objectContaining({ rooms: [], excludeRooms: [] })
    );
  });

  it("passes excludeRooms correctly", () => {
    const adapter = new MemoryAdapter();
    const handler = vi.fn();
    adapter.init(handler);

    adapter.broadcast("/ns", [], ["vip"], "notice", "hi", "ts-3");

    expect(handler).toHaveBeenCalledWith(
      expect.objectContaining({ excludeRooms: ["vip"] })
    );
  });

  it("does not call handler before init", () => {
    const adapter = new MemoryAdapter();
    // No init call — should not throw
    expect(() =>
      adapter.broadcast("/ns", [], [], "event", null, "id")
    ).not.toThrow();
  });

  it("stops calling handler after close", async () => {
    const adapter = new MemoryAdapter();
    const handler = vi.fn();
    adapter.init(handler);
    await adapter.close();

    adapter.broadcast("/ns", [], [], "event", null, "id");

    expect(handler).not.toHaveBeenCalled();
  });
});
