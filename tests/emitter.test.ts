import { describe, it, expect, vi } from "vitest";
import { ChainableEmitter } from "../src/emitter.js";

function makeEmitter() {
  const broadcast = vi.fn();
  const emitter = new ChainableEmitter(broadcast, new Set(), new Set());
  return { emitter, broadcast };
}

describe("ChainableEmitter — room accumulation", () => {
  it("to() passes the target room to broadcast", () => {
    const { emitter, broadcast } = makeEmitter();
    emitter.to("room-a").emit("evt", {});
    expect(broadcast).toHaveBeenCalledWith(["room-a"], [], "evt", {});
  });

  it("to().to() accumulates rooms across calls", () => {
    const { emitter, broadcast } = makeEmitter();
    emitter.to("room-a").to("room-b").emit("evt", {});
    const [rooms] = broadcast.mock.calls[0] as [string[]];
    expect(rooms).toContain("room-a");
    expect(rooms).toContain("room-b");
  });

  it("except() passes the excluded room to broadcast", () => {
    const { emitter, broadcast } = makeEmitter();
    emitter.except("vip").emit("evt", {});
    expect(broadcast).toHaveBeenCalledWith([], ["vip"], "evt", {});
  });

  it("except().except() accumulates exclusions across calls", () => {
    const { emitter, broadcast } = makeEmitter();
    emitter.except("vip").except("admin").emit("evt", {});
    const [, excludeRooms] = broadcast.mock.calls[0] as [string[], string[]];
    expect(excludeRooms).toContain("vip");
    expect(excludeRooms).toContain("admin");
  });

  it("to().except() targets a room while excluding another", () => {
    const { emitter, broadcast } = makeEmitter();
    emitter.to("subscribers").except("muted").emit("evt", {});
    const [rooms, excludeRooms] = broadcast.mock.calls[0] as [string[], string[]];
    expect(rooms).toContain("subscribers");
    expect(excludeRooms).toContain("muted");
  });

  it("except().to() works in either order", () => {
    const { emitter, broadcast } = makeEmitter();
    emitter.except("muted").to("subscribers").emit("evt", {});
    const [rooms, excludeRooms] = broadcast.mock.calls[0] as [string[], string[]];
    expect(rooms).toContain("subscribers");
    expect(excludeRooms).toContain("muted");
  });
});

describe("ChainableEmitter — immutability", () => {
  it("to() returns a new instance", () => {
    const { emitter } = makeEmitter();
    expect(emitter.to("room-a")).not.toBe(emitter);
  });

  it("except() returns a new instance", () => {
    const { emitter } = makeEmitter();
    expect(emitter.except("room-a")).not.toBe(emitter);
  });

  it("chaining to() does not mutate the parent emitter's room set", () => {
    const { emitter, broadcast } = makeEmitter();
    emitter.to("room-a"); // discard the chained result
    emitter.emit("evt", {});
    const [rooms] = broadcast.mock.calls[0] as [string[]];
    expect(rooms).toHaveLength(0);
  });

  it("chaining except() does not mutate the parent emitter's exclude set", () => {
    const { emitter, broadcast } = makeEmitter();
    emitter.except("vip"); // discard the chained result
    emitter.emit("evt", {});
    const [, excludeRooms] = broadcast.mock.calls[0] as [string[], string[]];
    expect(excludeRooms).toHaveLength(0);
  });
});
