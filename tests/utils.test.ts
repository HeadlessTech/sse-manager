import { describe, it, expect } from "vitest";
import { formatSSEMessage, makeId, parseQuery } from "../src/utils.js";

describe("makeId", () => {
  it("returns a string of the requested length", () => {
    expect(makeId(16)).toHaveLength(16);
    expect(makeId(20)).toHaveLength(20);
    expect(makeId(1)).toHaveLength(1);
  });

  it("defaults to length 16", () => {
    expect(makeId()).toHaveLength(16);
  });

  it("contains only alphanumeric characters", () => {
    const id = makeId(200);
    expect(id).toMatch(/^[A-Za-z0-9]+$/);
  });

  it("produces unique values", () => {
    const ids = new Set(Array.from({ length: 100 }, () => makeId()));
    expect(ids.size).toBe(100);
  });
});

describe("formatSSEMessage", () => {
  it("includes event field", () => {
    const out = formatSSEMessage({ event: "my_event", data: "hello" });
    expect(out).toContain("event: my_event");
  });

  it("includes data field", () => {
    const out = formatSSEMessage({ event: "e", data: "hello" });
    expect(out).toContain("data: hello");
  });

  it("includes id field when provided", () => {
    const out = formatSSEMessage({ event: "e", data: "x", id: "42" });
    expect(out).toContain("id: 42");
  });

  it("omits id field when not provided", () => {
    const out = formatSSEMessage({ event: "e", data: "x" });
    expect(out).not.toContain("id:");
  });

  it("JSON-serialises object data", () => {
    const out = formatSSEMessage({ event: "e", data: { orderId: "123" } });
    expect(out).toContain('data: {"orderId":"123"}');
  });

  it("prefixes each line with 'data: ' for multiline strings", () => {
    const out = formatSSEMessage({ event: "e", data: "line1\nline2" });
    expect(out).toContain("data: line1");
    expect(out).toContain("data: line2");
  });

  it("terminates the message with a double newline", () => {
    const out = formatSSEMessage({ event: "e", data: "x" });
    expect(out).toMatch(/\n\n$/);
  });

  it("places id before event before data", () => {
    const out = formatSSEMessage({ event: "e", data: "x", id: "1" });
    const idPos = out.indexOf("id:");
    const eventPos = out.indexOf("event:");
    const dataPos = out.indexOf("data:");
    expect(idPos).toBeLessThan(eventPos);
    expect(eventPos).toBeLessThan(dataPos);
  });
});

describe("parseQuery", () => {
  it("returns empty object when url is undefined", () => {
    expect(parseQuery(undefined)).toEqual({});
  });

  it("returns empty object when there is no query string", () => {
    expect(parseQuery("/stream/orders")).toEqual({});
  });

  it("parses a single param", () => {
    expect(parseQuery("/stream?foo=bar")).toEqual({ foo: "bar" });
  });

  it("parses multiple params", () => {
    expect(parseQuery("/?a=1&b=2")).toEqual({ a: "1", b: "2" });
  });

  it("collects repeated params into an array", () => {
    expect(parseQuery("/?tag=x&tag=y")).toEqual({ tag: ["x", "y"] });
  });

  it("handles encoded characters", () => {
    expect(parseQuery("/?name=hello%20world")).toEqual({
      name: "hello world",
    });
  });
});
