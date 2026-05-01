import { describe, it, expect, vi } from "vitest";
import { Client } from "../src/client.js";
import { mockReq, mockRes } from "./helpers.js";

function makeClient() {
  const req = mockReq();
  const res = mockRes();
  const handshake = { headers: {}, query: {}, url: "/" };
  const client = new Client(req as never, res as never, handshake);
  return { client, req, res };
}

describe("Client — before _bind()", () => {
  it("join() is a no-op and does not throw", () => {
    const { client } = makeClient();
    expect(() => client.join("room")).not.toThrow();
    expect(client.rooms.size).toBe(0);
  });

  it("leave() is a no-op and does not throw", () => {
    const { client } = makeClient();
    expect(() => client.leave("room")).not.toThrow();
  });
});

describe("Client — disconnect() ordering", () => {
  it("res.end() is called before the disconnect event fires", () => {
    const { client, res } = makeClient();
    let endCalledAtEventTime = false;
    client.on("disconnect", () => {
      endCalledAtEventTime = (res.end as ReturnType<typeof vi.fn>).mock.calls.length > 0;
    });

    client.disconnect();

    expect(endCalledAtEventTime).toBe(true);
  });

  it("disconnect event fires exactly once even when res.end throws", () => {
    const { client, res } = makeClient();
    (res.end as ReturnType<typeof vi.fn>).mockImplementation(() => {
      throw new Error("already ended");
    });
    const handler = vi.fn();
    client.on("disconnect", handler);

    client.disconnect();

    expect(handler).toHaveBeenCalledOnce();
    expect(client.disconnected).toBe(true);
  });
});

describe("Client — _write error handling", () => {
  it("fires the disconnect event when _write throws", () => {
    const { client, res } = makeClient();
    const handler = vi.fn();
    client.on("disconnect", handler);

    res.write = () => { throw new Error("socket hang up"); };
    client._write("data: test\n\n");

    expect(handler).toHaveBeenCalledOnce();
    expect(client.disconnected).toBe(true);
  });

  it("does not write after _write has triggered a disconnect", () => {
    const { client, res } = makeClient();
    let writeCount = 0;
    res.write = () => { writeCount++; throw new Error("broken pipe"); };

    client._write("first\n\n");  // triggers disconnect
    client._write("second\n\n"); // should be skipped

    expect(writeCount).toBe(1);
  });
});
