import { request } from "node:http";
import { vi } from "vitest";

/**
 * Creates a minimal mock of an SSE-compatible HTTP request.
 * Call `req.simulateClose()` in tests to fire the 'close' event.
 */
export function mockReq(url = "/", headers: Record<string, string> = {}) {
  const closeListeners: Array<() => void> = [];
  return {
    url,
    headers,
    on(event: string, handler: () => void) {
      if (event === "close") closeListeners.push(handler);
    },
    simulateClose() {
      closeListeners.forEach((fn) => fn());
    },
  };
}

/**
 * Creates a minimal mock of an HTTP response.
 * All written SSE chunks are captured in `res.written`.
 */
export function mockRes() {
  const written: string[] = [];
  return {
    writeHead: vi.fn(),
    write(chunk: string) {
      written.push(chunk);
      return true;
    },
    end: vi.fn(),
    written,
  };
}

export type MockReq = ReturnType<typeof mockReq>;
export type MockRes = ReturnType<typeof mockRes>;

// ── Real HTTP SSE client (for integration tests) ──────────────────────────────

export interface SSEEvent {
  id?: string;
  event?: string;
  data?: string;
}

/**
 * Opens a real HTTP connection to a live SSE endpoint.
 * Use `nextEvent(name)` to await a specific event by name.
 * Call `close()` to destroy the connection.
 */
export function createSSEClient(url: string): {
  nextEvent(eventName: string, timeout?: number): Promise<SSEEvent>;
  close(): void;
} {
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

/** Parse SSE chunks from a mock response into structured objects */
export function parseChunks(
  written: string[]
): Array<{ id?: string; event?: string; data?: string }> {
  return written
    .filter((c) => c.trim() !== "" && !c.startsWith(":"))
    .map((chunk) => {
      const result: { id?: string; event?: string; data?: string } = {};
      const dataLines: string[] = [];
      for (const line of chunk.split("\n")) {
        if (line.startsWith("id: ")) result.id = line.slice(4);
        else if (line.startsWith("event: ")) result.event = line.slice(7);
        else if (line.startsWith("data: ")) dataLines.push(line.slice(6));
      }
      if (dataLines.length > 0) result.data = dataLines.join("\n");
      return result;
    });
}
