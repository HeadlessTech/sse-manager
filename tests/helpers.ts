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

/** Parse SSE chunks from a mock response into structured objects */
export function parseChunks(
  written: string[]
): Array<{ id?: string; event?: string; data?: string }> {
  return written
    .filter((c) => c.trim() !== "" && !c.startsWith(":"))
    .map((chunk) => {
      const result: { id?: string; event?: string; data?: string } = {};
      for (const line of chunk.split("\n")) {
        if (line.startsWith("id: ")) result.id = line.slice(4);
        else if (line.startsWith("event: ")) result.event = line.slice(7);
        else if (line.startsWith("data: ")) result.data = line.slice(6);
      }
      return result;
    });
}
