import type { SSEMessage } from "./types.js";

const ID_CHARS =
  "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

export function makeId(length = 16): string {
  let result = "";
  for (let i = 0; i < length; i++) {
    result += ID_CHARS.charAt(Math.floor(Math.random() * ID_CHARS.length));
  }
  return result;
}

export function formatSSEMessage(msg: SSEMessage): string {
  const lines: string[] = [];

  if (msg.id !== undefined) {
    lines.push(`id: ${msg.id.replace(/\r?\n/g, "")}`);
  }

  lines.push(`event: ${msg.event.replace(/\r?\n/g, "")}`);

  const serialized =
    typeof msg.data === "string" ? msg.data : JSON.stringify(msg.data);
  const raw = serialized ?? "";

  // Each line of data must be prefixed with "data: " per SSE spec
  for (const line of raw.split(/\r?\n/)) {
    lines.push(`data: ${line}`);
  }

  // Blank line terminates the event
  return lines.join("\n") + "\n\n";
}

export function parseQuery(
  url: string | undefined
): Record<string, string | string[] | undefined> {
  if (!url) return {};
  const idx = url.indexOf("?");
  if (idx === -1) return {};
  const params = new URLSearchParams(url.slice(idx + 1));
  const result: Record<string, string | string[]> = {};
  for (const [key, value] of params.entries()) {
    const existing = result[key];
    if (existing === undefined) {
      result[key] = value;
    } else if (Array.isArray(existing)) {
      existing.push(value);
    } else {
      result[key] = [existing, value];
    }
  }
  return result;
}
