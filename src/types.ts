import type { ServerResponse } from "node:http";

export type Room = string;
export type ClientId = string;

export type HandshakeData = {
  headers: Record<string, string | string[] | undefined>;
  query: Record<string, unknown>;
  url: string;
};

export type SSEServerOptions = {
  /** Interval in ms for keepalive pings. Set to 0 to disable. Default: 30000 */
  heartbeatInterval?: number;
  /** CORS headers added to every SSE response */
  cors?: {
    origin?: string;
    credentials?: boolean;
  };
};

export type SSEMessage = {
  event: string;
  data: unknown;
  id?: string;
};

/** Payload relayed through adapters between server instances */
export type AdapterPayload = {
  namespaceName: string;
  rooms: Room[];
  excludeRooms: Room[];
  event: string;
  data: unknown;
  id: string;
};

/**
 * Framework-agnostic request shape.
 * Satisfied by Node's IncomingMessage, Express Request, and plain objects.
 * For Fastify, pass request.raw (the underlying IncomingMessage).
 */
export type SSERequest = {
  headers: Record<string, string | string[] | undefined>;
  /** Pre-parsed query params supplied by the framework (e.g. Express req.query). */
  query?: Record<string, unknown>;
  url?: string;
  on(event: string, listener: () => void): void;
};

export type SSEResponse = ServerResponse & {
  writeHead(statusCode: number, headers: Record<string, string>): unknown;
  write(chunk: string): boolean;
  end(): unknown;
};

/** Map of event names to their data shapes — used for typed namespaces */
export type EventMap = Record<string, unknown>;
