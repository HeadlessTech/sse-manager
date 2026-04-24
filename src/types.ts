import type { IncomingMessage, ServerResponse } from "node:http";

export type Room = string;
export type ClientId = string;

export type HandshakeData = {
  headers: Record<string, string | string[] | undefined>;
  query: Record<string, string | string[] | undefined>;
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
 * Framework-agnostic request/response types.
 * Express Request/Response satisfy this shape.
 */
export type SSERequest = IncomingMessage & {
  headers: Record<string, string | string[] | undefined>;
  query?: Record<string, string | string[] | undefined>;
  url?: string;
};

export type SSEResponse = ServerResponse & {
  writeHead(statusCode: number, headers: Record<string, string>): unknown;
  write(chunk: string): boolean;
  end(): unknown;
};

/** Map of event names to their data shapes — used for typed namespaces */
export type EventMap = Record<string, unknown>;

/** Default untyped event map */
export type DefaultEvents = Record<string, unknown>;
