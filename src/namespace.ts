import { EventEmitter } from "node:events";
import { Client } from "./client.js";
import { ChainableEmitter } from "./emitter.js";
import { formatSSEMessage, makeId, parseQuery } from "./utils.js";
import type { Adapter } from "./adapter.js";
import type {
  AdapterPayload,
  ClientId,
  EventMap,
  Room,
  SSERequest,
  SSEResponse,
  SSEServerOptions,
} from "./types.js";

type MiddlewareFn<Events extends EventMap> = (
  client: Client<Events>,
  next: (err?: Error) => void
) => void;

export class SSENamespace<Events extends EventMap = EventMap> {
  readonly name: string;

  private readonly _clients: Map<ClientId, Client<Events>> = new Map();
  /** room → Set of clientIds */
  private readonly _rooms: Map<Room, Set<ClientId>> = new Map();
  private readonly _middlewares: MiddlewareFn<Events>[] = [];
  private _adapter!: Adapter;
  private _heartbeatInterval: number;
  private _heartbeatTimer: ReturnType<typeof setInterval> | null = null;
  private readonly _cors: SSEServerOptions["cors"];

  /** Internal emitter for namespace-level events like 'connection' */
  private readonly _events = new EventEmitter();

  constructor(name: string, options: SSEServerOptions = {}) {
    this.name = name;
    this._heartbeatInterval = options.heartbeatInterval ?? 30_000;
    this._cors = options.cors;
  }

  /** @internal called by SSEServer when adapter is assigned */
  _setAdapter(adapter: Adapter): void {
    this._stopHeartbeat();
    this._adapter = adapter;
    adapter.init((payload) => this._onAdapterPayload(payload));
    if (this._heartbeatInterval > 0) {
      this._startHeartbeat();
    }
  }

  // ---------------------------------------------------------------------------
  // Middleware
  // ---------------------------------------------------------------------------

  use(fn: MiddlewareFn<Events>): this {
    this._middlewares.push(fn);
    return this;
  }

  // ---------------------------------------------------------------------------
  // Connection handler
  // ---------------------------------------------------------------------------

  /** Listen for new SSE connections on this namespace */
  on(event: "connection", listener: (client: Client<Events>) => void): this;
  on(event: string, listener: (...args: never[]) => void): this;
  on(event: string, listener: (...args: never[]) => void): this {
    this._events.on(event, listener as (...args: unknown[]) => void);
    return this;
  }

  // ---------------------------------------------------------------------------
  // Connect
  // ---------------------------------------------------------------------------

  /**
   * Register an incoming HTTP request as an SSE connection.
   * Call this from your HTTP route handler.
   *
   * @example
   * app.get('/stream/orders/:orderId', (req, res) => {
   *   const client = orders.connect(req, res);
   *   client.join(req.params.orderId);
   * });
   */
  connect(req: SSERequest, res: SSEResponse): Client<Events> {
    const headers: Record<string, string | string[] | undefined> = {};
    for (const [k, v] of Object.entries(req.headers)) {
      headers[k] = v;
    }
    const query = parseQuery(req.url);
    const handshake = { headers, query, url: req.url ?? "" };

    const client = new Client<Events>(req, res, handshake);
    client._bind(
      (c, rooms) => this._joinRooms(c, rooms),
      (c, rooms) => this._leaveRooms(c, rooms)
    );

    // Write SSE response headers
    const sseHeaders: Record<string, string> = {
      "Content-Type": "text/event-stream",
      Connection: "keep-alive",
      "Cache-Control": "no-cache",
      "X-Accel-Buffering": "no",
    };
    if (this._cors?.origin) sseHeaders["Access-Control-Allow-Origin"] = this._cors.origin;
    if (this._cors?.credentials) sseHeaders["Access-Control-Allow-Credentials"] = "true";
    res.writeHead(200, sseHeaders);

    // Initial connection acknowledgement
    res.write(
      formatSSEMessage({
        event: "connection",
        data: { clientId: client.id },
        id: makeId(12),
      })
    );

    req.on("close", () => {
      client._handleClose();
      this._removeClient(client);
    });

    this._runMiddleware(client, (err?: Error) => {
      if (err) {
        client.disconnect();
        return;
      }
      this._clients.set(client.id, client);
      this._events.emit("connection", client);
    });

    return client;
  }

  // ---------------------------------------------------------------------------
  // Emit API
  // ---------------------------------------------------------------------------

  /** Target one or more rooms. Returns a chainable emitter. */
  to(rooms: Room | Room[]): ChainableEmitter<Events> {
    const roomSet = new Set(Array.isArray(rooms) ? rooms : [rooms]);
    return new ChainableEmitter<Events>(
      (r, ex, event, data) => this._broadcast(r, ex, event, data),
      roomSet,
      new Set()
    );
  }

  /** Exclude rooms from a broadcast. Returns a chainable emitter. */
  except(rooms: Room | Room[]): ChainableEmitter<Events> {
    const excludeSet = new Set(Array.isArray(rooms) ? rooms : [rooms]);
    return new ChainableEmitter<Events>(
      (r, ex, event, data) => this._broadcast(r, ex, event, data),
      new Set(),
      excludeSet
    );
  }

  /** Broadcast an event to all clients in this namespace */
  emit<K extends keyof Events & string>(event: K, data: Events[K]): void;
  emit(event: string, data: unknown): void;
  emit(event: string, data: unknown): void {
    this._broadcast([], [], event, data);
  }

  // ---------------------------------------------------------------------------
  // Introspection
  // ---------------------------------------------------------------------------

  get clientCount(): number {
    return this._clients.size;
  }

  getClients(): ReadonlyMap<ClientId, Client<Events>> {
    return this._clients;
  }

  getRooms(): ReadonlyMap<Room, ReadonlySet<ClientId>> {
    return this._rooms;
  }

  getRoom(room: Room): ReadonlySet<ClientId> | undefined {
    return this._rooms.get(room);
  }

  // ---------------------------------------------------------------------------
  // Lifecycle
  // ---------------------------------------------------------------------------

  async close(): Promise<void> {
    this._stopHeartbeat();
    for (const client of this._clients.values()) {
      client.disconnect();
    }
    this._clients.clear();
    this._rooms.clear();
  }

  // ---------------------------------------------------------------------------
  // Internal helpers
  // ---------------------------------------------------------------------------

  private _runMiddleware(
    client: Client<Events>,
    done: (err?: Error) => void
  ): void {
    const fns = this._middlewares;
    if (fns.length === 0) {
      done();
      return;
    }
    let i = 0;
    const next = (err?: Error): void => {
      if (err) { done(err); return; }
      if (i >= fns.length) { done(); return; }
      const fn = fns[i++];
      try {
        fn(client, next);
      } catch (e) {
        done(e instanceof Error ? e : new Error(String(e)));
      }
    };
    next();
  }

  private _joinRooms(client: Client<Events>, rooms: Room[]): void {
    for (const room of rooms) {
      if (!this._rooms.has(room)) {
        this._rooms.set(room, new Set());
      }
      this._rooms.get(room)!.add(client.id);
      client.rooms.add(room);
    }
  }

  private _leaveRooms(client: Client<Events>, rooms: Room[]): void {
    for (const room of rooms) {
      this._rooms.get(room)?.delete(client.id);
      client.rooms.delete(room);
      if (this._rooms.get(room)?.size === 0) {
        this._rooms.delete(room);
      }
    }
  }

  private _removeClient(client: Client<Events>): void {
    this._clients.delete(client.id);
    for (const room of client.rooms) {
      this._rooms.get(room)?.delete(client.id);
      if (this._rooms.get(room)?.size === 0) {
        this._rooms.delete(room);
      }
    }
    client.rooms.clear();
  }

  private _broadcast(
    rooms: Room[],
    excludeRooms: Room[],
    event: string,
    data: unknown
  ): void {
    const id = String(Date.now());
    void this._adapter.broadcast(this.name, rooms, excludeRooms, event, data, id);
  }

  private _onAdapterPayload(payload: AdapterPayload): void {
    if (payload.namespaceName !== this.name) return;

    const raw = formatSSEMessage({
      event: payload.event,
      data: payload.data,
      id: payload.id,
    });

    const { rooms, excludeRooms } = payload;
    const excludeSet = new Set(excludeRooms);

    if (rooms.length === 0) {
      // Broadcast to all clients (excluding specified rooms)
      for (const client of this._clients.values()) {
        if (excludeSet.size > 0 && [...client.rooms].some((r) => excludeSet.has(r))) {
          continue;
        }
        client._write(raw);
      }
      return;
    }

    // Deliver to clients in target rooms, deduplicated
    const delivered = new Set<ClientId>();
    for (const room of rooms) {
      const clientIds = this._rooms.get(room);
      if (!clientIds) continue;
      for (const clientId of clientIds) {
        if (delivered.has(clientId)) continue;
        const client = this._clients.get(clientId);
        if (!client) continue;
        if (excludeSet.size > 0 && [...client.rooms].some((r) => excludeSet.has(r))) {
          continue;
        }
        client._write(raw);
        delivered.add(clientId);
      }
    }
  }

  private _startHeartbeat(): void {
    this._heartbeatTimer = setInterval(() => {
      const ping = ": ping\n\n";
      for (const client of this._clients.values()) {
        client._write(ping);
      }
    }, this._heartbeatInterval);
    this._heartbeatTimer.unref?.();
  }

  private _stopHeartbeat(): void {
    if (this._heartbeatTimer) {
      clearInterval(this._heartbeatTimer);
      this._heartbeatTimer = null;
    }
  }
}
