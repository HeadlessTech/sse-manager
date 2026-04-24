import { EventEmitter } from "node:events";
import { formatSSEMessage, makeId } from "./utils.js";
import type {
  ClientId,
  EventMap,
  HandshakeData,
  Room,
  SSERequest,
  SSEResponse,
} from "./types.js";

export class Client<Events extends EventMap = EventMap> {
  readonly id: ClientId;
  readonly handshake: HandshakeData;

  /** Rooms this client is currently joined to */
  readonly rooms: Set<Room> = new Set();

  private readonly _req: SSERequest;
  private readonly _res: SSEResponse;
  private _disconnected = false;
  private readonly _events = new EventEmitter();

  /** Injected by SSENamespace after construction */
  private _joinFn!: (client: Client<Events>, rooms: Room[]) => void;
  private _leaveFn!: (client: Client<Events>, rooms: Room[]) => void;

  constructor(req: SSERequest, res: SSEResponse, handshake: HandshakeData) {
    this.id = makeId(20);
    this._req = req;
    this._res = res;
    this.handshake = handshake;
  }

  /** @internal called by SSENamespace after construction */
  _bind(
    joinFn: (client: Client<Events>, rooms: Room[]) => void,
    leaveFn: (client: Client<Events>, rooms: Room[]) => void
  ): void {
    this._joinFn = joinFn;
    this._leaveFn = leaveFn;
  }

  /** Add this client to one or more rooms */
  join(rooms: Room | Room[]): this {
    this._joinFn(this, Array.isArray(rooms) ? rooms : [rooms]);
    return this;
  }

  /** Remove this client from one or more rooms */
  leave(rooms: Room | Room[]): this {
    this._leaveFn(this, Array.isArray(rooms) ? rooms : [rooms]);
    return this;
  }

  /** Send an SSE event directly to this client */
  emit<K extends keyof Events & string>(event: K, data: Events[K]): this;
  emit(event: string, data: unknown): this;
  emit(event: string, data: unknown): this {
    if (this._disconnected) return this;
    const id = String(Date.now());
    const msg = formatSSEMessage({ event, data, id });
    try {
      this._res.write(msg);
    } catch {
      this._handleClose();
    }
    return this;
  }

  /** Listen for client lifecycle events */
  on(event: "disconnect", listener: () => void): this;
  on(event: string, listener: (...args: unknown[]) => void): this;
  on(event: string, listener: (...args: unknown[]) => void): this {
    this._events.on(event, listener);
    return this;
  }

  /** Close this client's SSE connection */
  disconnect(): void {
    if (this._disconnected) return;
    this._handleClose();
    try {
      this._res.end();
    } catch {
      // already ended
    }
  }

  get disconnected(): boolean {
    return this._disconnected;
  }

  /** @internal called by SSENamespace when req 'close' fires */
  _handleClose(): void {
    if (this._disconnected) return;
    this._disconnected = true;
    this._events.emit("disconnect");
  }

  /** @internal used by namespace for direct raw writes (broadcast delivery) */
  _write(raw: string): void {
    if (this._disconnected) return;
    try {
      this._res.write(raw);
    } catch {
      this._handleClose();
    }
  }
}
