import type { AdapterPayload, Room } from "./types.js";

/**
 * Adapters decouple the emit path from in-process delivery.
 *
 * The default MemoryAdapter routes directly to the local namespace.
 * The RedisAdapter publishes to a channel so all server instances
 * receive the payload and deliver it to their local clients.
 */
export abstract class Adapter {
  /**
   * Called once per namespace when the adapter is attached.
   * The handler must be saved and invoked for every incoming payload
   * (including ones published by this same server instance).
   */
  abstract init(handler: (payload: AdapterPayload) => void): void;

  /**
   * Called when the library wants to emit to a set of rooms.
   * The adapter is responsible for ensuring every server instance
   * (including this one) receives the payload via the handler passed to init().
   */
  abstract broadcast(
    namespaceName: string,
    rooms: Room[],
    excludeRooms: Room[],
    event: string,
    data: unknown,
    id: string
  ): void | Promise<void>;

  /** Called on graceful shutdown. Release connections, timers, etc. */
  abstract close(): Promise<void>;
}
