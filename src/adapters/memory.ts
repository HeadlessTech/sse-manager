import { Adapter } from "../adapter.js";
import type { AdapterPayload, Room } from "../types.js";

/**
 * Default in-process adapter. No external dependencies.
 * Routes payloads directly back to the same process — no cross-server delivery.
 */
export class MemoryAdapter extends Adapter {
  private handler: ((payload: AdapterPayload) => void) | null = null;

  init(handler: (payload: AdapterPayload) => void): void {
    this.handler = handler;
  }

  broadcast(
    namespaceName: string,
    rooms: Room[],
    excludeRooms: Room[],
    event: string,
    data: unknown,
    id: string
  ): void {
    this.handler?.({ namespaceName, rooms, excludeRooms, event, data, id });
  }

  async close(): Promise<void> {
    this.handler = null;
  }
}
