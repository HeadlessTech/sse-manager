import { Adapter } from "../adapter.js";
import type { AdapterPayload, Room } from "../types.js";

/**
 * Default in-process adapter. No external dependencies.
 * Routes payloads directly back to the same process — no cross-server delivery.
 */
export class MemoryAdapter extends Adapter {
  private handlers = new Map<string, (payload: AdapterPayload) => void>();

  init(namespaceName: string, handler: (payload: AdapterPayload) => void): void {
    this.handlers.set(namespaceName, handler);
  }

  async broadcast(
    namespaceName: string,
    rooms: Room[],
    excludeRooms: Room[],
    event: string,
    data: unknown,
    id: string
  ): Promise<void> {
    this.handlers.get(namespaceName)?.({ namespaceName, rooms, excludeRooms, event, data, id });
  }

  uninit(namespaceName: string): void {
    this.handlers.delete(namespaceName);
  }

  async close(): Promise<void> {
    this.handlers.clear();
  }
}
