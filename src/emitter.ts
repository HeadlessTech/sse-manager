import type { EventMap, Room } from "./types.js";

type BroadcastFn = (
  rooms: Room[],
  excludeRooms: Room[],
  event: string,
  data: unknown
) => void;

/**
 * Returned by SSENamespace.to() and SSENamespace.except().
 * Accumulates room/exclude filters before the final .emit() call.
 */
export class ChainableEmitter<Events extends EventMap = EventMap> {
  private readonly _broadcast: BroadcastFn;
  private readonly _rooms: Set<Room>;
  private readonly _excludeRooms: Set<Room>;

  /** @internal — obtain instances via SSENamespace.to() / .except() */
  constructor(
    broadcast: BroadcastFn,
    rooms: Set<Room>,
    excludeRooms: Set<Room>
  ) {
    this._broadcast = broadcast;
    this._rooms = rooms;
    this._excludeRooms = excludeRooms;
  }

  /** Add more rooms to target */
  to(rooms: Room | Room[]): ChainableEmitter<Events> {
    const nextRooms = new Set(this._rooms);
    for (const r of Array.isArray(rooms) ? rooms : [rooms]) nextRooms.add(r);
    return new ChainableEmitter(this._broadcast, nextRooms, new Set(this._excludeRooms));
  }

  /** Exclude rooms from the target set */
  except(rooms: Room | Room[]): ChainableEmitter<Events> {
    const nextExclude = new Set(this._excludeRooms);
    for (const r of Array.isArray(rooms) ? rooms : [rooms]) nextExclude.add(r);
    return new ChainableEmitter(this._broadcast, new Set(this._rooms), nextExclude);
  }

  /** Dispatch the event to the accumulated room set */
  emit<K extends keyof Events & string>(event: K, data: Events[K]): void;
  emit(event: string, data: unknown): void;
  emit(event: string, data: unknown): void {
    this._broadcast(
      [...this._rooms],
      [...this._excludeRooms],
      event,
      data
    );
  }
}
