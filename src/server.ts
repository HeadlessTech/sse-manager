import { MemoryAdapter } from "./adapters/memory.js";
import { SSENamespace } from "./namespace.js";
import type { Adapter } from "./adapter.js";
import type { EventMap, SSEServerOptions } from "./types.js";

export class SSEServer {
  private readonly _namespaces: Map<string, SSENamespace<EventMap>> = new Map();
  private _adapter: Adapter;
  private readonly _options: SSEServerOptions;

  constructor(options: SSEServerOptions = {}) {
    this._options = options;
    this._adapter = new MemoryAdapter();
  }

  /**
   * Set the adapter used for all namespaces created after this call.
   * Must be called before io.of() to take effect on a namespace.
   *
   * @example
   * io.adapter(new RedisAdapter({ url: 'redis://localhost:6379' }));
   */
  adapter(adapter: Adapter): this {
    this._adapter = adapter;
    // Re-init already-created namespaces so they use the new adapter
    for (const ns of this._namespaces.values()) {
      ns._setAdapter(adapter);
    }
    return this;
  }

  /**
   * Get or create a namespace.
   *
   * @example
   * const orders = io.of('/orders');
   * const orders = io.of<OrderEvents>('/orders');
   */
  of<Events extends EventMap = EventMap>(name: string): SSENamespace<Events> {
    if (this._namespaces.has(name)) {
      return this._namespaces.get(name) as SSENamespace<Events>;
    }
    const ns = new SSENamespace<Events>(name, this._options);
    ns._setAdapter(this._adapter);
    this._namespaces.set(name, ns as unknown as SSENamespace<EventMap>);
    return ns;
  }

  /**
   * Convenience: emit on the default "/" namespace.
   * Equivalent to io.of('/').to(rooms).emit(event, data).
   */
  to(rooms: string | string[]) {
    return this.of("/").to(rooms);
  }

  /** Gracefully close all namespaces and the adapter */
  async close(): Promise<void> {
    await Promise.all([
      ...[...this._namespaces.values()].map((ns) => ns.close()),
      this._adapter.close(),
    ]);
    this._namespaces.clear();
  }
}
