import { Adapter } from "../adapter.js";
import type { AdapterPayload, Room } from "../types.js";

type RedisAdapterOptions = {
  /** redis connection URL, e.g. "redis://localhost:6379" */
  url?: string;
  /** Alternatively, pass existing redis clients (already connected) */
  pubClient?: RedisLike;
  subClient?: RedisLike;
  /** Redis channel prefix. Default: "sse-manager" */
  channelPrefix?: string;
};

/** Minimal node-redis interface so we avoid a hard import at the module level */
interface RedisLike {
  connect?(): Promise<unknown>;
  publish(channel: string, message: string): Promise<unknown>;
  subscribe(channel: string, listener: (message: string, channel: string) => void): Promise<unknown>;
  unsubscribe(channel: string): Promise<unknown>;
  quit(): Promise<unknown>;
}

/**
 * Redis pub/sub adapter for horizontal scaling.
 *
 * Requires the "redis" package as a peer dependency:
 *   npm install redis
 *
 * Usage:
 *   import { RedisAdapter } from 'sse-manager/adapters/redis';
 *   io.adapter(new RedisAdapter({ url: 'redis://localhost:6379' }));
 *
 * Each server subscribes to a per-namespace channel (`prefix:namespaceName`).
 * When any server broadcasts, all servers receive the payload on that channel
 * and deliver it to their locally connected clients.
 */
export class RedisAdapter extends Adapter {
  private pub: RedisLike;
  private sub: RedisLike;
  private channelPrefix: string;
  private handlers = new Map<string, (payload: AdapterPayload) => void>();
  private subscribedChannels = new Set<string>();
  private connectPromise: Promise<void> | null = null;

  constructor(options: RedisAdapterOptions = {}) {
    super();
    this.channelPrefix = options.channelPrefix ?? "sse-manager";

    if (options.pubClient && options.subClient) {
      this.pub = options.pubClient;
      this.sub = options.subClient;
    } else {
      const { createClient } = requireRedis();
      const url = options.url ?? "redis://localhost:6379";
      this.pub = createClient({ url }) as unknown as RedisLike;
      this.sub = createClient({ url }) as unknown as RedisLike;
      this.connectPromise = Promise.all([
        this.pub.connect!(),
        this.sub.connect!(),
      ]).then(() => undefined);
    }
  }

  init(namespaceName: string, handler: (payload: AdapterPayload) => void): void {
    this.handlers.set(namespaceName, handler);
    const channel = `${this.channelPrefix}::${namespaceName.replace(/^\//, "")}`;
    if (this.subscribedChannels.has(channel)) return;
    this.subscribedChannels.add(channel);
    void (async () => {
      if (this.connectPromise) await this.connectPromise;
      await this.sub.subscribe(channel, (raw) => {
        try {
          const payload = JSON.parse(raw) as AdapterPayload;
          this.handlers.get(payload.namespaceName)?.(payload);
        } catch {
          // malformed message — ignore
        }
      });
    })();
  }

  async broadcast(
    namespaceName: string,
    rooms: Room[],
    excludeRooms: Room[],
    event: string,
    data: unknown,
    id: string
  ): Promise<void> {
    if (this.connectPromise) await this.connectPromise;
    const payload: AdapterPayload = {
      namespaceName,
      rooms,
      excludeRooms,
      event,
      data,
      id,
    };
    const channel = `${this.channelPrefix}::${namespaceName.replace(/^\//, "")}`;
    await this.pub.publish(channel, JSON.stringify(payload));
  }

  uninit(namespaceName: string): void {
    this.handlers.delete(namespaceName);
    const channel = `${this.channelPrefix}::${namespaceName.replace(/^\//, "")}`;
    this.subscribedChannels.delete(channel);
    void (async () => {
      if (this.connectPromise) await this.connectPromise.catch(() => {});
      await this.sub.unsubscribe(channel);
    })();
  }

  async close(): Promise<void> {
    if (this.connectPromise) await this.connectPromise.catch(() => {});
    this.handlers.clear();
    this.subscribedChannels.clear();
    await Promise.all([this.pub.quit(), this.sub.quit()]);
  }
}

function requireRedis(): { createClient(opts: { url: string }): unknown } {
  try {
    // Uses require() so the import is synchronous (needed in the constructor).
    // This works in CJS and in Node ESM when the package is dual-published.
    // Pure-ESM environments that cannot execute require() should pass pre-built
    // pubClient/subClient instead of relying on this path.
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    return require("redis") as { createClient(opts: { url: string }): unknown };
  } catch {
    throw new Error(
      'sse-manager RedisAdapter requires the "redis" package. Run: npm install redis'
    );
  }
}
