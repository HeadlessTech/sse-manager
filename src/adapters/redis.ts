import { Adapter } from "../adapter.js";
import type { AdapterPayload, Room } from "../types.js";

type RedisAdapterOptions = {
  /** ioredis connection URL, e.g. "redis://localhost:6379" */
  url?: string;
  /** Alternatively, pass an existing ioredis client */
  pubClient?: RedisLike;
  subClient?: RedisLike;
  /** Redis channel prefix. Default: "sse-io" */
  channelPrefix?: string;
};

/** Minimal ioredis interface so we avoid a hard import at the module level */
interface RedisLike {
  publish(channel: string, message: string): Promise<unknown>;
  subscribe(channel: string): Promise<unknown>;
  on(event: "message", listener: (channel: string, message: string) => void): this;
  duplicate(): RedisLike;
  quit(): Promise<unknown>;
}

/**
 * Redis pub/sub adapter for horizontal scaling.
 *
 * Requires ioredis as a peer dependency:
 *   npm install ioredis
 *
 * Usage:
 *   import { RedisAdapter } from 'sse-io/adapters/redis';
 *   io.adapter(new RedisAdapter({ url: 'redis://localhost:6379' }));
 *
 * Each server subscribes to the shared channel. When any server broadcasts,
 * all servers (including the publisher) receive the payload and deliver it
 * to their locally connected clients.
 */
export class RedisAdapter extends Adapter {
  private pub: RedisLike;
  private sub: RedisLike;
  private channel: string;
  private handler: ((payload: AdapterPayload) => void) | null = null;

  constructor(options: RedisAdapterOptions = {}) {
    super();
    const prefix = options.channelPrefix ?? "sse-io";
    this.channel = prefix;

    if (options.pubClient && options.subClient) {
      this.pub = options.pubClient;
      this.sub = options.subClient;
    } else {
      // Dynamically require ioredis so the main bundle doesn't fail when ioredis is absent
      // eslint-disable-next-line @typescript-eslint/no-require-imports
      const Redis = requireIoRedis();
      const url = options.url ?? "redis://localhost:6379";
      this.pub = new Redis(url) as unknown as RedisLike;
      this.sub = (this.pub as unknown as { duplicate(): RedisLike }).duplicate();
    }
  }

  init(handler: (payload: AdapterPayload) => void): void {
    this.handler = handler;
    void this.sub.subscribe(this.channel);
    this.sub.on("message", (_ch, raw) => {
      try {
        const payload = JSON.parse(raw) as AdapterPayload;
        this.handler?.(payload);
      } catch {
        // malformed message — ignore
      }
    });
  }

  async broadcast(
    namespaceName: string,
    rooms: Room[],
    excludeRooms: Room[],
    event: string,
    data: unknown,
    id: string
  ): Promise<void> {
    const payload: AdapterPayload = {
      namespaceName,
      rooms,
      excludeRooms,
      event,
      data,
      id,
    };
    await this.pub.publish(this.channel, JSON.stringify(payload));
  }

  async close(): Promise<void> {
    this.handler = null;
    await Promise.all([this.pub.quit(), this.sub.quit()]);
  }
}

function requireIoRedis(): new (url: string) => unknown {
  try {
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    return require("ioredis") as new (url: string) => unknown;
  } catch {
    throw new Error(
      "sse-io RedisAdapter requires ioredis. Run: npm install ioredis"
    );
  }
}
