# `@mhbdev/redis`

Typed Redis infrastructure for Node.js 20+ and Bun. The package provides a managed
node-redis client, connection pools, atomic rate limiting, typed caching, distributed
locks, pub/sub, and reliable at-least-once queues.

## Install

```sh
npm install @mhbdev/redis
```

## Client and pooling

```ts
import { createRedisClient, createRedisPool, withRedisClient } from "@mhbdev/redis";

await withRedisClient(
  { url: process.env.REDIS_URL!, commandTimeoutMs: 3_000 },
  async (redis) => {
    await redis.execute(["SET", "health", "ok"]);
    await redis.ping();
  },
);

// Long-lived services can keep the client and close it during shutdown.
const redis = createRedisClient({ url: process.env.REDIS_URL! });
await redis.execute(["SET", "key", "value"]); // connects lazily

const pool = createRedisPool({
  url: process.env.REDIS_URL!,
  minimum: 1,
  maximum: 10,
  acquireTimeoutMs: 3_000,
});

await pool.execute(["SET", "key", "value"]);
await pool.close();
await redis.close();
```

Use `pool.use(async (client) => ...)` when a sequence must run on one exclusive
leased node-redis connection. `minimum`, `maximum`, and `acquireTimeoutMs` bound
resource usage; `withRedisPool` provides the same scoped lifecycle helper as the
client helper.

The high-level APIs use typed transport contracts. Node users can use `raw()` when a
command or Redis module is intentionally outside this package's focused abstractions.
Custom adapters can implement `RedisExecutor` for other runtimes.

### Adapter choices

The feature modules depend on `RedisExecutor`, so an existing client can be swapped
without changing cache, limiter, lock, or queue code:

```ts
import Redis from "ioredis";
import { createIoredisExecutor } from "@mhbdev/redis/ioredis";

const redis = createIoredisExecutor(new Redis(process.env.REDIS_URL!));
```

Install `ioredis` only when choosing that adapter:

```sh
npm install ioredis
```

Redis-backed queues and BullMQ share the typed `JobQueue` / `QueueWorker` contracts.
Use `createBullMqQueueAdapter` with an existing BullMQ `Queue` and an injected worker
factory when BullMQ's scheduling and retry model is preferred. Install `bullmq` only
for that integration.

## Rate limiting

```ts
import { createRateLimiter, fixedWindow } from "@mhbdev/redis/rate-limit";
import { createRedisClient } from "@mhbdev/redis";

const redis = createRedisClient({ url: process.env.REDIS_URL! });
const limiter = createRateLimiter(fixedWindow(10, 60_000), {
  redis,
  prefix: "api",
  ephemeralCache: true,
});

const result = await limiter.limit("user-123");
if (!result.success) {
  console.log(`Retry in ${result.retryAfterMs}ms`);
}
```

Use `slidingWindow(limit, windowMs)` when boundary bursts matter, or
`tokenBucket(refillRate, intervalMs, maxTokens)` when bursts should be smoothed.
Rate-limit identifiers are hashed before becoming keys. Redis failures are closed by
default; use `failureMode: "open"` only when availability is more important than
strict enforcement.

## Cache, locks, pub/sub, and queues

```ts
import { createCache, createLockManager, jsonCodec } from "@mhbdev/redis";

const cache = createCache<User>({ redis, prefix: "users" }); // JSON by default
const binaryCache = createCache({ redis, codec: jsonCodec<User>(), prefix: "users" });
const user = await cache.getOrSet("42", () => loadUser("42"), { ttlMs: 60_000 });

const locks = createLockManager({ redis });
await locks.withLock("user:42", async () => updateUser(user));
```

Queues use Redis Streams and consumer groups. Delivery is at-least-once: handlers must
be idempotent. Queue and pub/sub factories also use JSON by default and accept a custom
`RedisCodec<T>` when the payload is binary or must be schema-validated. The public
factory aliases `createRedisCache`, `createRedisQueue`, `createRedisPubSub`, and
`createRedisRateLimiter` are available for codebases that prefer explicit names.

## Typed failures

Operational failures are represented by exported error classes and stable `code` values;
application code never needs to inspect error messages:

```ts
import {
  RedisProtocolError,
  RedisTimeoutError,
  RedisUnavailableError,
} from "@mhbdev/redis";

try {
  await redis.ping();
} catch (error) {
  if (error instanceof RedisTimeoutError) return retryLater();
  if (error instanceof RedisUnavailableError) return failOpenForThisFeature();
  if (error instanceof RedisProtocolError) return reportProtocolFailure(error);
  throw error;
}
```

All causes are preserved on typed errors. Causes are never used for semantic handling by
message matching, and sensitive command arguments and payloads are not copied into error
messages.

## Development

```sh
npm install
npm run lint
npm run typecheck
npm test
npm run build
npm run pack:check
```

Live Redis tests are separate from the unit suite. Run them with a local Redis service:

```sh
npm run test:integration
```

## Documentation

- [Architecture](docs/architecture.md)
- [Rate limiting](docs/rate-limiting.md)
- [Queues](docs/queues.md)
- [Adapter bridges](docs/adapters.md)
- [Errors and failure handling](docs/errors.md)
- [Releasing to npm](docs/releasing.md)
- [Migration from `@dordoone/redis-runtime`](docs/migration.md)

## License

MIT © 2026 mhbdev
