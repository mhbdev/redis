# Migration from `@dordoone/redis-runtime`

```sh
npm uninstall @dordoone/redis-runtime
npm install @mhbdev/redis
```

The primary mapping is:

| Internal package | `@mhbdev/redis` |
| --- | --- |
| `new RedisClientManager({ url })` | `createRedisClient({ url })` |
| `manager.getClient()` | `client.execute()` or `client.raw()` |
| `manager.ping()` | `client.ping()` |
| `manager.close()` | `client.close()` |
| `new RedisRateLimiter(manager)` | `createRateLimiter(fixedWindow(...), { redis: client })` |
| `RateLimitPolicy` / `RateLimitResult` | same names from `@mhbdev/redis/rate-limit` |

The package remains application-policy neutral. The application still decides whether
an endpoint fails open or closed; the limiter only exposes the configured behavior and
typed result.
