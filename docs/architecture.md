# Architecture

The package is organized around a small transport contract and independent feature
modules. Modules do not create connections, read environment variables, or choose
application-level failure policy.

```text
application
    |
    v
feature module (cache, limiter, lock, pub/sub, queue)
    |
    v
RedisExecutor / RedisSubscriptionTransport
    |
    v
node-redis adapter, pool, or user adapter
```

`RedisExecutor` exposes command and script execution without coupling the feature
modules to node-redis. `@mhbdev/redis/node` owns TCP/TLS connection lifecycle,
timeouts, bounded reconnects, pooling, and the explicit raw-client escape hatch.

Every public module validates configuration at construction or before issuing a
command. Redis responses from scripts are validated before becoming public values.
Errors preserve their causes while avoiding connection URLs, credentials, raw
identifiers, and payloads in messages.

## Lifecycle

Create one managed client or pool per process and close it during graceful shutdown.
Do not create a connection per request. Pub/sub uses dedicated subscriber connections
because Redis subscriber clients cannot safely execute ordinary commands while
subscribed.

## Compatibility boundary

The new API is intentionally clean rather than a copy of the private
`@dordoone/redis-runtime` package. The existing atomic rate-limit behavior is
preserved through `RateLimiter.check`, `RateLimitPolicy`, and `RateLimitResult`; the
migration guide maps the old connection manager to `createRedisClient`.
