# Adapters

Feature modules are transport-neutral. They accept `RedisExecutor` or
`RedisSubscriptionTransport`, which means a project can replace its Redis client
without rewriting application-facing cache, limiter, lock, pub/sub, or queue code.

## node-redis

`createRedisClient` and `createRedisPool` are the default Node/Bun adapters and are
included with the package's normal `redis` dependency.

## ioredis

Install ioredis in the consuming project and pass its configured client to
`createIoredisExecutor`. The adapter does not create an ioredis client or read
environment variables, so authentication, TLS, retry, and sentinel/cluster choices
remain under application control.

`createIoredisPubSubTransport` accepts a publisher plus an explicit subscriber or a
publisher with `duplicate()`. Closing the transport unsubscribes channels it registered
and closes only a duplicate subscriber it created. The publisher and an explicitly
provided subscriber remain owned by the application.

The bridge applies a command timeout (3 seconds by default); configure it with
`commandTimeoutMs`. If an application uses ioredis-specific error classes, pass a
typed `normalizeError` function to map those classes to this package's exported
errors. The hook receives the original error object, so message or stack matching is
not required.

## BullMQ

`createBullMqQueueAdapter` accepts an existing BullMQ queue and an injected worker
factory. BullMQ remains optional and is not imported by the Redis package at runtime.
This keeps the package installable for consumers who use the native Streams queue or
only the lower-level modules.

Both queue implementations expose the same `JobQueue<T>` and `QueueWorker` contracts.
Both are at-least-once systems; handlers must be idempotent.

All adapters are structural contracts. They do not create hidden clients, read
environment variables, or force a retry policy, so switching from node-redis to
ioredis or BullMQ is an application-composition change rather than a feature-module
rewrite.
