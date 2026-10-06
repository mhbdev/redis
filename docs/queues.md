# Queues

`RedisQueue` stores immediate work in a Redis Stream and delayed work in a sorted set.
Workers consume through Redis consumer groups. A failed job is acknowledged and
re-enqueued with an incremented attempt count until `maxAttempts` is reached; then it
is written to the dead-letter stream.

Delivery is at-least-once. A process crash can leave a pending message, and handlers
must be safe to run more than once. Use application-level idempotency keys for external
side effects.

Use a pool when workers need blocking reads or concurrent command execution. Keep queue
names and prefixes stable during rolling deployments.
