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

Workers accept metadata-only lifecycle hooks through `QueueWorkerOptions.hooks`:

```ts
queue.worker(handleJob, {
  hooks: {
    onJobStart: ({ id, attempts }) => metrics.increment("queue.started", { id, attempts }),
    onJobFailure: ({ id }, error) => reportJobFailure(id, error),
    onDeadLetter: ({ id }) => metrics.increment("queue.dead_letter", { id }),
    onError: (error) => reportInfrastructureError(error),
  },
});
```

Hooks receive job identifiers and counters, never the job payload. Observer exceptions
are isolated from queue processing, and the package does not write to the console.
