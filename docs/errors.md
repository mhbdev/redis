# Errors and failure handling

Every package error is an instance of `RedisError` with a stable `code`, a safe
human-readable message, and an optional `cause`. Use `instanceof` or
`isRedisErrorCode(error, code)` for control flow; do not inspect `message`, `stack`, or
serialized causes.

```ts
import { isRedisErrorCode } from "@mhbdev/redis";

try {
  await redis.ping();
} catch (error) {
  if (isRedisErrorCode(error, "REDIS_TIMEOUT")) return retry();
  if (isRedisErrorCode(error, "REDIS_UNAVAILABLE")) return useFallback();
  throw error;
}
```

The node-redis adapter maps its exported error classes directly. The ioredis bridge
maps unknown failures to `REDIS_UNAVAILABLE` by default and accepts an optional typed
normalizer for application-specific or plugin-specific error classes. No adapter uses
error-message or stack-trace matching to determine behavior.

Configuration, protocol, serialization, lock, and queue errors are not converted into
availability failures. This distinction prevents `failureMode: "open"` in the rate
limiter from masking a programming error or malformed Redis response.
