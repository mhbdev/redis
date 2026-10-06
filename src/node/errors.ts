import {
	AbortError,
	ClientClosedError,
	ConnectionTimeoutError,
	ErrorReply,
	TimeoutError,
} from "redis";

import {
	RedisClientStateError,
	RedisError,
	RedisProtocolError,
	RedisRequestAbortedError,
	RedisTimeoutError,
	RedisUnavailableError,
} from "../core/errors.js";

export function normalizeNodeRedisError(error: unknown): RedisError {
	if (error instanceof RedisError) return error;
	if (error instanceof AbortError) return new RedisRequestAbortedError(error);
	if (error instanceof ConnectionTimeoutError || error instanceof TimeoutError)
		return new RedisTimeoutError("Redis command timed out", error);
	if (error instanceof ClientClosedError)
		return new RedisClientStateError("The Redis client has been closed");
	if (error instanceof ErrorReply)
		return new RedisProtocolError("Redis rejected the command", error);
	return new RedisUnavailableError(error);
}
