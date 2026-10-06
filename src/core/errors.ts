export type RedisErrorCode =
	| "REDIS_CONFIGURATION_ERROR"
	| "REDIS_CONNECTION_ERROR"
	| "REDIS_TIMEOUT"
	| "REDIS_ABORTED"
	| "REDIS_UNAVAILABLE"
	| "REDIS_CLIENT_STATE"
	| "REDIS_PROTOCOL_ERROR"
	| "REDIS_SERIALIZATION_ERROR"
	| "REDIS_LOCK_ERROR"
	| "REDIS_QUEUE_ERROR";

export class RedisError extends Error {
	readonly isOperational: boolean;
	override readonly cause: unknown;

	constructor(
		readonly code: RedisErrorCode,
		message: string,
		cause?: unknown,
		isOperational = true,
	) {
		super(message, { cause });
		this.name = "RedisError";
		this.isOperational = isOperational;
		this.cause = cause;
	}
}

export class RedisConfigurationError extends RedisError {
	constructor(message: string, cause?: unknown) {
		super("REDIS_CONFIGURATION_ERROR", message, cause);
		this.name = "RedisConfigurationError";
	}
}

export class RedisConnectionError extends RedisError {
	constructor(message = "Redis connection failed", cause?: unknown) {
		super("REDIS_CONNECTION_ERROR", message, cause);
		this.name = "RedisConnectionError";
	}
}

export class RedisTimeoutError extends RedisError {
	constructor(message = "Redis command timed out", cause?: unknown) {
		super("REDIS_TIMEOUT", message, cause);
		this.name = "RedisTimeoutError";
	}
}

export class RedisRequestAbortedError extends RedisError {
	constructor(cause?: unknown) {
		super("REDIS_ABORTED", "The Redis operation was aborted", cause);
		this.name = "RedisRequestAbortedError";
	}
}

export class RedisUnavailableError extends RedisError {
	constructor(cause?: unknown) {
		super("REDIS_UNAVAILABLE", "The Redis service is unavailable", cause);
		this.name = "RedisUnavailableError";
	}
}

export class RedisClientStateError extends RedisError {
	constructor(message = "The Redis client is not available") {
		super("REDIS_CLIENT_STATE", message);
		this.name = "RedisClientStateError";
	}
}

export class RedisProtocolError extends RedisError {
	constructor(message = "Redis returned an invalid response", cause?: unknown) {
		super("REDIS_PROTOCOL_ERROR", message, cause);
		this.name = "RedisProtocolError";
	}
}

export class RedisSerializationError extends RedisError {
	constructor(message = "Redis value serialization failed", cause?: unknown) {
		super("REDIS_SERIALIZATION_ERROR", message, cause);
		this.name = "RedisSerializationError";
	}
}

export class RedisLockError extends RedisError {
	constructor(message: string, cause?: unknown) {
		super("REDIS_LOCK_ERROR", message, cause);
		this.name = "RedisLockError";
	}
}

export class RedisQueueError extends RedisError {
	constructor(message: string, cause?: unknown) {
		super("REDIS_QUEUE_ERROR", message, cause);
		this.name = "RedisQueueError";
	}
}

export function asError(error: unknown): Error {
	return error instanceof Error
		? error
		: new Error("Unknown Redis failure", { cause: error });
}

export function isRedisError(error: unknown): error is RedisError {
	return error instanceof RedisError;
}

export function isRedisErrorCode<Code extends RedisErrorCode>(
	error: unknown,
	code: Code,
): error is RedisError & { readonly code: Code } {
	return error instanceof RedisError && error.code === code;
}
