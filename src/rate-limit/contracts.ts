import {
	RedisClientStateError,
	RedisConfigurationError,
	RedisConnectionError,
	RedisProtocolError,
	RedisTimeoutError,
	RedisUnavailableError,
} from "../core/errors.js";
import { type RedisExecutor, withTimeout } from "../core/executor.js";
import { assertKeyPart, hashIdentifier } from "../core/keys.js";

export type RateLimitFailureMode = "open" | "closed";
export type RateLimitReason = "limit" | "cache" | "timeout" | "redis" | "success";

export type RateLimitAlgorithm = Readonly<{
	kind: "fixed-window" | "sliding-window" | "token-bucket";
	limit: number;
	windowMs: number;
	maxTokens?: number;
	refillRate?: number;
}>;

export type RateLimitPolicy = Readonly<{
	name: string;
	maxRequests: number;
	windowMs: number;
	failureMode: RateLimitFailureMode;
}>;

export type RateLimitRequest = Readonly<{ rate?: number }>;

export type RateLimitResponse = Readonly<{
	success: boolean;
	allowed: boolean;
	limit: number;
	remaining: number;
	reset: number;
	resetAt: Date;
	retryAfterMs: number;
	reason: RateLimitReason;
	pending: Promise<void>;
}>;

export type RateLimitResult = Readonly<{
	allowed: boolean;
	limit: number;
	remaining: number;
	resetAt: Date;
}>;

export type RateLimitAnalyticsEvent = Readonly<{
	identifier: string;
	algorithm: RateLimitAlgorithm["kind"];
	allowed: boolean;
	limit: number;
	remaining: number;
	timestamp: number;
}>;

export interface RateLimitAnalyticsSink {
	record(event: RateLimitAnalyticsEvent): void | Promise<void>;
}

export type RateLimiterOptions = Readonly<{
	redis: RedisExecutor;
	prefix?: string;
	timeoutMs?: number;
	failureMode?: RateLimitFailureMode;
	ephemeralCache?: boolean | Map<string, number>;
	analytics?: RateLimitAnalyticsSink;
	dynamicLimits?: boolean;
	now?: () => number;
}>;

export interface RateLimiter {
	limit(identifier: string, request?: RateLimitRequest): Promise<RateLimitResponse>;
	check(identity: string, policy: RateLimitPolicy): Promise<RateLimitResult>;
	getRemaining(
		identifier: string,
	): Promise<Pick<RateLimitResponse, "remaining" | "reset" | "resetAt">>;
	reset(identifier: string): Promise<void>;
	blockUntilReady(
		identifier: string,
		timeoutMs: number,
		request?: RateLimitRequest,
	): Promise<RateLimitResponse>;
	setDynamicLimit(limit: number | false): Promise<void>;
	getDynamicLimit(): Promise<number | null>;
}

export function fixedWindow(limit: number, windowMs: number): RateLimitAlgorithm {
	assertAlgorithmValues(limit, windowMs);
	return { kind: "fixed-window", limit, windowMs };
}

export function slidingWindow(limit: number, windowMs: number): RateLimitAlgorithm {
	assertAlgorithmValues(limit, windowMs);
	return { kind: "sliding-window", limit, windowMs };
}

export function tokenBucket(
	refillRate: number,
	windowMs: number,
	maxTokens: number,
): RateLimitAlgorithm {
	assertAlgorithmValues(refillRate, windowMs);
	if (!Number.isSafeInteger(maxTokens) || maxTokens <= 0)
		throw new RedisConfigurationError("Token bucket maximum must be positive");
	return { kind: "token-bucket", limit: maxTokens, windowMs, maxTokens, refillRate };
}

export function createRateLimiter(
	algorithm: RateLimitAlgorithm,
	options: RateLimiterOptions,
): RateLimiter {
	return new RedisRateLimiter(algorithm, options);
}

/** A discoverable alias for applications that prefer Redis-prefixed factory names. */
export const createRedisRateLimiter = createRateLimiter;

export class RedisRateLimiter implements RateLimiter {
	private readonly prefix: string;
	private readonly timeoutMs: number;
	private readonly failureMode: RateLimitFailureMode;
	private readonly cache: Map<string, number> | null;
	private readonly now: () => number;

	constructor(
		private readonly algorithm: RateLimitAlgorithm,
		private readonly options: RateLimiterOptions,
	) {
		this.prefix = options.prefix ?? "mhbdev:rate-limit";
		this.timeoutMs = options.timeoutMs ?? 3_000;
		this.failureMode = options.failureMode ?? "closed";
		this.cache =
			options.ephemeralCache === false
				? null
				: options.ephemeralCache instanceof Map
					? options.ephemeralCache
					: new Map();
		this.now = options.now ?? Date.now;
		if (!this.prefix.trim())
			throw new RedisConfigurationError("Rate-limit prefix is required");
	}

	async limit(
		identifier: string,
		request: RateLimitRequest = {},
	): Promise<RateLimitResponse> {
		assertIdentifier(identifier);
		const weight = request.rate ?? 1;
		if (!Number.isSafeInteger(weight) || weight <= 0)
			throw new RedisConfigurationError("Rate-limit request rate must be positive");
		const cachedUntil = this.cache?.get(identifier);
		if (cachedUntil !== undefined) {
			if (cachedUntil > this.now())
				return this.response(false, this.algorithm.limit, 0, cachedUntil, "cache");
			this.cache?.delete(identifier);
		}

		const started = this.now();
		try {
			const dynamicLimit = this.options.dynamicLimits
				? await this.getDynamicLimit()
				: null;
			const result = await withTimeout(
				() =>
					this.runAlgorithm(
						identifier,
						dynamicLimit ?? this.algorithm.limit,
						weight,
						started,
					),
				this.timeoutMs,
			);
			if (!result.allowed && this.cache) this.cache.set(identifier, result.reset);
			const response = this.response(
				result.allowed,
				result.limit,
				result.remaining,
				result.reset,
				result.allowed ? "success" : "limit",
			);
			const pending = this.options.analytics?.record({
				identifier,
				algorithm: this.algorithm.kind,
				allowed: result.allowed,
				limit: result.limit,
				remaining: result.remaining,
				timestamp: started,
			});
			return { ...response, pending: Promise.resolve(pending) };
		} catch (error) {
			if (this.failureMode === "open" && isAvailabilityFailure(error))
				return this.response(
					true,
					this.algorithm.limit,
					this.algorithm.limit,
					started + this.algorithm.windowMs,
					error instanceof RedisTimeoutError ? "timeout" : "redis",
				);
			throw error;
		}
	}

	async check(identity: string, policy: RateLimitPolicy): Promise<RateLimitResult> {
		assertKeyPart(policy.name, "Rate-limit policy name");
		const limiter = new RedisRateLimiter(
			fixedWindow(policy.maxRequests, policy.windowMs),
			{
				...this.options,
				prefix: `${this.prefix}:${policy.name}`,
				failureMode: policy.failureMode,
			},
		);
		const result = await limiter.limit(identity);
		return {
			allowed: result.allowed,
			limit: result.limit,
			remaining: result.remaining,
			resetAt: result.resetAt,
		};
	}

	async getRemaining(
		identifier: string,
	): Promise<Pick<RateLimitResponse, "remaining" | "reset" | "resetAt">> {
		assertIdentifier(identifier);
		const now = this.now();
		const digest = await hashIdentifier(identifier);
		const limit = this.options.dynamicLimits
			? ((await this.getDynamicLimit()) ?? this.algorithm.limit)
			: this.algorithm.limit;
		if (this.algorithm.kind === "fixed-window") {
			const start = Math.floor(now / this.algorithm.windowMs) * this.algorithm.windowMs;
			const count = Number(
				(await this.options.redis.execute<string | null>([
					"GET",
					`${this.prefix}:${digest}:${start}`,
				])) ?? 0,
			);
			return {
				remaining: Math.max(0, limit - count),
				reset: start + this.algorithm.windowMs,
				resetAt: new Date(start + this.algorithm.windowMs),
			};
		}
		if (this.algorithm.kind === "sliding-window") {
			const start = Math.floor(now / this.algorithm.windowMs) * this.algorithm.windowMs;
			const values = await this.options.redis.execute<Array<string | null>>([
				"MGET",
				`${this.prefix}:${digest}:${start}`,
				`${this.prefix}:${digest}:${start - this.algorithm.windowMs}`,
			]);
			const current = Number(values?.[0] ?? 0);
			const previous = Number(values?.[1] ?? 0);
			const weighted =
				previous * ((this.algorithm.windowMs - (now - start)) / this.algorithm.windowMs) +
				current;
			return {
				remaining: Math.max(0, Math.floor(limit - weighted)),
				reset: start + this.algorithm.windowMs,
				resetAt: new Date(start + this.algorithm.windowMs),
			};
		}
		const values = await this.options.redis.execute<Array<string | null>>([
			"HMGET",
			`${this.prefix}:bucket:${digest}`,
			"tokens",
			"updated",
		]);
		const tokens = Number(values?.[0] ?? limit);
		const updated = Number(values?.[1] ?? now);
		const refilled = Math.min(
			limit,
			tokens +
				(Math.max(0, now - updated) * (this.algorithm.refillRate ?? 0)) /
					this.algorithm.windowMs,
		);
		const reset =
			now +
			Math.max(
				0,
				Math.ceil(
					((1 - refilled) * this.algorithm.windowMs) / (this.algorithm.refillRate ?? 1),
				),
			);
		return { remaining: Math.floor(refilled), reset, resetAt: new Date(reset) };
	}

	async reset(identifier: string): Promise<void> {
		assertIdentifier(identifier);
		const keys = await this.keysFor(identifier, this.now());
		if (keys.length) await this.options.redis.execute(["DEL", ...keys]);
		this.cache?.delete(identifier);
	}

	async blockUntilReady(
		identifier: string,
		timeoutMs: number,
		request: RateLimitRequest = {},
	): Promise<RateLimitResponse> {
		const deadline = this.now() + timeoutMs;
		while (this.now() <= deadline) {
			const result = await this.limit(identifier, request);
			if (result.success) return result;
			await new Promise((resolve) =>
				setTimeout(
					resolve,
					Math.min(
						Math.max(result.retryAfterMs, 10),
						Math.max(10, deadline - this.now()),
					),
				),
			);
		}
		return this.limit(identifier, request);
	}

	async setDynamicLimit(limit: number | false): Promise<void> {
		if (!this.options.dynamicLimits)
			throw new RedisConfigurationError("Dynamic limits are not enabled");
		const key = `${this.prefix}:dynamic-limit`;
		if (limit === false) await this.options.redis.execute(["DEL", key]);
		else {
			if (!Number.isSafeInteger(limit) || limit <= 0)
				throw new RedisConfigurationError("Dynamic limit must be positive");
			await this.options.redis.execute(["SET", key, String(limit)]);
		}
	}

	async getDynamicLimit(): Promise<number | null> {
		if (!this.options.dynamicLimits) return null;
		const value = await this.options.redis.execute<string | null>([
			"GET",
			`${this.prefix}:dynamic-limit`,
		]);
		if (value === null) return null;
		const limit = Number(value);
		if (!Number.isSafeInteger(limit) || limit <= 0)
			throw new RedisProtocolError("Invalid dynamic rate-limit response");
		return limit;
	}

	private async runAlgorithm(
		identifier: string,
		limit: number,
		weight: number,
		now: number,
	): Promise<{ allowed: boolean; limit: number; remaining: number; reset: number }> {
		const digest = await hashIdentifier(identifier);
		if (this.algorithm.kind === "fixed-window") {
			const start = Math.floor(now / this.algorithm.windowMs) * this.algorithm.windowMs;
			const reset = start + this.algorithm.windowMs;
			const value = await this.options.redis.eval<unknown>(FIXED_WINDOW_SCRIPT, {
				keys: [`${this.prefix}:${digest}:${start}`],
				arguments: [String(weight), String(reset - now)],
			});
			const count = parseInteger(value, "fixed-window");
			return {
				allowed: count <= limit,
				limit,
				remaining: Math.max(0, limit - count),
				reset,
			};
		}
		if (this.algorithm.kind === "sliding-window") {
			const start = Math.floor(now / this.algorithm.windowMs) * this.algorithm.windowMs;
			const value = await this.options.redis.eval<unknown>(SLIDING_WINDOW_SCRIPT, {
				keys: [
					`${this.prefix}:${digest}:${start}`,
					`${this.prefix}:${digest}:${start - this.algorithm.windowMs}`,
				],
				arguments: [
					String(weight),
					String(this.algorithm.windowMs),
					String(now - start),
					String(start + this.algorithm.windowMs - now),
				],
			});
			if (!Array.isArray(value) || value.length < 2)
				throw new RedisProtocolError("Invalid sliding-window response");
			const weighted = Number(value[1]);
			return {
				allowed: weighted <= limit,
				limit,
				remaining: Math.max(0, Math.floor(limit - weighted)),
				reset: start + this.algorithm.windowMs,
			};
		}
		const value = await this.options.redis.eval<unknown>(TOKEN_BUCKET_SCRIPT, {
			keys: [`${this.prefix}:bucket:${digest}`],
			arguments: [
				String(limit),
				String(this.algorithm.refillRate),
				String(this.algorithm.windowMs),
				String(weight),
				String(now),
			],
		});
		if (!Array.isArray(value) || value.length < 3)
			throw new RedisProtocolError("Invalid token-bucket response");
		const allowed = Number(value[0]) === 1;
		const remaining = Math.max(0, Math.floor(Number(value[1])));
		const reset = Number(value[2]);
		return { allowed, limit, remaining, reset };
	}

	private async keysFor(identifier: string, now: number): Promise<string[]> {
		const digest = await hashIdentifier(identifier);
		if (this.algorithm.kind === "fixed-window")
			return [
				`${this.prefix}:${digest}:${Math.floor(now / this.algorithm.windowMs) * this.algorithm.windowMs}`,
			];
		if (this.algorithm.kind === "sliding-window") {
			const start = Math.floor(now / this.algorithm.windowMs) * this.algorithm.windowMs;
			return [
				`${this.prefix}:${digest}:${start}`,
				`${this.prefix}:${digest}:${start - this.algorithm.windowMs}`,
			];
		}
		return [`${this.prefix}:bucket:${digest}`];
	}

	private response(
		allowed: boolean,
		limit: number,
		remaining: number,
		reset: number,
		reason: RateLimitReason,
	): RateLimitResponse {
		return {
			success: allowed,
			allowed,
			limit,
			remaining,
			reset,
			resetAt: new Date(reset),
			retryAfterMs: Math.max(0, reset - this.now()),
			reason,
			pending: Promise.resolve(),
		};
	}
}

const FIXED_WINDOW_SCRIPT = `local current = redis.call("INCRBY", KEYS[1], ARGV[1]) if current == tonumber(ARGV[1]) then redis.call("PEXPIRE", KEYS[1], ARGV[2]) end return current`;
const SLIDING_WINDOW_SCRIPT = `local current = redis.call("INCRBY", KEYS[1], ARGV[1]) if current == tonumber(ARGV[1]) then redis.call("PEXPIRE", KEYS[1], ARGV[2]) redis.call("PEXPIRE", KEYS[2], ARGV[2]) end local previous = tonumber(redis.call("GET", KEYS[2]) or "0") local weighted = previous * ((tonumber(ARGV[2]) - tonumber(ARGV[3])) / tonumber(ARGV[2])) + current return {current, weighted}`;
const TOKEN_BUCKET_SCRIPT = `local state = redis.call("HMGET", KEYS[1], "tokens", "updated") local tokens = tonumber(state[1]) or tonumber(ARGV[1]) local updated = tonumber(state[2]) or tonumber(ARGV[5]) local elapsed = math.max(0, tonumber(ARGV[5]) - updated) local refill = elapsed * tonumber(ARGV[2]) / tonumber(ARGV[3]) tokens = math.min(tonumber(ARGV[1]), tokens + refill) local requested = tonumber(ARGV[4]) local allowed = 0 if tokens >= requested then tokens = tokens - requested allowed = 1 end local reset = tonumber(ARGV[5]) + math.ceil((requested - tokens) * tonumber(ARGV[3]) / tonumber(ARGV[2])) redis.call("HSET", KEYS[1], "tokens", tokens, "updated", ARGV[5]) redis.call("PEXPIRE", KEYS[1], ARGV[3]) return {allowed, tokens, reset}`;

function assertIdentifier(identifier: string): void {
	if (!identifier.trim() || identifier.length > 512)
		throw new RedisConfigurationError(
			"Rate-limit identifier must be between one and 512 characters",
		);
}

function assertAlgorithmValues(limit: number, windowMs: number): void {
	if (!Number.isSafeInteger(limit) || limit <= 0)
		throw new RedisConfigurationError("Rate-limit limit must be positive");
	if (!Number.isSafeInteger(windowMs) || windowMs <= 0)
		throw new RedisConfigurationError("Rate-limit window must be positive");
}

function parseInteger(value: unknown, name: string): number {
	const result = Number(value);
	if (!Number.isSafeInteger(result))
		throw new RedisProtocolError(`Invalid ${name} response`);
	return result;
}

function isAvailabilityFailure(error: unknown): boolean {
	return (
		error instanceof RedisTimeoutError ||
		error instanceof RedisUnavailableError ||
		error instanceof RedisConnectionError ||
		error instanceof RedisClientStateError
	);
}
