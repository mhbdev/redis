import { describe, expect, it } from "vitest";
import { type RedisExecutor, RedisUnavailableError } from "../src/index.js";
import {
	fixedWindow,
	RedisRateLimiter,
	slidingWindow,
	tokenBucket,
} from "../src/rate-limit/index.js";

class ScriptExecutor implements RedisExecutor {
	readonly calls: Array<{ keys: readonly string[]; arguments: readonly string[] }> = [];
	readonly commands: Array<readonly string[]> = [];
	constructor(private readonly result: unknown) {}

	execute<T = unknown>(command: readonly string[]): Promise<T> {
		this.commands.push(command);
		return Promise.resolve(null as T);
	}

	eval<T = unknown>(
		_script: string,
		options: { keys?: readonly string[]; arguments?: readonly string[] },
	): Promise<T> {
		this.calls.push({ keys: options.keys ?? [], arguments: options.arguments ?? [] });
		return Promise.resolve(this.result as T);
	}
}

describe("RedisRateLimiter", () => {
	it("uses hashed identifiers and returns fixed-window quota", async () => {
		const redis = new ScriptExecutor(2);
		const limiter = new RedisRateLimiter(fixedWindow(5, 60_000), {
			redis,
			now: () => 1_000,
		});

		const result = await limiter.limit("user-1");

		expect(result).toMatchObject({
			success: true,
			allowed: true,
			limit: 5,
			remaining: 3,
		});
		expect(redis.calls[0]?.keys[0]).toMatch(/^mhbdev:rate-limit:[a-f0-9]{64}:0$/);
	});

	it("supports sliding-window and token-bucket algorithm factories", async () => {
		const sliding = new RedisRateLimiter(slidingWindow(10, 10_000), {
			redis: new ScriptExecutor([2, 3]),
			now: () => 1_000,
		});
		const bucket = new RedisRateLimiter(tokenBucket(5, 10_000, 10), {
			redis: new ScriptExecutor([1, 8, 2_000]),
			now: () => 1_000,
		});

		expect((await sliding.limit("id")).success).toBe(true);
		expect((await bucket.limit("id")).remaining).toBe(8);
	});

	it("resets sliding-window keys using the same hashed layout as the limiter", async () => {
		const redis = new ScriptExecutor([1, 1]);
		const limiter = new RedisRateLimiter(slidingWindow(10, 10_000), {
			redis,
			now: () => 1_000,
		});

		await limiter.reset("id");

		expect(redis.commands[0]?.[0]).toBe("DEL");
		expect(redis.commands[0]?.[1]).toMatch(/^mhbdev:rate-limit:[a-f0-9]{64}:0$/);
	});

	it("supports fail-open behavior", async () => {
		const redis: RedisExecutor = {
			execute: async () => {
				throw new RedisUnavailableError(new Error("offline"));
			},
			eval: async () => {
				throw new RedisUnavailableError(new Error("offline"));
			},
		};
		const limiter = new RedisRateLimiter(fixedWindow(1, 1_000), {
			redis,
			failureMode: "open",
		});
		expect((await limiter.limit("id")).reason).toBe("redis");
	});

	it("does not reuse a denial for a different request weight", async () => {
		const redis = new ScriptExecutor(6);
		const limiter = new RedisRateLimiter(fixedWindow(5, 10_000), {
			redis,
			ephemeralCache: true,
			now: () => 1_000,
		});

		await limiter.limit("id", { rate: 5 });
		const smallerRequest = await limiter.limit("id", { rate: 1 });

		expect(smallerRequest.reason).toBe("limit");
		expect(redis.calls).toHaveLength(2);
	});

	it("does not cache sliding-window denials across changing windows", async () => {
		const redis = new ScriptExecutor([2, 3]);
		const limiter = new RedisRateLimiter(slidingWindow(2, 10_000), {
			redis,
			ephemeralCache: true,
			now: () => 1_000,
		});

		await limiter.limit("id");
		const repeated = await limiter.limit("id");

		expect(repeated.reason).toBe("limit");
		expect(redis.calls).toHaveLength(2);
	});

	it("rejects token-bucket requests larger than capacity", async () => {
		const limiter = new RedisRateLimiter(tokenBucket(1, 1_000, 2), {
			redis: new ScriptExecutor([1, 0, 2_000]),
		});

		await expect(limiter.limit("id", { rate: 3 })).rejects.toThrow(
			"Request rate exceeds the token-bucket maximum",
		);
	});

	it("does not issue another request after blockUntilReady times out", async () => {
		const redis = new ScriptExecutor(2);
		const limiter = new RedisRateLimiter(fixedWindow(1, 60_000), {
			redis,
			now: () => 1_000,
		});

		const result = await limiter.blockUntilReady("id", 10);

		expect(result.allowed).toBe(false);
		expect(redis.calls).toHaveLength(1);
	});

	it("does not hide configuration or protocol failures in fail-open mode", async () => {
		const redis: RedisExecutor = {
			execute: async <T>() => null as T,
			eval: async () => {
				throw new Error("unexpected");
			},
		};
		const limiter = new RedisRateLimiter(fixedWindow(1, 1_000), {
			redis,
			failureMode: "open",
		});

		await expect(limiter.limit("id")).rejects.toThrow("unexpected");
	});
});
