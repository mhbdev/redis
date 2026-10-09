import { describe, expect, it } from "vitest";

import { createCache } from "../src/cache/index.js";
import { jsonCodec } from "../src/core/codec.js";
import { RedisConfigurationError, RedisSerializationError } from "../src/core/errors.js";
import type { RedisCommand, RedisExecutor } from "../src/index.js";
import { createLockManager } from "../src/locks/index.js";

class MemoryRedis implements RedisExecutor {
	readonly values = new Map<string, string>();
	readonly commands: RedisCommand[] = [];

	async execute<T = unknown>(command: RedisCommand): Promise<T> {
		this.commands.push(command);
		const [name, key, value] = command;
		if (name === "GET") return (this.values.get(key ?? "") ?? null) as T;
		if (name === "SET" && key && value) {
			this.values.set(key, value);
			return "OK" as T;
		}
		if (name === "DEL" && key) return (this.values.delete(key) ? 1 : 0) as T;
		if (name === "EXISTS" && key) return (this.values.has(key) ? 1 : 0) as T;
		return 0 as T;
	}

	async eval<T = unknown>(_script: string): Promise<T> {
		return 1 as T;
	}
}

describe("cache and locks", () => {
	it("uses JSON as the default cache codec", async () => {
		const redis = new MemoryRedis();
		const cache = createCache<{ answer: number }>({ redis });

		await cache.set("default", { answer: 42 });
		expect(await cache.get("default")).toEqual({ answer: 42 });
	});

	it("rejects values that JSON cannot represent", () => {
		expect(() => jsonCodec<undefined>().encode(undefined)).toThrow(
			RedisSerializationError,
		);
	});

	it("rejects conflicting conditional cache writes", async () => {
		const cache = createCache({ redis: new MemoryRedis() });
		await expect(
			cache.set("key", "value", { onlyIfAbsent: true, onlyIfPresent: true }),
		).rejects.toBeInstanceOf(RedisConfigurationError);
	});

	it("serializes typed cache values and coalesces cache misses", async () => {
		const redis = new MemoryRedis();
		const cache = createCache({ redis, codec: jsonCodec<{ answer: number }>() });
		let calls = 0;
		const [first, second] = await Promise.all([
			cache.getOrSet("key", async () => {
				calls += 1;
				return { answer: 42 };
			}),
			cache.getOrSet("key", async () => {
				calls += 1;
				return { answer: 42 };
			}),
		]);
		expect(first).toEqual({ answer: 42 });
		expect(second).toEqual({ answer: 42 });
		expect(calls).toBe(1);
	});

	it("executes lock ownership scripts", async () => {
		const redis = new MemoryRedis();
		const manager = createLockManager({ redis });
		const lease = await manager.acquire("resource");
		expect(lease).not.toBeNull();
		expect(await lease?.release()).toBe(true);
	});

	it("updates lease expiry after a successful extension", async () => {
		const manager = createLockManager({ redis: new MemoryRedis() });
		const lease = await manager.acquire("resource", 1_000);
		if (!lease) throw new Error("Expected lock acquisition to succeed");
		const originalExpiry = lease.expiresAt;

		expect(await lease.extend(60_000)).toBe(true);
		expect(lease.expiresAt).toBeGreaterThan(originalExpiry);
	});
});
