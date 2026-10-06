import { describe, expect, it } from "vitest";
import * as cache from "../src/cache/index.js";
import * as root from "../src/index.js";
import * as locks from "../src/locks/index.js";
import * as queue from "../src/queue/index.js";
import * as rateLimit from "../src/rate-limit/index.js";

describe("public entrypoints", () => {
	it("export stable module factories", () => {
		expect(root.createRedisClient).toBeTypeOf("function");
		expect(rateLimit.fixedWindow).toBeTypeOf("function");
		expect(cache.createCache).toBeTypeOf("function");
		expect(cache.createRedisCache).toBe(cache.createCache);
		expect(locks.createLockManager).toBeTypeOf("function");
		expect(locks.createRedisLockManager).toBe(locks.createLockManager);
		expect(queue.createQueue).toBeTypeOf("function");
		expect(queue.createRedisQueue).toBe(queue.createQueue);
		expect(rateLimit.createRedisRateLimiter).toBe(rateLimit.createRateLimiter);
	});
});
