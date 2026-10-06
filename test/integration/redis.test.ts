import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { createCache } from "../../src/cache/index.js";
import { createRedisClient, createRedisPool, jsonCodec } from "../../src/index.js";
import { createLockManager } from "../../src/locks/index.js";
import { createNodeTypedPubSub } from "../../src/node/index.js";
import { createQueue } from "../../src/queue/index.js";
import { createRateLimiter, fixedWindow } from "../../src/rate-limit/index.js";

const enabled = Boolean(process.env.REDIS_URL);
const describeRedis = describe.skipIf(!enabled);
const prefix = `integration:${Date.now()}`;
let client: ReturnType<typeof createRedisClient>;

describeRedis("Redis integration", () => {
	beforeAll(async () => {
		client = createRedisClient({
			url: process.env.REDIS_URL ?? "redis://127.0.0.1:6379",
		});
		await client.connect();
	});

	afterAll(async () => {
		await client?.close();
	});

	it("executes commands and uses the pool", async () => {
		await client.execute(["SET", `${prefix}:key`, "value"]);
		expect(await client.execute(["GET", `${prefix}:key`])).toBe("value");
		const pool = createRedisPool({
			url: process.env.REDIS_URL ?? "redis://127.0.0.1:6379",
			minimum: 1,
			maximum: 2,
		});
		expect(await pool.execute(["PING"])).toBe("PONG");
		await pool.close();
	});

	it("runs atomic rate limits, cache, and locks", async () => {
		const limiter = createRateLimiter(fixedWindow(2, 60_000), {
			redis: client,
			prefix: `${prefix}:limit`,
			ephemeralCache: false,
		});
		expect((await limiter.limit("id")).success).toBe(true);
		expect((await limiter.limit("id")).success).toBe(true);
		expect((await limiter.limit("id")).success).toBe(false);

		const cache = createCache({
			redis: client,
			codec: jsonCodec<{ value: number }>(),
			prefix: `${prefix}:cache`,
		});
		await cache.set("item", { value: 42 }, { ttlMs: 10_000 });
		expect(await cache.get("item")).toEqual({ value: 42 });

		const locks = createLockManager({ redis: client, prefix: `${prefix}:lock` });
		const lease = await locks.acquire("item");
		expect(lease).not.toBeNull();
		expect(await lease?.release()).toBe(true);
	});

	it("delivers typed pub/sub messages and queue jobs", async () => {
		const url = process.env.REDIS_URL ?? "redis://127.0.0.1:6379";
		const pubsub = createNodeTypedPubSub({
			url,
			codec: jsonCodec<{ value: string }>(),
		});
		const received = new Promise<{ value: string }>((resolve) => {
			void pubsub.subscribe(`${prefix}:events`, resolve);
		});
		await new Promise((resolve) => setTimeout(resolve, 100));
		await pubsub.publish(`${prefix}:events`, { value: "published" });
		expect(await received).toEqual({ value: "published" });
		await pubsub.close();

		const queue = createQueue({
			redis: client,
			codec: jsonCodec<{ value: string }>(),
			name: `${prefix}:jobs`,
			visibilityTimeoutMs: 100,
		});
		await queue.enqueue({ value: "queued" });
		let worker!: ReturnType<typeof queue.worker>;
		const processed = new Promise<unknown>((resolve) => {
			worker = queue.worker(async (job) => resolve(job.data), { blockMs: 50 });
		});
		const running = worker.start();
		expect(await processed).toEqual({ value: "queued" });
		await worker.close();
		await running;
	});
});
