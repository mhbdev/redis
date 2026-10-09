import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { createCache } from "../../src/cache/index.js";
import {
	createRedisClient,
	createRedisPool,
	jsonCodec,
	RedisRequestAbortedError,
} from "../../src/index.js";
import { createLockManager } from "../../src/locks/index.js";
import { createNodeTypedPubSub } from "../../src/node/index.js";
import { createQueue } from "../../src/queue/index.js";
import {
	createRateLimiter,
	fixedWindow,
	tokenBucket,
} from "../../src/rate-limit/index.js";

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
		const abortController = new AbortController();
		const blockedRead = client.execute(["BLPOP", `${prefix}:missing`, "0"], {
			signal: abortController.signal,
		});
		abortController.abort();
		await expect(blockedRead).rejects.toBeInstanceOf(RedisRequestAbortedError);
		const pool = createRedisPool({
			url: process.env.REDIS_URL ?? "redis://127.0.0.1:6379",
			minimum: 1,
			maximum: 2,
		});
		await Promise.all([pool.connect(), pool.connect()]);
		expect(pool.isReady).toBe(true);
		expect(await pool.execute(["PING"])).toBe("PONG");
		const poolAbortController = new AbortController();
		const poolRead = pool.execute(["BLPOP", `${prefix}:pool-missing`, "0"], {
			signal: poolAbortController.signal,
		});
		poolAbortController.abort();
		await expect(poolRead).rejects.toBeInstanceOf(RedisRequestAbortedError);
		await pool.close();
		expect(pool.isReady).toBe(false);
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

	it("returns correct token-bucket refill deadlines", async () => {
		const limiter = createRateLimiter(tokenBucket(1, 1_000, 2), {
			redis: client,
			prefix: `${prefix}:bucket`,
			ephemeralCache: false,
			now: () => 1_000,
		});

		const first = await limiter.limit("id");
		const second = await limiter.limit("id");
		const blocked = await limiter.limit("id");

		expect(first.reset).toBe(1_000);
		expect(second.reset).toBe(2_000);
		expect(blocked.retryAfterMs).toBe(1_000);
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

	it("promotes delayed jobs and retries failed jobs to the dead-letter stream", async () => {
		const delayedQueue = createQueue({
			redis: client,
			codec: jsonCodec<{ value: string }>(),
			name: `${prefix}:delayed-jobs`,
		});
		await delayedQueue.enqueue({ value: "delayed" }, { delayMs: 20 });
		let delayedWorker!: ReturnType<typeof delayedQueue.worker>;
		const delayedResult = new Promise<unknown>((resolve) => {
			delayedWorker = delayedQueue.worker(async (job) => resolve(job.data), {
				blockMs: 25,
			});
		});
		const delayedRunning = delayedWorker.start();
		try {
			expect(await withDeadline(delayedResult, 3_000)).toEqual({ value: "delayed" });
		} finally {
			await delayedWorker.close();
			await delayedRunning;
		}

		const retryQueue = createQueue({
			redis: client,
			codec: jsonCodec<{ value: string }>(),
			name: `${prefix}:retry-jobs`,
			maxAttempts: 2,
			visibilityTimeoutMs: 50,
		});
		await retryQueue.enqueue({ value: "fails" });
		let attempts = 0;
		let retryWorker!: ReturnType<typeof retryQueue.worker>;
		retryWorker = retryQueue.worker(
			async () => {
				attempts += 1;
				throw new Error("expected handler failure");
			},
			{ blockMs: 25 },
		);
		const running = retryWorker.start();
		try {
			await waitForDeadLetter(client, retryQueue.deadLetterKey, 3_000);
			expect(attempts).toBe(2);
		} finally {
			await retryWorker.close();
			await running;
		}
	});
});

async function waitForDeadLetter(
	redis: ReturnType<typeof createRedisClient>,
	key: string,
	timeoutMs: number,
): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	while ((await redis.execute<number>(["XLEN", key])) === 0) {
		if (Date.now() >= deadline)
			throw new Error("Timed out waiting for a dead-letter job");
		await new Promise((resolve) => setTimeout(resolve, 20));
	}
}

async function withDeadline<T>(promise: Promise<T>, timeoutMs: number): Promise<T> {
	let timer: ReturnType<typeof setTimeout> | undefined;
	try {
		return await Promise.race([
			promise,
			new Promise<T>((_, reject) => {
				timer = setTimeout(
					() => reject(new Error("Integration assertion timed out")),
					timeoutMs,
				);
			}),
		]);
	} finally {
		if (timer) clearTimeout(timer);
	}
}
