import { describe, expect, it } from "vitest";
import { RedisQueueError } from "../src/core/errors.js";
import type {
	RedisCommand,
	RedisExecutor,
	RedisScriptOptions,
} from "../src/core/executor.js";
import { createQueue } from "../src/queue/index.js";

class WorkerExecutor implements RedisExecutor {
	readonly commands: RedisCommand[] = [];
	readonly scripts: string[] = [];
	private readCount = 0;

	async execute<T = unknown>(command: RedisCommand): Promise<T> {
		this.commands.push(command);
		if (command[0] === "XAUTOCLAIM") return ["0-0", []] as T;
		if (command[0] === "XREADGROUP") {
			this.readCount += 1;
			if (this.readCount === 1) {
				return [
					[
						"queue:jobs:stream",
						[
							[
								"1-0",
								[
									"payload",
									JSON.stringify({
										id: "job-1",
										data: JSON.stringify({ value: "retry" }),
										attempts: 0,
										enqueuedAt: 1,
									}),
								],
							],
						],
					],
				] as T;
			}
			return [] as T;
		}
		return null as T;
	}

	async eval<T = unknown>(script: string, _options: RedisScriptOptions): Promise<T> {
		this.scripts.push(script);
		return 1 as T;
	}

	stopAfterNextRead(stop: () => void): void {
		const originalExecute = this.execute.bind(this);
		this.execute = async <T>(command: RedisCommand): Promise<T> => {
			const result = await originalExecute<T>(command);
			if (command[0] === "XREADGROUP" && this.readCount === 2) stop();
			return result;
		};
	}
}

describe("RedisQueue", () => {
	it("validates delayed enqueue values and worker blocking time", async () => {
		const queue = createQueue({ redis: new WorkerExecutor(), name: "jobs" });
		await expect(queue.enqueue("payload", { delayMs: -1 })).rejects.toBeInstanceOf(
			RedisQueueError,
		);
		expect(() => queue.worker(async () => undefined, { blockMs: 0 })).toThrow(
			RedisQueueError,
		);
	});

	it("promotes delayed entries atomically and enqueues retries before acknowledging", async () => {
		const redis = new WorkerExecutor();
		const queue = createQueue({ redis, name: "jobs", maxAttempts: 3 });
		let worker!: ReturnType<typeof queue.worker>;
		worker = queue.worker(async () => {
			throw new Error("retry me");
		});
		redis.stopAfterNextRead(() => void worker.close());

		await worker.start();

		const promotion = redis.scripts.find((script) => script.includes("ZRANGEBYSCORE"));
		const retry = redis.scripts.find((script) => script.includes("XACK"));
		if (!promotion || !retry) throw new Error("Expected queue scripts to be executed");
		expect(promotion.indexOf("XADD")).toBeLessThan(promotion.indexOf("ZREM"));
		expect(retry.indexOf("XADD")).toBeLessThan(retry.indexOf("XACK"));
		expect(redis.commands.some((command) => command[0] === "XACK")).toBe(false);
	});
});
