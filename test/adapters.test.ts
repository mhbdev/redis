import { AbortError, ErrorReply } from "redis";
import { describe, expect, it } from "vitest";

import { createBullMqQueueAdapter } from "../src/bullmq/index.js";
import { jsonCodec } from "../src/core/codec.js";
import { RedisProtocolError, RedisRequestAbortedError } from "../src/core/errors.js";
import {
	createIoredisExecutor,
	createIoredisPubSubTransport,
} from "../src/ioredis/index.js";
import { normalizeNodeRedisError } from "../src/node/errors.js";

describe("adapter boundaries", () => {
	it("maps known node-redis error classes without inspecting messages", () => {
		expect(normalizeNodeRedisError(new ErrorReply())).toBeInstanceOf(RedisProtocolError);
		expect(normalizeNodeRedisError(new AbortError())).toBeInstanceOf(
			RedisRequestAbortedError,
		);
	});

	it("adapts an existing ioredis client to the common executor", async () => {
		const calls: string[][] = [];
		const executor = createIoredisExecutor({
			status: "ready",
			call: async (command, ...args) => {
				calls.push([command, ...args]);
				return "OK";
			},
			eval: async () => 1,
			ping: async () => "PONG",
		});

		expect(await executor.execute(["SET", "key", "value"])).toBe("OK");
		expect(calls).toEqual([["SET", "key", "value"]]);
	});

	it("allows typed ioredis error normalization without message inspection", async () => {
		const protocolError = new RedisProtocolError("known reply type");
		const executor = createIoredisExecutor(
			{
				status: "ready",
				call: async () => {
					throw new Error("reply details");
				},
				eval: async () => 1,
				ping: async () => "PONG",
			},
			{ normalizeError: () => protocolError },
		);

		await expect(executor.execute(["GET", "key"])).rejects.toBe(protocolError);
	});

	it("honors abort signals for in-flight ioredis commands", async () => {
		const executor = createIoredisExecutor({
			status: "ready",
			call: () => new Promise(() => undefined),
			eval: async () => 1,
			ping: async () => "PONG",
		});
		const controller = new AbortController();
		const operation = executor.execute(["BLPOP", "queue", "0"], {
			signal: controller.signal,
		});
		controller.abort();

		await expect(operation).rejects.toBeInstanceOf(RedisRequestAbortedError);
	});

	it("adapts BullMQ queue operations to the common typed queue shape", async () => {
		let added: unknown;
		let processed = false;
		let runProcessor: (() => Promise<void>) | undefined;
		const adapter = createBullMqQueueAdapter(
			{
				add: async (name, data, options) => {
					added = { name, data, options };
					return { id: "job-1" };
				},
			},
			{
				codec: jsonCodec<{ value: string }>(),
				jobName: "events",
				workerFactory: (_name, processor) => {
					runProcessor = async () => {
						processed = true;
						await processor({ id: "job-1", data: JSON.stringify({ value: "ok" }) });
					};
					return { close: async () => undefined };
				},
			},
		);

		expect(await adapter.enqueue({ value: "ok" }, { delayMs: 10 })).toBe("job-1");
		expect(added).toMatchObject({ name: "events", options: { delay: 10 } });
		const worker = adapter.worker(async (job) => {
			expect(job.data).toEqual({ value: "ok" });
		});
		await runProcessor?.();
		await worker.close();
		expect(processed).toBe(true);
	});

	it("closes only the ioredis subscriber it creates", async () => {
		let publisherClosed = false;
		let subscriberClosed = false;
		const publisher = {
			publish: async () => 1,
			subscribe: async () => undefined,
			unsubscribe: async () => undefined,
			on() {
				return this;
			},
			duplicate: () => ({
				publish: async () => 1,
				subscribe: async () => undefined,
				unsubscribe: async () => undefined,
				on() {
					return this;
				},
				off() {
					return this;
				},
				quit: async () => {
					subscriberClosed = true;
				},
			}),
			quit: async () => {
				publisherClosed = true;
			},
		};
		const transport = createIoredisPubSubTransport({ publisher });

		await transport.close();
		expect(subscriberClosed).toBe(true);
		expect(publisherClosed).toBe(false);
	});
});
