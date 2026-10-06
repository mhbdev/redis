import { jsonCodec, type RedisCodec } from "../core/codec.js";
import { RedisQueueError } from "../core/errors.js";
import type { RedisExecutor } from "../core/executor.js";

export type QueueJob<T> = Readonly<{
	id: string;
	data: T;
	attempts: number;
	enqueuedAt: number;
}>;

export type EnqueueOptions = Readonly<{ delayMs?: number; jobId?: string }>;
export type QueueOptions<T> = Readonly<{
	redis: RedisExecutor;
	/** JSON is used by default. Provide a codec for binary or schema-validated jobs. */
	codec?: RedisCodec<T>;
	name: string;
	prefix?: string;
	maxAttempts?: number;
	visibilityTimeoutMs?: number;
}>;

export type QueueWorkerOptions = Readonly<{
	group?: string;
	consumer?: string;
	blockMs?: number;
	concurrency?: number;
}>;

export interface JobQueue<T> {
	enqueue(data: T, options?: EnqueueOptions): Promise<string>;
	worker(
		handler: (job: QueueJob<T>) => Promise<void>,
		options?: QueueWorkerOptions,
	): QueueWorker;
	close?(): Promise<void>;
}

export interface QueueWorker {
	start(): Promise<void>;
	close(): Promise<void>;
}

export class RedisQueue<T> implements JobQueue<T> {
	private readonly prefix: string;
	private readonly valueCodec: RedisCodec<T>;
	private readonly maxAttempts: number;
	private readonly visibilityTimeoutMs: number;

	constructor(private readonly options: QueueOptions<T>) {
		if (!options.name.trim()) throw new RedisQueueError("Queue name is required");
		this.prefix = options.prefix ?? "mhbdev:queue";
		this.valueCodec = options.codec ?? jsonCodec<T>();
		this.maxAttempts = options.maxAttempts ?? 3;
		this.visibilityTimeoutMs = options.visibilityTimeoutMs ?? 30_000;
		if (!Number.isSafeInteger(this.maxAttempts) || this.maxAttempts <= 0)
			throw new RedisQueueError("Queue max attempts must be positive");
	}

	async enqueue(data: T, enqueueOptions: EnqueueOptions = {}): Promise<string> {
		const id = enqueueOptions.jobId ?? crypto.randomUUID();
		const job: QueueJob<T> = { id, data, attempts: 0, enqueuedAt: Date.now() };
		const encoded = this.encode(job);
		if ((enqueueOptions.delayMs ?? 0) > 0) {
			await this.options.redis.execute([
				"ZADD",
				this.delayedKey,
				String(Date.now() + (enqueueOptions.delayMs ?? 0)),
				encoded,
			]);
		} else {
			await this.options.redis.execute(["XADD", this.streamKey, "*", "payload", encoded]);
		}
		return id;
	}

	worker(
		handler: (job: QueueJob<T>) => Promise<void>,
		workerOptions: QueueWorkerOptions = {},
	): RedisQueueWorker<T> {
		return new RedisQueueWorker(this, handler, workerOptions);
	}

	get streamKey(): string {
		return `${this.prefix}:${this.options.name}:stream`;
	}

	get delayedKey(): string {
		return `${this.prefix}:${this.options.name}:delayed`;
	}

	get deadLetterKey(): string {
		return `${this.prefix}:${this.options.name}:dead-letter`;
	}

	get codec(): RedisCodec<T> {
		return this.valueCodec;
	}

	get redis(): RedisExecutor {
		return this.options.redis;
	}

	get attempts(): number {
		return this.maxAttempts;
	}

	get visibilityTimeout(): number {
		return this.visibilityTimeoutMs;
	}

	private encode(job: QueueJob<T>): string {
		return JSON.stringify({
			id: job.id,
			data: this.valueCodec.encode(job.data),
			attempts: job.attempts,
			enqueuedAt: job.enqueuedAt,
		});
	}

	decode(value: string): QueueJob<T> {
		try {
			const parsed = JSON.parse(value) as {
				id: string;
				data: string;
				attempts: number;
				enqueuedAt: number;
			};
			return {
				id: parsed.id,
				data: this.valueCodec.decode(parsed.data),
				attempts: parsed.attempts,
				enqueuedAt: parsed.enqueuedAt,
			};
		} catch (error) {
			throw new RedisQueueError("Unable to decode Redis queue job", error);
		}
	}
}

export class RedisQueueWorker<T> implements QueueWorker {
	private readonly group: string;
	private readonly consumer: string;
	private readonly blockMs: number;
	private readonly concurrency: number;
	private stopping = false;
	private running: Promise<void> | null = null;

	constructor(
		private readonly queue: RedisQueue<T>,
		private readonly handler: (job: QueueJob<T>) => Promise<void>,
		options: QueueWorkerOptions,
	) {
		this.group = options.group ?? "default";
		this.consumer = options.consumer ?? crypto.randomUUID();
		this.blockMs = options.blockMs ?? 1_000;
		this.concurrency = options.concurrency ?? 1;
		if (!Number.isSafeInteger(this.concurrency) || this.concurrency <= 0)
			throw new RedisQueueError("Queue concurrency must be positive");
	}

	async start(): Promise<void> {
		if (this.running) return this.running;
		this.stopping = false;
		this.running = this.run().finally(() => {
			this.running = null;
		});
		return this.running;
	}

	async close(): Promise<void> {
		this.stopping = true;
		await this.running;
	}

	private async run(): Promise<void> {
		await this.ensureGroup();
		while (!this.stopping) {
			await this.promoteDue();
			const recovered = await this.recoverPending();
			if (recovered.length) {
				await Promise.all(recovered.map((entry) => this.process(entry)));
				continue;
			}
			const response = await this.queue.redis.execute<unknown>([
				"XREADGROUP",
				"GROUP",
				this.group,
				this.consumer,
				"COUNT",
				String(this.concurrency),
				"BLOCK",
				String(this.blockMs),
				"STREAMS",
				this.queue.streamKey,
				">",
			]);
			const entries = parseStreamResponse(response);
			await Promise.all(entries.map((entry) => this.process(entry)));
		}
	}

	private async ensureGroup(): Promise<void> {
		await this.queue.redis.eval<number>(ENSURE_GROUP_SCRIPT, {
			keys: [this.queue.streamKey],
			arguments: [this.group],
		});
	}

	private async promoteDue(): Promise<void> {
		const due = await this.queue.redis.execute<string[]>([
			"ZRANGEBYSCORE",
			this.queue.delayedKey,
			"-inf",
			String(Date.now()),
			"LIMIT",
			"0",
			"100",
		]);
		for (const encoded of due ?? []) {
			await this.queue.redis.execute(["ZREM", this.queue.delayedKey, encoded]);
			await this.queue.redis.execute([
				"XADD",
				this.queue.streamKey,
				"*",
				"payload",
				encoded,
			]);
		}
	}

	private async recoverPending(): Promise<ParsedEntry[]> {
		const response = await this.queue.redis.execute<unknown>([
			"XAUTOCLAIM",
			this.queue.streamKey,
			this.group,
			this.consumer,
			String(this.queue.visibilityTimeout),
			"0-0",
			"COUNT",
			String(this.concurrency),
		]);
		return parseAutoClaimResponse(response);
	}

	private async process(entry: ParsedEntry): Promise<void> {
		const job = this.queue.decode(entry.payload);
		try {
			await this.handler(job);
			await this.queue.redis.execute([
				"XACK",
				this.queue.streamKey,
				this.group,
				entry.id,
			]);
		} catch (_error) {
			await this.queue.redis.execute([
				"XACK",
				this.queue.streamKey,
				this.group,
				entry.id,
			]);
			if (job.attempts + 1 >= this.queue.attempts) {
				await this.queue.redis.execute([
					"XADD",
					this.queue.deadLetterKey,
					"*",
					"payload",
					this.encodeRetry(job),
				]);
				return;
			}
			const retry = { ...job, attempts: job.attempts + 1 };
			await this.queue.redis.execute([
				"XADD",
				this.queue.streamKey,
				"*",
				"payload",
				this.encodeRetry(retry),
			]);
		}
	}

	private encodeRetry(job: QueueJob<T>): string {
		return JSON.stringify({
			id: job.id,
			data: this.queue.codec.encode(job.data),
			attempts: job.attempts,
			enqueuedAt: job.enqueuedAt,
		});
	}
}

type ParsedEntry = Readonly<{ id: string; payload: string }>;

const ENSURE_GROUP_SCRIPT = `local groups = redis.pcall("XINFO", "GROUPS", KEYS[1]) if type(groups) == "table" then for _, group in ipairs(groups) do if group[2] == ARGV[1] then return 0 end end end local result = redis.pcall("XGROUP", "CREATE", KEYS[1], ARGV[1], "0-0", "MKSTREAM") if type(result) == "table" then return 1 end local groupsAfter = redis.pcall("XINFO", "GROUPS", KEYS[1]) if type(groupsAfter) == "table" then for _, group in ipairs(groupsAfter) do if group[2] == ARGV[1] then return 0 end end end return redis.error_reply("Redis consumer group creation failed")`;

function parseStreamResponse(value: unknown): ParsedEntry[] {
	if (!Array.isArray(value) || value.length === 0) return [];
	const streams = value[0];
	if (!Array.isArray(streams) || !Array.isArray(streams[1])) return [];
	const result: ParsedEntry[] = [];
	for (const rawEntry of streams[1]) {
		if (
			!Array.isArray(rawEntry) ||
			typeof rawEntry[0] !== "string" ||
			!Array.isArray(rawEntry[1])
		)
			continue;
		const fields = rawEntry[1] as unknown[];
		const payloadIndex = fields.indexOf("payload");
		const payload = payloadIndex >= 0 ? fields[payloadIndex + 1] : undefined;
		if (typeof payload === "string") result.push({ id: rawEntry[0], payload });
	}
	return result;
}

function parseAutoClaimResponse(value: unknown): ParsedEntry[] {
	if (!Array.isArray(value) || !Array.isArray(value[1])) return [];
	return parseStreamResponse([["recovered", value[1]]]);
}

export function createQueue<T>(options: QueueOptions<T>): RedisQueue<T> {
	return new RedisQueue(options);
}

/** A discoverable alias for applications that prefer Redis-prefixed factory names. */
export const createRedisQueue = createQueue;
