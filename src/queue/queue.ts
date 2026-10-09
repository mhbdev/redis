import { jsonCodec, type RedisCodec } from "../core/codec.js";
import { asError, RedisQueueError } from "../core/errors.js";
import type { RedisExecutor } from "../core/executor.js";
import { notifyError } from "../core/hooks.js";

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
	hooks?: QueueWorkerHooks;
}>;

export type QueueJobContext = Readonly<{
	id: string;
	attempts: number;
	enqueuedAt: number;
}>;

/** Lifecycle hooks receive job metadata without the potentially sensitive payload. */
export type QueueWorkerHooks = Readonly<{
	onJobStart?: (job: QueueJobContext) => void;
	onJobComplete?: (job: QueueJobContext) => void;
	onJobFailure?: (job: QueueJobContext, error: Error) => void;
	onDeadLetter?: (job: QueueJobContext, error?: Error) => void;
	onError?: (error: Error) => void;
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
		if (!options.name.trim() || options.name.includes("\n"))
			throw new RedisQueueError("Queue name is invalid");
		this.prefix = options.prefix ?? "mhbdev:queue";
		this.valueCodec = options.codec ?? jsonCodec<T>();
		this.maxAttempts = options.maxAttempts ?? 3;
		this.visibilityTimeoutMs = options.visibilityTimeoutMs ?? 30_000;
		if (!this.prefix.trim() || this.prefix.includes("\n"))
			throw new RedisQueueError("Queue prefix is invalid");
		if (!Number.isSafeInteger(this.maxAttempts) || this.maxAttempts <= 0)
			throw new RedisQueueError("Queue max attempts must be positive");
		if (!Number.isSafeInteger(this.visibilityTimeoutMs) || this.visibilityTimeoutMs <= 0)
			throw new RedisQueueError("Queue visibility timeout must be positive");
	}

	async enqueue(data: T, enqueueOptions: EnqueueOptions = {}): Promise<string> {
		const delayMs = enqueueOptions.delayMs ?? 0;
		if (!Number.isSafeInteger(delayMs) || delayMs < 0)
			throw new RedisQueueError("Queue delay must be a non-negative integer");
		if (!Number.isSafeInteger(Date.now() + delayMs))
			throw new RedisQueueError("Queue delivery time is outside the supported range");
		if (enqueueOptions.jobId !== undefined && !enqueueOptions.jobId.trim())
			throw new RedisQueueError("Queue job ID is required when provided");
		const id = enqueueOptions.jobId ?? crypto.randomUUID();
		const job: QueueJob<T> = { id, data, attempts: 0, enqueuedAt: Date.now() };
		const encoded = this.encode(job);
		if (delayMs > 0) {
			await this.options.redis.execute([
				"ZADD",
				this.delayedKey,
				String(Date.now() + delayMs),
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
			if (
				typeof parsed.id !== "string" ||
				!parsed.id.trim() ||
				typeof parsed.data !== "string" ||
				!Number.isSafeInteger(parsed.attempts) ||
				parsed.attempts < 0 ||
				!Number.isSafeInteger(parsed.enqueuedAt) ||
				parsed.enqueuedAt < 0
			)
				throw new TypeError("Queue job payload has an invalid shape");
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
	private readonly hooks?: QueueWorkerHooks;
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
		this.hooks = options.hooks;
		if (!this.group.trim() || !this.consumer.trim())
			throw new RedisQueueError("Queue consumer group and consumer are required");
		if (!Number.isSafeInteger(this.blockMs) || this.blockMs <= 0)
			throw new RedisQueueError("Queue block time must be a positive integer");
		if (!Number.isSafeInteger(this.concurrency) || this.concurrency <= 0)
			throw new RedisQueueError("Queue concurrency must be positive");
	}

	async start(): Promise<void> {
		if (this.running) return this.running;
		this.stopping = false;
		this.running = this.run()
			.catch((error: unknown) => {
				this.reportError(error);
				throw error;
			})
			.finally(() => {
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
		await this.queue.redis.eval(PROMOTE_DUE_SCRIPT, {
			keys: [this.queue.delayedKey, this.queue.streamKey],
			arguments: [String(Date.now()), "100"],
		});
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
		let job: QueueJob<T>;
		try {
			job = this.queue.decode(entry.payload);
		} catch (error) {
			this.reportError(error);
			await this.queue.redis.eval(REQUEUE_FAILED_SCRIPT, {
				keys: [this.queue.streamKey, this.queue.deadLetterKey],
				arguments: [this.group, entry.id, entry.payload],
			});
			return;
		}
		const context = toJobContext(job);
		this.notifyJobStart(context);
		try {
			await this.handler(job);
		} catch (error) {
			this.notifyJobFailure(context, error);
			const retry = { ...job, attempts: job.attempts + 1 };
			const target =
				retry.attempts >= this.queue.attempts
					? this.queue.deadLetterKey
					: this.queue.streamKey;
			await this.queue.redis.eval(REQUEUE_FAILED_SCRIPT, {
				keys: [this.queue.streamKey, target],
				arguments: [this.group, entry.id, this.encodeRetry(retry)],
			});
			if (target === this.queue.deadLetterKey)
				this.notifyDeadLetter(toJobContext(retry), error);
			return;
		}
		await this.queue.redis.execute(["XACK", this.queue.streamKey, this.group, entry.id]);
		this.notifyJobComplete(context);
	}

	private notifyJobStart(job: QueueJobContext): void {
		try {
			this.hooks?.onJobStart?.(job);
		} catch (error) {
			this.reportError(error);
		}
	}

	private notifyJobComplete(job: QueueJobContext): void {
		try {
			this.hooks?.onJobComplete?.(job);
		} catch (error) {
			this.reportError(error);
		}
	}

	private notifyJobFailure(job: QueueJobContext, error: unknown): void {
		const failure = asError(error);
		try {
			this.hooks?.onJobFailure?.(job, failure);
		} catch (hookError) {
			this.reportError(hookError);
		}
	}

	private notifyDeadLetter(job: QueueJobContext, error?: unknown): void {
		try {
			this.hooks?.onDeadLetter?.(job, error === undefined ? undefined : asError(error));
		} catch (hookError) {
			this.reportError(hookError);
		}
	}

	private reportError(error: unknown): void {
		notifyError(this.hooks?.onError, error);
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
const PROMOTE_DUE_SCRIPT = `local due = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", ARGV[1], "LIMIT", 0, ARGV[2]) for _, payload in ipairs(due) do redis.call("XADD", KEYS[2], "*", "payload", payload) redis.call("ZREM", KEYS[1], payload) end return #due`;
const REQUEUE_FAILED_SCRIPT = `redis.call("XADD", KEYS[2], "*", "payload", ARGV[3]) redis.call("XACK", KEYS[1], ARGV[1], ARGV[2]) return 1`;

function parseStreamResponse(value: unknown): ParsedEntry[] {
	const result: ParsedEntry[] = [];
	const streams = toPairs(value);
	for (const [streamName, rawEntries] of streams) {
		if (keyString(streamName) === undefined) continue;
		for (const rawEntry of toEntries(rawEntries)) {
			const [id, rawFields] = toEntry(rawEntry);
			if (!id) continue;
			const payload = getField(rawFields, "payload");
			if (typeof payload === "string") result.push({ id, payload });
		}
	}
	return result;
}

function parseAutoClaimResponse(value: unknown): ParsedEntry[] {
	if (Array.isArray(value) && Array.isArray(value[1]))
		return parseStreamResponse([["recovered", value[1]]]);
	if (isRecord(value) && Array.isArray(value.messages))
		return parseStreamResponse([["recovered", value.messages]]);
	return [];
}

function toPairs(value: unknown): Array<[unknown, unknown]> {
	if (value instanceof Map) return [...value.entries()];
	if (Array.isArray(value)) {
		if (value.length >= 2 && keyString(value[0]) !== undefined) {
			const pairs: Array<[unknown, unknown]> = [];
			for (let index = 0; index + 1 < value.length; index += 2)
				pairs.push([value[index], value[index + 1]]);
			return pairs;
		}
		return value.flatMap((entry) => {
			if (Array.isArray(entry) && entry.length >= 2) return [[entry[0], entry[1]]];
			if (isRecord(entry)) {
				const streamName = entry.name ?? entry.stream;
				const entries = entry.messages ?? entry.entries;
				if (streamName !== undefined && entries !== undefined)
					return [[streamName, entries]];
			}
			return [];
		});
	}
	if (isRecord(value)) {
		if (Array.isArray(value.streams)) return toPairs(value.streams);
		if (Array.isArray(value.messages))
			return [[value.stream ?? "stream", value.messages]];
		return Object.entries(value);
	}
	return [];
}

function toEntries(value: unknown): unknown[] {
	if (Array.isArray(value)) {
		if (
			value.length >= 2 &&
			keyString(value[0]) !== undefined &&
			!Array.isArray(value[0])
		) {
			const entries: unknown[] = [];
			for (let index = 0; index + 1 < value.length; index += 2)
				entries.push([value[index], value[index + 1]]);
			return entries;
		}
		return value;
	}
	if (value instanceof Map) return [...value.entries()];
	if (isRecord(value)) {
		if (Array.isArray(value.messages)) return value.messages;
		if (Array.isArray(value.entries)) return value.entries;
	}
	return [];
}

function toEntry(value: unknown): [string | undefined, unknown] {
	if (Array.isArray(value)) return [keyString(value[0]), value[1]];
	if (value instanceof Map) {
		const [id, fields] = value.entries().next().value ?? [];
		return [keyString(id), fields];
	}
	if (isRecord(value)) {
		const id = value.id;
		return [keyString(id), value.message ?? value.fields];
	}
	return [undefined, undefined];
}

function getField(fields: unknown, name: string): unknown {
	if (fields instanceof Map) {
		for (const [key, value] of fields) {
			if (keyString(key) === name) return value;
		}
		return undefined;
	}
	if (Array.isArray(fields)) {
		const index = fields.findIndex((key) => keyString(key) === name);
		return index < 0 ? undefined : fields[index + 1];
	}
	return isRecord(fields) ? fields[name] : undefined;
}

function keyString(value: unknown): string | undefined {
	if (typeof value === "string") return value;
	if (value instanceof Uint8Array) return new TextDecoder().decode(value);
	return undefined;
}

function isRecord(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}

function toJobContext<T>(job: QueueJob<T>): QueueJobContext {
	return { id: job.id, attempts: job.attempts, enqueuedAt: job.enqueuedAt };
}

export function createQueue<T>(options: QueueOptions<T>): RedisQueue<T> {
	return new RedisQueue(options);
}

/** A discoverable alias for applications that prefer Redis-prefixed factory names. */
export const createRedisQueue = createQueue;
