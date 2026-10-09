import { jsonCodec, type RedisCodec } from "../core/codec.js";
import { RedisConfigurationError } from "../core/errors.js";
import type {
	EnqueueOptions,
	QueueJob,
	QueueWorker,
	QueueWorkerOptions,
} from "../queue/queue.js";

export interface BullMqQueueLike {
	add(
		name: string,
		data: unknown,
		options?: Readonly<{ delay?: number; jobId?: string; attempts?: number }>,
	): Promise<{ id?: string | number }>;
	close?(): Promise<void>;
}

export interface BullMqJobLike {
	id?: string | number;
	data: unknown;
	attemptsMade?: number;
	timestamp?: number;
}

export interface BullMqWorkerLike {
	close(): Promise<void>;
}

export type BullMqWorkerFactory = (
	name: string,
	processor: (job: BullMqJobLike) => Promise<void>,
	options?: QueueWorkerOptions,
) => BullMqWorkerLike;

export class BullMqQueueAdapter<T> {
	private readonly jobName: string;

	constructor(
		private readonly queue: BullMqQueueLike,
		private readonly options: Readonly<{
			codec?: RedisCodec<T>;
			jobName?: string;
			maxAttempts?: number;
			workerFactory?: BullMqWorkerFactory;
		}>,
	) {
		this.jobName = options.jobName ?? "job";
		this.codec = options.codec ?? jsonCodec<T>();
		if (!this.jobName.trim())
			throw new RedisConfigurationError("BullMQ job name is required");
		if (
			options.maxAttempts !== undefined &&
			(!Number.isSafeInteger(options.maxAttempts) || options.maxAttempts <= 0)
		)
			throw new RedisConfigurationError("BullMQ max attempts must be positive");
	}

	private readonly codec: RedisCodec<T>;

	async enqueue(data: T, enqueueOptions: EnqueueOptions = {}): Promise<string> {
		const delay = enqueueOptions.delayMs ?? 0;
		if (!Number.isSafeInteger(delay) || delay < 0)
			throw new RedisConfigurationError("BullMQ delay must be a non-negative integer");
		if (enqueueOptions.jobId !== undefined && !enqueueOptions.jobId.trim())
			throw new RedisConfigurationError("BullMQ job ID is required when provided");
		const job = await this.queue.add(this.jobName, this.codec.encode(data), {
			delay,
			jobId: enqueueOptions.jobId,
			attempts: this.options.maxAttempts,
		});
		return String(job.id ?? enqueueOptions.jobId ?? crypto.randomUUID());
	}

	worker(
		handler: (job: QueueJob<T>) => Promise<void>,
		workerOptions: QueueWorkerOptions = {},
	): BullMqWorkerAdapter {
		if (!this.options.workerFactory)
			throw new RedisConfigurationError(
				"A BullMQ workerFactory is required to create workers",
			);
		const worker = this.options.workerFactory(
			this.jobName,
			async (job) => handler(this.decodeJob(job)),
			workerOptions,
		);
		return new BullMqWorkerAdapter(worker);
	}

	async close(): Promise<void> {
		await this.queue.close?.();
	}

	private decodeJob(job: BullMqJobLike): QueueJob<T> {
		const encoded = typeof job.data === "string" ? job.data : JSON.stringify(job.data);
		return {
			id: String(job.id ?? crypto.randomUUID()),
			data: this.codec.decode(encoded),
			attempts: job.attemptsMade ?? 0,
			enqueuedAt: job.timestamp ?? Date.now(),
		};
	}
}

export class BullMqWorkerAdapter implements QueueWorker {
	constructor(private readonly worker: BullMqWorkerLike) {}

	start(): Promise<void> {
		return Promise.resolve();
	}

	close(): Promise<void> {
		return this.worker.close();
	}
}

export function createBullMqQueueAdapter<T>(
	queue: BullMqQueueLike,
	options: Readonly<{
		codec?: RedisCodec<T>;
		jobName?: string;
		maxAttempts?: number;
		workerFactory?: BullMqWorkerFactory;
	}>,
): BullMqQueueAdapter<T> {
	return new BullMqQueueAdapter(queue, options);
}
