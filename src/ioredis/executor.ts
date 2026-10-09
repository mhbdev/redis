import {
	RedisClientStateError,
	RedisError,
	RedisUnavailableError,
} from "../core/errors.js";
import type {
	RedisCommand,
	RedisCommandOptions,
	RedisConnection,
	RedisScriptOptions,
} from "../core/executor.js";
import { withTimeout } from "../core/executor.js";

export interface IoredisClientLike {
	call(command: string, ...arguments_: string[]): Promise<unknown>;
	eval(script: string, numberOfKeys: number, ...arguments_: string[]): Promise<unknown>;
	ping(): Promise<unknown>;
	quit?(): Promise<unknown>;
	disconnect?(): void;
	readonly status?: string;
}

export type IoredisExecutorOptions = Readonly<{
	commandTimeoutMs?: number;
	/** Map known ioredis/plugin error classes without inspecting error messages. */
	normalizeError?: (error: unknown) => RedisError | undefined;
}>;

export class IoredisExecutor implements RedisConnection {
	private readonly commandTimeoutMs: number;

	constructor(
		private readonly client: IoredisClientLike,
		private readonly options: IoredisExecutorOptions = {},
	) {
		this.commandTimeoutMs = options.commandTimeoutMs ?? 3_000;
	}

	get isReady(): boolean {
		return this.client.status === undefined || this.client.status === "ready";
	}

	async connect(): Promise<void> {
		if (!this.isReady && this.client.status === "end")
			throw new RedisClientStateError("The ioredis client has ended");
		await this.run(() => this.client.ping());
	}

	async execute<T = unknown>(
		command: RedisCommand,
		options?: RedisCommandOptions,
	): Promise<T> {
		return this.run(
			() => this.client.call(command[0] ?? "", ...command.slice(1)) as Promise<T>,
			options?.signal,
		);
	}

	async eval<T = unknown>(
		script: string,
		options: RedisScriptOptions,
		request?: RedisCommandOptions,
	): Promise<T> {
		return this.run(
			() =>
				this.client.eval(
					script,
					options.keys?.length ?? 0,
					...(options.keys ?? []),
					...(options.arguments ?? []),
				) as Promise<T>,
			request?.signal,
		);
	}

	async ping(): Promise<void> {
		await this.run(() => this.client.ping());
	}

	async close(): Promise<void> {
		if (this.client.quit) await this.client.quit();
		else this.client.disconnect?.();
	}

	private async run<T>(operation: () => Promise<T>, signal?: AbortSignal): Promise<T> {
		try {
			return await withTimeout(
				() => operation(),
				this.commandTimeoutMs,
				"Redis command timed out",
				signal,
			);
		} catch (error) {
			const normalized = this.options.normalizeError?.(error);
			if (normalized) throw normalized;
			if (error instanceof RedisError) throw error;
			throw new RedisUnavailableError(error);
		}
	}
}

export function createIoredisExecutor(
	client: IoredisClientLike,
	options?: IoredisExecutorOptions,
): IoredisExecutor {
	return new IoredisExecutor(client, options);
}
