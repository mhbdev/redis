import {
	createClient,
	type RedisClientOptions as NodeRedisOptions,
	type RedisClientType,
} from "redis";

import {
	asError,
	RedisClientStateError,
	RedisConfigurationError,
	RedisConnectionError,
	RedisRequestAbortedError,
	RedisTimeoutError,
} from "../core/errors.js";
import {
	assertPositiveInteger,
	type RedisCommand,
	type RedisConnection,
	type RedisHooks,
	type RedisScriptOptions,
	withTimeout,
} from "../core/executor.js";
import { notifyError, notifyHook } from "../core/hooks.js";
import { normalizeNodeRedisError } from "./errors.js";

type EmptyModule = Record<never, never>;
export type NodeRedisClient = RedisClientType<
	EmptyModule,
	EmptyModule,
	EmptyModule,
	3,
	EmptyModule
>;

export type RedisClientOptions = Readonly<{
	url: string;
	connectTimeoutMs?: number;
	commandTimeoutMs?: number;
	maxReconnectAttempts?: number;
	reconnectBaseDelayMs?: number;
	reconnectMaxDelayMs?: number;
	disableOfflineQueue?: boolean;
	clientOptions?: Omit<NodeRedisOptions, "url" | "disableOfflineQueue">;
	onError?: (error: Error) => void;
	hooks?: RedisHooks;
}>;

export class RedisClient implements RedisConnection {
	private client: NodeRedisClient;
	private readonly options: Required<
		Pick<
			RedisClientOptions,
			| "connectTimeoutMs"
			| "commandTimeoutMs"
			| "maxReconnectAttempts"
			| "reconnectBaseDelayMs"
			| "reconnectMaxDelayMs"
			| "disableOfflineQueue"
		>
	> &
		Omit<
			RedisClientOptions,
			| "connectTimeoutMs"
			| "commandTimeoutMs"
			| "maxReconnectAttempts"
			| "reconnectBaseDelayMs"
			| "reconnectMaxDelayMs"
			| "disableOfflineQueue"
		>;
	private connecting: Promise<void> | null = null;
	private closing: Promise<void> | null = null;
	private closed = false;

	constructor(options: RedisClientOptions) {
		if (!options.url.trim()) throw new RedisConnectionError("Redis URL is required");
		this.options = {
			...options,
			connectTimeoutMs: options.connectTimeoutMs ?? 3_000,
			commandTimeoutMs: options.commandTimeoutMs ?? 3_000,
			maxReconnectAttempts: options.maxReconnectAttempts ?? 3,
			reconnectBaseDelayMs: options.reconnectBaseDelayMs ?? 100,
			reconnectMaxDelayMs: options.reconnectMaxDelayMs ?? 1_000,
			disableOfflineQueue: options.disableOfflineQueue ?? true,
		};
		assertPositiveInteger(this.options.connectTimeoutMs, "Redis connect timeout");
		assertPositiveInteger(this.options.commandTimeoutMs, "Redis command timeout");
		if (
			!Number.isSafeInteger(this.options.maxReconnectAttempts) ||
			this.options.maxReconnectAttempts < 0
		)
			throw new RedisConfigurationError("Redis reconnect attempts must be non-negative");
		assertPositiveInteger(
			this.options.reconnectBaseDelayMs,
			"Redis reconnect base delay",
		);
		assertPositiveInteger(this.options.reconnectMaxDelayMs, "Redis reconnect max delay");
		this.client = this.createClient();
	}

	get isReady(): boolean {
		return this.client.isReady;
	}

	get isOpen(): boolean {
		return this.client.isOpen;
	}

	private createClient(): NodeRedisClient {
		const client = createClient({
			...this.options.clientOptions,
			url: this.options.url,
			disableOfflineQueue: this.options.disableOfflineQueue,
			socket: {
				...this.options.clientOptions?.socket,
				connectTimeout: this.options.connectTimeoutMs,
				reconnectStrategy: (attempts) => {
					if (attempts >= this.options.maxReconnectAttempts) return false;
					return Math.min(
						this.options.reconnectBaseDelayMs * 2 ** attempts,
						this.options.reconnectMaxDelayMs,
					);
				},
			},
		});
		client.on("error", (error) => {
			const normalized = asError(error);
			notifyError(this.options.onError, normalized);
			notifyError(this.options.hooks?.onError, normalized);
			notifyHook(this.options.hooks?.onStateChange, this.options.hooks?.onError, "error");
		});
		client.on("ready", () =>
			notifyHook(this.options.hooks?.onStateChange, this.options.hooks?.onError, "ready"),
		);
		client.on("reconnecting", () =>
			notifyHook(
				this.options.hooks?.onStateChange,
				this.options.hooks?.onError,
				"connecting",
			),
		);
		return client;
	}

	private replaceClient(): void {
		if (this.closed) return;
		if (this.client.isOpen) this.client.destroy();
		this.client = this.createClient();
	}

	async connect(): Promise<void> {
		if (this.closed) throw new RedisClientStateError("The Redis client has been closed");
		if (this.client.isReady) return;
		const client = this.client;
		this.connecting ??= withTimeout(
			async () => {
				notifyHook(
					this.options.hooks?.onStateChange,
					this.options.hooks?.onError,
					"connecting",
				);
				await client.connect();
			},
			this.options.connectTimeoutMs,
			"Redis connection timed out",
		)
			.catch((error: unknown) => {
				if (this.client === client) this.replaceClient();
				if (
					error instanceof RedisConnectionError ||
					error instanceof RedisTimeoutError ||
					error instanceof RedisClientStateError
				)
					throw error;
				throw new RedisConnectionError("Redis connection failed", error);
			})
			.finally(() => {
				this.connecting = null;
			});
		await this.connecting;
		if (this.closed)
			throw new RedisClientStateError("The Redis client was closed while connecting");
	}

	async execute<T = unknown>(
		command: RedisCommand,
		options: { signal?: AbortSignal } = {},
	): Promise<T> {
		if (options.signal?.aborted)
			throw new RedisRequestAbortedError(options.signal.reason);
		await this.connect();
		const started = Date.now();
		notifyHook(this.options.hooks?.onCommandStart, this.options.hooks?.onError, command);
		try {
			return await withTimeout(
				(signal) => this.sendCommand<T>(command, signal),
				this.options.commandTimeoutMs,
				"Redis command timed out",
				options.signal,
			);
		} finally {
			notifyHook(
				this.options.hooks?.onCommandEnd,
				this.options.hooks?.onError,
				command,
				Date.now() - started,
			);
		}
	}

	private async sendCommand<T>(command: RedisCommand, signal: AbortSignal): Promise<T> {
		try {
			const client = this.client as unknown as {
				withAbortSignal(signal: AbortSignal): {
					sendCommand(command: string[]): Promise<unknown>;
				};
			};
			return (await client.withAbortSignal(signal).sendCommand([...command])) as T;
		} catch (error) {
			throw normalizeNodeRedisError(error);
		}
	}

	async eval<T = unknown>(
		script: string,
		options: RedisScriptOptions,
		request: { signal?: AbortSignal } = {},
	): Promise<T> {
		const command: RedisCommand = [
			"EVAL",
			script,
			String(options.keys?.length ?? 0),
			...(options.keys ?? []),
			...(options.arguments ?? []),
		];
		return this.execute<T>(command, request);
	}

	async ping(): Promise<void> {
		await this.execute(["PING"]);
	}

	raw(): NodeRedisClient {
		return this.client;
	}

	async close(): Promise<void> {
		this.closing ??= (async () => {
			this.closed = true;
			await this.connecting?.catch(() => undefined);
			if (this.client.isOpen) await this.client.close();
			notifyHook(
				this.options.hooks?.onStateChange,
				this.options.hooks?.onError,
				"closed",
			);
		})();
		await this.closing;
	}
}

export function createRedisClient(options: RedisClientOptions): RedisClient {
	return new RedisClient(options);
}

/**
 * Creates, connects, and closes a managed client around one operation.
 * This is useful for scripts, jobs, and request-scoped composition tests.
 */
export async function withRedisClient<T>(
	options: RedisClientOptions,
	operation: (redis: RedisClient) => Promise<T>,
): Promise<T> {
	const redis = createRedisClient(options);
	try {
		await redis.connect();
		return await operation(redis);
	} finally {
		await redis.close();
	}
}
