import { createClientPool, type RedisClientOptions as NodeRedisOptions } from "redis";

import {
	RedisClientStateError,
	RedisConfigurationError,
	RedisConnectionError,
	RedisTimeoutError,
} from "../core/errors.js";
import {
	assertPositiveInteger,
	type RedisCommand,
	type RedisExecutor,
	type RedisScriptOptions,
	withTimeout,
} from "../core/executor.js";
import type { RedisClientOptions } from "./client.js";
import { normalizeNodeRedisError } from "./errors.js";

export type RedisPoolOptions = RedisClientOptions &
	Readonly<{
		minimum?: number;
		maximum?: number;
		acquireTimeoutMs?: number;
		cleanupDelayMs?: number;
		clientOptions?: Omit<NodeRedisOptions, "url" | "disableOfflineQueue">;
	}>;

type PoolClient = ReturnType<typeof createClientPool>;

export class RedisPool implements RedisExecutor {
	private readonly pool: PoolClient;
	private readonly commandTimeoutMs: number;
	private readonly acquireTimeoutMs: number;
	private closed = false;
	private connected = false;

	constructor(private readonly options: RedisPoolOptions) {
		const minimum = options.minimum ?? 1;
		const maximum = options.maximum ?? 10;
		this.commandTimeoutMs = options.commandTimeoutMs ?? 3_000;
		this.acquireTimeoutMs = options.acquireTimeoutMs ?? 3_000;
		assertPositiveInteger(minimum, "Redis pool minimum");
		assertPositiveInteger(maximum, "Redis pool maximum");
		assertPositiveInteger(this.commandTimeoutMs, "Redis command timeout");
		assertPositiveInteger(this.acquireTimeoutMs, "Redis acquire timeout");
		if (minimum > maximum)
			throw new RedisConfigurationError("Redis pool minimum cannot exceed maximum");
		this.pool = createClientPool(
			{
				...options.clientOptions,
				url: options.url,
				disableOfflineQueue: options.disableOfflineQueue ?? true,
				socket: {
					...options.clientOptions?.socket,
					connectTimeout: options.connectTimeoutMs ?? 3_000,
					reconnectStrategy: (attempts) => {
						if (attempts >= (options.maxReconnectAttempts ?? 3)) return false;
						return Math.min(
							(options.reconnectBaseDelayMs ?? 100) * 2 ** attempts,
							options.reconnectMaxDelayMs ?? 1_000,
						);
					},
				},
			},
			{
				minimum,
				maximum,
				acquireTimeout: this.acquireTimeoutMs,
				cleanupDelay: options.cleanupDelayMs ?? 3_000,
			},
		);
		this.pool.on("error", (error) => this.reportError(normalizeNodeRedisError(error)));
	}

	get isReady(): boolean {
		return (this.pool as unknown as { isReady?: boolean }).isReady ?? true;
	}

	async connect(): Promise<void> {
		if (this.closed) throw new RedisClientStateError("The Redis pool has been closed");
		if (this.connected) return;
		try {
			this.options.hooks?.onStateChange?.("connecting");
			await withTimeout(
				() => this.pool.connect(),
				this.acquireTimeoutMs,
				"Redis pool connection timed out",
			);
			this.connected = true;
			this.options.hooks?.onStateChange?.("ready");
		} catch (error) {
			const normalized = normalizeNodeRedisError(error);
			this.reportError(normalized);
			throw normalized.code === "REDIS_CLIENT_STATE" ||
				normalized instanceof RedisTimeoutError
				? normalized
				: new RedisConnectionError("Redis pool connection failed", normalized);
		}
	}

	async execute<T = unknown>(command: RedisCommand): Promise<T> {
		if (this.closed) throw new RedisClientStateError("The Redis pool has been closed");
		await this.connect();
		const started = Date.now();
		this.options.hooks?.onCommandStart?.(command);
		try {
			return await withTimeout(
				() => this.pool.sendCommand([...command]) as Promise<T>,
				this.commandTimeoutMs,
			);
		} catch (error) {
			const normalized = normalizeNodeRedisError(error);
			this.reportError(normalized);
			throw normalized;
		} finally {
			this.options.hooks?.onCommandEnd?.(command, Date.now() - started);
		}
	}

	async eval<T = unknown>(script: string, options: RedisScriptOptions): Promise<T> {
		return this.execute<T>([
			"EVAL",
			script,
			String(options.keys?.length ?? 0),
			...(options.keys ?? []),
			...(options.arguments ?? []),
		]);
	}

	async ping(): Promise<void> {
		await this.execute(["PING"]);
	}

	async use<T>(operation: (client: unknown) => Promise<T>): Promise<T> {
		if (this.closed) throw new RedisClientStateError("The Redis pool has been closed");
		await this.connect();
		return this.pool.execute(operation as never) as Promise<T>;
	}

	async close(): Promise<void> {
		if (this.closed) return;
		this.closed = true;
		await this.pool.close();
		this.options.hooks?.onStateChange?.("closed");
	}

	private reportError(error: Error): void {
		this.options.onError?.(error);
		this.options.hooks?.onError?.(error);
		this.options.hooks?.onStateChange?.("error");
	}
}

export function createRedisPool(options: RedisPoolOptions): RedisPool {
	return new RedisPool(options);
}

/** Creates, connects, and closes a pool around one operation. */
export async function withRedisPool<T>(
	options: RedisPoolOptions,
	operation: (pool: RedisPool) => Promise<T>,
): Promise<T> {
	const pool = createRedisPool(options);
	try {
		await pool.connect();
		return await operation(pool);
	} finally {
		await pool.close();
	}
}
