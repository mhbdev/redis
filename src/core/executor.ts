import { RedisConfigurationError, RedisTimeoutError } from "./errors.js";

export type RedisCommand = readonly string[];
export type RedisCommandOptions = Readonly<{ signal?: AbortSignal }>;
export type RedisScriptOptions = Readonly<{
	keys?: readonly string[];
	arguments?: readonly string[];
}>;

export interface RedisExecutor {
	execute<T = unknown>(command: RedisCommand, options?: RedisCommandOptions): Promise<T>;
	eval<T = unknown>(
		script: string,
		options: RedisScriptOptions,
		request?: RedisCommandOptions,
	): Promise<T>;
}

export interface RedisConnection extends RedisExecutor {
	connect(): Promise<void>;
	close(): Promise<void>;
	ping(): Promise<void>;
	readonly isReady: boolean;
}

export type RedisHooks = Readonly<{
	onCommandStart?: (command: RedisCommand) => void;
	onCommandEnd?: (command: RedisCommand, durationMs: number) => void;
	onError?: (error: Error) => void;
	onStateChange?: (state: RedisConnectionState) => void;
}>;

export type RedisConnectionState = "idle" | "connecting" | "ready" | "closed" | "error";

export function assertPositiveInteger(value: number, name: string): void {
	if (!Number.isSafeInteger(value) || value <= 0) {
		throw new RedisConfigurationError(`${name} must be a positive integer`);
	}
}

export async function withTimeout<T>(
	operation: (signal: AbortSignal) => Promise<T>,
	timeoutMs: number,
	message = "Redis operation timed out",
): Promise<T> {
	assertPositiveInteger(timeoutMs, "Redis timeout");
	const controller = new AbortController();
	let timer: ReturnType<typeof setTimeout> | undefined;
	try {
		return await Promise.race([
			operation(controller.signal),
			new Promise<T>((_, reject) => {
				timer = setTimeout(() => {
					controller.abort();
					reject(new RedisTimeoutError(message));
				}, timeoutMs);
			}),
		]);
	} finally {
		if (timer) clearTimeout(timer);
	}
}
