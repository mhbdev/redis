import { jsonCodec, type RedisCodec } from "../core/codec.js";
import { RedisConfigurationError, RedisSerializationError } from "../core/errors.js";
import type { RedisExecutor } from "../core/executor.js";

export type CacheSetOptions = Readonly<{
	ttlMs?: number;
	onlyIfAbsent?: boolean;
	onlyIfPresent?: boolean;
}>;

export type RedisCacheOptions<T> = Readonly<{
	redis: RedisExecutor;
	/** JSON is used by default. Provide a codec for binary or schema-validated values. */
	codec?: RedisCodec<T>;
	prefix?: string;
	maxKeyLength?: number;
}>;

export class RedisCache<T> {
	private readonly prefix: string;
	private readonly codec: RedisCodec<T>;
	private readonly pending = new Map<string, Promise<T | undefined>>();

	constructor(private readonly options: RedisCacheOptions<T>) {
		this.prefix = options.prefix ?? "mhbdev:cache";
		this.codec = options.codec ?? jsonCodec<T>();
		if (!this.prefix.trim() || this.prefix.includes("\n"))
			throw new RedisConfigurationError("Cache prefix is required");
		if (
			options.maxKeyLength !== undefined &&
			(!Number.isSafeInteger(options.maxKeyLength) || options.maxKeyLength <= 0)
		)
			throw new RedisConfigurationError("Cache max key length must be positive");
	}

	async get(key: string): Promise<T | undefined> {
		const value = await this.options.redis.execute<string | null>(["GET", this.key(key)]);
		if (value === null) return undefined;
		try {
			return this.codec.decode(value);
		} catch (error) {
			if (error instanceof RedisSerializationError) throw error;
			throw new RedisSerializationError("Unable to decode cached value", error);
		}
	}

	async set(key: string, value: T, options: CacheSetOptions = {}): Promise<boolean> {
		if (options.onlyIfAbsent && options.onlyIfPresent)
			throw new RedisConfigurationError(
				"Cache set cannot require both onlyIfAbsent and onlyIfPresent",
			);
		const encoded = this.codec.encode(value);
		const command = ["SET", this.key(key), encoded] as string[];
		if (options.ttlMs !== undefined) {
			if (!Number.isSafeInteger(options.ttlMs) || options.ttlMs <= 0)
				throw new RedisConfigurationError("Cache TTL must be positive");
			command.push("PX", String(options.ttlMs));
		}
		if (options.onlyIfAbsent) command.push("NX");
		if (options.onlyIfPresent) command.push("XX");
		const result = await this.options.redis.execute<string | null>(command);
		return result === "OK";
	}

	async delete(key: string): Promise<boolean> {
		return (await this.options.redis.execute<number>(["DEL", this.key(key)])) > 0;
	}

	async exists(key: string): Promise<boolean> {
		return (await this.options.redis.execute<number>(["EXISTS", this.key(key)])) > 0;
	}

	async getOrSet(
		key: string,
		factory: () => Promise<T>,
		options: CacheSetOptions = {},
	): Promise<T> {
		const existing = await this.get(key);
		if (existing !== undefined) return existing;
		const pending = this.pending.get(key);
		if (pending) return (await pending) as T;
		const operation = factory()
			.then(async (value) => {
				const stored = await this.set(key, value, options);
				if (!stored && options.onlyIfAbsent) {
					const winner = await this.get(key);
					if (winner !== undefined) return winner;
				}
				return value;
			})
			.finally(() => this.pending.delete(key));
		this.pending.set(key, operation);
		return operation;
	}

	private key(key: string): string {
		if (
			!key.trim() ||
			key.length > (this.options.maxKeyLength ?? 512) ||
			key.includes("\n")
		)
			throw new RedisConfigurationError("Cache key is invalid");
		return `${this.prefix}:${key}`;
	}
}

export function createCache<T>(options: RedisCacheOptions<T>): RedisCache<T> {
	return new RedisCache(options);
}

/** A discoverable alias for applications that prefer Redis-prefixed factory names. */
export const createRedisCache = createCache;
