import { randomBytes } from "node:crypto";

import { RedisLockError } from "../core/errors.js";
import type { RedisExecutor } from "../core/executor.js";

export type RedisLockOptions = Readonly<{
	redis: RedisExecutor;
	prefix?: string;
	defaultTtlMs?: number;
}>;

export interface RedisLockLease {
	readonly key: string;
	readonly token: string;
	readonly expiresAt: number;
	release(): Promise<boolean>;
	extend(ttlMs: number): Promise<boolean>;
}

export class RedisLockManager {
	private readonly prefix: string;
	private readonly defaultTtlMs: number;

	constructor(private readonly options: RedisLockOptions) {
		this.prefix = options.prefix ?? "mhbdev:lock";
		this.defaultTtlMs = options.defaultTtlMs ?? 30_000;
	}

	async acquire(name: string, ttlMs = this.defaultTtlMs): Promise<RedisLockLease | null> {
		if (!name.trim()) throw new RedisLockError("Lock name is required");
		if (!Number.isSafeInteger(ttlMs) || ttlMs <= 0)
			throw new RedisLockError("Lock TTL must be positive");
		const key = `${this.prefix}:${name}`;
		const token = randomBytes(32).toString("hex");
		const result = await this.options.redis.execute<string | null>([
			"SET",
			key,
			token,
			"NX",
			"PX",
			String(ttlMs),
		]);
		if (result !== "OK") return null;
		const createdAt = Date.now();
		let released = false;
		return {
			key,
			token,
			expiresAt: createdAt + ttlMs,
			release: async () => {
				if (released) return false;
				const result = await this.options.redis.eval<number>(RELEASE_SCRIPT, {
					keys: [key],
					arguments: [token],
				});
				released = result === 1;
				return released;
			},
			extend: async (nextTtlMs) => {
				if (released) return false;
				if (!Number.isSafeInteger(nextTtlMs) || nextTtlMs <= 0)
					throw new RedisLockError("Lock TTL must be positive");
				const result = await this.options.redis.eval<number>(EXTEND_SCRIPT, {
					keys: [key],
					arguments: [token, String(nextTtlMs)],
				});
				return result === 1;
			},
		};
	}

	async withLock<T>(
		name: string,
		operation: (lease: RedisLockLease) => Promise<T>,
		ttlMs = this.defaultTtlMs,
	): Promise<T> {
		const lease = await this.acquire(name, ttlMs);
		if (!lease) throw new RedisLockError("Unable to acquire Redis lock");
		try {
			return await operation(lease);
		} finally {
			await lease.release();
		}
	}
}

const RELEASE_SCRIPT = `if redis.call("GET", KEYS[1]) == ARGV[1] then return redis.call("DEL", KEYS[1]) else return 0 end`;
const EXTEND_SCRIPT = `if redis.call("GET", KEYS[1]) == ARGV[1] then return redis.call("PEXPIRE", KEYS[1], ARGV[2]) else return 0 end`;

export function createLockManager(options: RedisLockOptions): RedisLockManager {
	return new RedisLockManager(options);
}

/** A discoverable alias for applications that prefer Redis-prefixed factory names. */
export const createRedisLockManager = createLockManager;
