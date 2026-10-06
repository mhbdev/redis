import { jsonCodec, type RedisCodec } from "../core/codec.js";
import { RedisConfigurationError } from "../core/errors.js";
import type { RedisSubscriptionTransport } from "./contracts.js";

export class RedisPubSub<T> {
	constructor(
		private readonly transport: RedisSubscriptionTransport,
		private readonly codec: RedisCodec<T> = jsonCodec<T>(),
	) {}

	async publish(channel: string, value: T): Promise<number> {
		if (!channel.trim()) throw new RedisConfigurationError("Pub/sub channel is required");
		return this.transport.publish(channel, this.codec.encode(value));
	}

	async subscribe(
		channel: string,
		handler: (value: T) => void | Promise<void>,
	): Promise<() => Promise<void>> {
		if (!channel.trim()) throw new RedisConfigurationError("Pub/sub channel is required");
		return this.transport.subscribe(channel, async (message) =>
			handler(this.codec.decode(message)),
		);
	}

	messages(channel: string): AsyncIterable<T> {
		const queue: T[] = [];
		const waiters: Array<(result: IteratorResult<T>) => void> = [];
		let closed = false;
		let unsubscribe: (() => Promise<void>) | undefined;
		const ready = this.subscribe(channel, (value) => {
			const waiter = waiters.shift();
			if (waiter) waiter({ done: false, value });
			else queue.push(value);
		}).then((stop) => {
			unsubscribe = stop;
		});

		const iterator: AsyncIterator<T> = {
			next: async (): Promise<IteratorResult<T>> => {
				await ready;
				if (queue.length) return { done: false, value: queue.shift() as T };
				if (closed) return { done: true, value: undefined };
				return new Promise((resolve) => waiters.push(resolve));
			},
			return: async (): Promise<IteratorResult<T>> => {
				closed = true;
				await unsubscribe?.();
				for (const waiter of waiters.splice(0)) waiter({ done: true, value: undefined });
				return { done: true, value: undefined };
			},
		};
		return { [Symbol.asyncIterator]: () => iterator };
	}

	close(): Promise<void> {
		return this.transport.close();
	}
}

export function createPubSub<T>(
	transport: RedisSubscriptionTransport,
	codec: RedisCodec<T> = jsonCodec<T>(),
): RedisPubSub<T> {
	return new RedisPubSub(transport, codec);
}

/** A discoverable alias for applications that prefer Redis-prefixed factory names. */
export const createRedisPubSub = createPubSub;
