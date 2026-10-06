import { createClient, type RedisClientType } from "redis";
import type { RedisCodec } from "../core/codec.js";
import type { RedisSubscriptionTransport } from "../pubsub/contracts.js";

type EmptyModule = Record<never, never>;
type Client = RedisClientType<EmptyModule, EmptyModule, EmptyModule, 3, EmptyModule>;

export function createNodePubSubTransport(
	options: Readonly<{ url: string; onError?: (error: Error) => void }>,
): RedisSubscriptionTransport {
	let publisher: Client | null = null;
	let subscriber: Client | null = null;
	return {
		async publish(channel, message) {
			publisher ??= createClient({ url: options.url });
			publisher.on("error", (error) => options.onError?.(error));
			if (!publisher.isReady) await publisher.connect();
			return publisher.publish(channel, message);
		},
		async subscribe(channel, handler) {
			subscriber ??= createClient({ url: options.url });
			subscriber.on("error", (error) => options.onError?.(error));
			if (!subscriber.isReady) await subscriber.connect();
			await subscriber.subscribe(channel, handler);
			return async () => {
				if (!subscriber) return;
				await subscriber.unsubscribe(channel);
			};
		},
		async close() {
			await Promise.all([
				publisher?.isOpen ? publisher.close() : undefined,
				subscriber?.isOpen ? subscriber.close() : undefined,
			]);
		},
	};
}

export function createNodeTypedPubSub<T>(
	options: Readonly<{
		url: string;
		codec: RedisCodec<T>;
		onError?: (error: Error) => void;
	}>,
): {
	publish(channel: string, value: T): Promise<number>;
	subscribe(
		channel: string,
		handler: (value: T) => void | Promise<void>,
	): Promise<() => Promise<void>>;
	close(): Promise<void>;
} {
	const transport = createNodePubSubTransport(options);
	return {
		publish: async (channel, value) =>
			transport.publish(channel, options.codec.encode(value)),
		subscribe: (channel, handler) =>
			transport.subscribe(channel, async (message) =>
				handler(options.codec.decode(message)),
			),
		close: () => transport.close(),
	};
}
