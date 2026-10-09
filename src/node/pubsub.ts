import { createClient, type RedisClientType } from "redis";
import type { RedisCodec } from "../core/codec.js";
import {
	asError,
	RedisClientStateError,
	RedisConfigurationError,
} from "../core/errors.js";
import type { RedisSubscriptionTransport } from "../pubsub/contracts.js";

type EmptyModule = Record<never, never>;
type Client = RedisClientType<EmptyModule, EmptyModule, EmptyModule, 3, EmptyModule>;

export function createNodePubSubTransport(
	options: Readonly<{ url: string; onError?: (error: Error) => void }>,
): RedisSubscriptionTransport {
	let publisher: Client | null = null;
	let subscriber: Client | null = null;
	let closed = false;
	const report = (error: unknown) => options.onError?.(asError(error));
	return {
		async publish(channel, message) {
			if (closed)
				throw new RedisClientStateError("The pub/sub transport has been closed");
			if (!channel.trim())
				throw new RedisConfigurationError("Pub/sub channel is required");
			publisher ??= createClient({ url: options.url });
			if (!publisher.isOpen) publisher.on("error", report);
			if (!publisher.isReady) await publisher.connect();
			return publisher.publish(channel, message);
		},
		async subscribe(channel, handler) {
			if (closed)
				throw new RedisClientStateError("The pub/sub transport has been closed");
			if (!channel.trim())
				throw new RedisConfigurationError("Pub/sub channel is required");
			subscriber ??= createClient({ url: options.url });
			if (!subscriber.isOpen) subscriber.on("error", report);
			if (!subscriber.isReady) await subscriber.connect();
			await subscriber.subscribe(channel, (message) => {
				try {
					void Promise.resolve(handler(message)).catch(report);
				} catch (error) {
					report(error);
				}
			});
			return async () => {
				if (!subscriber) return;
				await subscriber.unsubscribe(channel);
			};
		},
		async close() {
			if (closed) return;
			closed = true;
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
		publish: async (channel, value) => {
			if (!channel.trim())
				throw new RedisConfigurationError("Pub/sub channel is required");
			return transport.publish(channel, options.codec.encode(value));
		},
		subscribe: (channel, handler) => {
			if (!channel.trim())
				throw new RedisConfigurationError("Pub/sub channel is required");
			return transport.subscribe(channel, async (message) =>
				handler(options.codec.decode(message)),
			);
		},
		close: () => transport.close(),
	};
}
