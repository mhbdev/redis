import {
	asError,
	RedisClientStateError,
	RedisConfigurationError,
} from "../core/errors.js";
import type { RedisSubscriptionTransport } from "../pubsub/contracts.js";

export interface IoredisPubSubClientLike {
	publish(channel: string, message: string): Promise<number>;
	subscribe(channel: string): Promise<unknown>;
	unsubscribe(channel: string): Promise<unknown>;
	on(event: "message", listener: (channel: string, message: string) => void): this;
	off?(event: "message", listener: (channel: string, message: string) => void): this;
	duplicate?(): IoredisPubSubClientLike;
	quit?(): Promise<unknown>;
	disconnect?(): void;
}

export function createIoredisPubSubTransport(
	options: Readonly<{
		publisher: IoredisPubSubClientLike;
		subscriber?: IoredisPubSubClientLike;
		onError?: (error: Error) => void;
	}>,
): RedisSubscriptionTransport {
	const subscriber = options.subscriber ?? options.publisher.duplicate?.();
	const ownsSubscriber = options.subscriber === undefined;
	if (!subscriber)
		throw new RedisConfigurationError(
			"An ioredis subscriber or duplicate() method is required",
		);
	const listeners = new Map<string, Set<(message: string) => void | Promise<void>>>();
	let closed = false;
	const onMessage = (channel: string, message: string) => {
		for (const listener of listeners.get(channel) ?? []) {
			try {
				void Promise.resolve(listener(message)).catch((error: unknown) =>
					options.onError?.(asError(error)),
				);
			} catch (error) {
				options.onError?.(asError(error));
			}
		}
	};
	subscriber.on("message", onMessage);
	return {
		async publish(channel, message) {
			if (closed)
				throw new RedisClientStateError("The pub/sub transport has been closed");
			if (!channel.trim())
				throw new RedisConfigurationError("Pub/sub channel is required");
			return options.publisher.publish(channel, message);
		},
		async subscribe(channel, handler) {
			if (closed)
				throw new RedisClientStateError("The pub/sub transport has been closed");
			if (!channel.trim())
				throw new RedisConfigurationError("Pub/sub channel is required");
			let channelListeners = listeners.get(channel);
			if (!channelListeners) {
				channelListeners = new Set();
				listeners.set(channel, channelListeners);
				await subscriber.subscribe(channel);
			}
			channelListeners.add(handler);
			return async () => {
				channelListeners?.delete(handler);
				if (channelListeners?.size === 0) {
					listeners.delete(channel);
					await subscriber.unsubscribe(channel);
				}
			};
		},
		async close() {
			if (closed) return;
			closed = true;
			const channels = [...listeners.keys()];
			subscriber.off?.("message", onMessage);
			listeners.clear();
			try {
				await Promise.all(channels.map((channel) => subscriber.unsubscribe(channel)));
			} finally {
				if (ownsSubscriber) {
					if (subscriber.quit) await subscriber.quit();
					else subscriber.disconnect?.();
				}
			}
		},
	};
}
