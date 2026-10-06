import { RedisConfigurationError } from "../core/errors.js";
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
	}>,
): RedisSubscriptionTransport {
	const subscriber = options.subscriber ?? options.publisher.duplicate?.();
	if (!subscriber)
		throw new RedisConfigurationError(
			"An ioredis subscriber or duplicate() method is required",
		);
	const listeners = new Map<string, Set<(message: string) => void | Promise<void>>>();
	const onMessage = (channel: string, message: string) => {
		for (const listener of listeners.get(channel) ?? []) void listener(message);
	};
	subscriber.on("message", onMessage);
	return {
		publish: (channel, message) => options.publisher.publish(channel, message),
		async subscribe(channel, handler) {
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
			if (subscriber.quit) await subscriber.quit();
			else subscriber.disconnect?.();
			if (options.publisher.quit) await options.publisher.quit();
			else options.publisher.disconnect?.();
		},
	};
}
