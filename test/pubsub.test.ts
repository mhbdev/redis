import { describe, expect, it } from "vitest";

import { jsonCodec } from "../src/core/codec.js";
import type { RedisSubscriptionTransport } from "../src/pubsub/index.js";
import { createPubSub } from "../src/pubsub/index.js";

describe("RedisPubSub", () => {
	it("encodes, publishes, and decodes messages", async () => {
		let handler: ((message: string) => void) | undefined;
		const transport: RedisSubscriptionTransport = {
			publish: async (_channel, message) => {
				handler?.(message);
				return 1;
			},
			subscribe: async (_channel, next) => {
				handler = next;
				return async () => {
					handler = undefined;
				};
			},
			close: async () => undefined,
		};
		const pubsub = createPubSub(transport, jsonCodec<{ value: string }>());
		const received: Array<{ value: string }> = [];
		await pubsub.subscribe("events", (value) => {
			received.push(value);
		});
		await pubsub.publish("events", { value: "ok" });
		expect(received).toEqual([{ value: "ok" }]);
	});

	it("unsubscribes if an async iterator is closed before subscribe resolves", async () => {
		let completeSubscribe!: (unsubscribe: () => Promise<void>) => void;
		let unsubscribed = 0;
		const pubsub = createPubSub<{ value: string }>({
			publish: async () => 0,
			subscribe: async () =>
				new Promise((resolve) => {
					completeSubscribe = resolve;
				}),
			close: async () => undefined,
		});
		const iterator = pubsub.messages("events")[Symbol.asyncIterator]();
		const closing = iterator.return?.();

		completeSubscribe(async () => {
			unsubscribed += 1;
		});
		await closing;
		expect(unsubscribed).toBe(1);
	});
});
