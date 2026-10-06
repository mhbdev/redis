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
});
