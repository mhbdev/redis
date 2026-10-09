import { describe, expect, it } from "vitest";

import { RedisRequestAbortedError } from "../src/core/errors.js";
import { withTimeout } from "../src/core/executor.js";

describe("Redis operation timeouts", () => {
	it("rejects promptly when the caller aborts", async () => {
		const controller = new AbortController();
		const operation = withTimeout(
			() => new Promise<string>(() => undefined),
			1_000,
			undefined,
			controller.signal,
		);
		controller.abort();

		await expect(operation).rejects.toBeInstanceOf(RedisRequestAbortedError);
	});
});
