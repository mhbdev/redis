import { asError } from "./errors.js";

export type RedisErrorHook = (error: Error) => void;

export function notifyHook<Arguments extends readonly unknown[]>(
	hook: ((...args: Arguments) => void) | undefined,
	onError: RedisErrorHook | undefined,
	...args: Arguments
): void {
	if (!hook) return;
	try {
		hook(...args);
	} catch (error) {
		notifyError(onError, error);
	}
}

/** Error observers are isolated so monitoring code cannot interrupt Redis work. */
export function notifyError(hook: RedisErrorHook | undefined, error: unknown): void {
	if (!hook) return;
	try {
		hook(asError(error));
	} catch {
		// Error hooks are terminal observers and must not create unhandled failures.
	}
}
