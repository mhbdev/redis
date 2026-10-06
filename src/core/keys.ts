import { RedisConfigurationError } from "./errors.js";

const encoder = new TextEncoder();

export async function hashIdentifier(value: string): Promise<string> {
	const digest = await crypto.subtle.digest("SHA-256", encoder.encode(value));
	return Array.from(new Uint8Array(digest), (byte) =>
		byte.toString(16).padStart(2, "0"),
	).join("");
}

export function namespacedKey(prefix: string, ...parts: string[]): string {
	return [prefix, ...parts].join(":");
}

export function assertKeyPart(value: string, name: string): void {
	if (!value.trim() || value.length > 256 || value.includes("\n")) {
		throw new RedisConfigurationError(
			`${name} must be a non-empty value of at most 256 characters`,
		);
	}
}
