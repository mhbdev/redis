import { RedisSerializationError } from "./errors.js";

export interface RedisCodec<T> {
	encode(value: T): string;
	decode(value: string): T;
}

export const jsonCodec = <T>(): RedisCodec<T> => ({
	encode(value) {
		try {
			const encoded = JSON.stringify(value);
			if (encoded === undefined) {
				throw new TypeError("Value is not JSON-serializable");
			}
			return encoded;
		} catch (error) {
			throw new RedisSerializationError("Unable to encode Redis value", error);
		}
	},
	decode(value) {
		try {
			return JSON.parse(value) as T;
		} catch (error) {
			throw new RedisSerializationError("Unable to decode Redis value", error);
		}
	},
});

export const stringCodec: RedisCodec<string> = {
	encode: (value) => value,
	decode: (value) => value,
};
