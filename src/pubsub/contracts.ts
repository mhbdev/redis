export interface RedisSubscriptionTransport {
	publish(channel: string, message: string): Promise<number>;
	subscribe(
		channel: string,
		handler: (message: string) => void | Promise<void>,
	): Promise<() => Promise<void>>;
	close(): Promise<void>;
}
