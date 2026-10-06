import type {
	RedisCommand,
	RedisExecutor,
	RedisScriptOptions,
} from "../core/executor.js";

export class FakeRedisExecutor implements RedisExecutor {
	readonly commands: RedisCommand[] = [];
	constructor(private readonly scriptResult: unknown = [1, 1_000]) {}
	execute<T = unknown>(command: RedisCommand): Promise<T> {
		this.commands.push(command);
		return Promise.resolve(undefined as T);
	}
	eval<T = unknown>(_script: string, _options: RedisScriptOptions): Promise<T> {
		return Promise.resolve(this.scriptResult as T);
	}
}
