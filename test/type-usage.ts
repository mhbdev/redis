import type {
	JobQueue,
	RedisCodec,
	RedisExecutor,
	RedisSubscriptionTransport,
} from "../src/index.js";
import { createCache, createPubSub, createQueue, jsonCodec } from "../src/index.js";

type User = Readonly<{ id: string }>;

const redis = {} as RedisExecutor;
const transport = {} as RedisSubscriptionTransport;
const codec: RedisCodec<User> = jsonCodec<User>();

const cache = createCache<User>({ redis });
const customCache = createCache({ redis, codec });
const queue: JobQueue<User> = createQueue<User>({ redis, name: "users" });
const pubsub = createPubSub<User>(transport);

void cache;
void customCache;
void queue;
void pubsub;
