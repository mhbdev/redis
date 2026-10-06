import { execFileSync } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const npm =
	process.platform === "win32" && process.env.npm_execpath ? process.execPath : "npm";
const npmPrefix = npm === process.execPath ? [process.env.npm_execpath] : [];
const temporaryDirectory = mkdtempSync(join(tmpdir(), "mhbdev-redis-pack-"));

try {
	const packed = JSON.parse(
		execFileSync(
			npm,
			[...npmPrefix, "pack", "--json", "--pack-destination", temporaryDirectory],
			{
				encoding: "utf8",
				stdio: ["ignore", "pipe", "inherit"],
			},
		),
	);
	const tarball = join(temporaryDirectory, packed[0].filename);
	execFileSync(
		npm,
		[...npmPrefix, "install", "--ignore-scripts", "--no-audit", "--no-fund", tarball],
		{
			cwd: temporaryDirectory,
			stdio: "inherit",
		},
	);
	execFileSync(
		process.execPath,
		[
			"--input-type=module",
			"-e",
			'await Promise.all(["@mhbdev/redis", "@mhbdev/redis/node", "@mhbdev/redis/ioredis", "@mhbdev/redis/bullmq", "@mhbdev/redis/rate-limit", "@mhbdev/redis/cache", "@mhbdev/redis/locks", "@mhbdev/redis/pubsub", "@mhbdev/redis/queue", "@mhbdev/redis/testing"].map((entry) => import(entry)));',
		],
		{ cwd: temporaryDirectory, stdio: "inherit" },
	);
	console.log("Package tarball smoke test passed.");
} finally {
	rmSync(temporaryDirectory, { recursive: true, force: true });
}
