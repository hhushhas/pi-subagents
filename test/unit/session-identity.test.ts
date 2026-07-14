import assert from "node:assert/strict";
import test from "node:test";
import { resolveCurrentSessionId, resolveCurrentSessionIdentity } from "../../src/shared/session-identity.ts";

test("session identity keeps the Pi UUID separate from the JSONL session file", () => {
	const manager = { getSessionId: () => "019f-uuid", getSessionFile: () => "/sessions/parent.jsonl" };
	assert.deepEqual(resolveCurrentSessionIdentity(manager), { orchestratorSessionId: "019f-uuid", orchestratorSessionFile: "/sessions/parent.jsonl" });
	assert.equal(resolveCurrentSessionId(manager), "019f-uuid");
});

test("session identity fails closed when the UUID is unavailable", () => {
	assert.throws(() => resolveCurrentSessionIdentity({ getSessionId: () => undefined, getSessionFile: () => "/sessions/parent.jsonl" }), /UUID is unavailable/);
});
