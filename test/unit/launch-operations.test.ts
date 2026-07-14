import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import test from "node:test";
import { digestLaunchRequest, launchGoPath, publishLaunchReady, readLaunchOperation, releaseLaunch, reserveLaunchOperation, updateLaunchOperation } from "../../src/runs/background/launch-operations.ts";

function context(operationId: string) {
	return {
		operationId,
		kind: "spawn" as const,
		sessionIdentity: { orchestratorSessionId: "session", workflowId: "wf", nodeId: "node", attemptId: "attempt", ownerLeaseEpoch: 1, workflowCapabilityHash: "a".repeat(64) },
		provenance: { workflowId: "wf", nodeId: "node", attemptId: "attempt", ownerLeaseEpoch: 1 },
		effectiveExecution: { agent: "worker", cwd: "/repo", notificationMode: "event-only" as const },
	};
}

function resumeContext(operationId: string, sourceRunId: string) {
	return { ...context(operationId), kind: "resume" as const, sourceRunId };
}

test("launch operation reservation is idempotent and rejects a changed fingerprint", () => {
	const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-launch-operation-"));
	try {
		const digest = digestLaunchRequest({ agent: "worker" });
		const first = reserveLaunchOperation({ asyncDirRoot: root, operationId: "op-1", kind: "spawn", requestDigest: digest, workflowCapabilityHash: "a".repeat(64), context: context("op-1") });
		const replay = reserveLaunchOperation({ asyncDirRoot: root, operationId: "op-1", kind: "spawn", requestDigest: digest, workflowCapabilityHash: "a".repeat(64), context: context("op-1") });
		assert.equal(first.created, true);
		assert.equal(replay.created, false);
		assert.equal(first.record.runId, replay.record.runId);
		assert.throws(() => reserveLaunchOperation({ asyncDirRoot: root, operationId: "op-1", kind: "spawn", requestDigest: "different", workflowCapabilityHash: "a".repeat(64), context: context("op-1") }), /different launch parameters/);
		const launched = updateLaunchOperation(first.record, { state: "launched", pid: 42 }, root);
		assert.equal(readLaunchOperation({ asyncDirRoot: root, capabilityHash: "a".repeat(64), operationId: "op-1" })?.pid, 42);
		assert.equal(fs.statSync(path.join(root, ".workflow-operations")).mode & 0o777, 0o700);
		assert.equal(launched.state, "launched");
	} finally { fs.rmSync(root, { recursive: true, force: true }); }
});

test("workflow launch barrier releases the reserved run and one resume source has one claim", () => {
	const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-launch-barrier-"));
	try {
		const capabilityHash = "b".repeat(64);
		const spawn = reserveLaunchOperation({ asyncDirRoot: root, operationId: "spawn-op", kind: "spawn", requestDigest: "spawn", workflowCapabilityHash: capabilityHash, context: context("spawn-op") });
		fs.mkdirSync(spawn.record.asyncDir, { recursive: true });
		const ready = publishLaunchReady(spawn.record.asyncDir, spawn.record.operationId, spawn.record.runId, 10);
		releaseLaunch(spawn.record, ready, 20);
		assert.equal(JSON.parse(fs.readFileSync(launchGoPath(spawn.record.asyncDir), "utf8")).runId, spawn.record.runId);

		const first = reserveLaunchOperation({ asyncDirRoot: root, operationId: "resume-1", kind: "resume", requestDigest: "one", workflowCapabilityHash: capabilityHash, context: resumeContext("resume-1", "paused-run") });
		assert.equal(first.created, true);
		assert.throws(() => reserveLaunchOperation({ asyncDirRoot: root, operationId: "resume-2", kind: "resume", requestDigest: "two", workflowCapabilityHash: capabilityHash, context: resumeContext("resume-2", "paused-run") }), /already claimed/);
	} finally { fs.rmSync(root, { recursive: true, force: true }); }
});
