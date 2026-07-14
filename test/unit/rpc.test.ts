import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { describe, it } from "node:test";
import { acknowledgeSteer, markSteerReady, steerRequestsDir, stepSteerInboxDir, workflowControlRequestsDir } from "../../src/runs/background/control-channel.ts";
import { launchGoPath, publishLaunchReady } from "../../src/runs/background/launch-operations.ts";
import { hashWorkflowCapability } from "../../src/shared/runtime-protocol.ts";
import {
	SUBAGENT_RPC_PROTOCOL_VERSION,
	SUBAGENT_RPC_READY_EVENT,
	SUBAGENT_RPC_REQUEST_EVENT,
	SUBAGENT_RPC_V1_READY_EVENT,
	SUBAGENT_RPC_V1_REQUEST_EVENT,
	registerSubagentRpcBridge,
	subagentRpcReplyEvent,
	type SubagentRpcReplyEnvelope,
} from "../../src/extension/rpc.ts";

class FakeEvents {
	readonly emitted: Array<{ event: string; data: unknown }> = [];
	private handlers = new Map<string, Array<(data: unknown) => void>>();
	on(event: string, handler: (data: unknown) => void): () => void { const list = this.handlers.get(event) ?? []; list.push(handler); this.handlers.set(event, list); return () => this.handlers.set(event, (this.handlers.get(event) ?? []).filter((candidate) => candidate !== handler)); }
	emit(event: string, data: unknown): void { this.emitted.push({ event, data }); for (const handler of [...(this.handlers.get(event) ?? [])]) handler(data); }
}

function once(events: FakeEvents, event: string): Promise<unknown> { return new Promise((resolve) => { const unsubscribe = events.on(event, (payload) => { unsubscribe(); resolve(payload); }); }); }
function ctx() { return { cwd: "/repo", sessionManager: { getSessionId: () => "session-123", getSessionFile: () => "/sessions/parent.jsonl" } } as any; }
async function request(events: FakeEvents, requestId: string, method: string, params?: unknown): Promise<SubagentRpcReplyEnvelope> {
	const reply = once(events, subagentRpcReplyEvent(requestId)) as Promise<SubagentRpcReplyEnvelope>;
	events.emit(SUBAGENT_RPC_REQUEST_EVENT, { version: SUBAGENT_RPC_PROTOCOL_VERSION, requestId, method, ...(params !== undefined ? { params } : {}) });
	return reply;
}
async function requestV1(events: FakeEvents, requestId: string, method: string, params?: unknown): Promise<SubagentRpcReplyEnvelope> {
	const reply = once(events, subagentRpcReplyEvent(requestId, 1)) as Promise<SubagentRpcReplyEnvelope>;
	events.emit(SUBAGENT_RPC_V1_REQUEST_EVENT, { version: 1, requestId, method, ...(params !== undefined ? { params } : {}) });
	return reply;
}

const provenance = { workflowId: "wf-1", nodeId: "node-1", attemptId: "attempt-1", ownerLeaseEpoch: 1 };
const capability = "workflow-secret";

describe("subagent extension RPC bridge v2", () => {
	it("preserves the published v1 channels, envelopes, and execution methods", async () => {
		const events = new FakeEvents();
		const calls: Array<{ action?: string; async?: boolean }> = [];
		const bridge = registerSubagentRpcBridge({
			events,
			getContext: () => ctx(),
			execute: async (_id, params) => {
				calls.push(params);
				return { content: [{ type: "text", text: "ok" }], details: { mode: "management", results: [] } } as any;
			},
		});
		const readyPromise = once(events, SUBAGENT_RPC_V1_READY_EVENT);
		bridge.emitReady(ctx());
		const ready = await readyPromise as { version: number; methods: string[] };
		assert.equal(ready.version, 1);
		assert.deepEqual(ready.methods, ["ping", "status", "spawn", "interrupt", "stop"]);
		assert.equal((await requestV1(events, "v1-ping", "ping")).version, 1);
		assert.equal((await requestV1(events, "v1-spawn", "spawn", { agent: "worker", task: "work" })).success, true);
		assert.equal((await requestV1(events, "v1-status", "status", { id: "run-1" })).success, true);
		assert.equal((await requestV1(events, "v1-interrupt", "interrupt", { id: "run-1" })).success, true);
		assert.equal(calls[0]?.async, true);
		assert.equal(calls[1]?.action, "status");
		assert.equal(calls[2]?.action, "interrupt");
		bridge.dispose();
	});

	it("advertises the workflow reliability contract", async () => {
		const events = new FakeEvents();
		const bridge = registerSubagentRpcBridge({ events, getContext: () => ctx(), execute: async () => assert.fail("ping should not execute") });
		const readyPromise = once(events, SUBAGENT_RPC_READY_EVENT);
		bridge.emitReady(ctx());
		const ready = await readyPromise as { version: number; methods: string[]; capabilities: Record<string, boolean>; session: { sessionId?: string; sessionFile?: string } };
		assert.equal(ready.version, 2);
		assert.equal(ready.session.sessionId, "session-123");
		assert.equal(ready.session.sessionFile, "/sessions/parent.jsonl");
		assert.ok(ready.methods.includes("lookup") && ready.methods.includes("resume") && ready.methods.includes("steer"));
		assert.equal(ready.capabilities.causalControls, true);
		bridge.dispose();
	});

	it("keeps malformed request ids off unsafe reply channels", async () => {
		const events = new FakeEvents();
		const bridge = registerSubagentRpcBridge({ events, getContext: () => ctx(), execute: async () => assert.fail("should not execute") });
		const replyPromise = once(events, subagentRpcReplyEvent("unknown")) as Promise<SubagentRpcReplyEnvelope>;
		events.emit(SUBAGENT_RPC_REQUEST_EVENT, { version: 2, requestId: "bad\nchannel", method: "ping" });
		assert.equal((await replyPromise).success, false);
		bridge.dispose();
	});

	it("reserves one durable run for replayed spawn operation ids and supports lookup", async () => {
		const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-rpc-operation-"));
		try {
			const events = new FakeEvents();
			let executeCalls = 0;
			const bridge = registerSubagentRpcBridge({
				events, getContext: () => ctx(), asyncDirRoot: root,
				execute: async (_id, _params, _signal, _update, _ctx, runtime) => {
					executeCalls++;
					assert.ok(runtime);
					const dir = path.join(root, runtime.runId);
					fs.mkdirSync(dir, { recursive: true });
					fs.writeFileSync(path.join(dir, "status.json"), JSON.stringify({ runId: runtime.runId, state: "running", pid: 4242, startedAt: Date.now(), mode: "single", cwd: runtime.effectiveExecution.cwd, sessionIdentity: runtime.sessionIdentity, runtimeLaunch: runtime }));
					publishLaunchReady(dir, runtime.operationId, runtime.runId);
					return { content: [{ type: "text", text: "started" }], details: { mode: "single", results: [], runId: runtime.runId, asyncId: runtime.runId, asyncDir: dir } } as any;
				},
			});
			const params = { agent: "worker", task: "work", cwd: root, operationId: "operation-1", workflowCapability: capability, provenance, notificationMode: "event-only" };
			const first = await request(events, "spawn-1", "spawn", params);
			const replay = await request(events, "spawn-2", "spawn", params);
			const lookup = await request(events, "lookup-1", "lookup", { operationId: "operation-1", workflowCapability: capability, provenance });
			assert.equal(first.success, true);
			assert.equal(replay.success, true);
			assert.equal(lookup.success, true);
			assert.equal(executeCalls, 1);
			assert.equal((first as any).data.runId, (replay as any).data.runId);
			assert.equal((lookup as any).data.runId, (first as any).data.runId);
			bridge.dispose();
		} finally { fs.rmSync(root, { recursive: true, force: true }); }
	});

	it("waits for cold runner readiness before releasing the launch barrier", async () => {
		const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-rpc-cold-ready-"));
		try {
			const events = new FakeEvents();
			const bridge = registerSubagentRpcBridge({
				events,
				getContext: () => ctx(),
				asyncDirRoot: root,
				runtimeReadyTimeoutMs: 500,
				execute: async (_id, _params, _signal, _update, _ctx, runtime) => {
					assert.ok(runtime);
					const dir = path.join(root, runtime.runId);
					fs.mkdirSync(dir, { recursive: true });
					setTimeout(() => {
						fs.writeFileSync(path.join(dir, "status.json"), JSON.stringify({ runId: runtime.runId, state: "queued", pid: process.pid, startedAt: Date.now(), mode: "single", cwd: runtime.effectiveExecution.cwd, sessionIdentity: runtime.sessionIdentity, runtimeLaunch: runtime }));
						publishLaunchReady(dir, runtime.operationId, runtime.runId);
					}, 75);
					return { content: [{ type: "text", text: "started" }], details: { mode: "single", results: [], runId: runtime.runId, asyncId: runtime.runId, asyncDir: dir } } as any;
				},
			});
			const params = { agent: "worker", task: "work", cwd: root, operationId: "cold-operation", workflowCapability: capability, provenance, notificationMode: "event-only" };
			const reply = await request(events, "cold-spawn", "spawn", params);
			assert.equal(reply.success, true, JSON.stringify(reply));
			const asyncDir = (reply as any).data.asyncDir as string;
			assert.equal(fs.existsSync(launchGoPath(asyncDir)), true);
			assert.equal((reply as any).data.operation.state, "launched");
			bridge.dispose();
		} finally { fs.rmSync(root, { recursive: true, force: true }); }
	});

	it("returns compact status and requires matching capability/provenance for causal stop", async () => {
		const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-rpc-control-"));
		try {
			const events = new FakeEvents();
			const runId = "run-control";
			const asyncDir = path.join(root, runId);
			fs.mkdirSync(asyncDir, { recursive: true });
			fs.writeFileSync(path.join(asyncDir, "status.json"), JSON.stringify({ runId, mode: "single", state: "running", pid: 4242, startedAt: Date.now(), cwd: "/repo", sessionId: "/sessions/parent.jsonl", sessionIdentity: { orchestratorSessionId: "session-123", orchestratorSessionFile: "/sessions/parent.jsonl", ...provenance, workflowCapabilityHash: hashWorkflowCapability(provenance.workflowId, capability) }, runtimeLaunch: { operationId: "op", runId, provenance, effectiveExecution: { agent: "worker", cwd: "/repo", notificationMode: "event-only" } } }));
			const bridge = registerSubagentRpcBridge({ events, getContext: () => ctx(), execute: async () => assert.fail("control should not execute"), asyncDirRoot: root, resultsDir: path.join(root, "results"), kill: () => true });
			const status = await request(events, "status-1", "status", { runId });
			assert.equal(status.success, true);
			assert.equal((status as any).data.sessionIdentity.orchestratorSessionId, "session-123");
			const rejected = await request(events, "stop-bad", "stop", { runId, controlRequestId: "control-1", workflowCapability: "wrong", provenance });
			assert.equal(rejected.success, false);
			assert.equal((rejected as any).error.code, "unauthorized");
			const accepted = await request(events, "stop-good", "stop", { runId, controlRequestId: "control-1", workflowCapability: capability, provenance });
			assert.equal(accepted.success, true, JSON.stringify(accepted));
			assert.equal((accepted as any).data.requestedState, "stopping");
			const replayed = await request(events, "stop-replay", "stop", { runId, controlRequestId: "control-1", workflowCapability: capability, provenance });
			assert.equal(replayed.success, true);
			assert.equal(fs.readdirSync(workflowControlRequestsDir(asyncDir)).length, 1);
			bridge.dispose();
		} finally { fs.rmSync(root, { recursive: true, force: true }); }
	});

	it("replays an acknowledged steer without queueing duplicate guidance", async () => {
		const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-rpc-steer-replay-"));
		try {
			const events = new FakeEvents();
			const runId = "run-steer";
			const asyncDir = path.join(root, runId);
			const inbox = stepSteerInboxDir(asyncDir, 0);
			fs.mkdirSync(inbox, { recursive: true });
			markSteerReady(inbox);
			acknowledgeSteer(inbox, { type: "steer", id: "steer-1", ts: 1, message: "focus here", targetIndex: 0 });
			fs.writeFileSync(path.join(asyncDir, "status.json"), JSON.stringify({
				runId, mode: "single", state: "running", pid: process.pid, startedAt: 1, cwd: root,
				steps: [{ agent: "worker", status: "running" }],
				sessionIdentity: { orchestratorSessionId: "session-123", ...provenance, workflowCapabilityHash: hashWorkflowCapability(provenance.workflowId, capability) },
			}));
			const bridge = registerSubagentRpcBridge({ events, getContext: () => ctx(), execute: async () => assert.fail("steer should not execute"), asyncDirRoot: root });
			const replay = await request(events, "steer-replay", "steer", { runId, index: 0, controlRequestId: "steer-1", message: "focus here", workflowCapability: capability, provenance });
			assert.equal(replay.success, true, JSON.stringify(replay));
			assert.equal(fs.existsSync(steerRequestsDir(asyncDir)), false);
			const conflict = await request(events, "steer-conflict", "steer", { runId, index: 0, controlRequestId: "steer-1", message: "different", workflowCapability: capability, provenance });
			assert.equal(conflict.success, false);
			bridge.dispose();
		} finally { fs.rmSync(root, { recursive: true, force: true }); }
	});

	it("resumes a paused source once, preserving the complete execution contract", async () => {
		const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-rpc-resume-"));
		try {
			const events = new FakeEvents();
			const sourceRunId = "paused-run";
			const sourceDir = path.join(root, sourceRunId);
			const sessionFile = path.join(sourceDir, "child.jsonl");
			const sourceProvenance = { ...provenance, attemptId: "attempt-1" };
			const replacementProvenance = { ...provenance, attemptId: "attempt-2" };
			fs.mkdirSync(sourceDir, { recursive: true });
			fs.writeFileSync(sessionFile, "{}\n");
			fs.writeFileSync(path.join(sourceDir, "status.json"), JSON.stringify({
				runId: sourceRunId,
				mode: "single",
				state: "paused",
				startedAt: 1,
				endedAt: 2,
				cwd: root,
				sessionFile,
				terminal: { reason: "paused", at: 2, controlRequestId: "pause-1" },
				sessionIdentity: { orchestratorSessionId: "session-123", orchestratorSessionFile: "/sessions/parent.jsonl", ...sourceProvenance, workflowCapabilityHash: hashWorkflowCapability(sourceProvenance.workflowId, capability) },
				runtimeLaunch: { operationId: "source-op", runId: sourceRunId, kind: "spawn", provenance: sourceProvenance, effectiveExecution: { agent: "worker", model: "openai-codex/gpt-5.6-sol", thinking: "high", cwd: root, timeoutMs: 60_000, notificationMode: "event-only" } },
			}));
			let executeCalls = 0;
			const bridge = registerSubagentRpcBridge({
				events,
				getContext: () => ctx(),
				asyncDirRoot: root,
				execute: async (_id, params, _signal, _update, _ctx, runtime) => {
					executeCalls += 1;
					assert.equal(params.action, "resume");
					assert.equal(params.id, sourceRunId);
					assert.equal(runtime?.sourceRunId, sourceRunId);
					assert.equal(runtime?.sourceSessionFile, sessionFile);
					assert.deepEqual(runtime?.effectiveExecution, { agent: "worker", model: "openai-codex/gpt-5.6-sol", thinking: "high", cwd: root, timeoutMs: 60_000, notificationMode: "event-only" });
					const dir = path.join(root, runtime!.runId);
					fs.mkdirSync(dir, { recursive: true });
					fs.writeFileSync(path.join(dir, "status.json"), JSON.stringify({ runId: runtime!.runId, state: "running", pid: 4242, startedAt: Date.now(), cwd: root, sessionIdentity: runtime!.sessionIdentity, runtimeLaunch: runtime }));
					publishLaunchReady(dir, runtime!.operationId, runtime!.runId, 4242);
					return { content: [{ type: "text", text: "resumed" }], details: { mode: "single", results: [], runId: runtime!.runId, asyncId: runtime!.runId, asyncDir: dir } } as any;
				},
			});
			const params = {
				agent: "worker",
				model: "openai-codex/gpt-5.6-sol",
				thinking: "high",
				cwd: root,
				timeoutMs: 60_000,
				notificationMode: "event-only",
				operationId: "resume-op",
				workflowCapability: capability,
				provenance: replacementProvenance,
				sourceRunId,
				sourceSessionFile: sessionFile,
				sourceAttemptId: sourceProvenance.attemptId,
				sourceProvenance,
				message: "continue",
			};
			const first = await request(events, "resume-1", "resume", params);
			const replay = await request(events, "resume-2", "resume", params);
			const lookup = await request(events, "resume-lookup", "lookup", { operationId: "resume-op", workflowCapability: capability, provenance: replacementProvenance });
			assert.equal(first.success, true, JSON.stringify(first));
			assert.equal(replay.success, true, JSON.stringify(replay));
			assert.equal(lookup.success, true, JSON.stringify(lookup));
			assert.equal(executeCalls, 1);
			assert.equal((first as any).data.runId, (replay as any).data.runId);
			assert.equal((lookup as any).data.runId, (first as any).data.runId);
			bridge.dispose();
		} finally { fs.rmSync(root, { recursive: true, force: true }); }
	});
});
