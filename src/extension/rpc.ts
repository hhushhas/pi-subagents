import * as path from "node:path";
import * as fs from "node:fs";
import { randomUUID } from "node:crypto";
import type { AgentToolResult } from "@earendil-works/pi-agent-core";
import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { Compile } from "typebox/compile";
import { resolveAsyncRunLocation } from "../runs/background/async-resume.ts";
import { deliverTimeoutRequest, deliverWorkflowControl, isSteerReady, readSteerAck, requestAsyncSteer, stepSteerInboxDir, waitForSteerAck } from "../runs/background/control-channel.ts";
import { digestLaunchRequest, readLaunchOperation, readLaunchReady, releaseLaunch, reserveLaunchOperation, updateLaunchOperation, type LaunchOperationRecord } from "../runs/background/launch-operations.ts";
import { reconcileAsyncRun } from "../runs/background/stale-run-reconciler.ts";
import type { SubagentParamsLike } from "../runs/foreground/subagent-executor.ts";
import { type Details, ASYNC_DIR, RESULTS_DIR } from "../shared/types.ts";
import { readStatus } from "../shared/utils.ts";
import { resolveCurrentSessionIdentity } from "../shared/session-identity.ts";
import {
	SUBAGENT_RPC_PROTOCOL_VERSION,
	hashWorkflowCapability,
	verifyWorkflowCapability,
	type ControlResult,
	type NotificationMode,
	type RuntimeLaunchContext,
	type WorkflowLaunchProvenance,
} from "../shared/runtime-protocol.ts";
import { SubagentParams } from "./schemas.ts";

export { SUBAGENT_RPC_PROTOCOL_VERSION };
export const SUBAGENT_RPC_REQUEST_EVENT = "subagents:rpc:v2:request";
export const SUBAGENT_RPC_READY_EVENT = "subagents:rpc:v2:ready";
export const SUBAGENT_RPC_REPLY_EVENT_PREFIX = "subagents:rpc:v2:reply:";
export const SUBAGENT_RPC_V1_REQUEST_EVENT = "subagents:rpc:v1:request";
export const SUBAGENT_RPC_V1_READY_EVENT = "subagents:rpc:v1:ready";
export const SUBAGENT_RPC_V1_REPLY_EVENT_PREFIX = "subagents:rpc:v1:reply:";

export const SUBAGENT_RPC_METHODS = ["ping", "status", "lookup", "spawn", "interrupt", "stop", "steer", "resume"] as const;
const SUBAGENT_RPC_V1_METHODS = ["ping", "status", "spawn", "interrupt", "stop"] as const;
export type SubagentRpcMethod = typeof SUBAGENT_RPC_METHODS[number];
type SubagentRpcVersion = 1 | typeof SUBAGENT_RPC_PROTOCOL_VERSION;

export interface SubagentRpcRequestEnvelope {
	version: SubagentRpcVersion;
	requestId: string;
	method: SubagentRpcMethod;
	params?: unknown;
	source?: {
		extension?: string;
		[key: string]: unknown;
	};
}

export type SubagentRpcReplyEnvelope<T = unknown> = {
	version: SubagentRpcVersion;
	requestId: string;
	method?: SubagentRpcMethod;
	success: true;
	data: T;
} | {
	version: SubagentRpcVersion;
	requestId: string;
	method?: SubagentRpcMethod;
	success: false;
	error: {
		code: SubagentRpcErrorCode;
		message: string;
	};
};

type SubagentRpcErrorCode =
	| "invalid_request"
	| "invalid_params"
	| "unsupported_version"
	| "unsupported_method"
	| "no_active_session"
	| "execution_failed"
	| "not_found"
	| "invalid_state"
	| "unauthorized"
	| "unknown_outcome";

interface EventBus {
	on(event: string, handler: (data: unknown) => void): (() => void) | void;
	emit(event: string, data: unknown): void;
}

interface RegisterSubagentRpcBridgeOptions {
	events: EventBus;
	getContext: () => ExtensionContext | null;
	execute: (
		id: string,
		params: SubagentParamsLike,
		signal: AbortSignal,
		onUpdate: ((result: AgentToolResult<Details>) => void) | undefined,
		ctx: ExtensionContext,
		runtimeLaunch?: RuntimeLaunchContext,
	) => Promise<AgentToolResult<Details>>;
	asyncDirRoot?: string;
	resultsDir?: string;
	kill?: (pid: number, signal?: NodeJS.Signals | 0) => boolean;
	now?: () => number;
}

interface WorkflowMutation {
	workflowCapability: string;
	provenance: WorkflowLaunchProvenance;
}

class SubagentRpcError extends Error {
	readonly code: SubagentRpcErrorCode;

	constructor(code: SubagentRpcErrorCode, message: string) {
		super(message);
		this.name = "SubagentRpcError";
		this.code = code;
	}
}

const subagentParamsValidator = Compile(SubagentParams);
const inFlightLaunches = new Map<string, Promise<unknown>>();

async function waitForRuntimeReady(record: LaunchOperationRecord, timeoutMs = 2000): Promise<{ status: NonNullable<ReturnType<typeof readStatus>>; ready: NonNullable<ReturnType<typeof readLaunchReady>> } | undefined> {
	const deadline = Date.now() + timeoutMs;
	while (Date.now() < deadline) {
		const status = readStatus(record.asyncDir);
		const ready = readLaunchReady(record.asyncDir);
		if (status && ready && status.runId === record.runId && status.runtimeLaunch?.operationId === record.operationId && ready.runId === record.runId && ready.operationId === record.operationId) return { status, ready };
		await new Promise((resolve) => setTimeout(resolve, 20));
	}
	const status = readStatus(record.asyncDir);
	const ready = readLaunchReady(record.asyncDir);
	return status && ready && status.runId === record.runId && status.runtimeLaunch?.operationId === record.operationId && ready.runId === record.runId && ready.operationId === record.operationId ? { status, ready } : undefined;
}

function activatePreparedOperation(record: LaunchOperationRecord, options: RegisterSubagentRpcBridgeOptions): LaunchOperationRecord {
	const status = readStatus(record.asyncDir);
	const ready = readLaunchReady(record.asyncDir);
	if (!status || !ready || status.runId !== record.runId || status.runtimeLaunch?.operationId !== record.operationId || ready.runId !== record.runId || ready.operationId !== record.operationId) return record;
	try { (options.kill ?? process.kill)(ready.pid, 0); }
	catch (error) { if ((error as NodeJS.ErrnoException).code !== "EPERM") return record; }
	const launched = record.state === "launched" ? record : updateLaunchOperation(record, { state: "launched", pid: ready.pid }, options.asyncDirRoot);
	releaseLaunch(launched, ready, options.now?.());
	return launched;
}

export function subagentRpcReplyEvent(requestId: string, version: SubagentRpcVersion = SUBAGENT_RPC_PROTOCOL_VERSION): string {
	return `${version === 1 ? SUBAGENT_RPC_V1_REPLY_EVENT_PREFIX : SUBAGENT_RPC_REPLY_EVENT_PREFIX}${requestId}`;
}

function isRecord(value: unknown): value is Record<string, unknown> {
	return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function assertRequestId(value: unknown): string {
	if (typeof value !== "string" || value.trim().length === 0 || /[\r\n]/.test(value)) {
		throw new SubagentRpcError("invalid_request", "RPC requestId must be a non-empty string without newlines.");
	}
	return value;
}

function assertRecordParams(params: unknown, method: SubagentRpcMethod): Record<string, unknown> {
	if (params === undefined) return {};
	if (!isRecord(params)) throw new SubagentRpcError("invalid_params", `RPC ${method} params must be an object.`);
	return params;
}

function assertSubagentParams(params: SubagentParamsLike, label: string): void {
	if (subagentParamsValidator.Check(params)) return;
	const messages = [...subagentParamsValidator.Errors(params)]
		.slice(0, 4)
		.map((error) => error.message);
	throw new SubagentRpcError("invalid_params", `${label}: ${messages.join("; ") || "invalid subagent parameters"}`);
}

function textFromToolResult(result: AgentToolResult<Details>): string {
	return result.content
		.filter((part): part is { type: "text"; text: string } => part.type === "text")
		.map((part) => part.text)
		.join("\n");
}

function dataFromToolResult(result: AgentToolResult<Details>): { text: string; details?: Details; isError?: boolean } {
	return {
		text: textFromToolResult(result),
		...(result.details ? { details: result.details } : {}),
		...(result.isError ? { isError: true } : {}),
	};
}

function failIfToolError(result: AgentToolResult<Details>): void {
	if (!result.isError) return;
	throw new SubagentRpcError("execution_failed", textFromToolResult(result) || "Subagent RPC execution failed.");
}

function normalizeTargetParams(params: unknown, method: SubagentRpcMethod): Pick<SubagentParamsLike, "id" | "runId" | "dir" | "index"> {
	const input = assertRecordParams(params, method);
	const output: Pick<SubagentParamsLike, "id" | "runId" | "dir" | "index"> = {};
	if (input.id !== undefined) output.id = input.id as string;
	if (input.runId !== undefined) output.runId = input.runId as string;
	if (input.dir !== undefined) output.dir = input.dir as string;
	if (input.index !== undefined) output.index = input.index as number;
	return output;
}

function sessionData(ctx: ExtensionContext | null): { cwd?: string; sessionId?: string; sessionFile?: string | null } {
	if (!ctx) return {};
	return {
		cwd: ctx.cwd,
		sessionId: ctx.sessionManager.getSessionId() ?? undefined,
		sessionFile: ctx.sessionManager.getSessionFile() ?? null,
	};
}

function pingData(ctx: ExtensionContext | null, version: SubagentRpcVersion = SUBAGENT_RPC_PROTOCOL_VERSION) {
	if (version === 1) {
		return {
			version,
			methods: [...SUBAGENT_RPC_V1_METHODS],
			capabilities: { status: true, asyncSpawn: true, interrupt: true, stop: true },
			events: { ready: SUBAGENT_RPC_V1_READY_EVENT, request: SUBAGENT_RPC_V1_REQUEST_EVENT, replyPrefix: SUBAGENT_RPC_V1_REPLY_EVENT_PREFIX },
			session: sessionData(ctx),
		};
	}
	return {
		version: SUBAGENT_RPC_PROTOCOL_VERSION,
		methods: [...SUBAGENT_RPC_METHODS],
		capabilities: {
			status: true,
			asyncSpawn: true,
			idempotentSpawn: true,
			idempotentResume: true,
			causalControls: true,
			eventOnlyNotifications: true,
			steerAcknowledgement: true,
			interrupt: true,
			stop: true,
		},
		events: {
			ready: SUBAGENT_RPC_READY_EVENT,
			request: SUBAGENT_RPC_REQUEST_EVENT,
			replyPrefix: SUBAGENT_RPC_REPLY_EVENT_PREFIX,
		},
		session: sessionData(ctx),
	};
}

async function executeChecked(
	options: RegisterSubagentRpcBridgeOptions,
	ctx: ExtensionContext,
	requestId: string,
	method: SubagentRpcMethod,
	params: SubagentParamsLike,
	runtimeLaunch?: RuntimeLaunchContext,
): Promise<{ text: string; details?: Details; isError?: boolean }> {
	assertSubagentParams(params, `RPC ${method} params`);
	const controller = new AbortController();
	const result = await options.execute(`rpc-${method}-${requestId}`, params, controller.signal, undefined, ctx, runtimeLaunch);
	failIfToolError(result);
	return dataFromToolResult(result);
}

function requiredString(value: unknown, label: string): string {
	if (typeof value !== "string" || !value.trim() || /[\r\n]/.test(value)) {
		throw new SubagentRpcError("invalid_params", `${label} must be a non-empty string without newlines.`);
	}
	return value.trim();
}

function parseWorkflowMutation(input: Record<string, unknown>): WorkflowMutation {
	const workflowCapability = requiredString(input.workflowCapability, "workflowCapability");
	if (!isRecord(input.provenance)) throw new SubagentRpcError("invalid_params", "provenance is required.");
	const provenance: WorkflowLaunchProvenance = {
		workflowId: requiredString(input.provenance.workflowId, "provenance.workflowId"),
		nodeId: requiredString(input.provenance.nodeId, "provenance.nodeId"),
		attemptId: requiredString(input.provenance.attemptId, "provenance.attemptId"),
		ownerLeaseEpoch: Number(input.provenance.ownerLeaseEpoch),
	};
	if (!Number.isInteger(provenance.ownerLeaseEpoch) || provenance.ownerLeaseEpoch < 1) {
		throw new SubagentRpcError("invalid_params", "provenance.ownerLeaseEpoch must be a positive integer.");
	}
	return { workflowCapability, provenance };
}

function stripRuntimeParams(input: Record<string, unknown>): SubagentParamsLike {
	const output = { ...input };
	for (const key of ["operationId", "workflowCapability", "provenance", "notificationMode", "sourceRunId", "sourceSessionFile", "sourceAttemptId", "sourceProvenance", "thinking"]) delete output[key];
	return output as SubagentParamsLike;
}

function runtimeLaunchContext(input: Record<string, unknown>, ctx: ExtensionContext, kind: "spawn" | "resume", runId: string): RuntimeLaunchContext {
	const operationId = requiredString(input.operationId, "operationId");
	const { workflowCapability, provenance } = parseWorkflowMutation(input);
	const current = resolveCurrentSessionIdentity(ctx.sessionManager);
	const notificationMode: NotificationMode = input.notificationMode === "event-only" ? "event-only" : "default";
	const cwd = path.resolve(typeof input.cwd === "string" ? input.cwd : ctx.cwd);
	const capabilityHash = hashWorkflowCapability(provenance.workflowId, workflowCapability);
	return {
		operationId,
		runId,
		kind,
		sessionIdentity: {
			...current,
			workflowId: provenance.workflowId,
			nodeId: provenance.nodeId,
			attemptId: provenance.attemptId,
			ownerLeaseEpoch: provenance.ownerLeaseEpoch,
			workflowCapabilityHash: capabilityHash,
		},
		provenance,
		effectiveExecution: {
			agent: requiredString(input.agent, "agent"),
			...(typeof input.model === "string" ? { model: input.model } : {}),
			...(typeof input.thinking === "string" ? { thinking: input.thinking } : {}),
			cwd,
			...(typeof input.timeoutMs === "number" ? { timeoutMs: input.timeoutMs } : {}),
			notificationMode,
		},
		...(typeof input.sourceRunId === "string" ? { sourceRunId: input.sourceRunId } : {}),
		...(typeof input.sourceSessionFile === "string" ? { sourceSessionFile: input.sourceSessionFile } : {}),
		...(typeof input.sourceAttemptId === "string" ? { sourceAttemptId: input.sourceAttemptId } : {}),
		...(isRecord(input.sourceProvenance) ? { sourceProvenance: input.sourceProvenance as unknown as WorkflowLaunchProvenance } : {}),
	};
}

function operationData(record: ReturnType<typeof readLaunchOperation>) {
	if (!record) throw new SubagentRpcError("not_found", "Launch operation was not found.");
	return {
		operationId: record.operationId,
		kind: record.kind,
		state: record.state,
		runId: record.runId,
		asyncDir: record.asyncDir,
		pid: record.pid,
		error: record.error,
	};
}

function spawnParams(params: unknown): SubagentParamsLike {
	const input = assertRecordParams(params, "spawn");
	if (input.action !== undefined) {
		throw new SubagentRpcError("invalid_params", "RPC spawn does not accept management/control actions. Use status or interrupt RPC methods instead.");
	}
	if (input.async === false) {
		throw new SubagentRpcError("invalid_params", "RPC spawn only supports detached async launches; omit async or set async: true.");
	}
	if (input.clarify === true) {
		throw new SubagentRpcError("invalid_params", "RPC spawn cannot open the clarify UI; omit clarify or set clarify: false.");
	}
	return { ...stripRuntimeParams(input), async: true, clarify: false };
}

function legacyStopAsyncRun(
	params: unknown,
	options: RegisterSubagentRpcBridgeOptions,
	ctx: ExtensionContext,
): { runId: string; asyncDir: string; previousState: string; state: "stopping"; message: string } {
	const target = normalizeTargetParams(params, "stop");
	assertSubagentParams({ action: "status", ...target }, "RPC stop target params");
	const resultsDir = options.resultsDir ?? RESULTS_DIR;
	let location;
	try {
		location = resolveAsyncRunLocation(target, options.asyncDirRoot ?? ASYNC_DIR, resultsDir);
	} catch (error) {
		throw new SubagentRpcError("invalid_params", error instanceof Error ? error.message : String(error));
	}
	if (!location.asyncDir) throw new SubagentRpcError("not_found", "Async run not found or already completed; stop requires a live async run directory.");

	const currentSessionId = ctx.sessionManager.getSessionId();
	const currentSessionFile = ctx.sessionManager.getSessionFile();
	const initialStatus = readStatus(location.asyncDir);
	const initialRunId = initialStatus?.runId ?? location.resolvedId ?? path.basename(location.asyncDir);
	if (!initialStatus) throw new SubagentRpcError("not_found", `Status file not found for async run '${initialRunId}'.`);
	const belongsToSession = (sessionId: string | undefined) => Boolean(sessionId && (sessionId === currentSessionId || sessionId === currentSessionFile));
	if (!belongsToSession(initialStatus.sessionId)) throw new SubagentRpcError("not_found", `Async run '${initialRunId}' was not found in the active session.`);

	let status;
	try {
		status = reconcileAsyncRun(location.asyncDir, { resultsDir, kill: options.kill, now: options.now }).status;
	} catch (error) {
		throw new SubagentRpcError("execution_failed", error instanceof Error ? error.message : String(error));
	}
	const runId = status?.runId ?? initialRunId;
	if (!status) throw new SubagentRpcError("not_found", `Status file not found for async run '${runId}'.`);
	if (!belongsToSession(status.sessionId)) throw new SubagentRpcError("not_found", `Async run '${runId}' was not found in the active session.`);
	if (status.state !== "running") throw new SubagentRpcError("invalid_state", `Async run ${runId} is ${status.state}; stop only supports running async runs.`);

	try {
		deliverTimeoutRequest({ asyncDir: location.asyncDir, pid: status.pid, kill: options.kill, now: options.now, source: "rpc-v1-stop" });
	} catch (error) {
		throw new SubagentRpcError("execution_failed", error instanceof Error ? error.message : String(error));
	}
	return { runId, asyncDir: location.asyncDir, previousState: status.state, state: "stopping", message: `Stop requested for async run ${runId}.` };
}

function controlAsyncRun(
	params: unknown,
	options: RegisterSubagentRpcBridgeOptions,
	ctx: ExtensionContext,
	action: "pause" | "stop",
): ControlResult {
	const input = assertRecordParams(params, action === "pause" ? "interrupt" : "stop");
	const target = normalizeTargetParams(input, action === "pause" ? "interrupt" : "stop");
	assertSubagentParams({ action: "status", ...target }, `RPC ${action} target params`);
	const asyncDirRoot = options.asyncDirRoot ?? ASYNC_DIR;
	const resultsDir = options.resultsDir ?? RESULTS_DIR;
	let location;
	try {
		location = resolveAsyncRunLocation(target, asyncDirRoot, resultsDir);
	} catch (error) {
		throw new SubagentRpcError("invalid_params", error instanceof Error ? error.message : String(error));
	}
	if (!location.asyncDir) {
		throw new SubagentRpcError("not_found", `Async run not found or already completed; ${action} requires a live async run directory.`);
	}

	const initialStatus = readStatus(location.asyncDir);
	const initialRunId = initialStatus?.runId ?? location.resolvedId ?? path.basename(location.asyncDir);
	if (!initialStatus) throw new SubagentRpcError("not_found", `Status file not found for async run '${initialRunId}'.`);

	let status;
	try {
		status = reconcileAsyncRun(location.asyncDir, { resultsDir, kill: options.kill, now: options.now }).status;
	} catch (error) {
		throw new SubagentRpcError("execution_failed", error instanceof Error ? error.message : String(error));
	}
	const runId = status?.runId ?? initialRunId;
	if (!status) throw new SubagentRpcError("not_found", `Status file not found for async run '${runId}'.`);
	const identity = status.sessionIdentity;
	if (!identity?.workflowId || !identity.workflowCapabilityHash || !identity.nodeId || !identity.attemptId) {
		throw new SubagentRpcError("unauthorized", `Async run '${runId}' is a legacy or non-workflow run and is observation-only through workflow RPC.`);
	}
	const mutation = parseWorkflowMutation(input);
	if (mutation.provenance.workflowId !== identity.workflowId
		|| mutation.provenance.nodeId !== identity.nodeId
		|| mutation.provenance.attemptId !== identity.attemptId
		|| mutation.provenance.ownerLeaseEpoch !== identity.ownerLeaseEpoch
		|| !verifyWorkflowCapability(identity.workflowId, mutation.workflowCapability, identity.workflowCapabilityHash)) {
		throw new SubagentRpcError("unauthorized", `Workflow authority did not match async run '${runId}'.`);
	}
	const controlRequestId = requiredString(input.controlRequestId, "controlRequestId");
	const existingControl = status.controls?.find((control) => control.controlRequestId === controlRequestId);
	if (existingControl) {
		if (existingControl.action !== action) throw new SubagentRpcError("invalid_state", `Control '${controlRequestId}' already targets ${existingControl.action}, not ${action}.`);
		return {
			controlRequestId,
			accepted: Boolean(existingControl.acceptedAt),
			runId,
			asyncDir: location.asyncDir,
			previousState: status.state,
			requestedState: action === "stop" ? "stopping" : "pausing",
			message: `${action === "stop" ? "Stop" : "Pause"} ${controlRequestId} was already accepted for async run ${runId}.`,
		};
	}
	if (status.state !== "running") {
		throw new SubagentRpcError("invalid_state", `Async run ${runId} is ${status.state}; ${action} only supports running async runs.`);
	}

	try {
		deliverWorkflowControl({
			asyncDir: location.asyncDir,
			action,
			controlRequestId,
			pid: status.pid,
			kill: options.kill,
			now: options.now,
			source: `rpc-${action}`,
		});
	} catch (error) {
		throw new SubagentRpcError("execution_failed", error instanceof Error ? error.message : String(error));
	}

	return {
		controlRequestId,
		accepted: true,
		runId,
		asyncDir: location.asyncDir,
		previousState: status.state,
		requestedState: action === "stop" ? "stopping" : "pausing",
		message: `${action === "stop" ? "Stop" : "Pause"} requested for async run ${runId}; confirmation is pending.`,
	};
}

function lookupStatus(params: unknown, method: SubagentRpcMethod, options: RegisterSubagentRpcBridgeOptions) {
	const target = normalizeTargetParams(params, method);
	const location = resolveAsyncRunLocation(target, options.asyncDirRoot ?? ASYNC_DIR, options.resultsDir ?? RESULTS_DIR);
	if (!location.asyncDir) throw new SubagentRpcError("not_found", "Async run directory was not found.");
	const status = readStatus(location.asyncDir);
	if (!status) throw new SubagentRpcError("not_found", "Async run status was not found.");
	return { location, status };
}

function compactStatus(params: unknown, options: RegisterSubagentRpcBridgeOptions) {
	const { location, status } = lookupStatus(params, "status", options);
	return {
		runId: status.runId,
		asyncDir: location.asyncDir,
		state: status.state,
		pid: status.pid,
		cwd: status.cwd,
		startedAt: status.startedAt,
		endedAt: status.endedAt,
		lastUpdate: status.lastUpdate,
		activityState: status.activityState,
		currentTool: status.currentTool,
		turnCount: status.turnCount,
		toolCount: status.toolCount,
		totalTokens: status.totalTokens,
		totalCost: status.totalCost,
		sessionFile: status.sessionFile,
		sessionIdentity: status.sessionIdentity,
		runtimeLaunch: status.runtimeLaunch,
		notificationMode: status.notificationMode,
		controls: status.controls,
		terminal: status.terminal,
		error: status.error,
	};
}

async function launchWorkflowRun(
	input: Record<string, unknown>,
	kind: "spawn" | "resume",
	options: RegisterSubagentRpcBridgeOptions,
	ctx: ExtensionContext,
	requestId: string,
): Promise<unknown> {
	const operationId = requiredString(input.operationId, "operationId");
	const mutation = parseWorkflowMutation(input);
	const capabilityHash = hashWorkflowCapability(mutation.provenance.workflowId, mutation.workflowCapability);
	const runId = randomUUID();
	const runtime = runtimeLaunchContext(input, ctx, kind, runId);
	const requestDigest = digestLaunchRequest({
		...input,
		workflowCapability: "[redacted]",
		task: typeof input.task === "string" ? digestLaunchRequest(input.task) : undefined,
	});
	const reservation = reserveLaunchOperation({
		asyncDirRoot: options.asyncDirRoot,
		operationId,
		kind,
		requestDigest,
		workflowCapabilityHash: capabilityHash,
		context: runtime,
		now: options.now,
	});
	const key = `${capabilityHash}:${operationId}`;
	const existingPromise = inFlightLaunches.get(key);
	if (existingPromise) return existingPromise;
	if (!reservation.created) {
		const activated = activatePreparedOperation(reservation.record, options);
		if (activated.state === "launched") return operationData(activated);
		if (reservation.record.state === "failed") throw new SubagentRpcError("execution_failed", reservation.record.error ?? "Launch operation failed.");
		throw new SubagentRpcError("unknown_outcome", `Operation '${operationId}' is prepared but has no authoritative runtime status; it will not be relaunched automatically.`);
	}
	const promise = (async () => {
		try {
			const params = kind === "spawn"
				? spawnParams(input)
				: { ...stripRuntimeParams(input), action: "resume", id: requiredString(input.sourceRunId, "sourceRunId"), async: true, clarify: false } as SubagentParamsLike;
			const result = await executeChecked(options, ctx, requestId, kind, params, reservation.record.context);
			const runtimeReady = await waitForRuntimeReady(reservation.record);
			if (!runtimeReady) throw new SubagentRpcError("unknown_outcome", `Operation '${operationId}' started a runner but no authoritative runtime readiness appeared; lookup is required and replay will not launch another child.`);
			const launched = updateLaunchOperation(reservation.record, { state: "launched", pid: runtimeReady.ready.pid }, options.asyncDirRoot);
			releaseLaunch(launched, runtimeReady.ready, options.now?.());
			return { ...result, operation: operationData(launched), runId: launched.runId, asyncDir: launched.asyncDir };
		} catch (error) {
			if (!(error instanceof SubagentRpcError && error.code === "unknown_outcome")) {
				updateLaunchOperation(reservation.record, { state: "failed", error: error instanceof Error ? error.message : String(error) }, options.asyncDirRoot);
			}
			throw error;
		} finally {
			inFlightLaunches.delete(key);
		}
	})();
	inFlightLaunches.set(key, promise);
	return promise;
}

async function handleRequest(
	request: SubagentRpcRequestEnvelope,
	options: RegisterSubagentRpcBridgeOptions,
): Promise<unknown> {
	const ctx = options.getContext();
	if (request.method === "ping") return pingData(ctx, request.version);
	if (!ctx) throw new SubagentRpcError("no_active_session", "No active extension context for subagent RPC.");
	if (request.version === 1) {
		if (request.method === "spawn") return executeChecked(options, ctx, request.requestId, request.method, spawnParams(request.params));
		if (request.method === "status") return executeChecked(options, ctx, request.requestId, request.method, { action: "status", ...normalizeTargetParams(request.params, "status") });
		if (request.method === "interrupt") return executeChecked(options, ctx, request.requestId, request.method, { action: "interrupt", ...normalizeTargetParams(request.params, "interrupt") });
		if (request.method === "stop") return legacyStopAsyncRun(request.params, options, ctx);
		throw new SubagentRpcError("unsupported_method", `Unsupported subagent RPC v1 method: ${String(request.method)}`);
	}

	if (request.method === "spawn") {
		const input = assertRecordParams(request.params, "spawn");
		if (input.operationId !== undefined) return launchWorkflowRun(input, "spawn", options, ctx, request.requestId);
		return executeChecked(options, ctx, request.requestId, request.method, spawnParams(input));
	}
	if (request.method === "status") {
		return compactStatus(request.params, options);
	}
	if (request.method === "lookup") {
		const input = assertRecordParams(request.params, "lookup");
		const mutation = parseWorkflowMutation(input);
		const capabilityHash = hashWorkflowCapability(mutation.provenance.workflowId, mutation.workflowCapability);
		const record = readLaunchOperation({ asyncDirRoot: options.asyncDirRoot, capabilityHash, operationId: requiredString(input.operationId, "operationId") });
		return operationData(record ? activatePreparedOperation(record, options) : undefined);
	}
	if (request.method === "interrupt") {
		return controlAsyncRun(request.params, options, ctx, "pause");
	}
	if (request.method === "stop") {
		return controlAsyncRun(request.params, options, ctx, "stop");
	}
	if (request.method === "steer") {
		const input = assertRecordParams(request.params, "steer");
		const { location, status } = lookupStatus(input, "steer", options);
		const identity = status.sessionIdentity;
		const mutation = parseWorkflowMutation(input);
		if (!identity?.workflowId || !identity.workflowCapabilityHash
			|| mutation.provenance.workflowId !== identity.workflowId
			|| mutation.provenance.nodeId !== identity.nodeId
			|| mutation.provenance.attemptId !== identity.attemptId
			|| mutation.provenance.ownerLeaseEpoch !== identity.ownerLeaseEpoch
			|| !verifyWorkflowCapability(identity.workflowId, mutation.workflowCapability, identity.workflowCapabilityHash)) {
			throw new SubagentRpcError("unauthorized", `Workflow authority did not match async run '${status.runId}'.`);
		}
		if (status.state !== "running") throw new SubagentRpcError("invalid_state", `Async run ${status.runId} is ${status.state}; steer requires running.`);
		const controlRequestId = requiredString(input.controlRequestId, "controlRequestId");
		const requestedIndex = typeof input.index === "number" ? input.index : undefined;
		const runningIndex = requestedIndex ?? status.steps?.findIndex((step) => step.status === "running") ?? status.currentStep ?? 0;
		if (!Number.isInteger(runningIndex) || runningIndex < 0) throw new SubagentRpcError("invalid_state", `Async run ${status.runId} has no registered live steer target.`);
		const inbox = stepSteerInboxDir(location.asyncDir!, runningIndex);
		if (!isSteerReady(inbox)) throw new SubagentRpcError("invalid_state", `Async run ${status.runId} has no registered live steer target for step ${runningIndex}.`);
		const message = requiredString(input.message, "message");
		const existingAck = readSteerAck(inbox, controlRequestId);
		if (existingAck) {
			if (existingAck.message !== message || existingAck.targetIndex !== runningIndex) throw new SubagentRpcError("invalid_state", `Steer '${controlRequestId}' was already acknowledged with different guidance.`);
			return { controlRequestId, accepted: true, runId: status.runId, asyncDir: location.asyncDir, previousState: status.state, requestedState: status.state, message: `Steer ${controlRequestId} was already acknowledged by live run ${status.runId}.` } satisfies ControlResult;
		}
		try {
			requestAsyncSteer(location.asyncDir!, { message, targetIndex: runningIndex, id: controlRequestId, source: "rpc-steer" });
		} catch (error) {
			throw new SubagentRpcError("invalid_state", error instanceof Error ? error.message : String(error));
		}
		if (!await waitForSteerAck(inbox, controlRequestId)) throw new SubagentRpcError("unknown_outcome", `Steer ${controlRequestId} was queued but the live child did not acknowledge delivery.`);
		return { controlRequestId, accepted: true, runId: status.runId, asyncDir: location.asyncDir, previousState: status.state, requestedState: status.state, message: `Steer ${controlRequestId} was acknowledged by live run ${status.runId}.` } satisfies ControlResult;
	}
	if (request.method === "resume") {
		const input = assertRecordParams(request.params, "resume");
		const { status } = lookupStatus({ runId: input.sourceRunId }, "resume", options);
		if (status.state !== "paused" || status.terminal?.reason !== "paused") throw new SubagentRpcError("invalid_state", `Resume requires a causally confirmed paused source; ${status.runId} is ${status.state}.`);
		const identity = status.sessionIdentity;
		const sourceExecution = status.runtimeLaunch?.effectiveExecution;
		const mutation = parseWorkflowMutation(input);
		if (!isRecord(input.sourceProvenance)) throw new SubagentRpcError("invalid_params", "sourceProvenance is required.");
		const sourceProvenance: WorkflowLaunchProvenance = {
			workflowId: requiredString(input.sourceProvenance.workflowId, "sourceProvenance.workflowId"),
			nodeId: requiredString(input.sourceProvenance.nodeId, "sourceProvenance.nodeId"),
			attemptId: requiredString(input.sourceProvenance.attemptId, "sourceProvenance.attemptId"),
			ownerLeaseEpoch: Number(input.sourceProvenance.ownerLeaseEpoch),
		};
		if (!identity?.workflowId || !identity.workflowCapabilityHash || !identity.nodeId || !identity.attemptId
			|| mutation.provenance.workflowId !== identity.workflowId || mutation.provenance.nodeId !== identity.nodeId
			|| sourceProvenance.workflowId !== identity.workflowId || sourceProvenance.nodeId !== identity.nodeId
			|| sourceProvenance.attemptId !== identity.attemptId || sourceProvenance.ownerLeaseEpoch !== identity.ownerLeaseEpoch
			|| requiredString(input.sourceAttemptId, "sourceAttemptId") !== identity.attemptId
			|| !verifyWorkflowCapability(identity.workflowId, mutation.workflowCapability, identity.workflowCapabilityHash)) {
			throw new SubagentRpcError("unauthorized", `Workflow authority did not match paused source '${status.runId}'.`);
		}
		if (!sourceExecution) throw new SubagentRpcError("invalid_state", `Paused source '${status.runId}' lacks an effective execution contract.`);
		const sourceSessionFile = requiredString(input.sourceSessionFile, "sourceSessionFile");
		try {
			const sourceStat = fs.lstatSync(sourceSessionFile);
			if (!sourceStat.isFile() || sourceStat.isSymbolicLink() || fs.realpathSync(sourceSessionFile) !== fs.realpathSync(status.sessionFile ?? "")) throw new Error("mismatch");
		} catch {
			throw new SubagentRpcError("invalid_state", `Paused source '${status.runId}' session file is missing, unsafe, or does not match the resume request.`);
		}
		const expected = {
			agent: requiredString(input.agent, "agent"),
			model: typeof input.model === "string" ? input.model : undefined,
			thinking: typeof input.thinking === "string" ? input.thinking : undefined,
			cwd: path.resolve(typeof input.cwd === "string" ? input.cwd : ctx.cwd),
			timeoutMs: typeof input.timeoutMs === "number" ? input.timeoutMs : undefined,
			notificationMode: input.notificationMode === "event-only" ? "event-only" : "default",
		};
		if (expected.agent !== sourceExecution.agent || expected.model !== sourceExecution.model || expected.thinking !== sourceExecution.thinking
			|| expected.cwd !== path.resolve(sourceExecution.cwd) || expected.timeoutMs !== sourceExecution.timeoutMs || expected.notificationMode !== sourceExecution.notificationMode) {
			throw new SubagentRpcError("invalid_state", `Resume execution contract does not exactly match paused source '${status.runId}'.`);
		}
		return launchWorkflowRun(input, "resume", options, ctx, request.requestId);
	}
	throw new SubagentRpcError("unsupported_method", `Unsupported subagent RPC method: ${String(request.method)}`);
}

function parseRequest(raw: unknown, expectedVersion: SubagentRpcVersion): SubagentRpcRequestEnvelope {
	if (!isRecord(raw)) throw new SubagentRpcError("invalid_request", "Subagent RPC request must be an object.");
	const requestId = assertRequestId(raw.requestId);
	if (raw.version !== expectedVersion) {
		throw new SubagentRpcError("unsupported_version", `Unsupported subagent RPC version: ${String(raw.version)}.`);
	}
	const methods: readonly string[] = expectedVersion === 1 ? SUBAGENT_RPC_V1_METHODS : SUBAGENT_RPC_METHODS;
	if (typeof raw.method !== "string" || !methods.includes(raw.method)) {
		throw new SubagentRpcError("unsupported_method", `Unsupported subagent RPC method: ${String(raw.method)}.`);
	}
	return {
		version: expectedVersion,
		requestId,
		method: raw.method as SubagentRpcMethod,
		...(raw.params !== undefined ? { params: raw.params } : {}),
		...(isRecord(raw.source) ? { source: raw.source as SubagentRpcRequestEnvelope["source"] } : {}),
	};
}

function safeReplyRequestId(raw: unknown): string {
	if (!isRecord(raw)) return "unknown";
	const requestId = raw.requestId;
	return typeof requestId === "string" && requestId.trim().length > 0 && !/[\r\n]/.test(requestId)
		? requestId
		: "unknown";
}

function errorReply(raw: unknown, error: unknown, version: SubagentRpcVersion): SubagentRpcReplyEnvelope {
	const requestId = safeReplyRequestId(raw);
	const method = isRecord(raw) && typeof raw.method === "string" && (SUBAGENT_RPC_METHODS as readonly string[]).includes(raw.method)
		? raw.method as SubagentRpcMethod
		: undefined;
	const rpcError = error instanceof SubagentRpcError
		? error
		: new SubagentRpcError("execution_failed", error instanceof Error ? error.message : String(error));
	return {
		version,
		requestId,
		...(method ? { method } : {}),
		success: false,
		error: {
			code: rpcError.code,
			message: rpcError.message,
		},
	};
}

export function registerSubagentRpcBridge(options: RegisterSubagentRpcBridgeOptions): {
	emitReady: (ctx?: ExtensionContext | null) => void;
	dispose: () => void;
} {
	const subscribe = (event: string, version: SubagentRpcVersion) => options.events.on(event, async (raw) => {
		let request: SubagentRpcRequestEnvelope | undefined;
		try {
			request = parseRequest(raw, version);
			const data = await handleRequest(request, options);
			options.events.emit(subagentRpcReplyEvent(request.requestId, version), {
				version,
				requestId: request.requestId,
				method: request.method,
				success: true,
				data,
			} satisfies SubagentRpcReplyEnvelope);
		} catch (error) {
			const reply = errorReply(request ?? raw, error, version);
			options.events.emit(subagentRpcReplyEvent(reply.requestId, version), reply);
		}
	});
	const unsubscribeV1 = subscribe(SUBAGENT_RPC_V1_REQUEST_EVENT, 1);
	const unsubscribeV2 = subscribe(SUBAGENT_RPC_REQUEST_EVENT, SUBAGENT_RPC_PROTOCOL_VERSION);

	return {
		emitReady: (ctx) => {
			const current = ctx ?? options.getContext();
			options.events.emit(SUBAGENT_RPC_V1_READY_EVENT, pingData(current, 1));
			options.events.emit(SUBAGENT_RPC_READY_EVENT, pingData(current));
		},
		dispose: () => {
			if (typeof unsubscribeV1 === "function") unsubscribeV1();
			if (typeof unsubscribeV2 === "function") unsubscribeV2();
		},
	};
}
