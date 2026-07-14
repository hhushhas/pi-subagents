import { createHash, timingSafeEqual } from "node:crypto";

export const SUBAGENT_RPC_PROTOCOL_VERSION = 2 as const;
export const SUBAGENT_LIFECYCLE_ARTIFACT_VERSION = 2 as const;

export type NotificationMode = "default" | "event-only";
export type RuntimeOperationKind = "spawn" | "resume";

export interface AsyncSessionIdentity {
	orchestratorSessionId: string;
	orchestratorSessionFile?: string;
	workflowId?: string;
	nodeId?: string;
	attemptId?: string;
	workflowCapabilityHash?: string;
	ownerLeaseEpoch?: number;
}

export interface WorkflowLaunchProvenance {
	workflowId: string;
	nodeId: string;
	attemptId: string;
	ownerLeaseEpoch: number;
}

export interface EffectiveExecution {
	agent: string;
	model?: string;
	thinking?: string;
	cwd: string;
	timeoutMs?: number;
	notificationMode: NotificationMode;
}

export interface RuntimeLaunchContext {
	operationId: string;
	runId: string;
	kind: RuntimeOperationKind;
	sessionIdentity: AsyncSessionIdentity;
	provenance: WorkflowLaunchProvenance;
	effectiveExecution: EffectiveExecution;
	sourceRunId?: string;
	sourceSessionFile?: string;
	sourceAttemptId?: string;
	sourceProvenance?: WorkflowLaunchProvenance;
}

export interface ControlRecord {
	controlRequestId: string;
	action: "pause" | "stop" | "steer" | "resume";
	requestedAt: number;
	acceptedAt?: number;
	confirmedAt?: number;
	error?: string;
}

export interface TerminalRecord {
	reason: "completed" | "failed" | "paused" | "stopped" | "timed_out" | "process_lost";
	at: number;
	controlRequestId?: string;
}

export interface ControlResult {
	controlRequestId: string;
	accepted: boolean;
	runId: string;
	asyncDir: string;
	previousState: string;
	requestedState: string;
	replacementRunId?: string;
	replacementAsyncDir?: string;
	message: string;
}

export interface RuntimeContract {
	rpcVersion: 2;
	artifactVersion: 2;
	methods: readonly string[];
	capabilities: {
		idempotentSpawn: true;
		idempotentResume: true;
		causalControls: true;
		eventOnlyNotifications: true;
		steerAcknowledgement: true;
	};
}

export function hashWorkflowCapability(workflowId: string, capability: string): string {
	return createHash("sha256")
		.update("pi-workflow-capability-v1\0")
		.update(workflowId)
		.update("\0")
		.update(capability)
		.digest("hex");
}

export function verifyWorkflowCapability(workflowId: string, capability: string, expectedHash: string): boolean {
	if (!/^[a-f0-9]{64}$/i.test(expectedHash)) return false;
	const actual = Buffer.from(hashWorkflowCapability(workflowId, capability), "hex");
	const expected = Buffer.from(expectedHash, "hex");
	return actual.length === expected.length && timingSafeEqual(actual, expected);
}
