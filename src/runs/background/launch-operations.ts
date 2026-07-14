import { createHash, randomUUID } from "node:crypto";
import * as fs from "node:fs";
import * as path from "node:path";
import { writeAtomicJson } from "../../shared/atomic-json.ts";
import type { RuntimeLaunchContext, RuntimeOperationKind } from "../../shared/runtime-protocol.ts";
import { ASYNC_DIR } from "../../shared/types.ts";

export type LaunchOperationState = "prepared" | "launched" | "failed";

export interface LaunchReadyRecord {
	version: 1;
	operationId: string;
	runId: string;
	pid: number;
	readyAt: number;
}

export interface LaunchOperationRecord {
	version: 1;
	operationId: string;
	kind: RuntimeOperationKind;
	requestDigest: string;
	workflowCapabilityHash: string;
	runId: string;
	asyncDir: string;
	state: LaunchOperationState;
	createdAt: number;
	updatedAt: number;
	pid?: number;
	error?: string;
	sourceRunId?: string;
	context: RuntimeLaunchContext;
}

function operationRoot(asyncDirRoot: string): string {
	return path.join(asyncDirRoot, ".workflow-operations");
}

function operationKey(capabilityHash: string, operationId: string): string {
	return createHash("sha256").update(capabilityHash).update("\0").update(operationId).digest("hex");
}

function operationPath(asyncDirRoot: string, capabilityHash: string, operationId: string): string {
	return path.join(operationRoot(asyncDirRoot), `${operationKey(capabilityHash, operationId)}.json`);
}

function resumeClaimPath(asyncDirRoot: string, capabilityHash: string, sourceRunId: string): string {
	const key = createHash("sha256").update(capabilityHash).update("\0resume\0").update(sourceRunId).digest("hex");
	return path.join(operationRoot(asyncDirRoot), `resume-${key}.json`);
}

function claimResumeSource(asyncDirRoot: string, record: LaunchOperationRecord): void {
	if (record.kind !== "resume") return;
	if (!record.sourceRunId) throw new Error(`Resume operation '${record.operationId}' is missing sourceRunId.`);
	const file = resumeClaimPath(asyncDirRoot, record.workflowCapabilityHash, record.sourceRunId);
	const claim = { version: 1, sourceRunId: record.sourceRunId, operationId: record.operationId, runId: record.runId, createdAt: record.createdAt };
	try {
		const fd = fs.openSync(file, "wx", 0o600);
		try {
			fs.writeFileSync(fd, JSON.stringify(claim, null, 2), "utf-8");
			fs.fsyncSync(fd);
		} finally {
			fs.closeSync(fd);
		}
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
		const existing = JSON.parse(fs.readFileSync(file, "utf-8")) as typeof claim;
		if (existing.operationId !== record.operationId || existing.runId !== record.runId) {
			throw new Error(`Paused source '${record.sourceRunId}' is already claimed by resume operation '${existing.operationId}'.`);
		}
	}
}

export function digestLaunchRequest(value: unknown): string {
	return createHash("sha256").update(JSON.stringify(value)).digest("hex");
}

export function launchReadyPath(asyncDir: string): string { return path.join(asyncDir, "workflow-ready.json"); }
export function launchGoPath(asyncDir: string): string { return path.join(asyncDir, "workflow-go.json"); }

export function publishLaunchReady(asyncDir: string, operationId: string, runId: string, now = Date.now()): LaunchReadyRecord {
	const record: LaunchReadyRecord = { version: 1, operationId, runId, pid: process.pid, readyAt: now };
	writeAtomicJson(launchReadyPath(asyncDir), record);
	return record;
}

export function readLaunchReady(asyncDir: string): LaunchReadyRecord | undefined {
	try {
		const value = JSON.parse(fs.readFileSync(launchReadyPath(asyncDir), "utf-8")) as LaunchReadyRecord;
		return value.version === 1 && typeof value.pid === "number" ? value : undefined;
	} catch {
		return undefined;
	}
}

export function releaseLaunch(record: LaunchOperationRecord, ready: LaunchReadyRecord, now = Date.now()): void {
	if (ready.operationId !== record.operationId || ready.runId !== record.runId) throw new Error(`Launch readiness does not match operation '${record.operationId}'.`);
	writeAtomicJson(launchGoPath(record.asyncDir), { version: 1, operationId: record.operationId, runId: record.runId, pid: ready.pid, releasedAt: now });
}

export async function waitForLaunchRelease(asyncDir: string, operationId: string, runId: string, timeoutMs = 60_000): Promise<boolean> {
	const deadline = Date.now() + timeoutMs;
	while (Date.now() < deadline) {
		try {
			const value = JSON.parse(fs.readFileSync(launchGoPath(asyncDir), "utf-8")) as { version?: number; operationId?: string; runId?: string };
			if (value.version === 1 && value.operationId === operationId && value.runId === runId) return true;
		} catch { /* not released yet */ }
		await new Promise((resolve) => setTimeout(resolve, 20));
	}
	return false;
}

export function readLaunchOperation(input: { asyncDirRoot?: string; capabilityHash: string; operationId: string }): LaunchOperationRecord | undefined {
	const file = operationPath(input.asyncDirRoot ?? ASYNC_DIR, input.capabilityHash, input.operationId);
	try {
		const parsed = JSON.parse(fs.readFileSync(file, "utf-8")) as LaunchOperationRecord;
		return parsed?.version === 1 ? parsed : undefined;
	} catch {
		return undefined;
	}
}

export function reserveLaunchOperation(input: {
	asyncDirRoot?: string;
	operationId: string;
	kind: RuntimeOperationKind;
	requestDigest: string;
	workflowCapabilityHash: string;
	context: Omit<RuntimeLaunchContext, "runId"> & { runId?: string };
	now?: () => number;
}): { record: LaunchOperationRecord; created: boolean } {
	const asyncDirRoot = input.asyncDirRoot ?? ASYNC_DIR;
	const existing = readLaunchOperation({ asyncDirRoot, capabilityHash: input.workflowCapabilityHash, operationId: input.operationId });
	if (existing) {
		if (existing.requestDigest !== input.requestDigest || existing.kind !== input.kind) {
			throw new Error(`Operation '${input.operationId}' was already reserved with different launch parameters.`);
		}
		claimResumeSource(asyncDirRoot, existing);
		return { record: existing, created: false };
	}
	const now = input.now?.() ?? Date.now();
	const runId = input.context.runId ?? randomUUID();
	const context: RuntimeLaunchContext = { ...input.context, runId };
	const record: LaunchOperationRecord = {
		version: 1,
		operationId: input.operationId,
		kind: input.kind,
		requestDigest: input.requestDigest,
		workflowCapabilityHash: input.workflowCapabilityHash,
		runId,
		asyncDir: path.join(asyncDirRoot, runId),
		state: "prepared",
		createdAt: now,
		updatedAt: now,
		...(context.sourceRunId ? { sourceRunId: context.sourceRunId } : {}),
		context,
	};
	const root = operationRoot(asyncDirRoot);
	fs.mkdirSync(root, { recursive: true, mode: 0o700 });
	fs.chmodSync(root, 0o700);
	const file = operationPath(asyncDirRoot, input.workflowCapabilityHash, input.operationId);
	try {
		const fd = fs.openSync(file, "wx", 0o600);
		try {
			fs.writeFileSync(fd, JSON.stringify(record, null, 2), "utf-8");
			fs.fsyncSync(fd);
		} finally {
			fs.closeSync(fd);
		}
		claimResumeSource(asyncDirRoot, record);
		return { record, created: true };
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
		const raced = readLaunchOperation({ asyncDirRoot, capabilityHash: input.workflowCapabilityHash, operationId: input.operationId });
		if (!raced || raced.requestDigest !== input.requestDigest || raced.kind !== input.kind) {
			throw new Error(`Operation '${input.operationId}' was concurrently reserved with different launch parameters.`);
		}
		return { record: raced, created: false };
	}
}

export function updateLaunchOperation(record: LaunchOperationRecord, patch: Partial<Pick<LaunchOperationRecord, "state" | "pid" | "error">>, asyncDirRoot = ASYNC_DIR): LaunchOperationRecord {
	const updated: LaunchOperationRecord = { ...record, ...patch, updatedAt: Date.now() };
	const file = operationPath(asyncDirRoot, record.workflowCapabilityHash, record.operationId);
	writeAtomicJson(file, updated);
	fs.chmodSync(file, 0o600);
	return updated;
}
