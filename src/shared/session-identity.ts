interface SessionIdentityManager {
	getSessionFile(): string | null | undefined;
	getSessionId(): string | null | undefined;
}

export interface CurrentSessionIdentity {
	orchestratorSessionId: string;
	orchestratorSessionFile?: string;
}

export function resolveCurrentSessionIdentity(sessionManager: SessionIdentityManager): CurrentSessionIdentity {
	const orchestratorSessionId = sessionManager.getSessionId();
	if (!orchestratorSessionId) throw new Error("Current session UUID is unavailable.");
	const orchestratorSessionFile = sessionManager.getSessionFile() ?? undefined;
	return { orchestratorSessionId, ...(orchestratorSessionFile ? { orchestratorSessionFile } : {}) };
}

export function resolveCurrentSessionId(sessionManager: SessionIdentityManager): string {
	return resolveCurrentSessionIdentity(sessionManager).orchestratorSessionId;
}
