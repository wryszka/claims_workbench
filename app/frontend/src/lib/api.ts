const BASE = '/api';

async function fetchJson<T>(path: string, options?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE}${path}`, {
    headers: { 'Content-Type': 'application/json' },
    ...options,
  });
  if (!res.ok) throw new Error(`API error: ${res.status} ${res.statusText}`);
  return res.json();
}

export interface ClaimRow {
  claim_public_id: string; peril_type: string; total_incurred: string;
  claim_status: string; report_date: string;
}

export const api = {
  getConfig: () => fetchJson<any>('/config'),
  getCacheMode: () => fetchJson<{ use_cache: boolean; entries: number }>('/cache-mode'),
  setCacheMode: (use_cache: boolean) =>
    fetchJson<{ use_cache: boolean }>('/cache-mode', { method: 'POST', body: JSON.stringify({ use_cache }) }),

  listClaims: () => fetchJson<ClaimRow[]>('/claims'),
  getPanels: (cid: string) => fetchJson<any>(`/claim/panels?cid=${encodeURIComponent(cid)}`),
  getSynthesis: (cid: string, useCache?: boolean) =>
    fetchJson<any>(`/claim/synthesis?cid=${encodeURIComponent(cid)}` +
      (useCache === undefined ? '' : `&use_cache=${useCache}`)),

  postDecision: (body: any) => fetchJson<any>('/decision', { method: 'POST', body: JSON.stringify(body) }),
  getDecisions: (limit = 20) => fetchJson<any[]>(`/decisions?limit=${limit}`),
  resetDemo: () => fetchJson<any>('/reset-demo', { method: 'POST', body: JSON.stringify({}) }),
  getResetStatus: () => fetchJson<{ available: boolean }>('/reset-status'),
  getResetRun: (runId: number) => fetchJson<{ life_cycle: string; result: string }>(`/reset-run?run_id=${runId}`),

  getIngestion: () => fetchJson<any>('/ingestion'),
  getEnrichment: (cid: string) => fetchJson<any>(`/claim/enrichment?cid=${encodeURIComponent(cid)}`),
  getGovernance: () => fetchJson<any>('/governance'),

  // Phase 11 Stage B
  getControlTower: () => fetchJson<any>('/control-tower'),
  getMonitoringLens: () => fetchJson<any>('/monitoring-lens'),
  getAutoCloseConfig: () => fetchJson<any>('/auto-close/config'),
  segmentAutoClose: (conf: number, cap: number, fraud: number) =>
    fetchJson<any>(`/auto-close/segment?conf=${conf}&cap=${cap}&fraud=${fraud}`),
  ask: (question: string, cid?: string | null) =>
    fetchJson<any>('/ask', { method: 'POST', body: JSON.stringify({ question, cid: cid ?? null }) }),

  // Phase 11 Stage C / redesign
  getDisposition: (cid: string) => fetchJson<any>(`/claim/disposition?cid=${encodeURIComponent(cid)}`),
  getClaimReasoning: (cid: string) => fetchJson<any[]>(`/claim/reasoning?cid=${encodeURIComponent(cid)}`),
  getClaimTrack: (cid: string) => fetchJson<any>(`/claim/track?cid=${encodeURIComponent(cid)}`),
  getInventory: () => fetchJson<any>('/governance/inventory'),
  getAgents: () => fetchJson<any>('/agents'),

  // CCO uplift + expert agents
  getMondayBrief: () => fetchJson<any>('/monday-brief'),
  getWorklist: (kind: string) => fetchJson<any>(`/worklist?kind=${kind}`),
  getHandlers: () => fetchJson<any>('/handlers'),
  getFraud: () => fetchJson<any>('/fraud'),
  getTrends: () => fetchJson<any>('/trends'),
  getExperts: () => fetchJson<any>('/experts'),
  getExpertOpinion: (cid: string, role: string) =>
    fetchJson<any>(`/claim/expert?cid=${encodeURIComponent(cid)}&role=${role}`),
  getFairOutcomes: () => fetchJson<any>('/governance/fair-outcomes'),
  getRules: () => fetchJson<any>('/rules'),
  // Phase 12 Stage B — handler persona + create-a-claim
  getHandlerQueue: () => fetchJson<any>('/handler/queue'),
  getCreateClaimScenario: () => fetchJson<any>('/create-claim/scenario'),
  createClaim: (body: any) =>
    fetchJson<any>('/create-claim', { method: 'POST', body: JSON.stringify(body) }),
  getSandboxClaims: () => fetchJson<any>('/sandbox-claims'),
};

// ---- formatting helpers (£ commas, % 1dp, plain-English labels) ----
export const gbp = (v: any) => {
  const n = typeof v === 'number' ? v : parseFloat(v);
  return isNaN(n) ? '—' : '£' + n.toLocaleString('en-GB', { maximumFractionDigits: 0 });
};
export const pct = (v: any) => {
  const n = typeof v === 'number' ? v : parseFloat(v);
  return isNaN(n) ? '—' : `${n.toFixed(1)}%`;
};
export const DECISION_LABEL: Record<string, string> = {
  pay_direct: 'PAY DIRECT', escalate: 'ESCALATE', refer_siu: 'REFER TO SIU',
};
