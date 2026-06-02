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
