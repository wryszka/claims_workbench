import { useEffect, useState } from 'react';
import { Shield, ExternalLink, MessageCircleQuestion, GitBranch, ClipboardList } from 'lucide-react';
import { api, pct, DECISION_LABEL } from '../lib/api';

export default function Governance() {
  const [g, setG] = useState<any>(null);
  const [decisions, setDecisions] = useState<any[]>([]);
  useEffect(() => {
    api.getGovernance().then(setG).catch(() => {});
    api.getDecisions(15).then(setDecisions).catch(() => {});
  }, []);

  return (
    <div className="p-6 max-w-6xl mx-auto">
      <div className="flex items-center gap-3 mb-1">
        <Shield className="w-6 h-6 text-blue-600" />
        <h1 className="text-xl font-bold text-slate-800">Governance &amp; Portfolio</h1>
      </div>
      <p className="text-xs text-gray-500 mb-5">Board reporting, ask-the-book analytics, lineage and the human-in-the-loop audit trail.</p>

      {/* Dashboard */}
      <div className="rounded-xl border border-slate-200 bg-white p-4 mb-4">
        <div className="flex items-center justify-between mb-3">
          <h2 className="text-sm font-bold text-slate-800">Claims Portfolio — Board View</h2>
          <div className="flex items-center gap-3">
            {g?.genie_url && <a href={g.genie_url} target="_blank" rel="noreferrer" className="text-xs text-blue-600 hover:underline inline-flex items-center gap-1"><MessageCircleQuestion className="w-3.5 h-3.5" />Ask the Book <ExternalLink className="w-3 h-3" /></a>}
            {g?.dashboard_url && <a href={g.dashboard_url} target="_blank" rel="noreferrer" className="text-xs text-blue-600 hover:underline inline-flex items-center gap-1">Open dashboard <ExternalLink className="w-3 h-3" /></a>}
          </div>
        </div>
        {g?.dashboard_embed_url
          ? <iframe title="Board View" src={g.dashboard_embed_url} className="w-full rounded-lg border border-slate-100" style={{ height: 560 }} />
          : <p className="text-sm text-gray-400">Dashboard link resolving… (open via the link above)</p>}
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        {/* Lineage */}
        <div className="rounded-xl border border-slate-200 bg-white p-4">
          <h2 className="text-sm font-bold text-slate-800 flex items-center gap-2 mb-3"><GitBranch className="w-4 h-4 text-blue-600" />End-to-end lineage</h2>
          <div className="flex flex-wrap items-center gap-1 text-xs">
            {(g?.lineage || []).map((l: any, i: number) => (
              <span key={l.asset} className="flex items-center gap-1">
                <a href={l.explore_url} target="_blank" rel="noreferrer" className="font-mono px-1.5 py-0.5 rounded bg-slate-100 text-slate-700 hover:bg-slate-200">{l.asset}</a>
                {i < (g.lineage.length - 1) && <span className="text-gray-400">→</span>}
              </span>
            ))}
          </div>
          <p className="text-[11px] text-gray-400 mt-2">Every claim is traceable raw → governed → enriched → features → model → decision.</p>
        </div>

        {/* Audit trail */}
        <div className="rounded-xl border border-slate-200 bg-white p-4">
          <h2 className="text-sm font-bold text-slate-800 flex items-center gap-2 mb-3"><ClipboardList className="w-4 h-4 text-blue-600" />HITL audit trail</h2>
          {decisions.length === 0 && <p className="text-sm text-gray-400">No decisions logged yet.</p>}
          {decisions.length > 0 && (
            <table className="w-full text-xs">
              <thead><tr className="text-left text-gray-500 border-b border-slate-100">
                <th className="py-1">ID</th><th>Claim</th><th>Reco</th><th>Action</th><th>Reason</th></tr></thead>
              <tbody>
                {decisions.map((d) => (
                  <tr key={d.decision_id} className="border-b border-slate-50">
                    <td className="py-1 font-mono">{d.decision_id}</td>
                    <td>{d.claim_public_id}</td>
                    <td>{DECISION_LABEL[d.model_recommendation] || d.model_recommendation} <span className="text-gray-400">{d.model_confidence != null ? pct(d.model_confidence) : ''}</span></td>
                    <td className={d.override_flag ? 'text-amber-700 font-medium' : 'text-emerald-700'}>{d.override_flag ? 'OVERRIDE' : 'accept'}</td>
                    <td className="text-gray-500">{d.override_reason || '—'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
          <p className="text-[11px] text-gray-400 mt-2">Each row: what the model advised, what the handler did, and why — the FCA / Consumer-Duty trail.</p>
        </div>
      </div>
    </div>
  );
}
