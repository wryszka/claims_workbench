import { useEffect, useState } from 'react';
import { Database, ShieldCheck, ExternalLink, AlertTriangle } from 'lucide-react';
import { api } from '../lib/api';

export default function Ingestion() {
  const [d, setD] = useState<any>(null);
  const [err, setErr] = useState(false);
  useEffect(() => { api.getIngestion().then(setD).catch(() => setErr(true)); }, []);

  return (
    <div className="p-6 max-w-5xl mx-auto">
      <div className="flex items-center gap-3 mb-1">
        <Database className="w-6 h-6 text-blue-600" />
        <h1 className="text-xl font-bold text-slate-800">Ingestion — Guidewire CDA → governed bronze</h1>
      </div>
      <p className="text-xs text-gray-500 mb-5">The DLT pipeline enforces data-quality rules and quarantines bad records — nothing is silently dropped.</p>

      {err && <p className="text-sm text-gray-400">Pipeline status unavailable.</p>}
      {!d && !err && <p className="text-sm text-gray-500">Loading pipeline status…</p>}
      {d && (
        <>
          <div className="grid grid-cols-3 gap-4 mb-5">
            <Stat label="Records passing all rules" value={`${d.pass_rate}%`} tone="emerald" sub={`${(d.total_evaluated || 0).toLocaleString()} evaluated`} />
            <Stat label="Quarantined (not dropped)" value={`${(Number(d.quarantined_claims) || 0) + (Number(d.quarantined_fraud) || 0)}`} tone="amber"
                  sub={`${d.quarantined_claims ?? 0} claims · ${d.quarantined_fraud ?? 0} fraud`} />
            <Stat label="Pipeline state" value={(d.state || '—').replace('PipelineState.', '')} tone="slate" sub={d.pipeline_name} />
          </div>

          <div className="rounded-xl border border-emerald-200 bg-emerald-50/50 p-4 mb-5 flex items-start gap-3">
            <ShieldCheck className="w-5 h-5 text-emerald-600 shrink-0 mt-0.5" />
            <p className="text-sm text-emerald-800">
              <strong>No claims data lost.</strong> Every record that fails a quality rule is routed to a quarantine
              table where it stays inspectable — bad records are held back, not deleted.
            </p>
          </div>

          <div className="rounded-xl border border-slate-200 bg-white p-4">
            <div className="flex items-center justify-between mb-3">
              <h2 className="text-sm font-bold text-slate-800">Data-quality expectations</h2>
              {d.pipeline_url && <a href={d.pipeline_url} target="_blank" rel="noreferrer"
                className="text-xs text-blue-600 hover:underline inline-flex items-center gap-1">Open pipeline <ExternalLink className="w-3 h-3" /></a>}
            </div>
            <table className="w-full text-sm">
              <thead><tr className="text-left text-gray-500 border-b border-slate-100">
                <th className="py-1.5">Rule</th><th className="text-right">Passed</th><th className="text-right">Failed</th></tr></thead>
              <tbody>
                {(d.expectations || []).map((e: any) => (
                  <tr key={e.name} className="border-b border-slate-50">
                    <td className="py-1.5 font-mono text-xs">{e.name}</td>
                    <td className="text-right text-emerald-700">{e.passed.toLocaleString()}</td>
                    <td className={`text-right ${e.failed ? 'text-amber-700 font-medium' : 'text-gray-400'}`}>
                      {e.failed ? <span className="inline-flex items-center gap-1"><AlertTriangle className="w-3 h-3" />{e.failed.toLocaleString()}</span> : 0}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      )}
    </div>
  );
}

function Stat({ label, value, tone, sub }: any) {
  const tones: Record<string, string> = { emerald: 'text-emerald-600', amber: 'text-amber-600', slate: 'text-slate-700' };
  return (
    <div className="rounded-xl border border-slate-200 bg-white p-4">
      <p className="text-xs text-gray-500 mb-1">{label}</p>
      <p className={`text-2xl font-extrabold ${tones[tone]}`}>{value}</p>
      {sub && <p className="text-[11px] text-gray-400 mt-0.5">{sub}</p>}
    </div>
  );
}
