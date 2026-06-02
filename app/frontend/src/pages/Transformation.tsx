import { useEffect, useState } from 'react';
import { Layers } from 'lucide-react';
import { api, gbp, type ClaimRow } from '../lib/api';

const yn = (v: any) => (v === true || v === 'true' ? 'Yes' : v === false || v === 'false' ? 'No' : (v ?? '—'));

export default function Transformation() {
  const [claims, setClaims] = useState<ClaimRow[]>([]);
  const [cid, setCid] = useState('cc:900001');
  const [e, setE] = useState<any>(null);
  useEffect(() => { api.listClaims().then(setClaims).catch(() => {}); }, []);
  useEffect(() => { setE(null); api.getEnrichment(cid).then(setE).catch(() => {}); }, [cid]);

  return (
    <div className="p-6 max-w-5xl mx-auto">
      <div className="flex items-center justify-between mb-1">
        <div className="flex items-center gap-3">
          <Layers className="w-6 h-6 text-blue-600" />
          <h1 className="text-xl font-bold text-slate-800">Transformation — everything we know before the call</h1>
        </div>
        <select value={cid} onChange={(ev) => setCid(ev.target.value)} className="border border-gray-300 rounded-lg px-3 py-2 text-sm bg-white min-w-[240px]">
          {claims.map((c) => <option key={c.claim_public_id} value={c.claim_public_id}>{c.claim_public_id === 'cc:900001' ? '★ ' : ''}{c.claim_public_id}</option>)}
        </select>
      </div>
      <p className="text-xs text-gray-500 mb-5">The silver enrichment row — one tidy view of the claim, policy, location risk, reserves and the model labels.</p>

      {!e && <p className="text-sm text-gray-500">Loading enrichment…</p>}
      {e && (
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <Group title="Claim & incident" rows={[
            ['Claim number', e.claim_number], ['Peril', e.peril_type], ['Loss cause', e.loss_cause],
            ['Loss date', e.loss_date], ['Reported', e.report_date], ['Channel', e.report_channel],
            ['Reporting lag', `${e.reporting_lag_days} days`], ['Incident', e.description_text], ['Status', e.claim_status],
          ]} />
          <Group title="Policy" rows={[
            ['Policy number', e.policy_number], ['Product', e.product], ['Sum insured', gbp(e.sum_insured)],
            ['Annual premium', gbp(e.annual_premium)], ['Tenure (yrs)', e.policy_tenure_years],
            ['SI : reported', e.sum_insured_to_reported_ratio], ['Prior claims (12m)', e.prior_claims_12m],
          ]} />
          <Group title="Location & weather risk" rows={[
            ['Postcode district', e.postcode_district], ['Third party involved', yn(e.third_party_involved)],
            ['Flood risk', `${e.flood_risk_score}/10`], ['Wind risk', `${e.wind_risk_score}/10`],
            ['Freeze risk', `${e.freeze_risk_score}/10`], ['Weather composite', e.weather_risk_composite],
          ]} />
          <Group title="Reserves & lifecycle" rows={[
            ['Total incurred', gbp(e.total_incurred)], ['Paid', gbp(e.paid_amount)],
            ['Initial reserve', gbp(e.initial_reserve)], ['Ultimate reserve', gbp(e.ultimate_reserve)],
            ['Days to settle', e.days_to_settle ?? '—'], ['Leakage flag', yn(e.leakage_flag)],
            ['Handler', `${e.handler_id} (${e.handler_grade})`],
          ]} />
          <Group title="Risk & ML labels" rows={[
            ['Fraud score', `${e.fraud_score}/100`], ['Days since incident', e.days_since_incident],
            ['High value', yn(e.is_high_value)], ['Potential fraud', yn(e.is_potential_fraud)],
            ['At fault', yn(e.at_fault)], ['Triage label', e.triage_decision], ['Reserve bracket', e.reserve_bracket],
          ]} />
        </div>
      )}
    </div>
  );
}

function Group({ title, rows }: { title: string; rows: any[][] }) {
  return (
    <div className="rounded-xl border border-slate-200 bg-white p-4">
      <h2 className="text-sm font-bold text-slate-800 mb-2">{title}</h2>
      {rows.map(([k, v], i) => (
        <div key={i} className="flex justify-between gap-4 text-sm py-1 border-b border-slate-50 last:border-0">
          <span className="text-gray-500 shrink-0">{k}</span>
          <span className="text-slate-800 text-right">{v ?? '—'}</span>
        </div>
      ))}
    </div>
  );
}
