import { useEffect, useState } from 'react';
import { Sparkles, ShieldAlert, Wallet, FileText, Phone, Mail, Globe, CheckCircle2,
         AlertTriangle, History, CloudRain, Archive, Zap } from 'lucide-react';
import { api, gbp, pct, DECISION_LABEL, type ClaimRow } from '../lib/api';

const SYNTH_NAMES = ['A. Okafor', 'B. Lindqvist', 'C. Marchetti', 'D. Petrov', 'E. Nakamura', 'F. Dubois'];
const claimant = (cid: string) => SYNTH_NAMES[Math.abs(hash(cid)) % SYNTH_NAMES.length];
function hash(s: string) { let h = 0; for (const c of s) h = (h * 31 + c.charCodeAt(0)) | 0; return h; }

const perilLabel: Record<string, string> = {
  motor_tp: 'Motor Third Party', home_escape_water: 'Home — Escape of Water',
  home_storm: 'Home — Storm', home_fire: 'Home — Fire',
};
const isMotor = (p: string) => p === 'motor_tp';
const channelIcon = (c: string) => (c === 'phone' ? Phone : c === 'broker_email' ? Mail : Globe);

const decisionColour: Record<string, string> = {
  refer_siu: 'bg-red-600', escalate: 'bg-amber-500', pay_direct: 'bg-emerald-600',
};
const OVERRIDE_REASONS = ['Insufficient evidence', 'Customer relationship', 'Local knowledge', 'Other'];

export default function ClaimsAI() {
  const [claims, setClaims] = useState<ClaimRow[]>([]);
  const [cid, setCid] = useState<string>('cc:900001');
  const [panels, setPanels] = useState<any>(null);
  const [synth, setSynth] = useState<any>(null);
  const [loading, setLoading] = useState(false);
  const [overriding, setOverriding] = useState(false);
  const [reason, setReason] = useState(OVERRIDE_REASONS[0]);
  const [confirmation, setConfirmation] = useState<any>(null);
  const [posting, setPosting] = useState(false);

  useEffect(() => { api.listClaims().then(setClaims).catch(() => {}); }, []);

  useEffect(() => {
    if (!cid) return;
    setLoading(true); setPanels(null); setSynth(null); setConfirmation(null); setOverriding(false);
    api.getPanels(cid).then(setPanels).catch(() => {}).finally(() => setLoading(false));
    api.getSynthesis(cid).then(setSynth).catch(() => setSynth({ text: '', error: true }));
  }, [cid]);

  const t = panels?.triage || {}; const r = panels?.reserve || {}; const s = panels?.summary || {};
  const f = panels?.fraud || {}; const p = panels?.policy || {}; const x = panels?.extra || {};

  async function logDecision(action: 'accept' | 'override') {
    setPosting(true);
    try {
      const res = await api.postDecision({
        claim_public_id: cid, model_recommendation: t.decision || '',
        model_confidence: t.confidence ?? null, handler_action: action,
        override_flag: action === 'override', override_reason: action === 'override' ? reason : '',
      });
      setConfirmation(res); setOverriding(false);
    } finally { setPosting(false); }
  }

  return (
    <div className="p-6 max-w-7xl mx-auto">
      {/* Header + claim selector */}
      <div className="flex items-center justify-between mb-5">
        <div className="flex items-center gap-3">
          <Sparkles className="w-6 h-6 text-blue-600" />
          <div>
            <h1 className="text-xl font-bold text-slate-800">Claims AI — FNOL Triage</h1>
            <p className="text-xs text-gray-500">Incoming first-notice-of-loss · decide, reserve, and brief in one screen</p>
          </div>
        </div>
        <select value={cid} onChange={(e) => setCid(e.target.value)}
          className="border border-gray-300 rounded-lg px-3 py-2 text-sm bg-white min-w-[260px]">
          {claims.map((c) => (
            <option key={c.claim_public_id} value={c.claim_public_id}>
              {c.claim_public_id === 'cc:900001' ? '★ ' : ''}{c.claim_public_id} · {perilLabel[c.peril_type] || c.peril_type} · {gbp(c.total_incurred)}
            </option>
          ))}
        </select>
      </div>

      {loading && <div className="text-sm text-gray-500 py-10 text-center">Loading claim…</div>}

      {panels && (
        <>
          <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
            {/* LEFT — claim summary */}
            <Card title="Claim Summary" icon={FileText}>
              <Row label="Policy">{x.policy_number || '—'}</Row>
              <Row label="Claimant">{claimant(cid)} <span className="text-gray-400">(synthetic)</span></Row>
              <div className="flex items-center gap-2 my-2">
                <Badge className={isMotor(s.peril_type) ? 'bg-blue-100 text-blue-700' : 'bg-emerald-100 text-emerald-700'}>
                  {perilLabel[s.peril_type] || s.peril_type}
                </Badge>
                <Badge className="bg-slate-100 text-slate-600 inline-flex items-center gap-1">
                  {(() => { const I = channelIcon(s.report_channel); return <I className="w-3 h-3" />; })()}
                  {s.report_channel}
                </Badge>
              </div>
              <Row label="Reported amount"><span className="text-lg font-bold text-slate-800">{gbp(s.total_incurred)}</span></Row>
              <Row label="Status">{s.claim_status}</Row>
              <p className="mt-3 text-sm text-gray-600 italic">“{s.incident_description}”</p>
            </Card>

            {/* CENTRE — AI recommendation */}
            <Card title="AI Recommendation" icon={Sparkles}>
              <div className={`${decisionColour[t.decision] || 'bg-slate-500'} text-white rounded-lg px-4 py-3 text-center mb-3`}>
                <div className="text-xl font-extrabold tracking-wide">{DECISION_LABEL[t.decision] || t.decision || '—'}</div>
                <div className="text-sm opacity-90">confidence {pct(t.confidence)}</div>
              </div>
              <p className="text-xs font-semibold text-gray-500 uppercase mb-1">Why</p>
              <ul className="space-y-1 mb-3">
                {(t.top_reasons || []).map((rsn: string, i: number) => (
                  <li key={i} className="text-sm text-gray-700 flex gap-2"><span className="text-blue-500">•</span>{rsn}</li>
                ))}
              </ul>
              <div className="rounded-lg bg-slate-50 border border-slate-200 px-3 py-2 flex items-center gap-2">
                <Wallet className="w-4 h-4 text-slate-500" />
                <div>
                  <div className="text-sm font-semibold text-slate-800">Reserve: {r.bracket || '—'}</div>
                  <div className="text-xs text-gray-500">{r.estimated_range}</div>
                </div>
              </div>
            </Card>

            {/* RIGHT — risk signals */}
            <Card title="Risk Signals" icon={ShieldAlert}>
              <FraudGauge score={Number(f.fraud_score)} flag={f.fraud_flag} />
              <Row label="Prior claims (12m)">
                <span className="inline-flex items-center gap-1"><History className="w-3.5 h-3.5 text-gray-400" />{f.prior_claims_12m ?? '—'}</span>
              </Row>
              <Row label="Reporting lag">{f.reporting_lag_days ?? x.reporting_lag_days ?? '—'} days</Row>
              <Row label="Policy tenure">{p.policy_tenure_years ?? '—'} yrs</Row>
              <div className="mt-3">
                <p className="text-xs font-semibold text-gray-500 uppercase mb-1 flex items-center gap-1"><CloudRain className="w-3.5 h-3.5" />Weather risk</p>
                <Bar label="Flood" v={Number(x.flood_risk_score)} />
                <Bar label="Wind" v={Number(x.wind_risk_score)} />
                <Bar label="Freeze" v={Number(x.freeze_risk_score)} />
              </div>
            </Card>
          </div>

          {/* Synthesis box */}
          <div className="mt-4 rounded-xl border border-blue-200 bg-blue-50/40 p-4">
            <div className="flex items-center justify-between mb-2">
              <h2 className="text-sm font-bold text-slate-800 flex items-center gap-2"><Sparkles className="w-4 h-4 text-blue-600" />Claims AI — orchestrated brief</h2>
              {synth && !synth.error && (
                <span className={`text-[11px] px-2 py-0.5 rounded-full inline-flex items-center gap-1 ${synth.cache === 'hit' ? 'bg-amber-100 text-amber-700' : 'bg-emerald-100 text-emerald-700'}`}>
                  {synth.cache === 'hit' ? <Archive className="w-3 h-3" /> : <Zap className="w-3 h-3" />}{synth.cache === 'hit' ? 'cached' : 'live'}
                </span>
              )}
            </div>
            {!synth && <p className="text-sm text-gray-400">Synthesising…</p>}
            {synth && synth.error && <p className="text-sm text-gray-400">Synthesis unavailable.</p>}
            {synth && !synth.error && (
              <>
                <p className="text-sm text-gray-700 whitespace-pre-wrap leading-relaxed">{synth.text}</p>
                {!synth.supervisor && <p className="mt-2 text-[11px] text-gray-400">via Context agent — managed Supervisor pending (RUNBOOK Stage C).</p>}
              </>
            )}
          </div>

          {/* HITL action bar */}
          <div className="mt-4 rounded-xl border border-slate-200 bg-white p-4">
            {confirmation ? (
              <div className="flex items-center gap-3 text-emerald-700">
                <CheckCircle2 className="w-5 h-5" />
                <span className="font-medium">
                  Decision logged · {confirmation.decision_id} · {DECISION_LABEL[confirmation.model_recommendation] || confirmation.model_recommendation}
                  {confirmation.override_flag ? ` · OVERRIDE (${confirmation.override_reason})` : ' · ACCEPTED'} · {confirmation.time}
                </span>
                <button onClick={() => setConfirmation(null)} className="ml-auto text-sm text-blue-600 hover:underline">Next claim</button>
              </div>
            ) : overriding ? (
              <div className="flex items-center gap-3 flex-wrap">
                <AlertTriangle className="w-5 h-5 text-amber-500" />
                <span className="text-sm text-gray-700">Override reason:</span>
                <select value={reason} onChange={(e) => setReason(e.target.value)} className="border border-gray-300 rounded-lg px-3 py-1.5 text-sm">
                  {OVERRIDE_REASONS.map((rr) => <option key={rr}>{rr}</option>)}
                </select>
                <button disabled={posting} onClick={() => logDecision('override')}
                  className="px-4 py-1.5 rounded-lg bg-amber-500 text-white text-sm font-medium hover:bg-amber-600 disabled:opacity-50">
                  Confirm Override</button>
                <button onClick={() => setOverriding(false)} className="px-3 py-1.5 text-sm text-gray-500 hover:underline">Cancel</button>
              </div>
            ) : (
              <div className="flex items-center gap-3">
                <span className="text-sm text-gray-500">Human-in-the-loop — record the handler's decision:</span>
                <button disabled={posting} onClick={() => logDecision('accept')}
                  className="ml-auto px-4 py-2 rounded-lg bg-blue-600 text-white text-sm font-semibold hover:bg-blue-700 disabled:opacity-50 inline-flex items-center gap-2">
                  <CheckCircle2 className="w-4 h-4" />Accept AI Recommendation</button>
                <button disabled={posting} onClick={() => setOverriding(true)}
                  className="px-4 py-2 rounded-lg border border-amber-400 text-amber-700 text-sm font-semibold hover:bg-amber-50 disabled:opacity-50">
                  Override</button>
              </div>
            )}
          </div>
        </>
      )}
    </div>
  );
}

function Card({ title, icon: Icon, children }: any) {
  return (
    <div className="rounded-xl border border-slate-200 bg-white p-4">
      <h2 className="text-sm font-bold text-slate-800 flex items-center gap-2 mb-3"><Icon className="w-4 h-4 text-blue-600" />{title}</h2>
      {children}
    </div>
  );
}
function Row({ label, children }: any) {
  return <div className="flex justify-between text-sm py-1"><span className="text-gray-500">{label}</span><span className="text-slate-800 text-right">{children}</span></div>;
}
function Badge({ className, children }: any) {
  return <span className={`text-xs font-medium px-2 py-0.5 rounded-full ${className}`}>{children}</span>;
}
function FraudGauge({ score, flag }: { score: number; flag: any }) {
  const s = isNaN(score) ? 0 : score;
  const colour = s > 70 ? 'bg-red-500' : s >= 40 ? 'bg-amber-500' : 'bg-emerald-500';
  const txt = s > 70 ? 'text-red-600' : s >= 40 ? 'text-amber-600' : 'text-emerald-600';
  return (
    <div className="mb-2">
      <div className="flex justify-between items-baseline mb-1">
        <span className="text-xs font-semibold text-gray-500 uppercase">Fraud score</span>
        <span className={`text-lg font-bold ${txt}`}>{isNaN(score) ? '—' : s}<span className="text-xs text-gray-400">/100</span></span>
      </div>
      <div className="h-2 rounded-full bg-gray-100 overflow-hidden"><div className={`h-full ${colour}`} style={{ width: `${Math.min(100, Math.max(0, s))}%` }} /></div>
      {flag === true && <p className="text-[11px] text-red-600 mt-1">⚑ flagged by fraud rules</p>}
    </div>
  );
}
function Bar({ label, v }: { label: string; v: number }) {
  const val = isNaN(v) ? 0 : v;
  return (
    <div className="flex items-center gap-2 mb-1">
      <span className="text-xs text-gray-500 w-12">{label}</span>
      <div className="flex-1 h-1.5 rounded-full bg-gray-100 overflow-hidden"><div className="h-full bg-sky-400" style={{ width: `${(val / 10) * 100}%` }} /></div>
      <span className="text-xs text-gray-400 w-6 text-right">{isNaN(v) ? '—' : val}</span>
    </div>
  );
}
