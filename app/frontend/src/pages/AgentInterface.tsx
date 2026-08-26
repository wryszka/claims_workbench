/**
 * Agent Interface — the workbench's MCP tool surface, described and surfaced live.
 *
 * Reads the running server's own manifest (GET /api/mcp/manifest) so it never
 * drifts from what /api/mcp actually serves. This is the same surface the app,
 * notebooks and external agents (the Bricksurance control tower) all call —
 * MCP-first: one governed surface, no logic built twice.
 */
import { useEffect, useState } from 'react';
import { Server, Bot, ShieldAlert, Zap, BookOpen } from 'lucide-react';

interface Tool { name: string; description: string; }
interface Manifest { server?: { name?: string; version?: string }; protocol_version?: string; tools?: Tool[]; }

const GROUPS: { prefix: string; label: string; blurb: string }[] = [
  { prefix: 'claim_',  label: 'Per-claim desk',       blurb: 'Everything the handler sees for one claim — panels, synthesis, enrichment, disposition, reasoning, track, package, specialist opinions, reserve adequacy, vulnerability, calls and comms history.' },
  { prefix: 'ops_',    label: 'Operations & control tower', blurb: 'Portfolio KPIs, control tower, worklists, handlers, fraud, trends, agents, experts, suppliers, decisions and the auto-close configuration/segment.' },
  { prefix: 'gov_',    label: 'Governance',           blurb: 'Governance summary, asset inventory, fair-outcomes, vulnerability handling (Consumer Duty) and decision-quality QA.' },
  { prefix: 'ingest_', label: 'Ingestion (Guidewire CDA)', blurb: 'Feeds, freshness, quarantine, documents, quality profile, analytics, sample rows and labelled assets.' },
  { prefix: 'broker_', label: 'Broker portal',        blurb: 'The broker-facing portal view.' },
  { prefix: 'ai_',     label: 'Grounded assistant',   blurb: 'Ask the grounded claims assistant — answers only from workbench data.' },
  { prefix: 'act_',    label: 'Governed actions',     blurb: 'Write actions through the same governed handlers the UI uses: record a decision (override needs a reason), draft a communication, approve it (maker/checker), create a sandbox claim.' },
];

function badge(desc: string): { text: string; cls: string } | null {
  if (/^\[gated\]/i.test(desc)) return { text: 'gated', cls: 'bg-red-50 text-red-700 border-red-200' };
  if (/^\[action\]/i.test(desc)) return { text: 'action', cls: 'bg-amber-50 text-amber-700 border-amber-200' };
  return null;
}

function Meta({ label, value }: { label: string; value?: string }) {
  return (
    <div className="bg-white border border-slate-200 rounded px-2.5 py-1.5">
      <div className="text-[10px] uppercase tracking-wide text-slate-400">{label}</div>
      <div className="font-mono font-semibold text-slate-800 text-[13px]">{value || '—'}</div>
    </div>
  );
}

export default function AgentInterface() {
  const [m, setM] = useState<Manifest | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const origin = typeof window !== 'undefined' ? window.location.origin : '';

  useEffect(() => {
    fetch('/api/mcp/manifest').then((r) => r.json()).then(setM).catch((e) => setErr(String(e)));
  }, []);

  const tools = m?.tools || [];
  const prefixes = GROUPS.map((g) => g.prefix);
  const groupOf = (n: string) => prefixes.find((p) => n.startsWith(p)) || '';
  const rpcUrl = `${origin}/api/mcp`;

  return (
    <div className="max-w-5xl mx-auto p-6 space-y-5">
      <div>
        <h1 className="text-xl font-bold text-slate-900 flex items-center gap-2">
          <Bot className="w-5 h-5 text-blue-600" /> Agent Interface
        </h1>
        <p className="text-sm text-slate-500 mt-1">The claims workbench as a governed MCP tool surface.</p>
      </div>

      {err && <div className="text-[13px] text-red-700 bg-red-50 border border-red-200 rounded p-3">MCP manifest unavailable — {err}</div>}
      {!m && !err && <div className="text-sm text-slate-500">Reading the MCP server manifest…</div>}

      {m && (
        <>
          <div className="border border-blue-200 bg-blue-50/50 rounded-lg p-4">
            <div className="flex items-start justify-between gap-4 flex-wrap">
              <div className="max-w-2xl">
                <div className="font-semibold text-slate-900 flex items-center gap-2"><Server className="w-4 h-4 text-blue-600" /> MCP server — live</div>
                <p className="text-[13px] text-slate-700 leading-relaxed mt-1">
                  This workbench publishes its <strong>whole surface</strong> as a <strong>Model Context Protocol</strong> server, so an
                  outside agent — including the Bricksurance <strong>control tower</strong> — can operate it end to end using the
                  <em> same</em> tools the app and notebooks use (MCP-first: one surface, no logic built twice). Every write action
                  carries the <strong>same server-side gate</strong> as the UI, so an agent can't bypass what the app enforces.
                  This list is read live from the running server's manifest.
                </p>
              </div>
              <div className="flex flex-col items-end gap-1.5">
                <span className="text-[11px] font-semibold px-2 py-0.5 rounded-full bg-emerald-100 text-emerald-700 border border-emerald-200">running</span>
                <span className="text-[11px] text-slate-500">{tools.length} tools</span>
              </div>
            </div>
            <div className="grid grid-cols-2 md:grid-cols-4 gap-2 mt-4">
              <Meta label="Server" value={m.server?.name} />
              <Meta label="Version" value={m.server?.version} />
              <Meta label="MCP protocol" value={m.protocol_version} />
              <Meta label="Tools" value={String(tools.length)} />
            </div>
            <div className="mt-4 space-y-1.5">
              <div className="text-[11px] uppercase tracking-wide text-slate-400 font-semibold">Endpoints</div>
              <div className="flex items-center gap-2 text-[12.5px] flex-wrap">
                <span className="font-mono bg-slate-100 border border-slate-200 rounded px-1.5 py-0.5">POST</span>
                <code className="font-mono text-slate-700 break-all">{rpcUrl}</code>
                <span className="text-slate-400">· JSON-RPC (initialize · tools/list · tools/call)</span>
              </div>
              <div className="flex items-center gap-2 text-[12.5px] flex-wrap">
                <span className="font-mono bg-slate-100 border border-slate-200 rounded px-1.5 py-0.5">GET</span>
                <code className="font-mono text-slate-700 break-all">{rpcUrl}/manifest</code>
                <span className="text-slate-400">· this manifest</span>
              </div>
            </div>
            <div className="mt-3 flex items-center gap-3 text-[11px] text-slate-500">
              <span className="flex items-center gap-1"><ShieldAlert className="w-3 h-3 text-red-500" /> gated = re-checks RBAC/policy server-side</span>
              <span className="flex items-center gap-1"><Zap className="w-3 h-3 text-amber-500" /> action = writes through a governed handler</span>
            </div>
          </div>

          {GROUPS.map((g) => {
            const gt = tools.filter((t) => groupOf(t.name) === g.prefix);
            if (!gt.length) return null;
            return (
              <div key={g.label} className="bg-white border border-slate-200 rounded-lg p-4">
                <div className="flex items-center gap-2 mb-1">
                  <h2 className="text-sm font-bold text-slate-900">{g.label}</h2>
                  <span className="text-[11px] text-slate-400 ml-auto">{gt.length}</span>
                </div>
                <p className="text-[12.5px] text-slate-500 mb-3">{g.blurb}</p>
                <div className="divide-y divide-slate-100 border border-slate-200 rounded-lg overflow-hidden">
                  {gt.map((t) => {
                    const b = badge(t.description);
                    return (
                      <div key={t.name} className="px-3.5 py-2.5 flex items-start gap-3">
                        <code className="font-mono text-[11.5px] bg-slate-50 border border-slate-200 text-slate-800 px-1.5 py-0.5 rounded shrink-0">{t.name}</code>
                        {b && <span className={`text-[10px] font-semibold px-1.5 py-0.5 rounded border shrink-0 ${b.cls}`}>{b.text}</span>}
                        <span className="text-[13px] text-slate-700 leading-relaxed">{t.description.replace(/^\[(gated|action)\]\s*/i, '')}</span>
                      </div>
                    );
                  })}
                </div>
              </div>
            );
          })}

          <div className="bg-slate-50 border border-slate-200 rounded-lg p-4 text-[12.5px] text-slate-600">
            <div className="font-semibold text-slate-800 flex items-center gap-2 mb-1"><BookOpen className="w-4 h-4" /> Under the hood</div>
            <ul className="list-disc pl-5 space-y-0.5">
              <li><code className="font-mono">POST /api/mcp</code> — JSON-RPC 2.0 (initialize / tools/list / tools/call), one entry point.</li>
              <li><code className="font-mono">GET /api/mcp/manifest</code> — the plain tool manifest this tab reads.</li>
              <li>Every tool delegates to the app's own route handler — the same code (and gate) behind the UI. Nothing is built twice.</li>
            </ul>
          </div>
        </>
      )}
    </div>
  );
}
