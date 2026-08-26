import { BrowserRouter, Routes, Route, Link, useLocation } from 'react-router-dom';
import { useEffect, useState } from 'react';
import { Sparkles, Database, Layers, Shield, ShieldCheck, Zap, Archive, RotateCcw, Bot } from 'lucide-react';
import { api } from './lib/api';
import ClaimsAI from './pages/ClaimsAI';
import Ingestion from './pages/Ingestion';
import Transformation from './pages/Transformation';
import Governance from './pages/Governance';
import AgentInterface from './pages/AgentInterface';

const NAV = [
  { to: '/ingestion',      label: 'Ingestion',              icon: Database, match: (p: string) => p.startsWith('/ingestion') },
  { to: '/transformation', label: 'Transformation',         icon: Layers,   match: (p: string) => p.startsWith('/transformation') },
  { to: '/',               label: 'Claims AI',              icon: Sparkles, match: (p: string) => p === '/' },
  { to: '/agent-interface',label: 'Agent Interface',        icon: Bot,      match: (p: string) => p.startsWith('/agent-interface') },
  { to: '/governance',     label: 'Governance & Portfolio', icon: Shield,   match: (p: string) => p.startsWith('/governance') },
];

function Sidebar() {
  const { pathname } = useLocation();
  return (
    <aside className="w-56 bg-[#1e293b] text-white min-h-screen flex flex-col shrink-0">
      <Link to="/" className="px-4 py-5 flex items-center gap-3 hover:opacity-90 transition-opacity border-b border-white/10">
        <ShieldCheck className="w-7 h-7 text-blue-400" />
        <div>
          <h1 className="text-sm font-bold tracking-tight leading-tight">Claims Intelligence</h1>
          <p className="text-[10px] text-gray-400">Bricksurance SE</p>
        </div>
      </Link>
      <nav className="flex-1 px-2 py-3 space-y-0.5">
        {NAV.map(({ to, label, icon: Icon, match }) => (
          <Link key={to} to={to}
            className={`flex items-center gap-2.5 px-3 py-2 rounded-lg text-sm transition-colors ${
              match(pathname) ? 'bg-blue-600/20 text-white font-medium' : 'text-gray-400 hover:text-white hover:bg-white/5'}`}>
            <Icon className={`w-4 h-4 shrink-0 ${match(pathname) ? 'text-blue-400' : ''}`} />
            {label}
          </Link>
        ))}
      </nav>
      <div className="px-4 py-3 border-t border-white/10 text-[10px] text-gray-500">
        Synthetic demo accelerator — not a Databricks product
      </div>
    </aside>
  );
}

// Global top bar — USE_CACHE toggle + Reset demo. The synthesis box reflects
// the cache mode (cached/live pill) on the next claim load.
function TopBar() {
  const [cache, setCache] = useState<boolean | null>(null);
  const [entries, setEntries] = useState(0);
  const [busy, setBusy] = useState(false);
  const [resetOk, setResetOk] = useState<boolean | null>(null);
  const [resetMsg, setResetMsg] = useState('');

  useEffect(() => {
    api.getCacheMode().then((d) => { setCache(d.use_cache); setEntries(d.entries); }).catch(() => setCache(true));
    api.getResetStatus().then((d) => setResetOk(d.available)).catch(() => setResetOk(false));
  }, []);

  async function flip() {
    if (busy || cache === null) return;
    setBusy(true);
    try { const d = await api.setCacheMode(!cache); setCache(d.use_cache); } finally { setBusy(false); }
  }
  async function reset() {
    if (!resetOk) return;
    setResetMsg('Triggering…');
    try {
      const d = await api.resetDemo();
      if (!d.triggered) { setResetMsg(d.message || 'unavailable'); return; }
      const t0 = Date.now();
      const tick = async () => {
        try {
          const r = await api.getResetRun(d.run_id);
          if ((r.life_cycle || '').includes('TERMINATED')) {
            clearInterval(timer);
            setResetMsg((r.result || '').includes('SUCCESS') ? '✓ Reset complete — dates current' : 'Reset failed');
          } else { setResetMsg(`Reset running… ${Math.round((Date.now() - t0) / 60000)}m`); }
        } catch { /* keep polling */ }
      };
      tick();
      const timer = setInterval(tick, 15000);
    } catch { setResetMsg('Reset failed'); }
  }

  const cached = cache === true;
  const CacheIcon = cached ? Archive : Zap;
  return (
    <div className="h-12 bg-white border-b border-slate-200 flex items-center gap-3 px-5 shrink-0">
      <span className="text-xs text-gray-400">Bricksurance SE · Claims Intelligence Workbench</span>
      <div className="ml-auto flex items-center gap-2">
        <button type="button" onClick={flip} disabled={cache === null || busy}
          title={cached ? `Cache-first (${entries} stored) — fast & consistent. Click for live.` : 'Calling real endpoints. Click for cached/fast.'}
          className={`flex items-center gap-1.5 px-2.5 py-1.5 rounded-md border text-[11px] font-medium transition-colors disabled:opacity-50 ${
            cached ? 'bg-amber-50 text-amber-700 border-amber-300 hover:bg-amber-100'
                   : 'bg-emerald-50 text-emerald-700 border-emerald-300 hover:bg-emerald-100'}`}>
          <CacheIcon className="w-3.5 h-3.5" />AI: {cache === null ? '…' : cached ? 'cached' : 'live'}
        </button>
        <button type="button" onClick={reset} disabled={!resetOk}
          title={resetOk ? 'Re-anchor the demo (runs claims_workbench_99_reset_demo)' : 'Reset job is built in Phase 9'}
          className="flex items-center gap-1.5 px-2.5 py-1.5 rounded-md border text-[11px] font-medium transition-colors border-slate-300 text-slate-600 hover:bg-slate-50 disabled:opacity-40 disabled:cursor-not-allowed">
          <RotateCcw className="w-3.5 h-3.5" />Reset demo{resetOk === false ? ' (Phase 9)' : ''}
        </button>
        {resetMsg && <span className="text-[11px] text-gray-500">{resetMsg}</span>}
      </div>
    </div>
  );
}

export default function App() {
  return (
    <BrowserRouter>
      <div className="min-h-screen bg-gray-100 font-[system-ui] flex">
        <Sidebar />
        <div className="flex-1 flex flex-col min-w-0">
          <TopBar />
          <main className="flex-1 overflow-auto">
            <Routes>
              <Route path="/" element={<ClaimsAI />} />
              <Route path="/agent-interface" element={<AgentInterface />} />
              <Route path="/ingestion" element={<Ingestion />} />
              <Route path="/transformation" element={<Transformation />} />
              <Route path="/governance" element={<Governance />} />
            </Routes>
          </main>
        </div>
      </div>
    </BrowserRouter>
  );
}
