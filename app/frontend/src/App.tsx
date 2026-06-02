import { BrowserRouter, Routes, Route, Link, useLocation } from 'react-router-dom';
import { useEffect, useState } from 'react';
import { Sparkles, Database, Layers, Shield, ShieldCheck, Zap, Archive } from 'lucide-react';
import { api } from './lib/api';
import ClaimsAI from './pages/ClaimsAI';
import Ingestion from './pages/Ingestion';
import Transformation from './pages/Transformation';
import Governance from './pages/Governance';

const NAV = [
  { to: '/ingestion',      label: 'Ingestion',              icon: Database, match: (p: string) => p.startsWith('/ingestion') },
  { to: '/transformation', label: 'Transformation',         icon: Layers,   match: (p: string) => p.startsWith('/transformation') },
  { to: '/',               label: 'Claims AI',              icon: Sparkles, match: (p: string) => p === '/' },
  { to: '/governance',     label: 'Governance & Portfolio', icon: Shield,   match: (p: string) => p.startsWith('/governance') },
];

function CacheBadge() {
  const [mode, setMode] = useState<boolean | null>(null);
  const [entries, setEntries] = useState(0);
  const [busy, setBusy] = useState(false);
  useEffect(() => {
    api.getCacheMode().then((d) => { setMode(d.use_cache); setEntries(d.entries); }).catch(() => setMode(true));
  }, []);
  async function flip() {
    if (busy || mode === null) return;
    setBusy(true);
    try { const d = await api.setCacheMode(!mode); setMode(d.use_cache); } finally { setBusy(false); }
  }
  const cached = mode === true;
  const Icon = cached ? Archive : Zap;
  const colour = cached
    ? 'bg-amber-500/15 text-amber-300 hover:bg-amber-500/25 border-amber-400/30'
    : 'bg-emerald-500/15 text-emerald-300 hover:bg-emerald-500/25 border-emerald-400/30';
  return (
    <div className="px-3 py-2 border-t border-white/10">
      <button type="button" onClick={flip} disabled={mode === null || busy}
        title={cached ? `Cache-first (${entries} stored). Click for live.` : 'Calling real endpoints. Click for cached/fast.'}
        className={`w-full flex items-center gap-2 px-2.5 py-1.5 rounded-md border text-[11px] font-medium transition-colors disabled:opacity-50 ${colour}`}>
        <Icon className="w-3.5 h-3.5 shrink-0" />
        <span className="flex-1 text-left">AI: {mode === null ? '…' : cached ? 'cached' : 'live'}</span>
        {cached && entries > 0 && <span className="text-[10px] opacity-70">{entries}</span>}
      </button>
    </div>
  );
}

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
      <CacheBadge />
      <div className="px-4 py-3 border-t border-white/10 text-[10px] text-gray-500">
        Synthetic demo accelerator — not a Databricks product
      </div>
    </aside>
  );
}

export default function App() {
  return (
    <BrowserRouter>
      <div className="min-h-screen bg-gray-100 font-[system-ui] flex">
        <Sidebar />
        <main className="flex-1 overflow-auto">
          <Routes>
            <Route path="/" element={<ClaimsAI />} />
            <Route path="/ingestion" element={<Ingestion />} />
            <Route path="/transformation" element={<Transformation />} />
            <Route path="/governance" element={<Governance />} />
          </Routes>
        </main>
      </div>
    </BrowserRouter>
  );
}
