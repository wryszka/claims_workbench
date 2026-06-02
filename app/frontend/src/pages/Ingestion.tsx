import { Database } from 'lucide-react';
export default function Ingestion() {
  return <Stub title="Ingestion" sub="DLT pipeline status & data-quality evidence — the &quot;no claims data lost&quot; story." />;
}
function Stub({ title, sub }: { title: string; sub: string }) {
  return (
    <div className="p-10">
      <div className="flex items-center gap-3 mb-2"><Database className="w-6 h-6 text-blue-600" /><h1 className="text-2xl font-bold text-slate-800">{title}</h1></div>
      <p className="text-gray-500 max-w-2xl">{sub}</p>
      <div className="mt-6 inline-block rounded-lg border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-800">Built in Stage B.</div>
    </div>
  );
}
