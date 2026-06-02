import { Layers } from 'lucide-react';
export default function Transformation() {
  return (
    <div className="p-10">
      <div className="flex items-center gap-3 mb-2"><Layers className="w-6 h-6 text-blue-600" /><h1 className="text-2xl font-bold text-slate-800">Transformation</h1></div>
      <p className="text-gray-500 max-w-2xl">Silver enrichment — &quot;everything we know before the handler picks up the phone&quot; for the selected claim.</p>
      <div className="mt-6 inline-block rounded-lg border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-800">Built in Stage B.</div>
    </div>
  );
}
