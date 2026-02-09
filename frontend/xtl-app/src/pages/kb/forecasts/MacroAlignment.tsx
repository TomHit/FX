import React from "react";

export default function MacroAlignment() {
  return (
    <div className="space-y-4">
      <div>
        <div className="text-xl font-semibold text-slate-100">Macro Alignment</div>
        <div className="mt-1 text-sm text-slate-400">
          Macro features (DXY, yields, VIX, etc.) are not for “decoration”. They help when regime shifts matter.
        </div>
      </div>

      <div className="rounded-2xl border border-white/10 bg-white/5 p-4 space-y-2">
        <div className="text-sm font-semibold text-slate-100">When macro helps</div>
        <ul className="list-disc pl-5 text-sm text-slate-400 space-y-1">
          <li>Risk-on / risk-off transitions</li>
          <li>USD strength regime changes (DXY)</li>
          <li>Volatility spikes (VIX-like proxies)</li>
        </ul>
      </div>

      <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
        <div className="text-sm font-semibold text-slate-100">If it doesn’t change the forecast…</div>
        <div className="mt-1 text-sm text-slate-400">
          That means either the macro signal is stable right now, or the model learned it’s not useful for this symbol/horizon.
          We’ll expose “Reasons” as a small, explainable set (not a wall of text).
        </div>
      </div>
    </div>
  );
}
