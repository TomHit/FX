import React from "react";

export default function ExpectedMove() {
  return (
    <div className="space-y-4">
      <div>
        <div className="text-xl font-semibold text-slate-100">Expected Move</div>
        <div className="mt-1 text-sm text-slate-400">
          Expected move is magnitude (how far price can reasonably travel) for a given horizon.
        </div>
      </div>

      <div className="rounded-2xl border border-white/10 bg-white/5 p-4 space-y-2">
        <div className="text-sm font-semibold text-slate-100">How to use it</div>
        <ul className="list-disc pl-5 text-sm text-slate-400 space-y-1">
          <li>Use it to estimate room for TP and whether a trade is worth taking.</li>
          <li>Direction tells the side; expected move tells the distance.</li>
          <li>If expected move is small, best behavior is often ABSTAIN.</li>
        </ul>
      </div>

      <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
        <div className="text-sm font-semibold text-slate-100">Per-symbol behavior</div>
        <div className="mt-1 text-sm text-slate-400">
          Different symbols move differently. Thresholds should be ATR-based per symbol (tau/abstain band).
        </div>
      </div>
    </div>
  );
}
