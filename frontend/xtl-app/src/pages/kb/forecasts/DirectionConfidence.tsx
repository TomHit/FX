import React from "react";

export default function DirectionConfidence() {
  return (
    <div className="space-y-4">
      <div>
        <div className="text-xl font-semibold text-slate-100">Direction & Confidence</div>
        <div className="mt-1 text-sm text-slate-400">
          Direction is BUY/SELL/ABSTAIN. Confidence reflects how far above the threshold the probability is.
        </div>
      </div>

      <div className="grid gap-3 md:grid-cols-2">
        <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
          <div className="text-sm font-semibold text-slate-100">ABSTAIN</div>
          <div className="mt-1 text-sm text-slate-400">
            If the model’s probability is near 50/50 or expected move is tiny, abstain is correct.
          </div>
        </div>

        <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
          <div className="text-sm font-semibold text-slate-100">Confidence levels</div>
          <div className="mt-1 text-sm text-slate-400">
            Low / Medium / High are mapped from probability vs threshold (and can be symbol-specific).
          </div>
        </div>
      </div>

      <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
        <div className="text-sm font-semibold text-slate-100">Close-based</div>
        <div className="mt-1 text-sm text-slate-400">
          Direction is computed from closed candle features to avoid flip-flopping during ticks.
        </div>
      </div>
    </div>
  );
}
