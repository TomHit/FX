import React from "react";

export default function CommonMisunderstandings() {
  return (
    <div className="space-y-4">
      <div>
        <div className="text-xl font-semibold text-slate-100">Common Misunderstandings</div>
        <div className="mt-1 text-sm text-slate-400">
          Things that usually cause confusion in the first week.
        </div>
      </div>

      <div className="space-y-3">
        <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
          <div className="text-sm font-semibold text-slate-100">“Expected move keeps changing”</div>
          <div className="mt-1 text-sm text-slate-400">
            Only the fast horizon updates frequently. 1h/4h should be frozen to the candle boundary.
          </div>
        </div>

        <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
          <div className="text-sm font-semibold text-slate-100">“Model says BUY but price didn’t move”</div>
          <div className="mt-1 text-sm text-slate-400">
            If realized movement is below tau/ATR, the correct output is often ABSTAIN.
          </div>
        </div>

        <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
          <div className="text-sm font-semibold text-slate-100">“Broker time mismatch”</div>
          <div className="mt-1 text-sm text-slate-400">
            Always use broker candle timestamps. UI shows Broker and UTC so you can verify quickly.
          </div>
        </div>
      </div>
    </div>
  );
}
