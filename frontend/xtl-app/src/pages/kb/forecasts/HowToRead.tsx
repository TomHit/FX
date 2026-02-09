import React from "react";

export default function HowToRead() {
  return (
    <div className="space-y-4">
      <div>
        <div className="text-xl font-semibold text-slate-100">How to Read the Forecast Page</div>
        <div className="mt-1 text-sm text-slate-400">
          A simple order that matches how traders think.
        </div>
      </div>

      <div className="rounded-2xl border border-white/10 bg-white/5 p-4 space-y-2">
        <ol className="list-decimal pl-5 text-sm text-slate-400 space-y-1">
          <li>Confirm broker time and last candle close (match MT5).</li>
          <li>Check direction (BUY/SELL/ABSTAIN).</li>
          <li>Check expected move (is there enough room?).</li>
          <li>Check confidence (low/med/high).</li>
          <li>Only then look at macro “Reasons”.</li>
        </ol>
      </div>

      <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
        <div className="text-sm font-semibold text-slate-100">Practical tip</div>
        <div className="mt-1 text-sm text-slate-400">
          If you’re matching candles: always compare “last closed candle” time, not the forming candle.
        </div>
      </div>
    </div>
  );
}
