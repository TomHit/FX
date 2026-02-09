import React from "react";

export default function ForecastOverview() {
  return (
    <div className="space-y-4">
      <div>
        <div className="text-xl font-semibold text-slate-100">Forecasts — Overview</div>
        <div className="mt-1 text-sm text-slate-400">
          Forecasts answer two questions: <span className="text-slate-200">direction</span> (up/down/abstain)
          and <span className="text-slate-200">room</span> (expected move).
        </div>
      </div>

      <div className="grid gap-3 md:grid-cols-2">
        <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
          <div className="text-sm font-semibold text-slate-100">Direction</div>
          <div className="mt-1 text-sm text-slate-400">
            BUY/SELL/ABSTAIN is computed on closed candles (not ticking noise). If the market is too flat,
            the model should abstain.
          </div>
        </div>

        <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
          <div className="text-sm font-semibold text-slate-100">Expected move</div>
          <div className="mt-1 text-sm text-slate-400">
            Magnitude for the horizon (15m/1h/4h). This tells the “room” available if direction is correct.
          </div>
        </div>
      </div>

      <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
        <div className="text-sm font-semibold text-slate-100">Why broker time matters</div>
        <div className="mt-1 text-sm text-slate-400">
          All horizons are anchored to broker candle closes. So UI shows broker time next to prices to make
          MT5 matching easy.
        </div>
      </div>
    </div>
  );
}
