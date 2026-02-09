import React from "react";

export default function ForecastHorizons() {
  return (
    <div className="space-y-4">
      <div>
        <div className="text-xl font-semibold text-slate-100">Forecast Horizons</div>
        <div className="mt-1 text-sm text-slate-400">
          You asked for 3 levels: 15m (fast), 1h (freeze for 1h), 4h (freeze for 4h).
        </div>
      </div>

      <div className="space-y-3">
        <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
          <div className="text-sm font-semibold text-slate-100">15m</div>
          <div className="mt-1 text-sm text-slate-400">
            Updates every 15 minutes. Useful for short-term momentum and entries.
          </div>
        </div>

        <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
          <div className="text-sm font-semibold text-slate-100">1h (frozen)</div>
          <div className="mt-1 text-sm text-slate-400">
            Value is computed at the hour boundary and stays fixed until the next hour boundary.
            This prevents the “09:30→10:30 expected move” changing at 09:45.
          </div>
        </div>

        <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
          <div className="text-sm font-semibold text-slate-100">4h (frozen)</div>
          <div className="mt-1 text-sm text-slate-400">
            Same concept, but locked to the 4-hour candle schedule for stability.
          </div>
        </div>
      </div>
    </div>
  );
}
