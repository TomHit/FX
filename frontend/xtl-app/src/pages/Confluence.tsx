// src/pages/Confluence.tsx
import React, { useState, useEffect, useCallback } from "react";

// ─── types ────────────────────────────────────────────────────────────────────
interface SymbolStatus {
  verdict: "ALLOW" | "WAIT";
  reason: string | null;
  event_name: string | null;
  window: string | null;
  minutes_to_event: number | null;
}

interface UpcomingEvent {
  event: string;
  currency: string;
  datetime_utc: string;
  time_ms: number;
  pre_block_min: number;
  post_block_min: number;
  stabilization_min: number;
  minutes_until: number;
  is_blocking: boolean;
}

interface CalendarStatus {
  ok: boolean;
  source: string;
  events_count: number;
  age_minutes: number;
}

interface NewsRiskData {
  calendar_status: CalendarStatus;
  symbols: Record<string, SymbolStatus>;
  upcoming_events: UpcomingEvent[];
  any_blocked: boolean;
  generated_at_ms: number;
}

// ─── constants ────────────────────────────────────────────────────────────────
const SYMBOLS = ["XAUUSD", "EURUSD", "GBPUSD", "USDJPY", "USDCAD", "USDCHF"];

const SYMBOL_COLORS: Record<string, string> = {
  XAUUSD: "#F5C842",
  EURUSD: "#4A9EFF",
  GBPUSD: "#FF6B6B",
  USDJPY: "#FF9F43",
  USDCAD: "#4ECCA3",
  USDCHF: "#C3A6FF",
};

const CURRENCY_FLAG: Record<string, string> = {
  USD: "🇺🇸", EUR: "🇪🇺", GBP: "🇬🇧", JPY: "🇯🇵",
  CAD: "🇨🇦", CHF: "🇨🇭", AUD: "🇦🇺", NZD: "🇳🇿",
};

const API_URL = "/_api/trend/confluence/news";

// ─── mock fallback ────────────────────────────────────────────────────────────
const MOCK_DATA: NewsRiskData = {
  calendar_status: { ok: true, source: "local_csv", events_count: 15, age_minutes: 12 },
  any_blocked: false,
  symbols: Object.fromEntries(
    SYMBOLS.map((s) => [s, { verdict: "ALLOW", reason: null, event_name: null, window: null, minutes_to_event: null }])
  ) as Record<string, SymbolStatus>,
  upcoming_events: [
    { event: "ISM Manufacturing PMI",    currency: "USD", datetime_utc: "Jun 02  00:30", time_ms: 0, pre_block_min: 15, post_block_min: 15, stabilization_min: 0,  minutes_until: 2100,  is_blocking: false },
    { event: "BOE Gov Bailey Speaks",    currency: "GBP", datetime_utc: "Jun 03  00:30", time_ms: 0, pre_block_min: 15, post_block_min: 30, stabilization_min: 0,  minutes_until: 3540,  is_blocking: false },
    { event: "BOJ Gov Ueda Speaks",      currency: "JPY", datetime_utc: "Jun 03  13:30", time_ms: 0, pre_block_min: 15, post_block_min: 30, stabilization_min: 0,  minutes_until: 4050,  is_blocking: false },
    { event: "ISM Services PMI",         currency: "USD", datetime_utc: "Jun 04  00:30", time_ms: 0, pre_block_min: 15, post_block_min: 15, stabilization_min: 0,  minutes_until: 5580,  is_blocking: false },
    { event: "Unemployment Rate",        currency: "CAD", datetime_utc: "Jun 05  13:30", time_ms: 0, pre_block_min: 15, post_block_min: 15, stabilization_min: 0,  minutes_until: 7890,  is_blocking: false },
    { event: "Core CPI y/y",             currency: "USD", datetime_utc: "Jun 10  13:30", time_ms: 0, pre_block_min: 30, post_block_min: 30, stabilization_min: 15, minutes_until: 15090, is_blocking: false },
    { event: "CPI m/m",                  currency: "USD", datetime_utc: "Jun 10  13:30", time_ms: 0, pre_block_min: 30, post_block_min: 30, stabilization_min: 15, minutes_until: 15090, is_blocking: false },
    { event: "BOC Rate Statement",       currency: "CAD", datetime_utc: "Jun 11  00:15", time_ms: 0, pre_block_min: 60, post_block_min: 60, stabilization_min: 30, minutes_until: 15855, is_blocking: false },
    { event: "Monetary Policy Statement",currency: "EUR", datetime_utc: "Jun 11  13:30", time_ms: 0, pre_block_min: 60, post_block_min: 60, stabilization_min: 30, minutes_until: 16050, is_blocking: false },
    { event: "ECB Press Conference",     currency: "EUR", datetime_utc: "Jun 11  23:15", time_ms: 0, pre_block_min: 15, post_block_min: 30, stabilization_min: 0,  minutes_until: 16635, is_blocking: false },
  ],
  generated_at_ms: Date.now(),
};

// ─── helpers ──────────────────────────────────────────────────────────────────
function formatCountdown(mins: number): string {
  if (mins < 0) return "past";
  if (mins < 60) return `${Math.round(mins)}m`;
  const h = Math.floor(mins / 60);
  const m = Math.round(mins % 60);
  if (h >= 24) return `${Math.floor(h / 24)}d ${h % 24}h`;
  return m > 0 ? `${h}h ${m}m` : `${h}h`;
}

function getImpact(ev: UpcomingEvent): "critical" | "high" | "medium" {
  if (ev.pre_block_min >= 60) return "critical";
  if (ev.pre_block_min >= 30) return "high";
  return "medium";
}

// ─── NewsRiskCard ─────────────────────────────────────────────────────────────
function NewsRiskCard() {
  const [data, setData]         = useState<NewsRiskData | null>(null);
  const [loading, setLoading]   = useState(true);
  const [expanded, setExpanded] = useState(false);
  const [spinning, setSpinning] = useState(false);
  const [errored, setErrored]   = useState(false);

  const fetchData = useCallback(async () => {
    setSpinning(true);
    try {
      const res = await fetch(`${API_URL}?_=${Date.now()}`, {
        credentials: "include",
        cache: "no-store",
      });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      setData(await res.json());
      setErrored(false);
    } catch {
      // Do NOT silently show mock data as if it were real — that hides outages.
      // Keep last good data if we have it; otherwise fall back to mock and flag it.
      setErrored(true);
      setData((prev: NewsRiskData | null) => prev ?? MOCK_DATA);
    } finally {
      setLoading(false);
      setSpinning(false);
    }
  }, []);

  useEffect(() => {
    fetchData();
    const id = setInterval(fetchData, 60_000);
    return () => clearInterval(id);
  }, [fetchData]);

  const anyBlocked     = data?.any_blocked ?? false;
  const events         = data?.upcoming_events ?? [];
  const futureEvents   = events.filter(ev => ev.minutes_until > -ev.post_block_min);
  const visibleEvents  = expanded ? futureEvents : futureEvents.slice(0, 5);
  const calStat        = data?.calendar_status;

  const impactDot: Record<string, string> = {
    critical: "#FF6B6B",
    high:     "#FF9F43",
    medium:   "#4A9EFF",
  };
  const impactBorder: Record<string, string> = {
    critical: "border-red-500/20 bg-red-500/5",
    high:     "border-amber-500/20 bg-amber-500/5",
    medium:   "border-slate-700 bg-slate-800/40",
  };

  return (
    <div className={`rounded-xl border p-5 transition-colors duration-300 ${
      anyBlocked ? "border-red-500/30 bg-slate-900/80" : "border-slate-700 bg-slate-900/60"
    }`}>

      {/* header */}
      <div className="flex items-start justify-between mb-4">
        <div>
          <div className="flex items-center gap-2.5">
            <span className="text-sm font-semibold text-slate-100">News Risk</span>

            {/* status badge */}
            <span className={`flex items-center gap-1.5 px-2 py-0.5 rounded-full text-[10px] font-semibold tracking-wider font-mono border ${
              anyBlocked
                ? "bg-red-500/15 border-red-500/30 text-red-400"
                : "bg-emerald-500/12 border-emerald-500/25 text-emerald-400"
            }`}>
              <span className={`w-1.5 h-1.5 rounded-full ${anyBlocked ? "bg-red-400 animate-pulse" : "bg-emerald-400"}`} />
              {anyBlocked ? "BLOCKED" : "CLEAR"}
            </span>
          </div>

          {/* calendar meta */}
          <div className="mt-1 flex items-center gap-1.5 text-[11px] text-slate-500 font-mono">
            <span className={`w-1.5 h-1.5 rounded-full ${
              !calStat?.ok ? "bg-red-400" : (calStat.age_minutes > 480 ? "bg-amber-400" : "bg-emerald-400")
            }`} />
            {calStat?.ok
              ? `${calStat.events_count} events · ${Math.round(calStat.age_minutes ?? 0)}m ago · ${calStat.source}`
              : "calendar unavailable"
            }
            {errored && (
              <span className="ml-1 text-amber-400" title="Live fetch failed — showing fallback data">
                ⚠ offline
              </span>
            )}
          </div>
        </div>

        {/* refresh button */}
        <button
          onClick={fetchData}
          className="text-slate-500 hover:text-emerald-400 transition-colors p-1"
          title="Refresh"
        >
          <svg
            className={`w-4 h-4 ${spinning ? "animate-spin" : ""}`}
            fill="none" stroke="currentColor" viewBox="0 0 24 24"
          >
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
              d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" />
          </svg>
        </button>
      </div>

      {/* symbol grid */}
      <div className="grid grid-cols-3 gap-1.5 mb-4">
        {SYMBOLS.map((sym) => {
          const s       = data?.symbols?.[sym];
          const blocked = s?.verdict === "WAIT";
          const color   = SYMBOL_COLORS[sym];
          return (
            <div
              key={sym}
              className={`flex items-center gap-2 px-2.5 py-1.5 rounded-md border text-[11px] font-mono font-semibold transition-colors ${
                blocked
                  ? "border-red-500/30 bg-red-500/10 text-red-400"
                  : "border-slate-700 bg-slate-800/50 text-slate-300"
              }`}
            >
              <span
                className="w-2 h-2 rounded-full flex-shrink-0"
                style={{ background: blocked ? "#FF6B6B" : color }}
              />
              {sym}
              {blocked && (
                <span className="ml-auto text-amber-400 text-[10px]">⚡</span>
              )}
            </div>
          );
        })}
      </div>

      {/* divider */}
      <div className="h-px bg-slate-700/60 mb-3" />

      {/* events label */}
      <div className="text-[10px] font-semibold tracking-widest text-slate-500 font-mono uppercase mb-2">
        Upcoming Events
      </div>

      {/* event list */}
      {loading && !data ? (
        <div className="text-slate-500 text-xs text-center py-4">Loading calendar...</div>
      ) : events.length === 0 ? (
        <div className="text-slate-500 text-xs text-center py-4 italic">
          No HIGH impact events upcoming
        </div>
      ) : (
        <div className="flex flex-col gap-1">
          {visibleEvents.map((ev, i) => {
            const impact  = getImpact(ev);
            const dot     = impactDot[impact];
            const rowCls  = ev.is_blocking
              ? "border-red-500/30 bg-red-500/8"
              : impactBorder[impact];
            const soon    = ev.minutes_until < 120 && ev.minutes_until >= 0;
            const flag    = CURRENCY_FLAG[ev.currency] ?? "🌐";

            return (
              <div
                key={`${ev.event}_${ev.time_ms}_${i}`}
                className={`grid gap-2.5 items-center px-3 py-2 rounded-md border text-[11px] ${rowCls}`}
                style={{ gridTemplateColumns: "110px minmax(0,1fr) 36px 44px" }}
              >
                {/* date time */}
                <span className="font-mono text-[10px] text-slate-500 whitespace-nowrap">
                  {ev.datetime_utc}
                </span>

                {/* name */}
                <div className="flex items-center gap-1.5 min-w-0">
                  <span
                    className="w-1.5 h-1.5 rounded-full flex-shrink-0"
                    style={{ background: dot }}
                  />
                  <span className="text-slate-200 truncate">{ev.event}</span>
                </div>

                {/* currency */}
                 <span className="font-mono text-slate-500 text-center text-[10px]">
                   {ev.currency}
                 </span>

                {/* countdown */}
                <span className={`font-mono text-right font-medium ${
                  ev.is_blocking
                    ? "text-red-400"
                    : soon
                    ? "text-amber-400"
                    : impact === "critical"
                    ? "text-red-400/70"
                    : "text-slate-500"
                }`}>
                  {ev.is_blocking ? "🔴 LIVE" : formatCountdown(ev.minutes_until)}
                </span>
              </div>
            );
          })}
        </div>
      )}

      {/* expand / collapse */}
      {futureEvents.length > 5 && (
        <button
          onClick={() => setExpanded((e) => !e)}
          className="w-full mt-2 py-1.5 rounded-md border border-slate-700 bg-slate-800/40 hover:bg-slate-800 text-slate-500 hover:text-slate-300 text-[11px] font-mono tracking-wider transition-colors"
        >
          {expanded ? "▲  show less" : `▼  ${futureEvents.length - 5} more events`}
        </button>
      )}

      {/* legend */}
      <div className="flex flex-wrap gap-3 mt-3 pt-3 border-t border-slate-700/60">
        {[
          { color: "#FF6B6B", label: "Rate decision (60m)" },
          { color: "#FF9F43", label: "CPI / GDP (30m)" },
          { color: "#4A9EFF", label: "Speech / PMI (15m)" },
        ].map(({ color, label }) => (
          <div key={label} className="flex items-center gap-1.5">
            <span className="w-1.5 h-1.5 rounded-full" style={{ background: color }} />
            <span className="text-[10px] text-slate-500 font-mono">{label}</span>
          </div>
        ))}
      </div>
    </div>
  );
}

// ─── page ─────────────────────────────────────────────────────────────────────
export default function ConfluenceIntelligence() {
  return (
    <main className="min-h-screen bg-[#071120] text-slate-100 p-8">
      <section className="mx-auto max-w-7xl">
        <h1 className="text-3xl font-bold">Confluence Intelligence</h1>
        <p className="mt-2 text-slate-400">
          Macro, news risk, sentiment, and trade validation overview.
        </p>

        {/* grid — add more cards here as they're built */}
        <div className="mt-8 flex flex-col gap-6">

          {/* News Risk card — full width */}
          <NewsRiskCard />

          {/* second row */}
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">

          {/* Macro Bias placeholder */}
          <div className="rounded-xl border border-slate-700 bg-slate-900/60 p-5">
            <h2 className="text-sm font-semibold text-slate-100">Macro Bias</h2>
            <p className="mt-2 text-xs text-slate-500">DXY · US10Y · VIX — coming soon</p>
          </div>

          {/* Confluence Score placeholder */}
          <div className="rounded-xl border border-slate-700 bg-slate-900/60 p-5">
            <h2 className="text-sm font-semibold text-slate-100">Confluence Score</h2>
            <p className="mt-2 text-xs text-slate-500">Trade validation score — coming soon</p>
          </div>
        </div>{/* end second row grid */}
        </div>{/* end flex col */}
      
      </section>
    </main>
  );
}
