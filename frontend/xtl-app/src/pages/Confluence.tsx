// src/pages/Confluence.tsx
import React, { useCallback, useEffect, useMemo, useState } from "react";

// ─── types ────────────────────────────────────────────────────────────────────

type EventVerdict = "ALLOW" | "WAIT";

interface SymbolStatus {
  verdict: EventVerdict;
  reason: string | null;
  event_name: string | null;
  window: string | null;
  minutes_to_event: number | null;

  // New MT5-calendar fields are optional so the page remains compatible with
  // the current /confluence/news endpoint while the backend exposes more detail.
  event_tier?: string | null;
  event_mode?: string | null;
  calendar_source?: string | null;
}

interface UpcomingEvent {
  event: string;
  currency: string;
  datetime_utc: string;
  time_ms: number;
  pre_block_min: number;
  post_block_min: number;
  stabilization_min?: number;
  minutes_until: number;
  is_blocking: boolean;

  // Optional canonical MT5 metadata.
  event_code?: string | null;
  event_tier?: string | null;
  calendar_source?: string | null;
}

interface CalendarStatus {
  ok: boolean;
  source?: string | null;
  events_count?: number;
  age_minutes?: number | null;
  reason?: string | null;
  event_mode?: string | null;
  server_received_ms?: number | null;
  coverage_from_utc_ms?: number | null;
  coverage_to_utc_ms?: number | null;
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
  USD: "🇺🇸",
  EUR: "🇪🇺",
  GBP: "🇬🇧",
  JPY: "🇯🇵",
  CAD: "🇨🇦",
  CHF: "🇨🇭",
  AUD: "🇦🇺",
  NZD: "🇳🇿",
};

const API_URL = "/_api/trend/confluence/news";

// ─── helpers ──────────────────────────────────────────────────────────────────

function formatCountdown(mins: number): string {
  if (!Number.isFinite(mins)) return "—";
  if (mins < 0) return `${Math.abs(Math.round(mins))}m ago`;
  if (mins < 60) return `${Math.round(mins)}m`;

  const h = Math.floor(mins / 60);
  const m = Math.round(mins % 60);

  if (h >= 24) return `${Math.floor(h / 24)}d ${h % 24}h`;
  return m > 0 ? `${h}h ${m}m` : `${h}h`;
}

function inferredTier(ev: UpcomingEvent): "TIER_1_MAJOR" | "TIER_2_HIGH" | "INFO" {
  const explicit = String(ev.event_tier ?? "").toUpperCase();

  if (explicit.includes("TIER_1")) return "TIER_1_MAJOR";
  if (explicit.includes("TIER_2")) return "TIER_2_HIGH";

  // Current canonical policy:
  // Tier 1 = 30m PRE / 30m POST
  // Tier 2 = 15m PRE / 15m POST
  if ((ev.pre_block_min ?? 0) >= 30) return "TIER_1_MAJOR";
  if ((ev.pre_block_min ?? 0) >= 15) return "TIER_2_HIGH";
  return "INFO";
}

function tierLabel(tier: ReturnType<typeof inferredTier>): string {
  if (tier === "TIER_1_MAJOR") return "T1";
  if (tier === "TIER_2_HIGH") return "T2";
  return "INFO";
}

function isCalendarBypass(data: NewsRiskData | null): boolean {
  if (!data) return false;

  if (!data.calendar_status?.ok) return true;

  return SYMBOLS.some((sym) => {
    const s = data.symbols?.[sym];
    return (
      s?.reason === "EVENT_DATA_UNAVAILABLE_BYPASS" ||
      s?.event_mode === "UNAVAILABLE_BYPASS" ||
      s?.event_mode === "ERROR_BYPASS"
    );
  });
}

function formatAge(minutes?: number | null): string {
  if (minutes == null || !Number.isFinite(minutes)) return "age unknown";
  if (minutes < 60) return `${Math.round(minutes)}m ago`;
  if (minutes < 1440) return `${(minutes / 60).toFixed(1)}h ago`;
  return `${(minutes / 1440).toFixed(1)}d ago`;
}

function symbolStatusLabel(status?: SymbolStatus): "WAIT" | "BYPASS" | "CLEAR" {
  if (status?.verdict === "WAIT") return "WAIT";

  if (
    status?.reason === "EVENT_DATA_UNAVAILABLE_BYPASS" ||
    status?.event_mode === "UNAVAILABLE_BYPASS" ||
    status?.event_mode === "ERROR_BYPASS"
  ) {
    return "BYPASS";
  }

  return "CLEAR";
}

// ─── NewsRiskCard ─────────────────────────────────────────────────────────────

function NewsRiskCard() {
  const [data, setData] = useState<NewsRiskData | null>(null);
  const [loading, setLoading] = useState(true);
  const [expanded, setExpanded] = useState(false);
  const [spinning, setSpinning] = useState(false);
  const [fetchError, setFetchError] = useState<string | null>(null);

  const fetchData = useCallback(async () => {
    setSpinning(true);

    try {
      const res = await fetch(`${API_URL}?_=${Date.now()}`, {
        credentials: "include",
        cache: "no-store",
      });

      if (!res.ok) {
        throw new Error(`HTTP ${res.status}`);
      }

      const payload = (await res.json()) as NewsRiskData;
      setData(payload);
      setFetchError(null);
    } catch (err) {
      // Never fabricate calendar/events in production UI.
      // Keep the last good response if one exists and clearly mark the API issue.
      setFetchError(err instanceof Error ? err.message : "request failed");
    } finally {
      setLoading(false);
      setSpinning(false);
    }
  }, []);

  useEffect(() => {
    fetchData();
    const id = window.setInterval(fetchData, 60_000);
    return () => window.clearInterval(id);
  }, [fetchData]);

  const anyBlocked = data?.any_blocked ?? false;
  const calendarBypass = isCalendarBypass(data);
  const events = data?.upcoming_events ?? [];
  const calStat = data?.calendar_status;

  const futureEvents = useMemo(
    () =>
      events.filter(
        (ev) =>
          ev.minutes_until >
          -Math.max(0, Number(ev.post_block_min ?? 0), Number(ev.stabilization_min ?? 0))
      ),
    [events]
  );

  const visibleEvents = expanded ? futureEvents : futureEvents.slice(0, 6);

  const overallMode: "BLOCKED" | "BYPASS" | "PROTECTED" | "OFFLINE" =
    fetchError && !data
      ? "OFFLINE"
      : anyBlocked
      ? "BLOCKED"
      : calendarBypass
      ? "BYPASS"
      : "PROTECTED";

  const overallBadgeClass =
    overallMode === "BLOCKED"
      ? "bg-red-500/15 border-red-500/30 text-red-400"
      : overallMode === "BYPASS"
      ? "bg-amber-500/15 border-amber-500/30 text-amber-300"
      : overallMode === "OFFLINE"
      ? "bg-slate-500/15 border-slate-500/30 text-slate-300"
      : "bg-emerald-500/12 border-emerald-500/25 text-emerald-400";

  const overallDotClass =
    overallMode === "BLOCKED"
      ? "bg-red-400 animate-pulse"
      : overallMode === "BYPASS"
      ? "bg-amber-400"
      : overallMode === "OFFLINE"
      ? "bg-slate-400"
      : "bg-emerald-400";

  const calendarMeta = (() => {
    if (!data && fetchError) return "API unavailable";
    if (!calStat?.ok) return "event data unavailable · XTL trading normally (bypass)";

    const source = calStat.source || "MT5_CALENDAR";
    const count = Number(calStat.events_count ?? events.length ?? 0);
    return `${count} events · ${formatAge(calStat.age_minutes)} · ${source}`;
  })();

  return (
    <div
      className={`rounded-xl border p-5 transition-colors duration-300 ${
        anyBlocked
          ? "border-red-500/30 bg-slate-900/80"
          : calendarBypass
          ? "border-amber-500/25 bg-slate-900/70"
          : "border-slate-700 bg-slate-900/60"
      }`}
    >
      {/* header */}
      <div className="flex items-start justify-between gap-4 mb-4">
        <div className="min-w-0">
          <div className="flex flex-wrap items-center gap-2.5">
            <span className="text-sm font-semibold text-slate-100">
              Event Protection
            </span>

            <span
              className={`flex items-center gap-1.5 px-2 py-0.5 rounded-full text-[10px] font-semibold tracking-wider font-mono border ${overallBadgeClass}`}
            >
              <span className={`w-1.5 h-1.5 rounded-full ${overallDotClass}`} />
              {overallMode}
            </span>

            <span className="px-2 py-0.5 rounded-full border border-slate-700 bg-slate-800/60 text-[10px] text-slate-400 font-mono">
              MT5 CALENDAR
            </span>
          </div>

          <div className="mt-1.5 flex flex-wrap items-center gap-1.5 text-[11px] text-slate-500 font-mono">
            <span
              className={`w-1.5 h-1.5 rounded-full ${
                !calStat?.ok
                  ? "bg-amber-400"
                  : (calStat.age_minutes ?? 0) > 120
                  ? "bg-amber-400"
                  : "bg-emerald-400"
              }`}
            />
            <span>{calendarMeta}</span>

            {fetchError && data && (
              <span
                className="ml-1 text-amber-400"
                title={`Live refresh failed: ${fetchError}`}
              >
                ⚠ refresh delayed
              </span>
            )}
          </div>

          <p className="mt-2 text-[11px] leading-relaxed text-slate-500 max-w-3xl">
            Events are an optional execution-safety layer only. They never set trade
            direction or bias. If MT5 event data is unavailable, this layer bypasses
            automatically and the normal XTL RC → Point-A execution path remains active.
          </p>
        </div>

        <button
          onClick={fetchData}
          className="text-slate-500 hover:text-emerald-400 transition-colors p-1 flex-shrink-0"
          title="Refresh event protection"
          aria-label="Refresh event protection"
        >
          <svg
            className={`w-4 h-4 ${spinning ? "animate-spin" : ""}`}
            fill="none"
            stroke="currentColor"
            viewBox="0 0 24 24"
          >
            <path
              strokeLinecap="round"
              strokeLinejoin="round"
              strokeWidth={2}
              d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"
            />
          </svg>
        </button>
      </div>

      {/* current operational state */}
      {overallMode === "BYPASS" && (
        <div className="mb-4 rounded-lg border border-amber-500/20 bg-amber-500/5 px-3 py-2 text-[11px] text-amber-200/90">
          <span className="font-semibold">Event protection bypassed.</span>{" "}
          Calendar data is missing, stale, or unsupported by this broker. New XTL
          entries are not stopped; normal technical gates and Point-A remain authoritative.
        </div>
      )}

      {overallMode === "BLOCKED" && (
        <div className="mb-4 rounded-lg border border-red-500/25 bg-red-500/8 px-3 py-2 text-[11px] text-red-200/90">
          <span className="font-semibold">New entry held by event window.</span>{" "}
          The crossed RC opportunity is preserved. When the window clears, XTL
          re-evaluates the same RC/cross through current Point-A; no second breakout is
          required while price remains valid.
        </div>
      )}

      {overallMode === "OFFLINE" && (
        <div className="mb-4 rounded-lg border border-slate-600 bg-slate-800/50 px-3 py-2 text-[11px] text-slate-300">
          Confluence API is unavailable. No mock calendar is displayed.
        </div>
      )}

      {/* symbol grid */}
      <div className="grid grid-cols-2 md:grid-cols-3 gap-1.5 mb-4">
        {SYMBOLS.map((sym) => {
          const s = data?.symbols?.[sym];
          const label = symbolStatusLabel(s);
          const blocked = label === "WAIT";
          const bypass = label === "BYPASS";
          const color = SYMBOL_COLORS[sym];

          return (
            <div
              key={sym}
              title={
                blocked
                  ? s?.reason ?? "Event wait"
                  : bypass
                  ? "Event layer unavailable — normal XTL execution continues"
                  : "No event hold"
              }
              className={`flex items-center gap-2 px-2.5 py-2 rounded-md border text-[11px] font-mono font-semibold transition-colors ${
                blocked
                  ? "border-red-500/30 bg-red-500/10 text-red-300"
                  : bypass
                  ? "border-amber-500/20 bg-amber-500/5 text-amber-200"
                  : "border-slate-700 bg-slate-800/50 text-slate-300"
              }`}
            >
              <span
                className="w-2 h-2 rounded-full flex-shrink-0"
                style={{
                  background: blocked ? "#FF6B6B" : bypass ? "#F59E0B" : color,
                }}
              />

              <span>{sym}</span>

              <span
                className={`ml-auto text-[9px] tracking-wider ${
                  blocked
                    ? "text-red-400"
                    : bypass
                    ? "text-amber-400"
                    : "text-emerald-500"
                }`}
              >
                {label}
              </span>
            </div>
          );
        })}
      </div>

      {/* active blocking details */}
      {anyBlocked && data && (
        <div className="mb-4 grid grid-cols-1 md:grid-cols-2 gap-2">
          {SYMBOLS.map((sym) => {
            const s = data.symbols?.[sym];
            if (s?.verdict !== "WAIT") return null;

            const mins = s.minutes_to_event;
            const timing =
              mins == null
                ? ""
                : mins >= 0
                ? `${Math.round(mins)}m before release`
                : `${Math.abs(Math.round(mins))}m after release`;

            return (
              <div
                key={`blocking_${sym}`}
                className="rounded-md border border-red-500/20 bg-red-500/5 px-3 py-2"
              >
                <div className="flex items-center justify-between gap-3">
                  <span className="text-[11px] font-semibold text-red-300">
                    {sym} · EVENT WAIT
                  </span>
                  <span className="text-[10px] font-mono text-red-400">
                    {s.window ?? "EVENT"}
                  </span>
                </div>
                <div className="mt-1 text-[11px] text-slate-300 truncate">
                  {s.event_name ?? s.reason ?? "Relevant high-impact event"}
                </div>
                {timing && (
                  <div className="mt-0.5 text-[10px] font-mono text-slate-500">
                    {timing}
                  </div>
                )}
              </div>
            );
          })}
        </div>
      )}

      <div className="h-px bg-slate-700/60 mb-3" />

      {/* events label */}
      <div className="flex items-center justify-between gap-3 mb-2">
        <div className="text-[10px] font-semibold tracking-widest text-slate-500 font-mono uppercase">
          Upcoming XTL Gate Events
        </div>
        <div className="text-[10px] text-slate-600 font-mono">
          UTC · Tier 1 30/30 · Tier 2 15/15
        </div>
      </div>

      {/* event list */}
      {loading && !data ? (
        <div className="text-slate-500 text-xs text-center py-5">
          Loading MT5 calendar...
        </div>
      ) : !data && fetchError ? (
        <div className="text-slate-500 text-xs text-center py-5">
          Event feed UI unavailable
        </div>
      ) : futureEvents.length === 0 ? (
        <div className="text-slate-500 text-xs text-center py-5 italic">
          {calendarBypass
            ? "No usable MT5 event calendar — event layer is bypassed"
            : "No classified XTL gate events in current coverage"}
        </div>
      ) : (
        <div className="flex flex-col gap-1">
          {visibleEvents.map((ev, i) => {
            const tier = inferredTier(ev);
            const isTier1 = tier === "TIER_1_MAJOR";
            const isTier2 = tier === "TIER_2_HIGH";
            const soon = ev.minutes_until < 120 && ev.minutes_until >= 0;
            const flag = CURRENCY_FLAG[ev.currency] ?? "🌐";

            const rowCls = ev.is_blocking
              ? "border-red-500/30 bg-red-500/8"
              : isTier1
              ? "border-red-500/15 bg-red-500/[0.035]"
              : isTier2
              ? "border-amber-500/15 bg-amber-500/[0.035]"
              : "border-slate-700 bg-slate-800/40";

            const tierCls = isTier1
              ? "text-red-400 border-red-500/25 bg-red-500/10"
              : isTier2
              ? "text-amber-400 border-amber-500/25 bg-amber-500/10"
              : "text-slate-500 border-slate-700 bg-slate-800";

            return (
              <div
                key={`${ev.event}_${ev.time_ms}_${i}`}
                className={`grid gap-2.5 items-center px-3 py-2.5 rounded-md border text-[11px] ${rowCls}`}
                style={{
                  gridTemplateColumns:
                    "92px 44px minmax(0,1fr) 72px 76px",
                }}
              >
                {/* UTC time */}
                <span className="font-mono text-[10px] text-slate-500 whitespace-nowrap">
                  {ev.datetime_utc}
                </span>

                {/* currency */}
                <span className="font-mono text-slate-400 text-[10px] whitespace-nowrap">
                  {flag} {ev.currency}
                </span>

                {/* event */}
                <div className="min-w-0">
                  <div className="flex items-center gap-1.5 min-w-0">
                    <span
                      className={`w-1.5 h-1.5 rounded-full flex-shrink-0 ${
                        ev.is_blocking
                          ? "bg-red-400 animate-pulse"
                          : isTier1
                          ? "bg-red-400"
                          : isTier2
                          ? "bg-amber-400"
                          : "bg-slate-500"
                      }`}
                    />
                    <span className="text-slate-200 truncate">{ev.event}</span>
                  </div>
                  <div className="mt-0.5 text-[9px] text-slate-600 font-mono">
                    PRE {ev.pre_block_min ?? 0}m · POST {ev.post_block_min ?? 0}m
                  </div>
                </div>

                {/* tier */}
                <span
                  className={`justify-self-start px-1.5 py-0.5 rounded border text-[9px] font-mono font-semibold ${tierCls}`}
                >
                  {tierLabel(tier)}
                </span>

                {/* countdown */}
                <span
                  className={`font-mono text-right font-medium ${
                    ev.is_blocking
                      ? "text-red-400"
                      : soon
                      ? "text-amber-400"
                      : isTier1
                      ? "text-red-400/70"
                      : "text-slate-500"
                  }`}
                >
                  {ev.is_blocking ? "🔴 ACTIVE" : formatCountdown(ev.minutes_until)}
                </span>
              </div>
            );
          })}
        </div>
      )}

      {/* expand / collapse */}
      {futureEvents.length > 6 && (
        <button
          onClick={() => setExpanded((e) => !e)}
          className="w-full mt-2 py-1.5 rounded-md border border-slate-700 bg-slate-800/40 hover:bg-slate-800 text-slate-500 hover:text-slate-300 text-[11px] font-mono tracking-wider transition-colors"
        >
          {expanded ? "▲  show less" : `▼  ${futureEvents.length - 6} more events`}
        </button>
      )}

      {/* policy legend */}
      <div className="mt-3 pt-3 border-t border-slate-700/60 grid grid-cols-1 md:grid-cols-3 gap-2">
        <div className="rounded-md border border-red-500/15 bg-red-500/[0.03] px-2.5 py-2">
          <div className="text-[10px] font-semibold text-red-400 font-mono">
            TIER 1 · 30 / 30
          </div>
          <div className="mt-0.5 text-[10px] leading-relaxed text-slate-500">
            CPI, Core PCE, NFP, FOMC / major rate-policy shocks.
          </div>
        </div>

        <div className="rounded-md border border-amber-500/15 bg-amber-500/[0.03] px-2.5 py-2">
          <div className="text-[10px] font-semibold text-amber-400 font-mono">
            TIER 2 · 15 / 15
          </div>
          <div className="mt-0.5 text-[10px] leading-relaxed text-slate-500">
            GDP, durable goods, jobless claims, retail / meaningful high macro.
          </div>
        </div>

        <div className="rounded-md border border-slate-700 bg-slate-800/30 px-2.5 py-2">
          <div className="text-[10px] font-semibold text-slate-400 font-mono">
            NO CALENDAR · BYPASS
          </div>
          <div className="mt-0.5 text-[10px] leading-relaxed text-slate-500">
            Event filter is skipped; existing XTL gates and Point-A continue normally.
          </div>
        </div>
      </div>

      {data?.generated_at_ms ? (
        <div className="mt-3 text-right text-[9px] text-slate-700 font-mono">
          snapshot {new Date(data.generated_at_ms).toISOString().replace("T", " ").slice(0, 19)} UTC
        </div>
      ) : null}
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
          Execution safety, market context, and trade validation overview.
        </p>

        <div className="mt-8 flex flex-col gap-6">
          {/* Event Protection — canonical MT5 calendar safety layer */}
          <NewsRiskCard />

          {/* second row */}
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            {/* Bias / Regime remains deliberately separate from events */}
            <div className="rounded-xl border border-slate-700 bg-slate-900/60 p-5">
              <h2 className="text-sm font-semibold text-slate-100">
                Multi-day Bias &amp; Regime
              </h2>
              <p className="mt-2 text-xs text-slate-500">
                D1 · H4 · H1 structure and DXY context — next phase
              </p>
              <div className="mt-3 text-[10px] text-slate-600 font-mono">
                Separate engine · events do not set direction
              </div>
            </div>

            {/* Confluence Score placeholder */}
            <div className="rounded-xl border border-slate-700 bg-slate-900/60 p-5">
              <h2 className="text-sm font-semibold text-slate-100">
                Confluence Score
              </h2>
              <p className="mt-2 text-xs text-slate-500">
                Trade validation score — coming soon
              </p>
            </div>
          </div>
        </div>
      </section>
    </main>
  );
}
