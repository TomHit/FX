import React from "react";

type Counts = { hit: number; sl_hit: number; expired: number; other: number };

type StatsResp = {
  ok: boolean;
  day: string; // YYYYMMDD UTC
  uid: string;
  total_closed: number;
  counts: Counts;
  win_rate_vs_sl: number | null;
  by_symbol: Record<string, Counts>;
};

function pad2(n: number) {
  return String(n).padStart(2, "0");
}

// UTC today in YYYYMMDD
function utcTodayYYYYMMDD() {
  const d = new Date();
  const y = d.getUTCFullYear();
  const m = pad2(d.getUTCMonth() + 1);
  const dd = pad2(d.getUTCDate());
  return `${y}${m}${dd}`;
}

// YYYYMMDD -> YYYY-MM-DD (for <input type="date">)
function yyyymmddToDateInput(day: string) {
  if (!day || day.length !== 8) return "";
  return `${day.slice(0, 4)}-${day.slice(4, 6)}-${day.slice(6, 8)}`;
}

// YYYY-MM-DD -> YYYYMMDD
function dateInputToYYYYMMDD(x: string) {
  // x = "YYYY-MM-DD"
  if (!x || x.length !== 10) return "";
  return x.replace(/-/g, "");
}

function addUtcDays(yyyymmdd: string, delta: number) {
  if (!yyyymmdd || yyyymmdd.length !== 8) return yyyymmdd;
  const y = Number(yyyymmdd.slice(0, 4));
  const m = Number(yyyymmdd.slice(4, 6)) - 1;
  const d = Number(yyyymmdd.slice(6, 8));
  const dt = new Date(Date.UTC(y, m, d));
  dt.setUTCDate(dt.getUTCDate() + delta);
  const yy = dt.getUTCFullYear();
  const mm = pad2(dt.getUTCMonth() + 1);
  const dd = pad2(dt.getUTCDate());
  return `${yy}${mm}${dd}`;
}

function pct(n: number | null | undefined) {
  if (typeof n !== "number" || !isFinite(n)) return "—";
  return `${(n * 100).toFixed(1)}%`;
}

function safeNum(x: any) {
  const n = typeof x === "number" ? x : Number(x);
  return isFinite(n) ? n : 0;
}

function Card({
  title,
  value,
  sub,
  right,
  loading,
}: {
  title: string;
  value: React.ReactNode;
  sub?: React.ReactNode;
  right?: React.ReactNode;
  loading?: boolean;
}) {
  return (
    <div className="relative overflow-hidden rounded-2xl border border-slate-800/60 bg-slate-950/60 shadow-[0_0_0_1px_rgba(255,255,255,0.03)]">
      <div className="absolute inset-x-0 top-0 h-12 bg-gradient-to-b from-slate-800/30 to-transparent" />
      <div className="relative p-4">
        <div className="flex items-start justify-between gap-3">
          <div>
            <div className="text-xs font-medium tracking-wide text-slate-400">{title}</div>
            <div className="mt-2">
              {loading ? (
                <div className="h-8 w-28 animate-pulse rounded-lg bg-slate-800/60" />
              ) : (
                <div className="text-2xl font-semibold text-slate-100">{value}</div>
              )}
            </div>
            <div className="mt-1 text-xs text-slate-500">
              {loading ? <div className="h-3 w-36 animate-pulse rounded bg-slate-800/50" /> : sub}
            </div>
          </div>
          {right ? (
            <div className="pt-1 text-xs text-slate-400">{right}</div>
          ) : null}
        </div>
      </div>
    </div>
  );
}

function ProgressRow({
  label,
  count,
  total,
  loading,
}: {
  label: string;
  count: number;
  total: number;
  loading?: boolean;
}) {
  const p = total > 0 ? (count / total) * 100 : 0;
  return (
    <div className="space-y-1">
      <div className="flex items-center justify-between text-xs">
        <div className="text-slate-300">{label}</div>
        {loading ? (
          <div className="h-3 w-20 animate-pulse rounded bg-slate-800/60" />
        ) : (
          <div className="text-slate-400">
            {count} <span className="text-slate-600">({p.toFixed(1)}%)</span>
          </div>
        )}
      </div>
      <div className="h-2 w-full overflow-hidden rounded-full bg-slate-900">
        <div
          className="h-full rounded-full bg-slate-700"
          style={{ width: `${Math.max(0, Math.min(100, p))}%` }}
        />
      </div>
    </div>
  );
}

function SkeletonTable({ rows = 6 }: { rows?: number }) {
  return (
    <div className="space-y-2">
      {Array.from({ length: rows }).map((_, i) => (
        <div key={i} className="h-10 w-full animate-pulse rounded-xl bg-slate-900/70" />
      ))}
    </div>
  );
}

export default function PerformancePage() {
  const todayUtc = React.useMemo(() => utcTodayYYYYMMDD(), []);
  const [day, setDay] = React.useState<string>(todayUtc);

  const [loading, setLoading] = React.useState<boolean>(true);
  const [err, setErr] = React.useState<string | null>(null);
  const [data, setData] = React.useState<StatsResp | null>(null);

  const [q, setQ] = React.useState<string>("");
  const [sortKey, setSortKey] = React.useState<
    "symbol" | "total" | "hit" | "sl_hit" | "expired" | "win"
  >("total");
  const [sortDir, setSortDir] = React.useState<"asc" | "desc">("desc");

  const canNext = day < todayUtc;

  async function load(d: string) {
    setLoading(true);
    setErr(null);
    try {
      const API_ORIGIN = (import.meta.env.VITE_API_ORIGIN || "").replace(/\/+$/, "");
      const res = await fetch(`${API_ORIGIN}/trend/opportunities/stats?day=${encodeURIComponent(d)}`, {

        method: "GET",
        credentials: "include",
        headers: { "Accept": "application/json" },
      });
      if (!res.ok) {
        const t = await res.text().catch(() => "");
        throw new Error(`HTTP ${res.status} ${t || ""}`.trim());
      }
      const json = (await res.json()) as StatsResp;
      if (!json?.ok) throw new Error("API returned ok=false");
      setData(json);
    } catch (e: any) {
      setData(null);
      setErr(e?.message || "Failed to load");
    } finally {
      setLoading(false);
    }
  }

  // initial + on day change
  React.useEffect(() => {
    load(day);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [day]);

  const totals = React.useMemo(() => {
    const c = data?.counts;
    return {
      total: safeNum(data?.total_closed),
      hit: safeNum(c?.hit),
      sl: safeNum(c?.sl_hit),
      exp: safeNum(c?.expired),
      other: safeNum(c?.other),
      win: data?.win_rate_vs_sl ?? null,
    };
  }, [data]);

  const rows = React.useMemo(() => {
    const by = data?.by_symbol || {};
    const out = Object.entries(by).map(([symbol, c]) => {
      const hit = safeNum(c.hit);
      const sl = safeNum(c.sl_hit);
      const expired = safeNum(c.expired);
      const other = safeNum(c.other);
      const total = hit + sl + expired + other;
      const denom = hit + sl;
      const win = denom > 0 ? hit / denom : null;
      return { symbol, hit, sl_hit: sl, expired, other, total, win };
    });

    const qq = q.trim().toUpperCase();
    const filtered = qq ? out.filter(r => r.symbol.toUpperCase().includes(qq)) : out;

    const dir = sortDir === "asc" ? 1 : -1;
    filtered.sort((a, b) => {
      let av: any;
      let bv: any;
      switch (sortKey) {
        case "symbol":
          av = a.symbol;
          bv = b.symbol;
          return av.localeCompare(bv) * dir;
        case "hit":
          return (a.hit - b.hit) * dir;
        case "sl_hit":
          return (a.sl_hit - b.sl_hit) * dir;
        case "expired":
          return (a.expired - b.expired) * dir;
        case "win":
          av = a.win ?? -1;
          bv = b.win ?? -1;
          return (av - bv) * dir;
        case "total":
        default:
          return (a.total - b.total) * dir;
      }
    });

    return filtered;
  }, [data, q, sortKey, sortDir]);

  function toggleSort(k: typeof sortKey) {
    if (sortKey === k) {
      setSortDir(sortDir === "asc" ? "desc" : "asc");
    } else {
      setSortKey(k);
      setSortDir("desc");
    }
  }

  return (
    <div className="min-h-screen bg-slate-950 text-slate-100">
      <div className="mx-auto max-w-7xl px-4 py-6 sm:px-6 lg:px-8">
        {/* Header */}
        <div className="flex flex-col gap-3 sm:flex-row sm:items-end sm:justify-between">
          <div>
            <div className="text-2xl font-semibold tracking-tight">Performance</div>
            <div className="mt-1 text-sm text-slate-400">
              Daily outcomes from Redis <span className="text-slate-600">(UTC day)</span>
            </div>
          </div>

          <div className="flex flex-wrap items-center gap-2">
            <button
              className="rounded-xl border border-slate-800 bg-slate-950/40 px-3 py-2 text-sm text-slate-200 hover:bg-slate-900 disabled:opacity-50"
              onClick={() => setDay(addUtcDays(day, -1))}
              disabled={loading}
              title="Previous day (UTC)"
            >
              ? Prev
            </button>

            <div className="rounded-xl border border-slate-800 bg-slate-950/40 px-3 py-2">
              <div className="text-[11px] text-slate-500">Day</div>
              <input
                type="date"
                className="mt-1 w-[150px] bg-transparent text-sm text-slate-100 outline-none"
                value={yyyymmddToDateInput(day)}
                onChange={(e) => {
                  const v = dateInputToYYYYMMDD(e.target.value);
                  if (v) setDay(v);
                }}
                disabled={loading}
              />
            </div>

            <button
              className="rounded-xl border border-slate-800 bg-slate-950/40 px-3 py-2 text-sm text-slate-200 hover:bg-slate-900 disabled:opacity-50"
              onClick={() => setDay(addUtcDays(day, +1))}
              disabled={loading || !canNext}
              title="Next day (UTC)"
            >
              Next ?
            </button>

            <button
              className="rounded-xl bg-slate-100 px-3 py-2 text-sm font-semibold text-slate-950 hover:bg-white disabled:opacity-60"
              onClick={() => load(day)}
              disabled={loading}
            >
              Refresh
            </button>
          </div>
        </div>

        {/* KPI grid */}
        <div className="mt-6 grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-4">
          <Card
            title="Total Closed"
            value={totals.total}
            sub={<span>Outcomes logged for {day}</span>}
            loading={loading}
          />
          <Card
            title="HIT"
            value={totals.hit}
            sub={<span>Target reached</span>}
            loading={loading}
          />
          <Card
            title="SL Hit"
            value={totals.sl}
            sub={<span>Stopped out</span>}
            loading={loading}
          />
          <Card
            title="Win Rate vs SL"
            value={pct(totals.win)}
            sub={<span>HIT / (HIT + SL)</span>}
            right={!loading ? <span className="text-slate-500">Expired ignored</span> : null}
            loading={loading}
          />
        </div>

        {/* Breakdown */}
        <div className="mt-3 grid grid-cols-1 gap-3 lg:grid-cols-2">
          <div className="rounded-2xl border border-slate-800/60 bg-slate-950/60 p-4 shadow-[0_0_0_1px_rgba(255,255,255,0.03)]">
            <div className="flex items-center justify-between">
              <div className="text-sm font-semibold">Outcome mix</div>
              {!loading ? (
                <div className="text-xs text-slate-500">Total: {totals.total}</div>
              ) : (
                <div className="h-3 w-20 animate-pulse rounded bg-slate-800/60" />
              )}
            </div>

            <div className="mt-4 space-y-4">
              <ProgressRow label="HIT" count={totals.hit} total={totals.total} loading={loading} />
              <ProgressRow label="SL Hit" count={totals.sl} total={totals.total} loading={loading} />
              <ProgressRow label="Expired" count={totals.exp} total={totals.total} loading={loading} />
              {!loading && totals.other > 0 ? (
                <div className="text-xs text-slate-500">
                  Other: <span className="text-slate-300">{totals.other}</span>
                </div>
              ) : null}
            </div>
          </div>

          <div className="rounded-2xl border border-slate-800/60 bg-slate-950/60 p-4 shadow-[0_0_0_1px_rgba(255,255,255,0.03)]">
            <div className="text-sm font-semibold">Notes</div>
            <div className="mt-3 space-y-2 text-sm text-slate-400">
              {loading ? (
                <>
                  <div className="h-4 w-5/6 animate-pulse rounded bg-slate-800/60" />
                  <div className="h-4 w-4/6 animate-pulse rounded bg-slate-800/60" />
                  <div className="h-4 w-3/6 animate-pulse rounded bg-slate-800/60" />
                </>
              ) : (
                <>
                  <div className="rounded-xl border border-slate-800 bg-slate-950/40 p-3">
                    <div className="text-xs font-semibold text-slate-200">Win rate definition</div>
                    <div className="mt-1 text-xs text-slate-500">
                      <span className="text-slate-300">win_rate_vs_sl</span> =
                      hits / (hits + sl_hit). Expired outcomes are excluded.
                    </div>
                  </div>
                  <div className="rounded-xl border border-slate-800 bg-slate-950/40 p-3">
                    <div className="text-xs font-semibold text-slate-200">UTC day</div>
                    <div className="mt-1 text-xs text-slate-500">
                      Stats are keyed by <span className="text-slate-300">YYYYMMDD (UTC)</span>.
                      The selected day is <span className="text-slate-300">{day}</span>.
                    </div>
                  </div>
                </>
              )}
              {err ? (
                <div className="mt-2 rounded-xl border border-red-900/60 bg-red-950/30 p-3 text-xs text-red-200">
                  Failed to load stats: <span className="font-mono">{err}</span>
                </div>
              ) : null}
            </div>
          </div>
        </div>

        {/* Table */}
        <div className="mt-3 rounded-2xl border border-slate-800/60 bg-slate-950/60 p-4 shadow-[0_0_0_1px_rgba(255,255,255,0.03)]">
          <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
            <div>
              <div className="text-sm font-semibold">By symbol</div>
              <div className="mt-1 text-xs text-slate-500">
                Closed outcomes grouped by symbol for the selected day.
              </div>
            </div>

            <div className="flex items-center gap-2">
              <div className="rounded-xl border border-slate-800 bg-slate-950/40 px-3 py-2">
                <div className="text-[11px] text-slate-500">Search</div>
                <input
                  className="mt-1 w-[220px] bg-transparent text-sm text-slate-100 outline-none placeholder:text-slate-600"
                  placeholder="XAUUSD, EURUSD…"
                  value={q}
                  onChange={(e) => setQ(e.target.value)}
                  disabled={loading}
                />
              </div>
            </div>
          </div>

          <div className="mt-4 overflow-hidden rounded-2xl border border-slate-800">
            {loading ? (
              <div className="p-4">
                <SkeletonTable rows={6} />
              </div>
            ) : (
              <table className="w-full border-collapse text-sm">
                <thead className="bg-slate-950/80">
                  <tr className="text-left text-xs text-slate-400">
                    <th className="px-4 py-3">
                      <button
                        className="hover:text-slate-200"
                        onClick={() => toggleSort("symbol")}
                      >
                        Symbol
                      </button>
                    </th>
                    <th className="px-4 py-3 text-right">
                      <button
                        className="hover:text-slate-200"
                        onClick={() => toggleSort("total")}
                      >
                        Total
                      </button>
                    </th>
                    <th className="px-4 py-3 text-right">
                      <button
                        className="hover:text-slate-200"
                        onClick={() => toggleSort("hit")}
                      >
                        HIT
                      </button>
                    </th>
                    <th className="px-4 py-3 text-right">
                      <button
                        className="hover:text-slate-200"
                        onClick={() => toggleSort("sl_hit")}
                      >
                        SL Hit
                      </button>
                    </th>
                    <th className="px-4 py-3 text-right">
                      <button
                        className="hover:text-slate-200"
                        onClick={() => toggleSort("expired")}
                      >
                        Expired
                      </button>
                    </th>
                    <th className="px-4 py-3 text-right">
                      <button
                        className="hover:text-slate-200"
                        onClick={() => toggleSort("win")}
                      >
                        Win%
                      </button>
                    </th>
                  </tr>
                </thead>

                <tbody>
                  {rows.length === 0 ? (
                    <tr>
                      <td className="px-4 py-10 text-center text-slate-500" colSpan={6}>
                        No symbols found for this day.
                      </td>
                    </tr>
                  ) : (
                    rows.map((r) => (
                      <tr
                        key={r.symbol}
                        className="border-t border-slate-900/70 hover:bg-slate-900/30"
                      >
                        <td className="px-4 py-3 font-medium text-slate-200">
                          {r.symbol}
                        </td>
                        <td className="px-4 py-3 text-right text-slate-200">{r.total}</td>
                        <td className="px-4 py-3 text-right text-slate-200">{r.hit}</td>
                        <td className="px-4 py-3 text-right text-slate-200">{r.sl_hit}</td>
                        <td className="px-4 py-3 text-right text-slate-200">{r.expired}</td>
                        <td className="px-4 py-3 text-right text-slate-200">
                          {pct(r.win)}
                        </td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            )}
          </div>

          {!loading && data ? (
            <div className="mt-3 text-xs text-slate-500">
              Source key: <span className="font-mono text-slate-400">xtl:outcomes:{data.uid}:{data.day}</span>
            </div>
          ) : null}
        </div>
      </div>
    </div>
  );
}
