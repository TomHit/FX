import React from "react";

type BrokerAccount = {
  balance?: number;
  equity?: number;
  margin?: number;
  free_margin?: number;
  floating_pnl?: number;
  leverage?: number;
};

type PropConfig = {
  enabled: boolean;
  firm: string;
  phase: string;
  account_size: number;
  risk_pct: number;
  target_rr: number;
  max_open_risk_pct: number;
  max_open_positions: number;
  account_name?: string;
  account_id?: string;
};

type PropRules = {
  target_pct?: number;
  daily_loss_pct?: number;
  max_loss_pct?: number;
  min_days?: number;
  risk_per_idea_pct?: number | null;
};

type PropLimits = {
  target_usd?: number;
  daily_limit_usd?: number;
  max_loss_limit_usd?: number;
};

type OpenRiskPosition = {
  trade_id?: string;
  symbol?: string;
  side?: string;
  risk_usd?: number;
  risk_pct?: number;
  lots?: number;
  entry?: number;
  sl?: number;
  tp?: number;
  firm?: string;
  phase?: string;
  source?: string;
  mt5_job_id?: string;
  mt5_ticket?: number;
  device_id?: string;
  reserved_ts_ms?: number;
};

type PropRisk = {
  day: string;
  daily_key: string;
  daily_loss_used: number;
  daily_risk_reserved: number;
  max_loss_used: number;
  open_risk_usd: number;
  open_positions: OpenRiskPosition[];
  wins_today: number;
  losses_today: number;
};

type PropStatusResp = {
  ok: boolean;
  config: PropConfig;
  rules: PropRules;
  broker: BrokerAccount;
  limits: PropLimits;
};

type PropRiskResp = {
  ok: boolean;
  config: PropConfig;
  risk: PropRisk;
};

function safeNum(x: any, fallback = 0) {
  const n = typeof x === "number" ? x : Number(x);
  return Number.isFinite(n) ? n : fallback;
}

function money(x: any, digits = 2) {
  const n = safeNum(x, NaN);
  if (!Number.isFinite(n)) return "—";
  return `$${n.toLocaleString(undefined, {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  })}`;
}

function num(x: any, digits = 2) {
  const n = safeNum(x, NaN);
  if (!Number.isFinite(n)) return "—";
  return n.toLocaleString(undefined, {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
}

function pct(x: any, digits = 1) {
  const n = safeNum(x, NaN);
  if (!Number.isFinite(n)) return "—";
  return `${n.toFixed(digits)}%`;
}

function fmtPrice(x: any) {
  const n = safeNum(x, NaN);
  if (!Number.isFinite(n)) return "—";
  if (Math.abs(n) >= 100) return n.toFixed(2);
  return n.toFixed(5);
}

function statusTone(ok: boolean) {
  return ok
    ? "border-emerald-900/50 bg-emerald-950/20 text-emerald-300"
    : "border-red-900/50 bg-red-950/20 text-red-300";
}

function Card({
  title,
  value,
  sub,
  loading,
  tone = "default",
}: {
  title: string;
  value: React.ReactNode;
  sub?: React.ReactNode;
  loading?: boolean;
  tone?: "default" | "good" | "warn" | "bad";
}) {
  const toneClass =
    tone === "good"
      ? "from-emerald-500/10"
      : tone === "warn"
        ? "from-amber-500/10"
        : tone === "bad"
          ? "from-red-500/10"
          : "from-slate-700/20";

  return (
    <div className="relative overflow-hidden rounded-2xl border border-slate-800/70 bg-slate-950/70 shadow-[0_0_0_1px_rgba(255,255,255,0.03)]">
      <div className={`absolute inset-x-0 top-0 h-16 bg-gradient-to-b ${toneClass} to-transparent`} />
      <div className="relative p-4">
        <div className="text-xs font-medium tracking-wide text-slate-400">{title}</div>
        <div className="mt-2">
          {loading ? (
            <div className="h-8 w-32 animate-pulse rounded-lg bg-slate-800/60" />
          ) : (
            <div className="text-2xl font-semibold text-slate-100">{value}</div>
          )}
        </div>
        <div className="mt-1 min-h-[18px] text-xs text-slate-500">
          {loading ? <div className="h-3 w-36 animate-pulse rounded bg-slate-800/50" /> : sub}
        </div>
      </div>
    </div>
  );
}

function HealthPill({ ok, label }: { ok: boolean; label: string }) {
  return (
    <div className={`rounded-full border px-3 py-1 text-xs ${statusTone(ok)}`}>
      {ok ? "OK" : "WARN"} {label}
    </div>
  );
}

function ProgressBar({ value, max, label }: { value: number; max: number; label: string }) {
  const p = max > 0 ? Math.max(0, Math.min(100, (value / max) * 100)) : 0;
  const tone = p >= 90 ? "bg-red-500" : p >= 70 ? "bg-amber-500" : "bg-emerald-500";

  return (
    <div>
      <div className="mb-2 flex items-center justify-between text-xs">
        <span className="text-slate-300">{label}</span>
        <span className="text-slate-500">
          {money(value)} / {money(max)} · {p.toFixed(1)}%
        </span>
      </div>
      <div className="h-3 overflow-hidden rounded-full bg-slate-900">
        <div className={`h-full rounded-full ${tone}`} style={{ width: `${p}%` }} />
      </div>
    </div>
  );
}

function SkeletonTable({ rows = 5 }: { rows?: number }) {
  return (
    <div className="space-y-2 p-4">
      {Array.from({ length: rows }).map((_, i) => (
        <div key={i} className="h-10 w-full animate-pulse rounded-xl bg-slate-900/70" />
      ))}
    </div>
  );
}

export default function PerformancePage() {
  const [status, setStatus] = React.useState<PropStatusResp | null>(null);
  const [risk, setRisk] = React.useState<PropRiskResp | null>(null);
  const [loading, setLoading] = React.useState(true);
  const [err, setErr] = React.useState<string | null>(null);
  const [lastUpdated, setLastUpdated] = React.useState<Date | null>(null);

  async function load() {
    try {
      setErr(null);
      const API_ORIGIN = (import.meta.env.VITE_API_ORIGIN || "").replace(/\/+$/, "");

      const [statusRes, riskRes] = await Promise.all([
        fetch(`${API_ORIGIN}/trend/prop/status`, {
          method: "GET",
          credentials: "include",
          headers: { Accept: "application/json" },
        }),
        fetch(`${API_ORIGIN}/trend/prop/risk`, {
          method: "GET",
          credentials: "include",
          headers: { Accept: "application/json" },
        }),
      ]);

      if (!statusRes.ok) {
        const t = await statusRes.text().catch(() => "");
        throw new Error(`prop/status HTTP ${statusRes.status} ${t}`.trim());
      }
      if (!riskRes.ok) {
        const t = await riskRes.text().catch(() => "");
        throw new Error(`prop/risk HTTP ${riskRes.status} ${t}`.trim());
      }

      const statusJson = (await statusRes.json()) as PropStatusResp;
      const riskJson = (await riskRes.json()) as PropRiskResp;

      if (!statusJson?.ok) throw new Error("prop/status returned ok=false");
      if (!riskJson?.ok) throw new Error("prop/risk returned ok=false");

      setStatus(statusJson);
      setRisk(riskJson);
      setLastUpdated(new Date());
    } catch (e: any) {
      setErr(e?.message || "Failed to load prop dashboard");
    } finally {
      setLoading(false);
    }
  }

  React.useEffect(() => {
    load();
    const t = window.setInterval(load, 2000);
    return () => window.clearInterval(t);
  }, []);

  const cfg = status?.config || risk?.config;
  const broker = status?.broker || {};
  const limits = status?.limits || {};
  const rules = status?.rules || {};
  const riskState = risk?.risk;

  const equity = safeNum(broker.equity);
  const balance = safeNum(broker.balance);
  const floating = safeNum(broker.floating_pnl);
  const margin = safeNum(broker.margin);
  const freeMargin = safeNum(broker.free_margin);
  const openRisk = safeNum(riskState?.open_risk_usd);
  const maxOpenRiskUsd = equity * (safeNum(cfg?.max_open_risk_pct) / 100);
  const riskRoom = Math.max(0, maxOpenRiskUsd - openRisk);
  const openPositions = riskState?.open_positions || [];

  const brokerOk = equity > 0 && balance > 0;
  const riskOk = maxOpenRiskUsd <= 0 || openRisk <= maxOpenRiskUsd;
  const posOk = openPositions.length <= safeNum(cfg?.max_open_positions, 0);

  return (
    <div className="min-h-screen bg-slate-950 text-slate-100">
      <div className="mx-auto max-w-7xl px-4 py-6 sm:px-6 lg:px-8">
        <div className="flex flex-col gap-3 sm:flex-row sm:items-end sm:justify-between">
          <div>
            <div className="text-2xl font-semibold tracking-tight">Prop Dashboard</div>
            <div className="mt-1 text-sm text-slate-400">
              Broker account is source of truth for FundingPips risk sizing.
            </div>
          </div>

          <div className="flex flex-wrap items-center gap-2">
            <HealthPill ok={!!cfg?.enabled} label={cfg?.enabled ? "Prop mode ON" : "Prop mode OFF"} />
            <HealthPill ok={brokerOk} label="Broker sync" />
            <HealthPill ok={riskOk} label="Risk room" />
            <button
              className="rounded-xl bg-slate-100 px-3 py-2 text-sm font-semibold text-slate-950 hover:bg-white disabled:opacity-60"
              onClick={load}
              disabled={loading}
            >
              Refresh
            </button>
          </div>
        </div>

        {err ? (
          <div className="mt-4 rounded-2xl border border-red-900/60 bg-red-950/30 p-4 text-sm text-red-200">
            Failed to load dashboard: <span className="font-mono">{err}</span>
          </div>
        ) : null}

        <div className="mt-6 grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-4">
          <Card title="Balance" value={money(balance)} sub="Broker balance" loading={loading} />
          <Card title="Equity" value={money(equity)} sub="Used for dynamic lot sizing" tone="good" loading={loading} />
          <Card
            title="Floating PnL"
            value={money(floating)}
            sub="Live unrealized PnL"
            tone={floating < 0 ? "bad" : floating > 0 ? "good" : "default"}
            loading={loading}
          />
          <Card title="Free Margin" value={money(freeMargin)} sub={`Margin used: ${money(margin)}`} loading={loading} />
        </div>

        <div className="mt-3 grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-4">
          <Card title="Target" value={money(limits.target_usd)} sub={`${safeNum(rules.target_pct)}% target`} loading={loading} />
          <Card title="Daily Loss Limit" value={money(limits.daily_limit_usd)} sub={`${safeNum(rules.daily_loss_pct)}% rule`} tone="warn" loading={loading} />
          <Card title="Max Loss Limit" value={money(limits.max_loss_limit_usd)} sub={`${safeNum(rules.max_loss_pct)}% rule`} tone="bad" loading={loading} />
          <Card
            title="Open Positions"
            value={`${openPositions.length} / ${safeNum(cfg?.max_open_positions, 0)}`}
            sub={posOk ? "Within configured cap" : "Above configured cap"}
            tone={posOk ? "good" : "bad"}
            loading={loading}
          />
        </div>

        <div className="mt-3 grid grid-cols-1 gap-3 lg:grid-cols-3">
          <div className="rounded-2xl border border-slate-800/60 bg-slate-950/60 p-4 lg:col-span-2">
            <div className="flex items-center justify-between">
              <div>
                <div className="text-sm font-semibold">Risk Utilization</div>
                <div className="mt-1 text-xs text-slate-500">
                  Max open risk = equity × {pct(cfg?.max_open_risk_pct)}
                </div>
              </div>
              <div className="text-right text-xs text-slate-500">
                Risk room
                <div className="text-sm font-semibold text-slate-200">{money(riskRoom)}</div>
              </div>
            </div>

            <div className="mt-5">
              <ProgressBar value={openRisk} max={maxOpenRiskUsd} label="Open risk" />
            </div>

            <div className="mt-5 grid grid-cols-1 gap-3 sm:grid-cols-3">
              <div className="rounded-xl border border-slate-800 bg-slate-950/50 p-3">
                <div className="text-xs text-slate-500">Reserved today</div>
                <div className="mt-1 text-lg font-semibold">{money(riskState?.daily_risk_reserved)}</div>
              </div>
              <div className="rounded-xl border border-slate-800 bg-slate-950/50 p-3">
                <div className="text-xs text-slate-500">Wins today</div>
                <div className="mt-1 text-lg font-semibold">{safeNum(riskState?.wins_today, 0)}</div>
              </div>
              <div className="rounded-xl border border-slate-800 bg-slate-950/50 p-3">
                <div className="text-xs text-slate-500">Losses today</div>
                <div className="mt-1 text-lg font-semibold">{safeNum(riskState?.losses_today, 0)}</div>
              </div>
            </div>
          </div>

          <div className="rounded-2xl border border-slate-800/60 bg-slate-950/60 p-4">
            <div className="text-sm font-semibold">Account</div>
            <div className="mt-4 space-y-3 text-sm">
              <div className="flex justify-between gap-3"><span className="text-slate-500">Firm</span><span className="text-slate-200">{cfg?.firm || "—"}</span></div>
              <div className="flex justify-between gap-3"><span className="text-slate-500">Phase</span><span className="text-slate-200">{cfg?.phase || "—"}</span></div>
              <div className="flex justify-between gap-3"><span className="text-slate-500">Risk per idea</span><span className="text-slate-200">{pct(cfg?.risk_pct)}</span></div>
              <div className="flex justify-between gap-3"><span className="text-slate-500">Target RR</span><span className="text-slate-200">{num(cfg?.target_rr, 1)}R</span></div>
              <div className="flex justify-between gap-3"><span className="text-slate-500">Leverage</span><span className="text-slate-200">{safeNum(broker.leverage, 0)}x</span></div>
              <div className="flex justify-between gap-3"><span className="text-slate-500">Last update</span><span className="text-slate-200">{lastUpdated ? lastUpdated.toLocaleTimeString() : "—"}</span></div>
            </div>
          </div>
        </div>

        <div className="mt-3 rounded-2xl border border-slate-800/60 bg-slate-950/60 p-4">
          <div className="flex flex-col gap-1 sm:flex-row sm:items-end sm:justify-between">
            <div>
              <div className="text-sm font-semibold">Open Prop Risk</div>
              <div className="mt-1 text-xs text-slate-500">
                Comes from <span className="font-mono text-slate-400">/trend/prop/risk</span>.
              </div>
            </div>
            <div className="text-xs text-slate-500">
              Day: <span className="font-mono text-slate-300">{riskState?.day || "—"}</span>
            </div>
          </div>

          <div className="mt-4 overflow-hidden rounded-2xl border border-slate-800">
            {loading ? (
              <SkeletonTable />
            ) : (
              <table className="w-full border-collapse text-sm">
                <thead className="bg-slate-950/80">
                  <tr className="text-left text-xs text-slate-400">
                    <th className="px-4 py-3">Symbol</th>
                    <th className="px-4 py-3">Side</th>
                    <th className="px-4 py-3 text-right">Lots</th>
                    <th className="px-4 py-3 text-right">Risk</th>
                    <th className="px-4 py-3 text-right">Entry</th>
                    <th className="px-4 py-3 text-right">SL</th>
                    <th className="px-4 py-3 text-right">TP</th>
                    <th className="px-4 py-3">Source</th>
                  </tr>
                </thead>
                <tbody>
                  {openPositions.length === 0 ? (
                    <tr>
                      <td className="px-4 py-10 text-center text-slate-500" colSpan={8}>
                        No open prop risk. Account is clean.
                      </td>
                    </tr>
                  ) : (
                    openPositions.map((p, idx) => (
                      <tr key={`${p.trade_id || p.symbol || idx}`} className="border-t border-slate-900/70 hover:bg-slate-900/30">
                        <td className="px-4 py-3 font-medium text-slate-200">{p.symbol || "—"}</td>
                        <td className="px-4 py-3">
                          <span className={String(p.side).toUpperCase() === "BUY" ? "text-emerald-300" : "text-red-300"}>{p.side || "—"}</span>
                        </td>
                        <td className="px-4 py-3 text-right text-slate-200">{num(p.lots, 2)}</td>
                        <td className="px-4 py-3 text-right text-slate-200">{money(p.risk_usd)}</td>
                        <td className="px-4 py-3 text-right text-slate-200">{fmtPrice(p.entry)}</td>
                        <td className="px-4 py-3 text-right text-slate-200">{fmtPrice(p.sl)}</td>
                        <td className="px-4 py-3 text-right text-slate-200">{fmtPrice(p.tp)}</td>
                        <td className="px-4 py-3 text-xs text-slate-500">{p.source || "—"}</td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
