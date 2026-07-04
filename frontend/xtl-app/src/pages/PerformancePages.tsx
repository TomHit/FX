import React from "react";

type BrokerAccount = {
  balance?: number;
  equity?: number;
  margin?: number;
  used_margin?: number;
  free_margin?: number;
  floating_pnl?: number;
  leverage?: number;
  login?: number | string;
  server?: string;
  company?: string;
  currency?: string;
  account_type?: string;
  is_demo?: boolean;
  margin_level?: number;
};

type PropConfig = {
  enabled?: boolean;
  firm?: string;
  phase?: string;
  account_size?: number;
  risk_pct?: number;
  target_rr?: number;
  max_open_risk_pct?: number;
  max_open_positions?: number;
  account_name?: string;
  account_id?: string;
  profile_id?: string;
  account_login?: string;
  account_server?: string;
  broker_company?: string;
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
  trade_state?: string;
  status?: string;
  profile_id?: string;
};

type PropRisk = {
  day?: string;
  daily_key?: string;
  daily_loss_used?: number;
  daily_risk_reserved?: number;
  max_loss_used?: number;
  open_risk_usd?: number;
  open_positions?: OpenRiskPosition[];
  wins_today?: number;
  losses_today?: number;
  start_balance?: number;
  broker_balance?: number;
  broker_equity?: number;
  floating_pnl?: number;
  today_closed_pnl?: number;
  ftmo_current_daily_result?: number;
  ftmo_daily_loss_used?: number;
  ftmo_daily_loss_limit?: number;
  ftmo_daily_loss_remaining?: number;
  projected_daily_loss_if_all_sl?: number;
  configured_risk_pct?: number;
  effective_risk_pct?: number;
  drawdown_pct?: number;
  drawdown_band?: string;
  daily_r?: number;
  daily_r_blocked?: boolean;
  daily_r_block_reason?: string;
  trading_halted?: boolean;
  halt_reason?: string;
  halt_ts?: number;
  halt_until_manual_reset?: boolean;
  consecutive_losing_days?: number;
  open_positions_count?: number;
  free_margin?: number;
  used_margin?: number;
  margin_level?: number;
  margin_utilization_pct?: number;
  broker_free_margin?: number;
  broker_used_margin?: number;
};

type DashboardResp = {
  profile_id?: string;
  trading_allowed?: boolean;
  reasons?: string[];
  risk?: number;
  drawdown?: { drawdown_pct?: number; drawdown_band?: string };
  account?: { balance?: number; equity?: number; floating_pnl?: number };
  margin?: {
    equity?: number;
    balance?: number;
    free_margin?: number;
    used_margin?: number;
    margin_level?: number;
    margin_utilization_pct?: number;
  };
  daily?: {
    day?: string;
    daily_key?: string;
    start_balance?: number;
    today_closed_pnl?: number;
    floating_pnl?: number;
    ftmo_current_daily_result?: number;
    ftmo_daily_loss_used?: number;
    ftmo_daily_loss_limit?: number;
    ftmo_daily_loss_remaining?: number;
    daily_r?: number;
    wins_today?: number;
    losses_today?: number;
  };
  open_risk?: {
    open_risk_usd?: number;
    max_open_risk_usd?: number;
    daily_risk_reserved?: number;
    projected_daily_loss_if_all_sl?: number;
    open_positions_count?: number;
    open_positions?: OpenRiskPosition[];
  };
  halt?: {
    trading_halted?: boolean;
    halt_reason?: string;
    halt_ts?: number;
    halt_until_manual_reset?: boolean;
    daily_r_blocked?: boolean;
    daily_r_block_reason?: string;
    consecutive_losing_days?: number;
  };
};

type PropStatusResp = {
  ok: boolean;
  profile_id?: string;
  profile_device?: { ok?: boolean; device_id?: string; reason?: string };
  config?: PropConfig;
  rules?: PropRules;
  account?: BrokerAccount;
  broker?: BrokerAccount;
  limits?: PropLimits;
  risk?: Partial<PropRisk>;
};

type PropRiskResp = {
  ok: boolean;
  config?: PropConfig;
  risk?: PropRisk;
};

function safeNum(x: any, fallback = 0) {
  const n = typeof x === "number" ? x : Number(x);
  return Number.isFinite(n) ? n : fallback;
}

function money(x: any, digits = 2) {
  const n = safeNum(x, NaN);
  if (!Number.isFinite(n)) return "—";
  return `$${n.toLocaleString(undefined, { minimumFractionDigits: digits, maximumFractionDigits: digits })}`;
}

function num(x: any, digits = 2) {
  const n = safeNum(x, NaN);
  if (!Number.isFinite(n)) return "—";
  return n.toLocaleString(undefined, { minimumFractionDigits: digits, maximumFractionDigits: digits });
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

function fmtAge(ts: any) {
  const n = safeNum(ts, 0);
  if (n <= 0) return "—";
  const ageSec = Math.max(0, Math.floor((Date.now() - n) / 1000));
  if (ageSec < 60) return `${ageSec}s ago`;
  const ageMin = Math.floor(ageSec / 60);
  if (ageMin < 60) return `${ageMin}m ago`;
  return `${Math.floor(ageMin / 60)}h ago`;
}

function statusTone(ok: boolean) {
  return ok
    ? "border-emerald-900/50 bg-emerald-950/20 text-emerald-300"
    : "border-red-900/50 bg-red-950/20 text-red-300";
}

function Card({ title, value, sub, loading, tone = "default" }: {
  title: string;
  value: React.ReactNode;
  sub?: React.ReactNode;
  loading?: boolean;
  tone?: "default" | "good" | "warn" | "bad";
}) {
  const toneClass =
    tone === "good" ? "from-emerald-500/10" : tone === "warn" ? "from-amber-500/10" : tone === "bad" ? "from-red-500/10" : "from-slate-700/20";

  return (
    <div className="relative overflow-hidden rounded-2xl border border-slate-800/70 bg-slate-950/70 shadow-[0_0_0_1px_rgba(255,255,255,0.03)]">
      <div className={`absolute inset-x-0 top-0 h-16 bg-gradient-to-b ${toneClass} to-transparent`} />
      <div className="relative p-4">
        <div className="text-xs font-medium tracking-wide text-slate-400">{title}</div>
        <div className="mt-2">
          {loading ? <div className="h-8 w-32 animate-pulse rounded-lg bg-slate-800/60" /> : <div className="text-2xl font-semibold text-slate-100">{value}</div>}
        </div>
        <div className="mt-1 min-h-[18px] text-xs text-slate-500">
          {loading ? <div className="h-3 w-36 animate-pulse rounded bg-slate-800/50" /> : sub}
        </div>
      </div>
    </div>
  );
}

function HealthPill({ ok, label }: { ok: boolean; label: string }) {
  return <div className={`rounded-full border px-3 py-1 text-xs ${statusTone(ok)}`}>{ok ? "OK" : "WARN"} {label}</div>;
}

function SmallStatus({ ok, label, value }: { ok: boolean; label: string; value?: React.ReactNode }) {
  return (
    <div className="flex items-center justify-between gap-3 rounded-xl border border-slate-800 bg-slate-950/50 px-3 py-2">
      <div className="flex items-center gap-2">
        <span className={`h-2.5 w-2.5 rounded-full ${ok ? "bg-emerald-400" : "bg-red-400"}`} />
        <span className="text-sm text-slate-300">{label}</span>
      </div>
      <div className={ok ? "text-xs text-emerald-300" : "text-xs text-red-300"}>{value ?? (ok ? "OK" : "WARN")}</div>
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
        <span className="text-slate-500">{money(value)} / {money(max)} · {p.toFixed(1)}%</span>
      </div>
      <div className="h-3 overflow-hidden rounded-full bg-slate-900">
        <div className={`h-full rounded-full ${tone}`} style={{ width: `${p}%` }} />
      </div>
    </div>
  );
}

function SkeletonTable({ rows = 5 }: { rows?: number }) {
  return <div className="space-y-2 p-4">{Array.from({ length: rows }).map((_, i) => <div key={i} className="h-10 w-full animate-pulse rounded-xl bg-slate-900/70" />)}</div>;
}

export default function PerformancePage() {
  const [status, setStatus] = React.useState<PropStatusResp | null>(null);
  const [risk, setRisk] = React.useState<PropRiskResp | null>(null);
  const [dash, setDash] = React.useState<DashboardResp | null>(null);
  const [loading, setLoading] = React.useState(true);
  const [err, setErr] = React.useState<string | null>(null);
  const [lastUpdated, setLastUpdated] = React.useState<Date | null>(null);

  async function load() {
    try {
      setErr(null);
      const API_ORIGIN = (import.meta.env.VITE_API_ORIGIN || "").replace(/\/+$/, "");
      const [statusRes, riskRes, dashRes] = await Promise.all([
        fetch(`${API_ORIGIN}/trend/prop/status`, { method: "GET", credentials: "include", headers: { Accept: "application/json" } }),
        fetch(`${API_ORIGIN}/trend/prop/risk`, { method: "GET", credentials: "include", headers: { Accept: "application/json" } }),
        fetch(`${API_ORIGIN}/trend/prop/dashboard`, { method: "GET", credentials: "include", headers: { Accept: "application/json" } }).catch(() => null),
      ]);
      if (!statusRes.ok) throw new Error(`prop/status HTTP ${statusRes.status} ${(await statusRes.text().catch(() => "")).trim()}`);
      if (!riskRes.ok) throw new Error(`prop/risk HTTP ${riskRes.status} ${(await riskRes.text().catch(() => "")).trim()}`);
      const statusJson = (await statusRes.json()) as PropStatusResp;
      const riskJson = (await riskRes.json()) as PropRiskResp;
      const dashJson = dashRes && dashRes.ok ? ((await dashRes.json()) as DashboardResp) : null;
      if (!statusJson?.ok) throw new Error("prop/status returned ok=false");
      if (!riskJson?.ok) throw new Error("prop/risk returned ok=false");
      setStatus(statusJson);
      setRisk(riskJson);
      setDash(dashJson);
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

  const cfg = status?.config || risk?.config || {};
  const rules = status?.rules || {};
  const limits = status?.limits || {};
  const account = status?.account || status?.broker || {};
  const riskState = risk?.risk || {};

  const brokerBalance = safeNum(dash?.account?.balance ?? account.balance ?? riskState.broker_balance);
  const brokerEquity = safeNum(dash?.account?.equity ?? account.equity ?? riskState.broker_equity);
  const floating = safeNum(dash?.account?.floating_pnl ?? account.floating_pnl ?? riskState.floating_pnl);
  const freeMargin = safeNum(dash?.margin?.free_margin ?? account.free_margin ?? riskState.free_margin ?? riskState.broker_free_margin);
  const usedMargin = safeNum(dash?.margin?.used_margin ?? account.margin ?? account.used_margin ?? riskState.used_margin ?? riskState.broker_used_margin);
  const marginLevel = safeNum(dash?.margin?.margin_level ?? account.margin_level ?? riskState.margin_level);
  const marginUtil = safeNum(dash?.margin?.margin_utilization_pct ?? riskState.margin_utilization_pct);

  const profileId = dash?.profile_id || status?.profile_id || cfg.profile_id || "—";
  const openRisk = safeNum(dash?.open_risk?.open_risk_usd ?? riskState.open_risk_usd);
  const maxOpenRiskUsd = safeNum(dash?.open_risk?.max_open_risk_usd, brokerEquity * (safeNum(cfg.max_open_risk_pct, 3) / 100));
  const riskRoom = Math.max(0, maxOpenRiskUsd - openRisk);
  const dailyReserved = safeNum(dash?.open_risk?.daily_risk_reserved ?? riskState.daily_risk_reserved);
  const projectedDailyLoss = safeNum(dash?.open_risk?.projected_daily_loss_if_all_sl ?? riskState.projected_daily_loss_if_all_sl);

  const openPositions = dash?.open_risk?.open_positions || riskState.open_positions || [];
  const openPositionCount = safeNum(dash?.open_risk?.open_positions_count ?? riskState.open_positions_count ?? openPositions.length);
  const maxOpenPositions = safeNum(cfg.max_open_positions, 0);

  const daily = dash?.daily || {};
  const day = daily.day || riskState.day || "—";
  const startBalance = safeNum(daily.start_balance ?? riskState.start_balance);
  const todayClosed = safeNum(daily.today_closed_pnl ?? riskState.today_closed_pnl);
  const currentDailyResult = safeNum(daily.ftmo_current_daily_result ?? riskState.ftmo_current_daily_result);
  const dailyLossUsed = safeNum(daily.ftmo_daily_loss_used ?? riskState.ftmo_daily_loss_used ?? riskState.daily_loss_used);
  const dailyLossLimit = safeNum(daily.ftmo_daily_loss_limit ?? riskState.ftmo_daily_loss_limit ?? limits.daily_limit_usd);
  const dailyLossRemaining = safeNum(daily.ftmo_daily_loss_remaining ?? riskState.ftmo_daily_loss_remaining);
  const dailyR = safeNum(daily.daily_r ?? riskState.daily_r);
  const winsToday = safeNum(daily.wins_today ?? riskState.wins_today);
  const lossesToday = safeNum(daily.losses_today ?? riskState.losses_today);

  const drawdownPct = safeNum(dash?.drawdown?.drawdown_pct ?? riskState.drawdown_pct);
  const drawdownBand = String(dash?.drawdown?.drawdown_band ?? riskState.drawdown_band ?? "NORMAL");
  const effectiveRiskPct = safeNum(dash?.risk ?? riskState.effective_risk_pct ?? cfg.risk_pct);
  const configuredRiskPct = safeNum(riskState.configured_risk_pct ?? cfg.risk_pct);

  const halt = dash?.halt || {};
  const tradingHalted = Boolean(halt.trading_halted ?? riskState.trading_halted);
  const haltReason = String(halt.halt_reason ?? riskState.halt_reason ?? "");
  const dailyRBlocked = Boolean(halt.daily_r_blocked ?? riskState.daily_r_blocked);
  const dailyRBlockReason = String(halt.daily_r_block_reason ?? riskState.daily_r_block_reason ?? "");
  const consecutiveLosingDays = safeNum(halt.consecutive_losing_days ?? riskState.consecutive_losing_days);

  const dashboardReasons = dash?.reasons || [];
  const tradingAllowed = typeof dash?.trading_allowed === "boolean" ? dash.trading_allowed : !tradingHalted && !dailyRBlocked && openPositionCount < maxOpenPositions;

  const brokerOk = brokerEquity > 0 && brokerBalance > 0;
  const riskOk = maxOpenRiskUsd <= 0 || openRisk <= maxOpenRiskUsd;
  const posOk = maxOpenPositions <= 0 || openPositionCount <= maxOpenPositions;
  const marginOk = usedMargin <= 0 || marginLevel >= 500;
  const dailyOk = dailyLossLimit <= 0 || dailyLossUsed < dailyLossLimit;
  const capacityOk = tradingAllowed && brokerOk && riskOk && posOk && marginOk && dailyOk;
  const drawdownTone = drawdownBand === "HIGH" ? "bad" : drawdownBand === "MEDIUM" ? "warn" : "good";

  return (
    <div className="min-h-screen bg-slate-950 text-slate-100">
      <div className="mx-auto max-w-7xl px-4 py-6 sm:px-6 lg:px-8">
        <div className="flex flex-col gap-3 sm:flex-row sm:items-end sm:justify-between">
          <div>
            <div className="text-2xl font-semibold tracking-tight">Prop Dashboard</div>
            <div className="mt-1 text-sm text-slate-400">Broker account is the source of truth for FTMO risk, margin, drawdown and execution capacity.</div>
          </div>
          <div className="flex flex-wrap items-center gap-2">
            <HealthPill ok={!!cfg?.enabled} label={cfg?.enabled ? "Prop mode ON" : "Prop mode OFF"} />
            <HealthPill ok={brokerOk} label="Broker sync" />
            <HealthPill ok={capacityOk} label={capacityOk ? "Ready to trade" : "Trade blocked"} />
            <button className="rounded-xl bg-slate-100 px-3 py-2 text-sm font-semibold text-slate-950 hover:bg-white disabled:opacity-60" onClick={load} disabled={loading}>Refresh</button>
          </div>
        </div>

        {err ? <div className="mt-4 rounded-2xl border border-red-900/60 bg-red-950/30 p-4 text-sm text-red-200">Failed to load dashboard: <span className="font-mono">{err}</span></div> : null}

        <div className="mt-6 grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-4">
          <Card title="Balance" value={money(brokerBalance)} sub="Broker balance" loading={loading} />
          <Card title="Equity" value={money(brokerEquity)} sub="Used for dynamic risk sizing" tone="good" loading={loading} />
          <Card title="Floating PnL" value={money(floating)} sub="Live unrealized P/L" tone={floating < 0 ? "bad" : floating > 0 ? "good" : "default"} loading={loading} />
          <Card title="Free Margin" value={money(freeMargin)} sub={`Used margin: ${money(usedMargin)}`} loading={loading} />
        </div>

        <div className="mt-3 grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-4">
          <Card title="Drawdown Band" value={drawdownBand} sub={`Drawdown ${pct(drawdownPct, 2)}`} tone={drawdownTone} loading={loading} />
          <Card title="Effective Risk" value={pct(effectiveRiskPct, 2)} sub={`Configured ${pct(configuredRiskPct, 2)}`} tone={effectiveRiskPct < configuredRiskPct ? "warn" : "good"} loading={loading} />
          <Card title="Daily R" value={`${num(dailyR, 2)}R`} sub={`${winsToday} wins / ${lossesToday} losses`} tone={dailyR <= -2 ? "bad" : dailyR < 0 ? "warn" : "good"} loading={loading} />
          <Card title="Open Positions" value={`${openPositionCount} / ${maxOpenPositions}`} sub={posOk ? "Within configured cap" : "Max positions reached"} tone={posOk ? "good" : "bad"} loading={loading} />
        </div>

        <div className="mt-3 grid grid-cols-1 gap-3 lg:grid-cols-3">
          <div className="rounded-2xl border border-slate-800/60 bg-slate-950/60 p-4 lg:col-span-2">
            <div className="flex flex-col gap-4 sm:flex-row sm:items-start sm:justify-between">
              <div><div className="text-sm font-semibold">Trade Capacity</div><div className="mt-1 text-xs text-slate-500">Final decision comes from <span className="font-mono text-slate-400">/trend/prop/dashboard</span>.</div></div>
              <div className={capacityOk ? "rounded-2xl border border-emerald-900/50 bg-emerald-950/20 px-4 py-2 text-sm font-semibold text-emerald-300" : "rounded-2xl border border-red-900/50 bg-red-950/20 px-4 py-2 text-sm font-semibold text-red-300"}>{capacityOk ? "READY TO TRADE" : "BLOCKED"}</div>
            </div>
            <div className="mt-5 grid grid-cols-1 gap-3 sm:grid-cols-2">
              <SmallStatus ok={brokerOk} label="Broker account" value={brokerOk ? "Connected" : "Missing"} />
              <SmallStatus ok={riskOk} label="Risk room" value={money(riskRoom)} />
              <SmallStatus ok={posOk} label="Position capacity" value={`${openPositionCount}/${maxOpenPositions}`} />
              <SmallStatus ok={marginOk} label="Margin level" value={usedMargin > 0 ? pct(marginLevel, 1) : "No margin used"} />
              <SmallStatus ok={dailyOk} label="Daily loss room" value={money(dailyLossRemaining)} />
              <SmallStatus ok={!tradingHalted && !dailyRBlocked} label="Halt state" value={tradingHalted ? haltReason : dailyRBlocked ? dailyRBlockReason : "Clear"} />
            </div>
            {dashboardReasons.length > 0 ? <div className="mt-4 rounded-xl border border-amber-900/40 bg-amber-950/20 p-3 text-sm text-amber-200">Block reasons: <span className="font-mono">{dashboardReasons.join(", ")}</span></div> : null}
          </div>

          <div className="rounded-2xl border border-slate-800/60 bg-slate-950/60 p-4">
            <div className="text-sm font-semibold">Account</div>
            <div className="mt-4 space-y-3 text-sm">
              <div className="flex justify-between gap-3"><span className="text-slate-500">Profile</span><span className="text-slate-200">{profileId}</span></div>
              <div className="flex justify-between gap-3"><span className="text-slate-500">Firm</span><span className="text-slate-200">{cfg?.firm || "—"}</span></div>
              <div className="flex justify-between gap-3"><span className="text-slate-500">Phase</span><span className="text-slate-200">{cfg?.phase || "—"}</span></div>
              <div className="flex justify-between gap-3"><span className="text-slate-500">Login</span><span className="text-slate-200">{account.login || cfg.account_login || "—"}</span></div>
              <div className="flex justify-between gap-3"><span className="text-slate-500">Server</span><span className="text-slate-200">{account.server || cfg.account_server || "—"}</span></div>
              <div className="flex justify-between gap-3"><span className="text-slate-500">Leverage</span><span className="text-slate-200">{safeNum(account.leverage, 0)}x</span></div>
              <div className="flex justify-between gap-3"><span className="text-slate-500">Last update</span><span className="text-slate-200">{lastUpdated ? lastUpdated.toLocaleTimeString() : "—"}</span></div>
            </div>
          </div>
        </div>

        <div className="mt-3 grid grid-cols-1 gap-3 lg:grid-cols-2">
          <div className="rounded-2xl border border-slate-800/60 bg-slate-950/60 p-4">
            <div className="flex items-center justify-between"><div><div className="text-sm font-semibold">Risk Utilization</div><div className="mt-1 text-xs text-slate-500">Max open risk = equity × {pct(cfg?.max_open_risk_pct ?? 3)}</div></div><div className="text-right text-xs text-slate-500">Risk room<div className="text-sm font-semibold text-slate-200">{money(riskRoom)}</div></div></div>
            <div className="mt-5 space-y-5"><ProgressBar value={openRisk} max={maxOpenRiskUsd} label="Open risk" /><ProgressBar value={dailyLossUsed} max={dailyLossLimit} label="Daily loss used" /></div>
            <div className="mt-5 grid grid-cols-1 gap-3 sm:grid-cols-3">
              <div className="rounded-xl border border-slate-800 bg-slate-950/50 p-3"><div className="text-xs text-slate-500">Reserved today</div><div className="mt-1 text-lg font-semibold">{money(dailyReserved)}</div></div>
              <div className="rounded-xl border border-slate-800 bg-slate-950/50 p-3"><div className="text-xs text-slate-500">Projected if all SL</div><div className="mt-1 text-lg font-semibold">{money(projectedDailyLoss)}</div></div>
              <div className="rounded-xl border border-slate-800 bg-slate-950/50 p-3"><div className="text-xs text-slate-500">Remaining daily loss</div><div className="mt-1 text-lg font-semibold">{money(dailyLossRemaining)}</div></div>
            </div>
          </div>

          <div className="rounded-2xl border border-slate-800/60 bg-slate-950/60 p-4">
            <div className="text-sm font-semibold">FTMO Daily Ledger</div><div className="mt-1 text-xs text-slate-500">CE(S)T day: <span className="font-mono text-slate-300">{day}</span></div>
            <div className="mt-4 grid grid-cols-1 gap-3 sm:grid-cols-2">
              <div className="rounded-xl border border-slate-800 bg-slate-950/50 p-3"><div className="text-xs text-slate-500">Start balance</div><div className="mt-1 text-lg font-semibold">{money(startBalance)}</div></div>
              <div className="rounded-xl border border-slate-800 bg-slate-950/50 p-3"><div className="text-xs text-slate-500">Closed P/L today</div><div className={`mt-1 text-lg font-semibold ${todayClosed < 0 ? "text-red-300" : todayClosed > 0 ? "text-emerald-300" : ""}`}>{money(todayClosed)}</div></div>
              <div className="rounded-xl border border-slate-800 bg-slate-950/50 p-3"><div className="text-xs text-slate-500">Current daily result</div><div className={`mt-1 text-lg font-semibold ${currentDailyResult < 0 ? "text-red-300" : currentDailyResult > 0 ? "text-emerald-300" : ""}`}>{money(currentDailyResult)}</div></div>
              <div className="rounded-xl border border-slate-800 bg-slate-950/50 p-3"><div className="text-xs text-slate-500">Consecutive losing days</div><div className="mt-1 text-lg font-semibold">{num(consecutiveLosingDays, 0)}</div></div>
            </div>
          </div>
        </div>

        <div className="mt-3 rounded-2xl border border-slate-800/60 bg-slate-950/60 p-4">
          <div className="flex flex-col gap-1 sm:flex-row sm:items-end sm:justify-between"><div><div className="text-sm font-semibold">Margin Engine</div><div className="mt-1 text-xs text-slate-500">Broker margin values from MT5 account snapshot.</div></div><div className="text-xs text-slate-500">Margin utilization: <span className="font-mono text-slate-300">{pct(marginUtil, 2)}</span></div></div>
          <div className="mt-4 grid grid-cols-1 gap-3 sm:grid-cols-4">
            <Card title="Used Margin" value={money(usedMargin)} sub="Current broker margin" loading={loading} />
            <Card title="Free Margin" value={money(freeMargin)} sub="Available margin" loading={loading} />
            <Card title="Margin Level" value={usedMargin > 0 ? pct(marginLevel, 1) : "—"} sub={usedMargin > 0 ? "Block below 500%" : "No open margin"} tone={usedMargin > 0 && marginLevel < 500 ? "bad" : usedMargin > 0 && marginLevel < 1000 ? "warn" : "good"} loading={loading} />
            <Card title="Utilization" value={pct(marginUtil, 2)} sub="Used margin / equity" loading={loading} />
          </div>
        </div>

        <div className="mt-3 rounded-2xl border border-slate-800/60 bg-slate-950/60 p-4">
          <div className="flex flex-col gap-1 sm:flex-row sm:items-end sm:justify-between"><div><div className="text-sm font-semibold">Open Broker Risk</div><div className="mt-1 text-xs text-slate-500">Comes from <span className="font-mono text-slate-400">/trend/prop/risk</span> and broker reconciliation.</div></div><div className="text-xs text-slate-500">Day: <span className="font-mono text-slate-300">{day}</span></div></div>
          <div className="mt-4 overflow-hidden rounded-2xl border border-slate-800">
            {loading ? <SkeletonTable /> : (
              <table className="w-full border-collapse text-sm">
                <thead className="bg-slate-950/80"><tr className="text-left text-xs text-slate-400"><th className="px-4 py-3">Symbol</th><th className="px-4 py-3">Side</th><th className="px-4 py-3 text-right">Lots</th><th className="px-4 py-3 text-right">Risk</th><th className="px-4 py-3 text-right">Risk %</th><th className="px-4 py-3 text-right">Entry</th><th className="px-4 py-3 text-right">SL</th><th className="px-4 py-3 text-right">TP</th><th className="px-4 py-3">Ticket</th><th className="px-4 py-3">State</th><th className="px-4 py-3">Source</th></tr></thead>
                <tbody>
                  {openPositions.length === 0 ? <tr><td className="px-4 py-10 text-center text-slate-500" colSpan={11}>No open broker risk. FTMO account is clean.</td></tr> : openPositions.map((p, idx) => (
                    <tr key={`${p.trade_id || p.symbol || idx}`} className="border-t border-slate-900/70 hover:bg-slate-900/30"><td className="px-4 py-3 font-medium text-slate-200">{p.symbol || "—"}</td><td className="px-4 py-3"><span className={String(p.side).toUpperCase() === "BUY" ? "text-emerald-300" : "text-red-300"}>{p.side || "—"}</span></td><td className="px-4 py-3 text-right text-slate-200">{num(p.lots, 2)}</td><td className="px-4 py-3 text-right text-slate-200">{money(p.risk_usd)}</td><td className="px-4 py-3 text-right text-slate-200">{pct(p.risk_pct, 2)}</td><td className="px-4 py-3 text-right text-slate-200">{fmtPrice(p.entry)}</td><td className="px-4 py-3 text-right text-slate-200">{fmtPrice(p.sl)}</td><td className="px-4 py-3 text-right text-slate-200">{fmtPrice(p.tp)}</td><td className="px-4 py-3 text-xs text-slate-400">{p.mt5_ticket || "—"}</td><td className="px-4 py-3 text-xs text-slate-400">{p.trade_state || p.status || "—"}</td><td className="px-4 py-3 text-xs text-slate-500">{p.source || "—"}</td></tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        </div>

        <div className="mt-3 grid grid-cols-1 gap-3 lg:grid-cols-3">
          <div className="rounded-2xl border border-slate-800/60 bg-slate-950/60 p-4"><div className="text-sm font-semibold">System Health</div><div className="mt-4 space-y-2"><SmallStatus ok={!!cfg.enabled} label="Prop mode" value={cfg.enabled ? "ON" : "OFF"} /><SmallStatus ok={brokerOk} label="Broker sync" value={brokerOk ? "Healthy" : "No account"} /><SmallStatus ok={!tradingHalted} label="Trading halt" value={tradingHalted ? haltReason || "HALTED" : "Clear"} /><SmallStatus ok={!dailyRBlocked} label="Daily R breaker" value={dailyRBlocked ? dailyRBlockReason || "Blocked" : "Clear"} /></div></div>
          <div className="rounded-2xl border border-slate-800/60 bg-slate-950/60 p-4 lg:col-span-2"><div className="text-sm font-semibold">Raw State Summary</div><div className="mt-4 grid grid-cols-1 gap-3 sm:grid-cols-2 text-sm"><div className="flex justify-between gap-3"><span className="text-slate-500">Daily key</span><span className="font-mono text-xs text-slate-300">{daily.daily_key || riskState.daily_key || "—"}</span></div><div className="flex justify-between gap-3"><span className="text-slate-500">Halt timestamp</span><span className="text-slate-300">{fmtAge(halt.halt_ts ?? riskState.halt_ts)}</span></div><div className="flex justify-between gap-3"><span className="text-slate-500">Account size</span><span className="text-slate-300">{money(cfg.account_size)}</span></div><div className="flex justify-between gap-3"><span className="text-slate-500">Target</span><span className="text-slate-300">{money(limits.target_usd)} / {pct(rules.target_pct, 0)}</span></div><div className="flex justify-between gap-3"><span className="text-slate-500">Max loss limit</span><span className="text-slate-300">{money(limits.max_loss_limit_usd)}</span></div><div className="flex justify-between gap-3"><span className="text-slate-500">Minimum days</span><span className="text-slate-300">{safeNum(rules.min_days, 0)}</span></div></div></div>
        </div>
      </div>
    </div>
  );
}
