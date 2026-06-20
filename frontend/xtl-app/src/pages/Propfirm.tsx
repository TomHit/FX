import React, { useEffect, useMemo, useState } from "react";

const API_BASE =
  (
    (window as any).__PUBLIC_API_BASE__ ||
    (import.meta as any).env?.VITE_API_BASE ||
    "/_api"
  ).replace(/\/$/, "");

const apiUrl = (path: string) =>
  `${API_BASE}${path.startsWith("/") ? "" : "/"}${path}`;

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

const accountProfiles = [
  {
    id: "fundingpips_25k_p1",
    label: "FundingPips 25K Phase 1",
    firm: "fundingpips",
    phase: "phase_1_8",
    size: 25000,
    targetPct: 8,
    dailyPct: 5,
    maxLossPct: 10,
  },
  {
    id: "ftmo_25k_challenge",
    label: "FTMO 25K Challenge",
    firm: "ftmo",
    phase: "challenge",
    size: 25000,
    targetPct: 10,
    dailyPct: 5,
    maxLossPct: 10,
  },
  {
    id: "ftmo_100k_challenge",
    label: "FTMO 100K Challenge",
    firm: "ftmo",
    phase: "challenge",
    size: 100000,
    targetPct: 10,
    dailyPct: 5,
    maxLossPct: 10,
  },
  {
    id: "fundingpips_100k_funded",
    label: "FundingPips 100K Funded",
    firm: "fundingpips",
    phase: "funded",
    size: 100000,
    targetPct: null,
    dailyPct: 5,
    maxLossPct: 10,
  },
];

const money = (v: any) =>
  typeof v === "number"
    ? `$${v.toLocaleString(undefined, { maximumFractionDigits: 2 })}`
    : "NA";

export default function Propfirm() {
  const [status, setStatus] = useState<any>(null);
  const [risk, setRisk] = useState<any>(null);
  const [saving, setSaving] = useState(false);
  const [err, setErr] = useState("");

  async function load() {
    setErr("");
    try {
      const [s, r] = await Promise.all([
        fetch(apiUrl("/trend/prop/status"), {
          credentials: "include",
          cache: "no-store",
        }).then((x) => x.json()),
        fetch(apiUrl("/trend/prop/risk"), {
          credentials: "include",
          cache: "no-store",
        }).then((x) => x.json()),
      ]);
      setStatus(s);
      setRisk(r);
    } catch (e: any) {
      setErr(String(e?.message || e));
    }
  }

  useEffect(() => {
    load();
  }, []);

  const cfg: PropConfig | undefined = status?.config;
  const rs = risk?.risk;
  const limits = status?.limits;

  const selectedProfile = useMemo(() => {
    if (!cfg) return accountProfiles[0];
    return (
      accountProfiles.find(
        (a) =>
          a.id === cfg.account_id ||
          (a.firm === cfg.firm &&
            a.phase === cfg.phase &&
            Number(a.size) === Number(cfg.account_size))
      ) || accountProfiles[0]
    );
  }, [cfg]);

  async function activateProfile(id: string) {
    const p = accountProfiles.find((x) => x.id === id);
    if (!p || !cfg) return;

    const nextConfig = {
      ...cfg,
      enabled: true,
      firm: p.firm,
      phase: p.phase,
      account_size: p.size,
      account_name: p.label,
      account_id: p.id,
    };

    setSaving(true);

    setStatus((prev: any) =>
      prev
        ? {
            ...prev,
            config: nextConfig,
            limits: {
              target_usd:
                p.targetPct === null ? null : p.size * (p.targetPct / 100),
              daily_limit_usd: p.size * (p.dailyPct / 100),
              max_loss_limit_usd: p.size * (p.maxLossPct / 100),
            },
          }
        : prev
    );

    try {
      const res = await fetch(apiUrl("/trend/prop/config"), {
        method: "POST",
        credentials: "include",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(nextConfig),
      }).then((x) => x.json());

      if (!res?.ok) throw new Error(res?.error || "Save failed");
      await load();
    } catch (e: any) {
      setErr(String(e?.message || e));
    } finally {
      setSaving(false);
    }
  }

  return (
    <div className="min-h-screen bg-slate-950 text-slate-100 p-6">
      <div className="max-w-7xl mx-auto space-y-6">
        <div className="flex flex-col xl:flex-row xl:items-start xl:justify-between gap-4">
          <div>
            <h1 className="text-2xl font-bold">Prop Firm</h1>
            <p className="text-sm text-slate-400">
              One active execution account controls XTL lot size, SL/TP and risk guards.
            </p>
          </div>

          <div className="rounded-2xl border border-slate-800 bg-slate-900/70 p-4 min-w-[280px]">
            <label className="block text-xs text-slate-400 mb-1">
              Active Execution Account
            </label>
            <select
              className="w-full bg-slate-950 border border-slate-700 rounded-lg px-3 py-2 text-sm"
              value={selectedProfile.id}
              onChange={(e) => activateProfile(e.target.value)}
            >
              {accountProfiles.map((a) => (
                <option key={a.id} value={a.id}>
                  {a.label}
                </option>
              ))}
            </select>
            <div className="text-xs text-emerald-300 mt-2">
              Executor uses this selected account.
            </div>
            {saving && <div className="text-xs text-amber-300 mt-1">Saving...</div>}
          </div>
        </div>

        {err ? (
          <div className="rounded-xl border border-red-900 bg-red-950/40 p-3 text-sm text-red-200">
            {err}
          </div>
        ) : null}

        <div className="rounded-2xl border border-emerald-800/60 bg-emerald-950/20 p-5">
          <div className="text-xs text-emerald-300">ACTIVE FOR XTL EXECUTION</div>
          <div className="text-2xl font-bold mt-1">
            {cfg?.account_name || selectedProfile.label}
          </div>
          <div className="text-sm text-slate-300 mt-1">
            {cfg?.firm?.toUpperCase()} / {cfg?.phase} / {money(cfg?.account_size)}
          </div>
          <div className="text-xs text-slate-400 mt-2">
            Every ENTRY_CAND is checked against this account before MT5 enqueue or Discord manual signal.
          </div>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
          <Card title="Target" value={money(limits?.target_usd)} sub="Profit target" />
          <Card title="Daily Loss Limit" value={money(limits?.daily_limit_usd)} sub="Hard firm rule" />
          <Card title="Max Loss Limit" value={money(limits?.max_loss_limit_usd)} sub="Overall drawdown" />
          <Card title="Open Risk" value={money(rs?.open_risk_usd)} sub={`${rs?.open_positions?.length || 0} open positions`} />
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
          <Panel title="Active Rules">
            <Row label="Firm" value={cfg?.firm?.toUpperCase()} />
            <Row label="Phase" value={cfg?.phase} />
            <Row label="Account Size" value={money(cfg?.account_size)} />
            <Row label="Risk / Trade" value={`${cfg?.risk_pct ?? "NA"}%`} />
            <Row label="Target RR" value={`${cfg?.target_rr ?? "NA"}R`} />
            <Row label="Max Open Risk" value={`${cfg?.max_open_risk_pct ?? "NA"}%`} />
            <Row label="Max Positions" value={cfg?.max_open_positions ?? "NA"} />
          </Panel>

          <Panel title="Risk State">
            <Row label="Day" value={rs?.day} />
            <Row label="Daily Loss Used" value={money(rs?.daily_loss_used)} />
            <Row label="Risk Reserved" value={money(rs?.daily_risk_reserved)} />
            <Row label="Max Loss Used" value={money(rs?.max_loss_used)} />
            <Row label="Wins Today" value={rs?.wins_today ?? "NA"} />
            <Row label="Losses Today" value={rs?.losses_today ?? "NA"} />
          </Panel>
        </div>

        <Panel title="Open Positions">
          {!rs?.open_positions?.length ? (
            <div className="text-sm text-slate-400">No open prop risk reserved.</div>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead className="text-slate-400">
                  <tr>
                    <th className="text-left py-2">Symbol</th>
                    <th className="text-left py-2">Side</th>
                    <th className="text-left py-2">Lots</th>
                    <th className="text-left py-2">Risk</th>
                    <th className="text-left py-2">Entry</th>
                    <th className="text-left py-2">SL</th>
                    <th className="text-left py-2">TP</th>
                  </tr>
                </thead>
                <tbody>
                  {rs.open_positions.map((p: any, i: number) => (
                    <tr key={i} className="border-t border-slate-800">
                      <td className="py-2">{p.symbol}</td>
                      <td>{p.side}</td>
                      <td>{p.lots}</td>
                      <td>{money(Number(p.risk_usd || 0))}</td>
                      <td>{p.entry}</td>
                      <td>{p.sl}</td>
                      <td>{p.tp}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </Panel>
      </div>
    </div>
  );
}

function Card({ title, value, sub }: { title: string; value: any; sub: string }) {
  return (
    <div className="rounded-2xl border border-slate-800 bg-slate-900/60 p-5">
      <div className="text-xs text-slate-400">{title}</div>
      <div className="text-2xl font-bold mt-1">{value}</div>
      <div className="text-xs text-slate-500 mt-1">{sub}</div>
    </div>
  );
}

function Panel({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <section className="rounded-2xl border border-slate-800 bg-slate-900/60 p-5">
      <h2 className="text-lg font-semibold mb-4">{title}</h2>
      <div className="space-y-2 text-sm">{children}</div>
    </section>
  );
}

function Row({ label, value }: { label: string; value: any }) {
  return (
    <div className="flex justify-between gap-4 border-b border-slate-800/60 pb-2">
      <span className="text-slate-400">{label}</span>
      <span className="font-medium text-right">{value ?? "NA"}</span>
    </div>
  );
}
