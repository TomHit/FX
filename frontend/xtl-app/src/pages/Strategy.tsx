import React from "react";

/**
 * API base:
 * - uses VITE_API_BASE if defined
 * - otherwise defaults to same-origin "/_api"
 *   (handled by FastAPI _StripApiPrefix middleware)
 */
const API_BASE =
  (import.meta as any).env?.VITE_API_BASE?.replace(/\/$/, "") || "/_api";

type StrategyId = "indicator" | "priceAction" | "opportunity";
type ExecMode = "paper" | "mt5";

type Mt5Account = "demo" | "live";
type TargetKind = "price" | "pips" | "r";

type BotState = {
  enabled: boolean;
  strategy_type: StrategyId;
  config: Record<string, any>;
  updated_ms: number;
};

type Target = {
  id: string;
  kind: TargetKind;
  value: number; // price / pips / R
  qty_pct: number; // 0..100
  runner?: boolean; // if true, no fixed TP (managed by trailing)
};

const STRATEGY_LABELS: Record<StrategyId, string> = {
  indicator: "Indicator Strategy",
  priceAction: "Price Action Strategy",
  opportunity: "Opportunity Strategy",
};

const STRATEGY_TAGLINES: Record<StrategyId, string> = {
  indicator: "EMA/RSI/ADX trend + pullback. Conservative gates.",
  priceAction: "Candles at SR with trend filters and confirmation.",
  opportunity: "Follows XTL opportunities with strict safety rails.",
};

function cx(...xs: Array<string | false | null | undefined>) {
  return xs.filter(Boolean).join(" ");
}

function uid() {
  return Math.random().toString(16).slice(2) + Date.now().toString(16);
}

function clamp(n: number, a: number, b: number) {
  return Math.max(a, Math.min(b, n));
}


function sanitizeQtyBySymbol(raw: any): Record<string, number> {
  const out: Record<string, number> = {};
  if (!raw || typeof raw !== "object") return out;
  for (const [k, v] of Object.entries(raw)) {
    const sym = String(k || "").toUpperCase().trim();
    const q = Number(v);
    if (!sym) continue;
    if (!Number.isFinite(q) || q <= 0) continue;
    out[sym] = q;
  }
  return out;
}

function round2(x: number) {
  return Math.round(x * 100) / 100;
}

function fmtTs(ms?: number | null) {
  if (!ms || !Number.isFinite(ms)) return "-";
  try {
    return new Date(ms).toLocaleString();
  } catch {
    return String(ms);
  }
}

function pnlTone(pnl?: number | null) {
  const x = typeof pnl === "number" ? pnl : 0;
  if (x > 0) return "text-emerald-200";
  if (x < 0) return "text-rose-200";
  return "text-slate-200";
}


/**
 * IMPORTANT: keep DEFAULT_CONFIG defined above StrategyConfigurator usage.
 * TS errors you saw were because it was accidentally deleted/moved.
 */
const DEFAULT_CONFIG = {
  execution: {
    mode: "mt5" as ExecMode,
    mt5_account: "demo" as Mt5Account,
    require_confirm: true,
  },
  risk: {
    qty: 1,
    max_positions: 1,
    risk_mode: "qty_by_symbol" as any, // "qty" | "qty_fx_metal" | "qty_by_symbol" | "risk_pct"
    risk_pct: 1,
    // presets when risk_mode === "qty_fx_metal"
    qty_fx: 10000,
    qty_metals: 10,
    // per-symbol overrides when risk_mode === "qty_by_symbol"
    qty_by_symbol: {} as Record<string, number>,
  },
  // shared entry defaults
  entry: {
    side_mode: "follow" as "follow" | "force_buy" | "force_sell",
    entry_type: "market" as "market" | "limit",
    limit_price: null as number | null,

    // used by indicator + priceAction
    confirm_pullback: true,
    pullback: {
      zone: "vwap" as "vwap" | "ema20" | "ema50" | "sr",
      max_retrace_pct: 0.8,
      reversal: "close_reclaim" as
        | "close_reclaim"
        | "engulfing"
        | "break_swing",
    },

    // opportunity-only knobs
    opportunity: {
      min_confidence: "medium" as "low" | "medium" | "high",
      side_mode: "follow" as "follow" | "force_buy" | "force_sell",
      require_same_tf_trend: true,
    },
  },
  exits: {
    sl: {
      mode: "pips" as "pips" | "price" | "atr",
      value: 120,
      atr_mult: 1.2,
    },
    targets: {
      mode: "single" as "single" | "two_plus_runner" | "multi",
      list: [
        {
          id: uid(),
          kind: "r" as TargetKind,
          value: 1.5,
          qty_pct: 100,
          runner: false,
        },
      ] as Target[],
    },
    trailing: {
      enabled: true,
      kind: "step" as "step" | "atr",
      step_pips: 80,
      step_lock_pips: 40,
      atr_mult: 1.0,
      activate_after_r: 1.0,
    },
    breakeven: {
      enabled: true,
      at_r: 1.0,
      buffer_pips: 10,
    },
  },
  guards: {
    stale_bar_sec: 3 * 60,
    disable_weekends: true,
    only_if_recent_bar: true,
  },
};

/**
 * Unified API helper
 * Always call with relative paths like "/strategy/bot/state"
 */
async function apiJson<T>(
  path: string,
  init?: RequestInit & { json?: any }
): Promise<T> {
  const url = path.startsWith("http")
    ? path
    : `${API_BASE}${path.startsWith("/") ? "" : "/"}${path}`;

  const headers: Record<string, string> = {
    Accept: "application/json",
    ...(init?.headers as any),
  };

  let body: any = init?.body;

  if (init?.json !== undefined) {
    headers["Content-Type"] = "application/json";
    body = JSON.stringify(init.json);
  }

  const res = await fetch(url, {
    credentials: "include",
    ...init,
    headers,
    body,
  });

  const txt = await res.text().catch(() => "");
  if (!res.ok) throw new Error(`${res.status} ${txt || res.statusText}`);

  try {
    return (txt ? JSON.parse(txt) : {}) as T;
  } catch {
    return {} as T;
  }
}

function QtyOverridesEditor({
  value,
  onChange,
}: {
  value: Record<string, number>;
  onChange: (next: Record<string, number>) => void;
}) {
  const entries = Object.entries(value || {}).map(([k, v]) => [String(k || "").toUpperCase(), Number(v || 0)] as const);

  const setEntry = (idx: number, sym: string, qty: number) => {
    const nextArr = [...entries];
    nextArr[idx] = [sym.toUpperCase().trim(), qty];
    const next: Record<string, number> = {};
    for (const [s, q] of nextArr) {
      if (s && Number.isFinite(q) && q > 0) next[s] = q;
    }
    onChange(next);
  };

  const removeEntry = (idx: number) => {
    const nextArr = entries.filter((_, i) => i !== idx);
    const next: Record<string, number> = {};
    for (const [s, q] of nextArr) {
      if (s && Number.isFinite(q) && q > 0) next[s] = q;
    }
    onChange(next);
  };

  const addRow = () => {
    const next: Record<string, number> = { ...(value || {}) };
    // add a placeholder unique key
    let k = "EURUSD";
    let n = 1;
    while (next[k]) {
      n += 1;
      k = `SYM${n}`;
    }
    next[k] = 10000;
    onChange(next);
  };

  return (
    <div className="mt-4 rounded-2xl border border-slate-800/80 bg-slate-950/40 p-3">
      <div className="flex items-center justify-between">
        <div className="text-sm font-semibold text-slate-100">Qty Overrides</div>
        <button
          type="button"
          className="rounded-xl bg-white/10 px-3 py-1.5 text-xs font-semibold text-slate-100 ring-1 ring-white/10 hover:bg-white/15"
          onClick={addRow}
        >
          + Add
        </button>
      </div>

      <div className="mt-3 overflow-hidden rounded-xl border border-slate-800/80">
        <div className="grid grid-cols-[1fr_1fr_52px] gap-0 bg-slate-900/60 px-3 py-2 text-xs font-semibold text-slate-300">
          <div>Symbol</div>
          <div>Qty</div>
          <div className="text-right"> </div>
        </div>

        {entries.length === 0 ? (
          <div className="px-3 py-3 text-xs text-slate-400">No overrides yet. Click “Add”.</div>
        ) : (
          <div className="divide-y divide-slate-800/70">
            {entries.map(([sym, qty], idx) => (
              <div key={`${sym}-${idx}`} className="grid grid-cols-[1fr_1fr_52px] items-center gap-2 px-3 py-2">
                <Input
                  value={sym}
                  onChange={(e) => setEntry(idx, String((e.target as any).value || ""), qty)}
                  placeholder="EURUSD"
                />
                <Input
                  type="number"
                  value={qty || ""}
                  min={0}
                  onChange={(e) => setEntry(idx, sym, Number((e.target as any).value || 0))}
                />
                <button
                  type="button"
                  className="text-right text-xs text-rose-300 hover:text-rose-200"
                  onClick={() => removeEntry(idx)}
                  title="Remove"
                >
                  ✕
                </button>
              </div>
            ))}
          </div>
        )}
      </div>

      <div className="mt-2 text-xs text-slate-400">
        Overrides apply at entry time. Example: EURUSD=10000, XAUUSD=10. Symbols are stored uppercase.
      </div>
    </div>
  );
}


// ---------------- OPPT Strategy API ----------------
type OpptState = {
  enabled: boolean;
  execution_mode: "paper" | "mt5";
  mt5_account: "demo" | "live";
  qty: number;
  qty_fx?: number;
  qty_metals?: number;
  risk_mode?: "qty" | "qty_fx_metal" | "qty_by_symbol" | "risk_pct" | string | null;
  qty_by_symbol?: Record<string, number>;
  max_positions: number;
  cooldown_min: number;
  min_score: number;
  min_confidence?: "low" | "medium" | "high" | null;
  started_at_ms?: number | null;
  updated_at_ms?: number | null;
};

type PaperTrade = {
  trade_id: string;
  symbol: string;
  side: "BUY" | "SELL";
  entry_price: number;
  qty: number;
  tp_price?: number | null;
  sl_price?: number | null;
  opened_at_ms?: number | null;

  // closed-only fields
  exit_price?: number | null;
  exit_reason?: "HIT" | "SL_HIT" | "EXPIRED" | string | null;
  pnl?: number | null;
  closed_at_ms?: number | null;
};

type PaperTradesResp = {
  open: PaperTrade[];
  closed: PaperTrade[];
};
const opptApi = {
  getState: () => apiJson<OpptState>("/strategy/oppt/state"),

  // ✅ FIX: do NOT include "/api" here (VITE_API_ORIGIN already adds "/_api")
  start: (json?: any) =>
    apiJson<OpptState>("/strategy/oppt/start", { method: "POST", json: json ?? {} }),

  stop: (json?: any) =>
    apiJson<OpptState>("/strategy/oppt/stop", { method: "POST", json: json ?? {} }),

  patch: (json: Partial<OpptState>) =>
    apiJson<OpptState>("/strategy/oppt/patch", { method: "POST", json }),

  // paper trades
  getPaperTrades: () => apiJson<PaperTradesResp>("/strategy/oppt/paper/trades"),
};

/* ------------------------------ UI atoms ------------------------------ */

function Badge({
  children,
  tone = "slate",
}: {
  children: React.ReactNode;
  tone?: "slate" | "emerald" | "rose" | "sky" | "amber";
}) {
  const tones: Record<string, string> = {
    slate: "bg-slate-800/60 text-slate-200 border-slate-700/70",
    emerald: "bg-emerald-500/10 text-emerald-200 border-emerald-500/25",
    rose: "bg-rose-500/10 text-rose-200 border-rose-500/25",
    sky: "bg-sky-500/10 text-sky-200 border-sky-500/25",
    amber: "bg-amber-500/10 text-amber-200 border-amber-500/25",
  };
  return (
    <span
      className={cx(
        "inline-flex items-center gap-1 rounded-full border px-2 py-0.5 text-[10px] font-semibold tracking-wide",
        tones[tone]
      )}
    >
      {children}
    </span>
  );
}

function SectionTitle({
  title,
  desc,
  hint,
  right,
}: {
  title: string;
  desc?: string;
  hint?: string;
  right?: React.ReactNode;
}) {
  const _desc = desc ?? hint;
  return (
    <div className="flex items-start justify-between gap-3">
      <div className="min-w-0">
        <h3 className="text-sm font-semibold text-slate-50">{title}</h3>
        {_desc ? (
          <p className="mt-0.5 text-[11px] text-slate-400">{_desc}</p>
        ) : null}
      </div>
      {right ? <div className="shrink-0">{right}</div> : null}
    </div>
  );
}

function Field({
  label,
  hint,
  children,
}: {
  label: string;
  hint?: string;
  children: React.ReactNode;
}) {
  return (
    <div className="space-y-1">
      <div className="flex items-center justify-between gap-2">
        <p className="text-[11px] font-medium text-slate-300">{label}</p>
        {hint ? <p className="text-[10px] text-slate-500">{hint}</p> : null}
      </div>
      {children}
    </div>
  );
}

function Input(props: React.InputHTMLAttributes<HTMLInputElement>) {
  return (
    <input
      {...props}
      className={cx(
        "w-full rounded-xl border border-slate-800/80 bg-slate-950/70 px-3 py-2 text-sm text-slate-100",
        "placeholder:text-slate-600 focus:border-sky-500/70 focus:outline-none focus:ring-2 focus:ring-sky-500/15",
        props.className
      )}
    />
  );
}

function Select(props: React.SelectHTMLAttributes<HTMLSelectElement>) {
  return (
    <select
      {...props}
      className={cx(
        "w-full rounded-xl border border-slate-800/80 bg-slate-950/70 px-3 py-2 text-sm text-slate-100",
        "focus:border-sky-500/70 focus:outline-none focus:ring-2 focus:ring-sky-500/15",
        props.className
      )}
    />
  );
}

function Switch({
  checked,
  value,
  onChange,
  label,
  sub,
  hint,
}: {
  checked?: boolean;
  value?: boolean;
  onChange: (v: boolean) => void;
  label: string;
  sub?: string;
  hint?: string;
}) {
  const _v = value ?? checked ?? false;
  const _sub = sub ?? hint;
  return (
    <button
      type="button"
      onClick={() => onChange(!_v)}
      className={cx(
        "flex w-full items-center justify-between gap-3 rounded-2xl border px-3 py-2 text-left transition",
        "border-slate-800/80 bg-slate-950/60 hover:bg-slate-900/60"
      )}
    >
      <div className="min-w-0">
        <p className="text-xs font-semibold text-slate-100">{label}</p>
        {_sub ? <p className="mt-0.5 text-[11px] text-slate-400">{_sub}</p> : null}
      </div>
      <span
        className={cx(
          "relative inline-flex h-6 w-11 shrink-0 items-center rounded-full border transition",
          _v
            ? "border-emerald-500/40 bg-emerald-500/20"
            : "border-slate-700/70 bg-slate-900/60"
        )}
      >
        <span
          className={cx(
            "inline-block h-5 w-5 transform rounded-full bg-slate-200 shadow transition",
            _v ? "translate-x-5" : "translate-x-0.5"
          )}
        />
      </span>
    </button>
  );
}

function prettyJson(v: any) {
  try {
    return JSON.stringify(v, null, 2);
  } catch {
    return "{}";
  }
}

function deepEqualJson(a: any, b: any) {
  return prettyJson(a) === prettyJson(b);
}

/* ------------------------------ Page ------------------------------ */

export default function Strategy() {
  return <StrategyConfigurator />;
}

export function StrategyConfigurator() {
  const [loading, setLoading] = React.useState(true);
  const [saveBusy, setSaveBusy] = React.useState(false);
  const [toggleBusy, setToggleBusy] = React.useState(false);
  const [err, setErr] = React.useState<string | null>(null);

  const [serverState, setServerState] = React.useState<OpptState | null>(null);
  const [enabled, setEnabled] = React.useState(false);

  // This page is OPPT-only (auto trading rails)
  const [config, setConfig] = React.useState<any>({ ...DEFAULT_CONFIG });

  
  const qtyOverrides = React.useMemo(() => {
    return sanitizeQtyBySymbol((config?.risk as any)?.qty_by_symbol);
  }, [config]);

  const requireQtyOverrides =
    String((config?.risk as any)?.risk_mode || "qty_by_symbol") === "qty_by_symbol";

  const hasQtyOverrides = Object.keys(qtyOverrides).length > 0;

  const missingQtyOverrides = requireQtyOverrides && !hasQtyOverrides;
const [showAdvanced, setShowAdvanced] = React.useState(false);
  const [advancedJson, setAdvancedJson] = React.useState("");

  const [paperTrades, setPaperTrades] = React.useState<PaperTradesResp | null>(null);
  const [paperBusy, setPaperBusy] = React.useState(false);

  const isPaper = (config?.execution?.mode || "mt5") === "paper";

  const loadPaperTrades = React.useCallback(async () => {
    setPaperBusy(true);
    try {
      const t = await opptApi.getPaperTrades();
      setPaperTrades(t);
    } catch (e: any) {
      console.warn("paper trades load failed:", e?.message || e);
    } finally {
      setPaperBusy(false);
    }
  }, []);

  const mergeFromServer = React.useCallback((st: OpptState) => {
    const merged: any = { ...DEFAULT_CONFIG };

    merged.execution = {
      ...DEFAULT_CONFIG.execution,
      mode: st?.execution_mode === "mt5" ? "mt5" : "paper",
      mt5_account: st?.mt5_account === "live" ? "live" : "demo",
      require_confirm: (merged?.execution?.require_confirm ?? true) === false ? false : true,
    };

    merged.risk = {
      ...DEFAULT_CONFIG.risk,
      // OPPT is now per-symbol qty
      risk_mode: "qty_by_symbol",
      qty_by_symbol:
        (st as any)?.qty_by_symbol && typeof (st as any).qty_by_symbol === "object"
          ? (st as any).qty_by_symbol
          : (DEFAULT_CONFIG.risk as any)?.qty_by_symbol || {},
      max_positions: typeof st?.max_positions === "number" ? st.max_positions : DEFAULT_CONFIG.risk.max_positions,
      cooldown_min: typeof st?.cooldown_min === "number" ? st.cooldown_min : 0,
    };

    merged.entry = { ...DEFAULT_CONFIG.entry };
    merged.entry.opportunity = {
      ...DEFAULT_CONFIG.entry.opportunity,
      min_score: typeof st?.min_score === "number" ? st.min_score : 0,
      min_confidence: (st?.min_confidence as any) || DEFAULT_CONFIG.entry.opportunity.min_confidence || "medium",
    };

    merged.exits = { ...DEFAULT_CONFIG.exits };
    merged.guards = { ...DEFAULT_CONFIG.guards };

    return merged;
  }, []);

  const load = React.useCallback(async () => {
    setErr(null);
    setLoading(true);
    try {
      const st = await opptApi.getState();
      setServerState(st);
      setEnabled(!!st?.enabled);

      const merged = mergeFromServer(st);
      setConfig(merged);
      setAdvancedJson(prettyJson(merged));

      if ((merged?.execution?.mode || "mt5") === "paper") {
        setTimeout(() => loadPaperTrades(), 0);
      }
    } catch (e: any) {
      setErr(e?.message || String(e));
    } finally {
      setLoading(false);
    }
  }, [loadPaperTrades, mergeFromServer]);

  React.useEffect(() => {
    load();
  }, [load]);

  React.useEffect(() => {
    if (!enabled) return;
    if (!isPaper) return;
    const id = window.setInterval(() => {
      loadPaperTrades();
    }, 2000);
    return () => window.clearInterval(id);
  }, [enabled, isPaper, loadPaperTrades]);

  const paperPnl = React.useMemo(() => {
    const closed = paperTrades?.closed || [];
    let realized = 0;
    let wins = 0;
    let losses = 0;

    for (const t of closed) {
      const p = typeof t.pnl === "number" ? t.pnl : 0;
      realized += p;
      if (p > 0) wins += 1;
      else if (p < 0) losses += 1;
    }

    return { realized, trades: closed.length, wins, losses };
  }, [paperTrades]);

  const isDirty = React.useMemo(() => {
    if (!serverState) return true;

    const want = {
      execution_mode: config?.execution?.mode,
      mt5_account: config?.execution?.mt5_account,
      risk_mode: "qty_by_symbol",
      qty_by_symbol: (config?.risk as any)?.qty_by_symbol || {},
      max_positions: Number(config?.risk?.max_positions ?? 1),
      cooldown_min: Number((config?.risk as any)?.cooldown_min ?? 0),
      min_score: Number(config?.entry?.opportunity?.min_score ?? 0),
      min_confidence: (config?.entry?.opportunity?.min_confidence || "medium") as any,
    };

    const have = {
      execution_mode: serverState.execution_mode,
      mt5_account: serverState.mt5_account,
      risk_mode: (serverState as any)?.risk_mode || "qty_by_symbol",
      qty_by_symbol: (serverState as any)?.qty_by_symbol || {},
      max_positions: serverState.max_positions,
      cooldown_min: serverState.cooldown_min,
      min_score: serverState.min_score,
      min_confidence: (serverState.min_confidence as any) ?? "medium",
    };

    return !deepEqualJson(want, have);
  }, [serverState, config]);

  const setCfg = (patch: any) => setConfig((prev: any) => ({ ...prev, ...patch }));

  const save = async () => {
    setErr(null);
    setSaveBusy(true);
    try {
      let nextCfg = config;

      if (showAdvanced) {
        try {
          const parsed = JSON.parse(advancedJson || "{}");
          if (parsed && typeof parsed === "object") nextCfg = parsed;
          else throw new Error("Advanced JSON must be an object.");
        } catch {
          throw new Error("Advanced JSON is invalid. Fix it or disable Advanced editor.");
        }
      }

      
      const qbs = sanitizeQtyBySymbol((nextCfg?.risk as any)?.qty_by_symbol);
      if (String((nextCfg?.risk as any)?.risk_mode || "qty_by_symbol") === "qty_by_symbol") {
        if (Object.keys(qbs).length === 0) {
          throw new Error("Please add at least one Qty Override (symbol + qty) before saving or starting auto.");
        }
      }

const payload: Partial<OpptState> = {
        execution_mode: nextCfg?.execution?.mode,
        mt5_account: nextCfg?.execution?.mt5_account,
        risk_mode: "qty_by_symbol",
        qty_by_symbol: qbs,
        max_positions: Number(nextCfg?.risk?.max_positions ?? 1),
        cooldown_min: Number((nextCfg?.risk as any)?.cooldown_min ?? 0),
        min_score: Number(nextCfg?.entry?.opportunity?.min_score ?? 0),
        min_confidence: (nextCfg?.entry?.opportunity?.min_confidence || "medium") as any,
      };

      const st = await opptApi.patch(payload);
      setServerState(st);
      setEnabled(!!st.enabled);
      await load();
    } catch (e: any) {
      setErr(e?.message || String(e));
    } finally {
      setSaveBusy(false);
    }
  };

  const toggleBot = async () => {
    setErr(null);
    setToggleBusy(true);
    try {
      if (!enabled) {
        const requireConfirm = !!config?.execution?.require_confirm;
        if (requireConfirm) {
          const ok = window.confirm("Start auto trading now?");
          if (!ok) return;
        }
        if (missingQtyOverrides) {
          const msg = "Qty Overrides are required. Please add at least one symbol + qty before starting auto.";
          setErr(msg);
          window.alert(msg);
          return;
        }

        if (isDirty) {
          try {
            await save();
          } catch (e) {
            // Don't block START if save fails; user can fix and save later.
            console.warn("Save failed; continuing to start auto.", e);
          }
        }

        const st = await opptApi.start({ enabled: true });
        setServerState(st);
        setEnabled(!!st.enabled);
        await load();
        if (isPaper) setTimeout(() => loadPaperTrades(), 0);
      } else {
        const requireConfirm = !!config?.execution?.require_confirm;
        if (requireConfirm) {
          const ok = window.confirm("Stop auto trading?");
          if (!ok) return;
        }
        const st = await opptApi.stop({ enabled: false });
        
        setServerState(st);
        setEnabled(!!st.enabled);
        await load();
      }
    } catch (e: any) {
      setErr(e?.message || String(e));
    } finally {
      setToggleBusy(false);
    }
  };

  const reset = async () => {
    const ok = window.confirm("Reset Opportunity strategy settings to defaults?");
    if (!ok) return;
    setErr(null);
    setSaveBusy(true);
    try {
      const merged: any = { ...DEFAULT_CONFIG };
      merged.risk = { ...(merged.risk || {}), risk_mode: "qty_by_symbol", qty_by_symbol: {} };
      merged.entry = { ...(merged.entry || {}) };
      merged.entry.opportunity = { ...(merged.entry?.opportunity || {}), min_score: 0, min_confidence: "medium" };

      setConfig(merged);
      setAdvancedJson(prettyJson(merged));

      const st = await opptApi.patch({
        execution_mode: merged?.execution?.mode,
        mt5_account: merged?.execution?.mt5_account,
        risk_mode: "qty_by_symbol",
        qty_by_symbol: (merged?.risk as any)?.qty_by_symbol || {},
        max_positions: Number(merged?.risk?.max_positions ?? 1),
        cooldown_min: Number((merged?.risk as any)?.cooldown_min ?? 0),
        min_score: Number(merged?.entry?.opportunity?.min_score ?? 0),
        min_confidence: (merged?.entry?.opportunity?.min_confidence || "medium") as any,
      });

      setServerState(st);
      setEnabled(!!st.enabled);
      if ((merged?.execution?.mode || "mt5") === "paper") setTimeout(() => loadPaperTrades(), 0);
    } catch (e: any) {
      setErr(e?.message || String(e));
    } finally {
      setSaveBusy(false);
    }
  };

  const execBadge = (() => {
    const m: ExecMode = config?.execution?.mode || "mt5";
    if (m === "paper") return <Badge tone="sky">EXEC: Paper</Badge>;
    const acct: Mt5Account = (config as any)?.execution?.mt5_account || "demo";
    return <Badge tone="amber">EXEC: MT5 ({acct})</Badge>;
  })();

  return (
    <div className="mx-auto max-w-6xl px-6 py-8">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div className="min-w-0">
          <h1 className="text-xl font-semibold text-slate-50">Strategy</h1>
          <p className="mt-1 text-sm text-slate-400">
            Opportunity Strategy auto-trading rails (paper / MT5). Entry, SL and targets are computed by XTL.
          </p>
        </div>

        <div className="flex items-center gap-2">
          {execBadge}
          <Badge tone={enabled ? "emerald" : "slate"}>{enabled ? "AUTO: ON" : "AUTO: OFF"}</Badge>

          <button
            type="button"
            onClick={toggleBot}
            disabled={toggleBusy || loading || (!enabled && missingQtyOverrides)}
            className={cx(
              "rounded-xl border px-4 py-2 text-sm font-semibold transition",
              enabled
                ? "border-rose-500/40 bg-rose-500/10 text-rose-200 hover:bg-rose-500/15"
                : "border-emerald-500/40 bg-emerald-500/10 text-emerald-200 hover:bg-emerald-500/15",
              (toggleBusy || loading) && "opacity-60"
            )}
          >
            {toggleBusy ? "Working..." : enabled ? "Stop Auto" : "Start Auto"}
          </button>

          <button
            type="button"
            onClick={save}
            disabled={saveBusy || loading}
            className={cx(
              "rounded-xl border border-slate-800/80 bg-slate-950/70 px-4 py-2 text-sm font-semibold text-slate-100 hover:bg-slate-900/70",
              (saveBusy || loading) && "opacity-60"
            )}
          >
            {saveBusy ? "Saving..." : isDirty ? "Save" : "Saved"}
          </button>

          <button
            type="button"
            onClick={reset}
            disabled={saveBusy || loading}
            className={cx(
              "rounded-xl border border-slate-800/80 bg-slate-950/50 px-4 py-2 text-sm font-semibold text-slate-200 hover:bg-slate-900/60",
              (saveBusy || loading) && "opacity-60"
            )}
          >
            Reset
          </button>
        </div>

        {!enabled && missingQtyOverrides ? (
          <div className="mt-2 text-xs text-rose-300">
            Qty Overrides are required (per-symbol sizing). Add at least one symbol + qty to enable Start Auto.
          </div>
        ) : null}
      </div>

      {err ? (
        <div className="mt-4 rounded-2xl border border-rose-500/30 bg-rose-500/10 p-3 text-sm text-rose-100">
          {err}
        </div>
      ) : null}

      <div className="mt-6 grid gap-4 lg:grid-cols-2">
        <div className="space-y-4">
          <div className="rounded-2xl border border-slate-800/70 bg-slate-950/60 p-4">
            <SectionTitle title="Execution" hint="Paper is safest. MT5 Demo is next. Live is real money." />
            <div className="mt-3 grid grid-cols-1 gap-3 sm:grid-cols-3">
              <button
                type="button"
                onClick={() => setCfg({ execution: { ...(config.execution || {}), mode: "paper" as ExecMode } })}
                className={[
                  "rounded-2xl border p-3 text-left transition",
                  isPaper
                    ? "border-emerald-400/40 bg-emerald-500/10"
                    : "border-slate-800/80 bg-slate-950/40 hover:bg-slate-900/40",
                ].join(" ")}
              >
                <div className="flex items-center justify-between gap-3">
                  <div>
                    <div className="text-sm font-semibold text-slate-100">Paper</div>
                    <div className="text-xs text-slate-400">Simulated fills & PnL.</div>
                  </div>
                  <span className="rounded-full border border-slate-700/70 bg-slate-900/60 px-2 py-0.5 text-xs text-slate-200">
                    SAFE
                  </span>
                </div>
              </button>

              <button
                type="button"
                onClick={() =>
                  setCfg({ execution: { ...(config.execution || {}), mode: "mt5" as ExecMode, mt5_account: "demo" } })
                }
                className={[
                  "rounded-2xl border p-3 text-left transition",
                  !isPaper && (config.execution?.mt5_account || "demo") === "demo"
                    ? "border-cyan-400/40 bg-cyan-500/10"
                    : "border-slate-800/80 bg-slate-950/40 hover:bg-slate-900/40",
                ].join(" ")}
              >
                <div className="text-sm font-semibold text-slate-100">Demo</div>
                <div className="text-xs text-slate-400">MT5 demo account.</div>
              </button>

              <button
                type="button"
                onClick={() =>
                  setCfg({ execution: { ...(config.execution || {}), mode: "mt5" as ExecMode, mt5_account: "live" } })
                }
                className={[
                  "rounded-2xl border p-3 text-left transition",
                  !isPaper && (config.execution?.mt5_account || "demo") === "live"
                    ? "border-amber-400/40 bg-amber-500/10"
                    : "border-slate-800/80 bg-slate-950/40 hover:bg-slate-900/40",
                ].join(" ")}
              >
                <div className="flex items-center justify-between gap-3">
                  <div>
                    <div className="text-sm font-semibold text-slate-100">Live</div>
                    <div className="text-xs text-slate-400">MT5 live account.</div>
                  </div>
                  <span className="rounded-full border border-amber-400/30 bg-amber-500/10 px-2 py-0.5 text-xs text-amber-200">
                    RISK
                  </span>
                </div>
              </button>
            </div>

            <div className="mt-4">
              <Switch
                checked={!!config?.execution?.require_confirm}
                onChange={(v) => setCfg({ execution: { ...(config.execution || {}), require_confirm: !!v } })}
                label="Require confirmation for Start/Stop"
                sub="Adds a safety confirm dialog when toggling AUTO."
              />
            </div>
          </div>

          <div className="rounded-2xl border border-slate-800/70 bg-slate-950/60 p-4">
            <SectionTitle title="Sizing" hint="Per-symbol quantity only (recommended)." />
            <QtyOverridesEditor
              value={((config?.risk as any)?.qty_by_symbol as any) || {}}
              onChange={(next) =>
                setCfg({
                  risk: { ...(config.risk || {}), risk_mode: "qty_by_symbol", qty_by_symbol: next },
                })
              }
            />

            <div className="mt-4 grid gap-3 sm:grid-cols-2">
              <Field label="Max open positions">
                <Input
                  type="number"
                  value={Number(config?.risk?.max_positions ?? 1)}
                  min={1}
                  max={20}
                  onChange={(e) =>
                    setCfg({
                      risk: {
                        ...(config.risk || {}),
                        max_positions: clamp(Number((e.target as any).value || 1), 1, 20),
                      },
                    })
                  }
                />
              </Field>

              <Field label="Cooldown (minutes)" hint="0 = no cooldown">
                <Input
                  type="number"
                  value={Number((config?.risk as any)?.cooldown_min ?? 0)}
                  min={0}
                  max={240}
                  onChange={(e) =>
                    setCfg({
                      risk: {
                        ...(config.risk || {}),
                        cooldown_min: clamp(Number((e.target as any).value || 0), 0, 240),
                      },
                    })
                  }
                />
              </Field>
            </div>
          </div>

          <div className="rounded-2xl border border-slate-800/70 bg-slate-950/60 p-4">
            <SectionTitle title="Opportunity filters" hint="Controls which opportunities can be auto-executed." />
            <div className="mt-3 grid gap-3 sm:grid-cols-2">
              <Field label="Min score">
                <Input
                  type="number"
                  value={Number(config?.entry?.opportunity?.min_score ?? 0)}
                  min={0}
                  max={100}
                  onChange={(e) =>
                    setCfg({
                      entry: {
                        ...(config.entry || {}),
                        opportunity: {
                          ...(config?.entry?.opportunity || {}),
                          min_score: clamp(Number((e.target as any).value || 0), 0, 100),
                        },
                      },
                    })
                  }
                />
              </Field>

              <Field label="Min confidence">
                <Select
                  value={String(config?.entry?.opportunity?.min_confidence || "medium")}
                  onChange={(e) =>
                    setCfg({
                      entry: {
                        ...(config.entry || {}),
                        opportunity: {
                          ...(config?.entry?.opportunity || {}),
                          min_confidence: (e.target as any).value,
                        },
                      },
                    })
                  }
                >
                  <option value="low">low</option>
                  <option value="medium">medium</option>
                  <option value="high">high</option>
                </Select>
              </Field>
            </div>

            <div className="mt-3 text-xs text-slate-400">
              Server state last updated: <span className="text-slate-200">{fmtTs(serverState?.updated_at_ms ?? null)}</span>
            </div>
          </div>

          <div className="rounded-2xl border border-slate-800/80 bg-slate-950/70 p-4 shadow-xl shadow-slate-950/50">
            <SectionTitle
              title="Advanced"
              hint="Optional raw JSON editor (power users)."
              right={
                <Switch value={showAdvanced} onChange={(v: boolean) => setShowAdvanced(v)} label={showAdvanced ? "On" : "Off"} />
              }
            />
            {showAdvanced ? (
              <div className="mt-3">
                <textarea
                  className="h-64 w-full rounded-2xl border border-slate-800/80 bg-slate-950/60 p-3 font-mono text-[12px] text-slate-100 outline-none focus:border-sky-500/70 focus:ring-2 focus:ring-sky-500/15"
                  value={advancedJson}
                  onChange={(e) => setAdvancedJson((e.target as any).value)}
                />
                <p className="mt-2 text-[11px] text-slate-500">Invalid JSON will block saving.</p>
              </div>
            ) : (
              <p className="mt-2 text-[11px] text-slate-500">Keep Advanced off unless you know what you're doing.</p>
            )}
          </div>
        </div>

        <div className="space-y-4">
          {isPaper ? (
            <div className="rounded-2xl border border-slate-800/80 bg-slate-950/70 p-4 shadow-xl shadow-slate-950/50">
              <SectionTitle
                title="Paper Trades"
                desc="Open positions and recently closed results (PnL)."
                right={
                  <button
                    type="button"
                    onClick={loadPaperTrades}
                    disabled={paperBusy || loading}
                    className={cx(
                      "rounded-full border px-3 py-1.5 text-xs font-semibold transition",
                      "border-slate-800/80 bg-slate-950/70 text-slate-200 hover:bg-slate-900/70",
                      (paperBusy || loading) && "opacity-60"
                    )}
                  >
                    {paperBusy ? "Refreshing..." : "Refresh"}
                  </button>
                }
              />

              <div className="mb-4 mt-4 grid grid-cols-2 gap-3 sm:grid-cols-4">
                <div className="rounded-xl border border-slate-800/70 bg-slate-950/60 p-3">
                  <div className="text-[11px] text-slate-400">Realized PnL</div>
                  <div
                    className={cx(
                      "mt-1 text-lg font-bold",
                      paperPnl.realized > 0 && "text-emerald-300",
                      paperPnl.realized < 0 && "text-rose-300",
                      paperPnl.realized === 0 && "text-slate-200"
                    )}
                  >
                    {paperPnl.realized.toFixed(2)}
                  </div>
                </div>

                <div className="rounded-xl border border-slate-800/70 bg-slate-950/60 p-3">
                  <div className="text-[11px] text-slate-400">Closed Trades</div>
                  <div className="mt-1 text-lg font-bold text-slate-200">{paperPnl.trades}</div>
                </div>

                <div className="rounded-xl border border-slate-800/70 bg-slate-950/60 p-3">
                  <div className="text-[11px] text-slate-400">Wins</div>
                  <div className="mt-1 text-lg font-bold text-emerald-300">{paperPnl.wins}</div>
                </div>

                <div className="rounded-xl border border-slate-800/70 bg-slate-950/60 p-3">
                  <div className="text-[11px] text-slate-400">Losses</div>
                  <div className="mt-1 text-lg font-bold text-rose-300">{paperPnl.losses}</div>
                </div>
              </div>

              <div className="mt-4 grid gap-4">
                <div className="rounded-2xl border border-slate-800/70 bg-slate-950/60 p-3">
                  <div className="mb-2 flex items-center justify-between">
                    <div className="text-sm font-semibold text-slate-100">Open</div>
                    <Badge tone="sky">{paperTrades?.open?.length ?? 0}</Badge>
                  </div>

                  {(paperTrades?.open?.length ?? 0) === 0 ? (
                    <div className="text-xs text-slate-400">No open paper trades.</div>
                  ) : (
                    <div className="overflow-x-auto">
                      <table className="w-full text-left text-xs">
                        <thead className="text-[11px] text-slate-400">
                          <tr>
                            <th className="py-2 pr-3">Symbol</th>
                            <th className="py-2 pr-3">Side</th>
                            <th className="py-2 pr-3">Entry</th>
                            <th className="py-2 pr-3">TP</th>
                            <th className="py-2 pr-3">SL</th>
                            <th className="py-2 pr-3">Qty</th>
                            <th className="py-2 pr-3">Opened</th>
                          </tr>
                        </thead>
                        <tbody className="text-slate-200">
                          {paperTrades!.open.map((t) => (
                            <tr key={t.trade_id} className="border-t border-slate-800/60">
                              <td className="py-2 pr-3 font-semibold">{t.symbol}</td>
                              <td className="py-2 pr-3">
                                <Badge tone={t.side === "BUY" ? "emerald" : "rose"}>{t.side}</Badge>
                              </td>
                              <td className="py-2 pr-3">{t.entry_price}</td>
                              <td className="py-2 pr-3">{t.tp_price ?? "-"}</td>
                              <td className="py-2 pr-3">{t.sl_price ?? "-"}</td>
                              <td className="py-2 pr-3">{t.qty}</td>
                              <td className="py-2 pr-3 text-slate-400">{fmtTs(t.opened_at_ms ?? null)}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  )}
                </div>

                <div className="rounded-2xl border border-slate-800/70 bg-slate-950/60 p-3">
                  <div className="mb-2 flex items-center justify-between">
                    <div className="text-sm font-semibold text-slate-100">Closed (latest)</div>
                    <Badge tone="slate">{paperTrades?.closed?.length ?? 0}</Badge>
                  </div>

                  {(paperTrades?.closed?.length ?? 0) === 0 ? (
                    <div className="text-xs text-slate-400">No closed paper trades yet.</div>
                  ) : (
                    <div className="overflow-x-auto">
                      <table className="w-full text-left text-xs">
                        <thead className="text-[11px] text-slate-400">
                          <tr>
                            <th className="py-2 pr-3">Symbol</th>
                            <th className="py-2 pr-3">Side</th>
                            <th className="py-2 pr-3">Entry</th>
                            <th className="py-2 pr-3">Exit</th>
                            <th className="py-2 pr-3">Reason</th>
                            <th className="py-2 pr-3">PnL</th>
                            <th className="py-2 pr-3">Closed</th>
                          </tr>
                        </thead>
                        <tbody className="text-slate-200">
                          {paperTrades!.closed.map((t) => (
                            <tr key={`${t.trade_id}:${t.closed_at_ms || ""}`} className="border-t border-slate-800/60">
                              <td className="py-2 pr-3 font-semibold">{t.symbol}</td>
                              <td className="py-2 pr-3">
                                <Badge tone={t.side === "BUY" ? "emerald" : "rose"}>{t.side}</Badge>
                              </td>
                              <td className="py-2 pr-3">{t.entry_price}</td>
                              <td className="py-2 pr-3">{t.exit_price ?? "-"}</td>
                              <td className="py-2 pr-3">{t.exit_reason ?? "-"}</td>
                              <td className={cx("py-2 pr-3 font-semibold", pnlTone(t.pnl ?? 0))}>
                                {typeof t.pnl === "number" ? t.pnl.toFixed(2) : "-"}
                              </td>
                              <td className="py-2 pr-3 text-slate-400">{fmtTs(t.closed_at_ms ?? null)}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  )}
                </div>
              </div>

              <div className="mt-3 text-[11px] text-slate-500">
                Tip: When AUTO is ON, this panel auto-refreshes every 2 seconds.
              </div>
            </div>
          ) : (
            <div className="rounded-2xl border border-sky-500/30 bg-sky-500/10 p-4 text-sm text-sky-100">
              Paper trades panel is available only when Execution is set to Paper.
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
