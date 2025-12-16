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

function round2(x: number) {
  return Math.round(x * 100) / 100;
}

/**
 * IMPORTANT: keep DEFAULT_CONFIG defined above StrategyConfigurator usage.
 * TS errors you saw were because it was accidentally deleted/moved.
 */
const DEFAULT_CONFIG = {
  execution: {
    mode: "paper" as ExecMode,
    require_live_ack: true,
  },
  risk: {
    qty: 1,
    max_positions: 1,
    risk_mode: "qty" as "qty" | "risk_pct",
    risk_pct: 1,
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

  const [serverState, setServerState] = React.useState<BotState | null>(null);

  const [active, setActive] = React.useState<StrategyId>("indicator");
  const [enabled, setEnabled] = React.useState(false);
  const [config, setConfig] = React.useState<any>({ ...DEFAULT_CONFIG });

  const [showAdvanced, setShowAdvanced] = React.useState(false);
  const [advancedJson, setAdvancedJson] = React.useState("");

  // --- load state ---
  const load = React.useCallback(async () => {
    setErr(null);
    setLoading(true);
    try {
      const st = await apiJson<BotState>("/strategy/bot/state");
      setServerState(st);
      setEnabled(!!st?.enabled);
      setActive((st?.strategy_type as StrategyId) || "indicator");

      const cfg = st?.config && typeof st.config === "object" ? st.config : {};
      const merged: any = { ...DEFAULT_CONFIG, ...cfg };

      // Deep merge nested keys (keeps new defaults):
      merged.execution = { ...DEFAULT_CONFIG.execution, ...(cfg as any).execution };
      merged.risk = { ...DEFAULT_CONFIG.risk, ...(cfg as any).risk };
      merged.entry = { ...DEFAULT_CONFIG.entry, ...(cfg as any).entry };
      merged.entry.pullback = {
        ...DEFAULT_CONFIG.entry.pullback,
        ...((cfg as any).entry?.pullback || {}),
      };
      merged.entry.opportunity = {
        ...DEFAULT_CONFIG.entry.opportunity,
        ...((cfg as any).entry?.opportunity || {}),
      };
      merged.exits = { ...DEFAULT_CONFIG.exits, ...(cfg as any).exits };
      merged.exits.sl = { ...DEFAULT_CONFIG.exits.sl, ...((cfg as any).exits?.sl || {}) };
      merged.exits.targets = {
        ...DEFAULT_CONFIG.exits.targets,
        ...((cfg as any).exits?.targets || {}),
      };
      merged.exits.trailing = {
        ...DEFAULT_CONFIG.exits.trailing,
        ...((cfg as any).exits?.trailing || {}),
      };
      merged.exits.breakeven = {
        ...DEFAULT_CONFIG.exits.breakeven,
        ...((cfg as any).exits?.breakeven || {}),
      };
      merged.guards = { ...DEFAULT_CONFIG.guards, ...((cfg as any).guards || {}) };

      // Normalize targets list
      let list: Target[] = Array.isArray(merged.exits?.targets?.list)
        ? merged.exits.targets.list
        : [];
      list = list
        .filter((t: any) => t && typeof t === "object")
        .map((t: any) => ({
          id: String(t.id || uid()),
          kind: t.kind === "price" || t.kind === "pips" || t.kind === "r" ? t.kind : "r",
          value: typeof t.value === "number" ? t.value : Number(t.value || 0),
          qty_pct: typeof t.qty_pct === "number" ? t.qty_pct : Number(t.qty_pct || 0),
          runner: !!t.runner,
        }));
      if (!list.length) list = [{ id: uid(), kind: "r", value: 1.5, qty_pct: 100, runner: false }];
      merged.exits.targets.list = list;

      setConfig(merged);
      setAdvancedJson(prettyJson(merged));
    } catch (e: any) {
      setErr(e?.message || String(e));
    } finally {
      setLoading(false);
    }
  }, []);

  React.useEffect(() => {
    load();
  }, [load]);

  const isDirty = React.useMemo(() => {
    if (!serverState) return true;
    const stCfg = serverState.config && typeof serverState.config === "object" ? serverState.config : {};
    const stType = (serverState.strategy_type as StrategyId) || "indicator";
    return stType !== active || !deepEqualJson({ ...DEFAULT_CONFIG, ...stCfg }, config);
  }, [serverState, active, config]);

  const targetSummary = React.useMemo(() => {
    const list: Target[] = config?.exits?.targets?.list || [];
    const pctSum = list.reduce((a, t) => a + (Number.isFinite(t.qty_pct) ? t.qty_pct : 0), 0);
    const hasRunner = list.some((t) => !!t.runner);
    const mode = config?.exits?.targets?.mode;
    return { pctSum: round2(pctSum), hasRunner, mode, count: list.length };
  }, [config]);

  const validation = React.useMemo(() => {
    const issues: string[] = [];
    const list: Target[] = config?.exits?.targets?.list || [];
    const mode: string = config?.exits?.targets?.mode || "single";

    if (mode === "single") {
      if (list.length !== 1) issues.push("Single TP mode must have exactly 1 target.");
      if (list[0] && list[0].qty_pct !== 100) issues.push("Single TP must be 100% quantity.");
      if (list[0]?.runner) issues.push("Single TP cannot be a runner.");
    }

    if (mode === "two_plus_runner") {
      if (list.length !== 3) issues.push("2 Targets + Runner needs exactly 3 rows (TP1, TP2, Runner).");
      const runnerCount = list.filter((t) => !!t.runner).length;
      if (runnerCount !== 1) issues.push("Runner: exactly one row must be marked as runner.");
      const sum = list.reduce((a, t) => a + (t.qty_pct || 0), 0);
      if (Math.abs(sum - 100) > 0.001) issues.push("Targets quantity % must total 100.");
    }

    if (mode === "multi") {
      if (list.length < 1 || list.length > 3) issues.push("Multi-target supports 1 to 3 targets max.");
      const sum = list.reduce((a, t) => a + (t.qty_pct || 0), 0);
      if (Math.abs(sum - 100) > 0.001) issues.push("Targets quantity % must total 100.");
      const runnerCount = list.filter((t) => !!t.runner).length;
      if (runnerCount > 1) issues.push("Only one runner is allowed.");
    }

    // SL sanity
    const slMode = config?.exits?.sl?.mode;
    const slVal = Number(config?.exits?.sl?.value || 0);
    if (slMode === "pips" && slVal <= 0) issues.push("Stop Loss (pips) must be > 0.");
    if (slMode === "price" && slVal <= 0) issues.push("Stop Loss (price) must be > 0.");
    if (slMode === "atr" && Number(config?.exits?.sl?.atr_mult || 0) <= 0) issues.push("Stop Loss (ATR mult) must be > 0.");

    // trailing sanity
    if (config?.exits?.trailing?.enabled) {
      const k = config.exits.trailing.kind;
      if (k === "step") {
        if (Number(config.exits.trailing.step_pips || 0) <= 0) issues.push("Trailing step pips must be > 0.");
      } else {
        if (Number(config.exits.trailing.atr_mult || 0) <= 0) issues.push("Trailing ATR mult must be > 0.");
      }
    }

    return { ok: issues.length === 0, issues };
  }, [config]);

  const save = async () => {
    setErr(null);
    setSaveBusy(true);
    try {
      if (showAdvanced) {
        try {
          const parsed = JSON.parse(advancedJson || "{}");
          if (parsed && typeof parsed === "object") setConfig(parsed);
        } catch {
          throw new Error("Advanced JSON is invalid. Fix it or disable Advanced editor.");
        }
      }

      if (!validation.ok) throw new Error(validation.issues[0] || "Fix validation issues before saving.");

      const payload = { enabled, strategy_type: active, config };
      const st = await apiJson<BotState>("/strategy/bot/state", { method: "POST", json: payload });
      setServerState(st);
      setEnabled(!!st.enabled);
      setActive((st.strategy_type as StrategyId) || active);
      setConfig(st.config || config);
      setAdvancedJson(prettyJson(st.config || config));
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
      // safety: require saved config to start
      if (!enabled) {
        if (isDirty) throw new Error("Save your changes before starting auto trading.");
        if ((config?.execution?.mode || "paper") === "mt5" && config?.execution?.require_live_ack) {
          const ok = window.confirm(
            "MT5 execution is enabled. If your MT5 is logged into a LIVE account, real orders will be placed.\n\nContinue?"
          );
          if (!ok) return;
        }
      }
      const st = await apiJson<BotState>("/strategy/bot/toggle", { method: "POST", json: { enabled: !enabled } });
      setServerState(st);
      setEnabled(!!st.enabled);
    } catch (e: any) {
      setErr(e?.message || String(e));
    } finally {
      setToggleBusy(false);
    }
  };

  const reset = async () => {
    if (!window.confirm("Reset strategy configuration to defaults?")) return;
    setErr(null);
    setSaveBusy(true);
    try {
      const st = await apiJson<BotState>("/strategy/bot/reset", { method: "POST", json: {} });
      setServerState(st);
      setEnabled(!!st.enabled);
      setActive((st.strategy_type as StrategyId) || "indicator");
      const cfg = st.config || { ...DEFAULT_CONFIG };
      setConfig(cfg);
      setAdvancedJson(prettyJson(cfg));
    } catch (e: any) {
      setErr(e?.message || String(e));
    } finally {
      setSaveBusy(false);
    }
  };

  const setCfg = (patch: any) => setConfig((prev: any) => ({ ...prev, ...patch }));

  const setTargetMode = (mode: "single" | "two_plus_runner" | "multi") => {
    setConfig((prev: any) => {
      const next = { ...prev };
      next.exits = { ...(prev.exits || {}) };
      next.exits.targets = { ...(prev.exits?.targets || {}) };
      next.exits.targets.mode = mode;

      if (mode === "single") {
        next.exits.targets.list = [{ id: uid(), kind: "r", value: 1.5, qty_pct: 100, runner: false }];
      } else if (mode === "two_plus_runner") {
        next.exits.targets.list = [
          { id: uid(), kind: "r", value: 1.0, qty_pct: 50, runner: false },
          { id: uid(), kind: "r", value: 2.0, qty_pct: 30, runner: false },
          { id: uid(), kind: "r", value: 0, qty_pct: 20, runner: true },
        ];
      } else {
        let list: Target[] = Array.isArray(prev.exits?.targets?.list) ? prev.exits.targets.list : [];
        if (!list.length) list = [{ id: uid(), kind: "r", value: 1.5, qty_pct: 100, runner: false }];
        if (list.length > 3) list = list.slice(0, 3);
        next.exits.targets.list = list;
      }
      return next;
    });
  };

  const updateTarget = (id: string, patch: Partial<Target>) => {
    setConfig((prev: any) => {
      const next = { ...prev };
      const list: Target[] = (next?.exits?.targets?.list || []).map((t: Target) =>
        t.id === id ? { ...t, ...patch } : t
      );
      if (patch.runner) {
        for (let i = 0; i < list.length; i++) if (list[i].id !== id) list[i].runner = false;
      }
      next.exits = { ...(next.exits || {}) };
      next.exits.targets = { ...(next.exits.targets || {}) };
      next.exits.targets.list = list;
      return next;
    });
  };

  const addTarget = () => {
    setConfig((prev: any) => {
      const next = { ...prev };
      const mode: string = next?.exits?.targets?.mode || "multi";
      if (mode !== "multi") return next;
      let list: Target[] = Array.isArray(next?.exits?.targets?.list) ? next.exits.targets.list : [];
      if (list.length >= 3) return next;
      list = [...list, { id: uid(), kind: "r", value: 2.0, qty_pct: 0, runner: false }];
      next.exits = { ...(next.exits || {}) };
      next.exits.targets = { ...(next.exits.targets || {}) };
      next.exits.targets.list = list;
      return next;
    });
  };

  const removeTarget = (id: string) => {
    setConfig((prev: any) => {
      const next = { ...prev };
      const mode: string = next?.exits?.targets?.mode || "multi";
      if (mode !== "multi") return next;
      let list: Target[] = Array.isArray(next?.exits?.targets?.list) ? next.exits.targets.list : [];
      if (list.length <= 1) return next;
      list = list.filter((t) => t.id !== id);
      next.exits = { ...(next.exits || {}) };
      next.exits.targets = { ...(next.exits.targets || {}) };
      next.exits.targets.list = list;
      return next;
    });
  };

  const normalizePct = () => {
    setConfig((prev: any) => {
      const next = { ...prev };
      let list: Target[] = Array.isArray(next?.exits?.targets?.list) ? next.exits.targets.list : [];
      const sum = list.reduce((a, t) => a + (t.qty_pct || 0), 0);
      if (sum <= 0) return next;
      list = list.map((t) => ({ ...t, qty_pct: round2((t.qty_pct || 0) * 100 / sum) }));
      const sum2 = list.reduce((a, t) => a + (t.qty_pct || 0), 0);
      const drift = round2(100 - sum2);
      const idx = list.findIndex((t) => !t.runner);
      if (idx >= 0) list[idx].qty_pct = round2(list[idx].qty_pct + drift);

      next.exits = { ...(next.exits || {}) };
      next.exits.targets = { ...(next.exits.targets || {}) };
      next.exits.targets.list = list;
      return next;
    });
  };

  const execBadge = (() => {
    const m: ExecMode = config?.execution?.mode || "paper";
    if (m === "paper") return <Badge tone="slate">EXEC: PAPER</Badge>;
    return <Badge tone="amber">EXEC: MT5 (AUTO demo/live)</Badge>;
  })();

  const renderStrategyFields = () => {
    if (active === "indicator") {
      return (
        <div className="rounded-2xl border border-slate-800/70 bg-slate-950/60 p-3">
          <SectionTitle title="Indicator entry" hint="Trend gates + pullback confirmation." />
          <div className="mt-3 grid gap-3">
            <Field label="Side mode">
              <Select
                value={config.entry?.side_mode || "follow"}
                onChange={(e) => setCfg({ entry: { ...(config.entry || {}), side_mode: e.target.value } })}
              >
                <option value="follow">Follow signal</option>
                <option value="force_buy">Force BUY</option>
                <option value="force_sell">Force SELL</option>
              </Select>
            </Field>
            <Switch
              value={!!config.entry?.confirm_pullback}
              onChange={(v: boolean) => setCfg({ entry: { ...(config.entry || {}), confirm_pullback: v } })}
              label="Confirm pullback before entry"
              sub="Recommended. Prevents chasing; waits for pullback then reversal confirmation."
            />
            <Field label="Pullback zone">
              <Select
                value={config.entry?.pullback?.zone || "vwap"}
                onChange={(e) =>
                  setCfg({
                    entry: {
                      ...(config.entry || {}),
                      pullback: { ...(config.entry?.pullback || {}), zone: e.target.value },
                    },
                  })
                }
              >
                <option value="vwap">VWAP</option>
                <option value="ema20">EMA20</option>
                <option value="ema50">EMA50</option>
                <option value="sr">Support/Resistance</option>
              </Select>
            </Field>
            <Field label="Max retrace %" hint="0.2–1.0 (fraction)">
              <Input
                type="number"
                step="0.05"
                min={0.1}
                max={2}
                value={config.entry?.pullback?.max_retrace_pct ?? 0.8}
                onChange={(e) =>
                  setCfg({
                    entry: {
                      ...(config.entry || {}),
                      pullback: {
                        ...(config.entry?.pullback || {}),
                        max_retrace_pct: clamp(Number(e.target.value || 0), 0.1, 2),
                      },
                    },
                  })
                }
              />
            </Field>
            <Field label="Reversal confirmation">
              <Select
                value={config.entry?.pullback?.reversal || "close_reclaim"}
                onChange={(e) =>
                  setCfg({
                    entry: {
                      ...(config.entry || {}),
                      pullback: { ...(config.entry?.pullback || {}), reversal: e.target.value },
                    },
                  })
                }
              >
                <option value="close_reclaim">Close reclaim</option>
                <option value="engulfing">Engulfing candle</option>
                <option value="break_swing">Break swing</option>
              </Select>
            </Field>
          </div>
        </div>
      );
    }

    if (active === "priceAction") {
      return (
        <div className="rounded-2xl border border-slate-800/70 bg-slate-950/60 p-3">
          <SectionTitle title="Price action entry" hint="SR zone + candle confirmation." />
          <div className="mt-3 grid gap-3">
            <Switch
              value={!!config.price_action?.require_close_confirm}
              onChange={(v: boolean) =>
                setCfg({ price_action: { ...(config.price_action || {}), require_close_confirm: v } })
              }
              label="Require candle close confirmation"
              sub="Wait for the candle to close back in direction before entry."
            />
            <Field label="Notes" hint="More PA knobs will come after we wire backend execution.">
              <div className="rounded-xl border border-slate-800/70 bg-slate-950/70 px-3 py-2 text-xs text-slate-300">
                This profile is UI-ready. We’ll connect SR/candle conditions in backend next.
              </div>
            </Field>
          </div>
        </div>
      );
    }

    // opportunity
    return (
      <div className="rounded-2xl border border-slate-800/70 bg-slate-950/60 p-3">
        <SectionTitle title="Opportunity entry" hint="Follow XTL opportunities with safety rails." />
        <div className="mt-3 grid gap-3">
          <Field label="Minimum confidence">
            <Select
              value={config.entry?.opportunity?.min_confidence || "medium"}
              onChange={(e) =>
                setCfg({
                  entry: {
                    ...(config.entry || {}),
                    opportunity: { ...(config.entry?.opportunity || {}), min_confidence: e.target.value },
                  },
                })
              }
            >
              <option value="low">Low</option>
              <option value="medium">Medium</option>
              <option value="high">High</option>
            </Select>
          </Field>

          <Field label="Side mode">
            <Select
              value={config.entry?.opportunity?.side_mode || "follow"}
              onChange={(e) =>
                setCfg({
                  entry: {
                    ...(config.entry || {}),
                    opportunity: { ...(config.entry?.opportunity || {}), side_mode: e.target.value },
                  },
                })
              }
            >
              <option value="follow">Follow opportunity direction</option>
              <option value="force_buy">Force BUY</option>
              <option value="force_sell">Force SELL</option>
            </Select>
          </Field>

          <Switch
            value={!!config.entry?.opportunity?.require_same_tf_trend}
            onChange={(v: boolean) =>
              setCfg({
                entry: {
                  ...(config.entry || {}),
                  opportunity: { ...(config.entry?.opportunity || {}), require_same_tf_trend: v },
                },
              })
            }
            label="Require same-TF trend alignment"
            sub="Extra filter to reduce noise."
          />
        </div>
      </div>
    );
  };

  return (
    <div className="mx-auto w-full max-w-6xl px-3 py-6">
      {/* Top bar */}
      <div className="mb-4 flex flex-col gap-3 md:flex-row md:items-end md:justify-between">
        <div>
          <div className="flex items-center gap-2">
            <h1 className="text-xl font-semibold text-slate-50">Strategy Studio</h1>
            {execBadge}
            {enabled ? <Badge tone="emerald">AUTO: ON</Badge> : <Badge tone="rose">AUTO: OFF</Badge>}
            {isDirty ? <Badge tone="sky">UNSAVED</Badge> : <Badge tone="slate">SAVED</Badge>}
          </div>
          <p className="mt-1 text-sm text-slate-400">
            Configure entries, targets, and execution mode. Start auto-trading only after saving.
          </p>
        </div>

        <div className="flex flex-wrap items-center gap-2">
          <button
            type="button"
            onClick={load}
            disabled={loading}
            className={cx(
              "rounded-full border px-3 py-1.5 text-xs font-semibold transition",
              "border-slate-800/80 bg-slate-950/70 text-slate-200 hover:bg-slate-900/70",
              loading && "opacity-60"
            )}
          >
            Refresh
          </button>

          <button
            type="button"
            onClick={reset}
            disabled={saveBusy || loading}
            className={cx(
              "rounded-full border px-3 py-1.5 text-xs font-semibold transition",
              "border-slate-800/80 bg-slate-950/70 text-slate-200 hover:bg-slate-900/70",
              (saveBusy || loading) && "opacity-60"
            )}
          >
            Reset
          </button>

          <button
            type="button"
            onClick={save}
            disabled={saveBusy || loading}
            className={cx(
              "rounded-full px-4 py-1.5 text-xs font-semibold shadow-lg transition",
              "bg-sky-500 text-slate-950 hover:bg-sky-400",
              (saveBusy || loading) && "opacity-60"
            )}
          >
            {saveBusy ? "Saving..." : "Save settings"}
          </button>

          <button
            type="button"
            onClick={toggleBot}
            disabled={toggleBusy || loading}
            className={cx(
              "rounded-full px-4 py-1.5 text-xs font-semibold shadow-lg transition",
              enabled ? "bg-rose-500 text-slate-950 hover:bg-rose-400" : "bg-emerald-500 text-slate-950 hover:bg-emerald-400",
              (toggleBusy || loading) && "opacity-60"
            )}
            title={enabled ? "Stop auto trading" : "Start auto trading"}
          >
            {toggleBusy ? "Working..." : enabled ? "Stop auto trading" : "Start auto trading"}
          </button>
        </div>
      </div>

      {err ? (
        <div className="mb-4 rounded-2xl border border-rose-500/30 bg-rose-500/10 px-4 py-3 text-sm text-rose-100">
          {err}
        </div>
      ) : null}

      {!validation.ok ? (
        <div className="mb-4 rounded-2xl border border-amber-500/30 bg-amber-500/10 px-4 py-3 text-sm text-amber-100">
          <p className="font-semibold">Fix before saving:</p>
          <ul className="mt-1 list-disc pl-5 text-[12px] text-amber-100/90">
            {validation.issues.slice(0, 5).map((x) => (
              <li key={x}>{x}</li>
            ))}
          </ul>
        </div>
      ) : null}

      {/* main grid */}
      <div className="grid gap-4 lg:grid-cols-[340px,1fr]">
        {/* left rail */}
        <div className="space-y-4">
          <div className="rounded-2xl border border-slate-800/80 bg-slate-950/70 p-3 shadow-xl shadow-slate-950/50">
            <SectionTitle title="Strategy profile" hint="Pick a profile. Only its fields will show on the right." />
            <div className="mt-3 grid gap-2">
              {(["indicator", "priceAction", "opportunity"] as StrategyId[]).map((id) => {
                const isActive = active === id;
                return (
                  <button
                    key={id}
                    type="button"
                    onClick={() => setActive(id)}
                    className={cx(
                      "rounded-2xl border px-3 py-3 text-left transition",
                      "border-slate-800/80 bg-slate-950/60 hover:bg-slate-900/60",
                      isActive && "border-sky-500/70 bg-slate-900/70"
                    )}
                  >
                    <div className="flex items-start justify-between gap-2">
                      <div className="min-w-0">
                        <p className="text-sm font-semibold text-slate-50">{STRATEGY_LABELS[id]}</p>
                        <p className="mt-0.5 text-[11px] text-slate-400">{STRATEGY_TAGLINES[id]}</p>
                      </div>
                      {isActive ? <Badge tone="sky">ACTIVE</Badge> : null}
                    </div>
                  </button>
                );
              })}
            </div>
          </div>

          <div className="rounded-2xl border border-slate-800/80 bg-slate-950/70 p-3 shadow-xl shadow-slate-950/50">
            <SectionTitle
              title="Execution mode"
              hint="Paper simulates fills. MT5 executes on whatever account is logged in (demo/live)."
            />
            <div className="mt-3 grid gap-2">
              <button
                type="button"
                onClick={() => setCfg({ execution: { ...(config.execution || {}), mode: "paper" } })}
                className={cx(
                  "rounded-2xl border px-3 py-3 text-left transition",
                  config.execution?.mode === "paper"
                    ? "border-emerald-500/40 bg-emerald-500/10"
                    : "border-slate-800/80 bg-slate-950/60 hover:bg-slate-900/60"
                )}
              >
                <div className="flex items-start justify-between gap-2">
                  <div>
                    <p className="text-xs font-semibold text-slate-100">Paper</p>
                    <p className="mt-0.5 text-[11px] text-slate-400">
                      No MT5 orders. Use this for safe validation and UI testing.
                    </p>
                  </div>
                  <Badge tone={config.execution?.mode === "paper" ? "emerald" : "slate"}>SAFE</Badge>
                </div>
              </button>

              <button
                type="button"
                onClick={() => setCfg({ execution: { ...(config.execution || {}), mode: "mt5" } })}
                className={cx(
                  "rounded-2xl border px-3 py-3 text-left transition",
                  config.execution?.mode === "mt5"
                    ? "border-amber-500/40 bg-amber-500/10"
                    : "border-slate-800/80 bg-slate-950/60 hover:bg-slate-900/60"
                )}
              >
                <div className="flex items-start justify-between gap-2">
                  <div>
                    <p className="text-xs font-semibold text-slate-100">MT5 Execute</p>
                    <p className="mt-0.5 text-[11px] text-slate-400">
                      Orders execute on the MT5 account currently logged in on the worker (demo or live).
                    </p>
                  </div>
                  <Badge tone={config.execution?.mode === "mt5" ? "amber" : "slate"}>RISK</Badge>
                </div>
              </button>

              <Switch
                value={!!config.execution?.require_live_ack}
                onChange={(v: boolean) => setCfg({ execution: { ...(config.execution || {}), require_live_ack: v } })}
                label="Require confirmation before starting MT5 execution"
                sub="Adds a safety confirm dialog when turning on auto trading with MT5."
              />
            </div>
          </div>

          <div className="rounded-2xl border border-slate-800/80 bg-slate-950/70 p-3 shadow-xl shadow-slate-950/50">
            <SectionTitle title="Guards" hint="Avoid stale signals & weekend noise." />
            <div className="mt-3 grid gap-3">
              <Switch
                value={!!config.guards?.disable_weekends}
                onChange={(v: boolean) => setCfg({ guards: { ...(config.guards || {}), disable_weekends: v } })}
                label="Disable on weekends"
                sub="Prevents entries when market is typically closed (broker-dependent)."
              />
              <Switch
                value={!!config.guards?.only_if_recent_bar}
                onChange={(v: boolean) => setCfg({ guards: { ...(config.guards || {}), only_if_recent_bar: v } })}
                label="Only if last closed bar is recent"
                sub="Avoid late opportunities; requires last closed candle to be within stale window."
              />
              <Field label="Stale bar window (seconds)" hint="Typical: 120–300s">
                <Input
                  type="number"
                  value={config.guards?.stale_bar_sec ?? 180}
                  min={30}
                  max={3600}
                  onChange={(e) =>
                    setCfg({
                      guards: {
                        ...(config.guards || {}),
                        stale_bar_sec: clamp(Number((e.target as any).value || 0), 30, 3600),
                      },
                    })
                  }
                />
              </Field>
            </div>
          </div>
        </div>

        {/* right editor */}
        <div className="space-y-4">
          {/* Trade Management */}
          <div className="rounded-2xl border border-slate-800/80 bg-slate-950/70 p-4 shadow-xl shadow-slate-950/50">
            <SectionTitle
              title="Trade management"
              hint="Targets, stop-loss, trailing, and break-even behavior."
              right={
                <div className="flex items-center gap-2">
                  <Badge tone="slate">Targets: {targetSummary.count}</Badge>
                  <Badge tone={Math.abs(targetSummary.pctSum - 100) < 0.01 ? "emerald" : "amber"}>
                    Qty% {targetSummary.pctSum}%
                  </Badge>
                </div>
              }
            />

            <div className="mt-4 grid gap-4 lg:grid-cols-2">
              {/* Stop loss */}
              <div className="rounded-2xl border border-slate-800/70 bg-slate-950/60 p-3">
                <SectionTitle title="Stop Loss" hint="Choose pips/price or ATR-based stop." />
                <div className="mt-3 grid gap-3">
                  <Field label="SL mode">
                    <Select
                      value={config.exits?.sl?.mode || "pips"}
                      onChange={(e) =>
                        setCfg({
                          exits: {
                            ...(config.exits || {}),
                            sl: { ...(config.exits?.sl || {}), mode: (e.target as any).value },
                          },
                        })
                      }
                    >
                      <option value="pips">Pips / points</option>
                      <option value="price">Absolute price</option>
                      <option value="atr">ATR multiple</option>
                    </Select>
                  </Field>

                  {config.exits?.sl?.mode === "pips" ? (
                    <Field label="SL distance (pips)">
                      <Input
                        type="number"
                        value={config.exits?.sl?.value ?? 120}
                        min={1}
                        onChange={(e) =>
                          setCfg({
                            exits: {
                              ...(config.exits || {}),
                              sl: { ...(config.exits?.sl || {}), value: Number((e.target as any).value || 0) },
                            },
                          })
                        }
                      />
                    </Field>
                  ) : null}

                  {config.exits?.sl?.mode === "price" ? (
                    <Field label="SL price">
                      <Input
                        type="number"
                        value={config.exits?.sl?.value ?? 0}
                        min={0}
                        onChange={(e) =>
                          setCfg({
                            exits: {
                              ...(config.exits || {}),
                              sl: { ...(config.exits?.sl || {}), value: Number((e.target as any).value || 0) },
                            },
                          })
                        }
                      />
                    </Field>
                  ) : null}

                  {config.exits?.sl?.mode === "atr" ? (
                    <Field label="ATR multiple">
                      <Input
                        type="number"
                        step="0.1"
                        value={config.exits?.sl?.atr_mult ?? 1.2}
                        min={0.1}
                        onChange={(e) =>
                          setCfg({
                            exits: {
                              ...(config.exits || {}),
                              sl: { ...(config.exits?.sl || {}), atr_mult: Number((e.target as any).value || 0) },
                            },
                          })
                        }
                      />
                    </Field>
                  ) : null}
                </div>
              </div>

              {/* Targets */}
              <div className="rounded-2xl border border-slate-800/70 bg-slate-950/60 p-3">
                <SectionTitle title="Targets" hint="Single TP, 2 targets + runner, or up to 3 targets." />
                <div className="mt-3 grid gap-3">
                  <Field label="Target mode">
                    <Select
                      value={config.exits?.targets?.mode || "single"}
                      onChange={(e) => setTargetMode((e.target as any).value)}
                    >
                      <option value="single">Single</option>
                      <option value="two_plus_runner">2 targets + runner</option>
                      <option value="multi">Multi (1–3)</option>
                    </Select>
                  </Field>

                  <div className="space-y-2">
                    {(config.exits?.targets?.list || []).map((t: Target) => (
                      <div
                        key={t.id}
                        className="grid grid-cols-[1.1fr,1fr,1fr,auto] items-center gap-2 rounded-xl border border-slate-800/70 bg-slate-950/60 p-2"
                      >
                        <Select
                          value={t.kind}
                          onChange={(e) => updateTarget(t.id, { kind: (e.target as any).value as any })}
                        >
                          <option value="r">R</option>
                          <option value="pips">Pips</option>
                          <option value="price">Price</option>
                        </Select>

                        <Input
                          type="number"
                          step="0.1"
                          value={t.value}
                          onChange={(e) => updateTarget(t.id, { value: Number((e.target as any).value || 0) })}
                        />

                        <Input
                          type="number"
                          step="1"
                          value={t.qty_pct}
                          onChange={(e) => updateTarget(t.id, { qty_pct: clamp(Number((e.target as any).value || 0), 0, 100) })}
                        />

                        <div className="flex items-center gap-2">
                          <label className="flex items-center gap-1 text-[10px] text-slate-300">
                            <input
                              type="checkbox"
                              checked={!!t.runner}
                              onChange={(e) => updateTarget(t.id, { runner: (e.target as any).checked })}
                            />
                            Runner
                          </label>
                          {config.exits?.targets?.mode === "multi" ? (
                            <button
                              type="button"
                              onClick={() => removeTarget(t.id)}
                              className="rounded-lg border border-slate-800/70 bg-slate-950/60 px-2 py-1 text-[10px] text-slate-200 hover:bg-slate-900/60"
                              title="Remove"
                            >
                              -
                            </button>
                          ) : null}
                        </div>
                      </div>
                    ))}
                  </div>

                  <div className="flex flex-wrap items-center gap-2">
                    {config.exits?.targets?.mode === "multi" ? (
                      <button
                        type="button"
                        onClick={addTarget}
                        className="rounded-xl border border-slate-800/70 bg-slate-950/60 px-3 py-2 text-xs font-semibold text-slate-200 hover:bg-slate-900/60"
                      >
                        Add target
                      </button>
                    ) : null}

                    <button
                      type="button"
                      onClick={normalizePct}
                      className="rounded-xl border border-slate-800/70 bg-slate-950/60 px-3 py-2 text-xs font-semibold text-slate-200 hover:bg-slate-900/60"
                      title="Normalize target qty% to 100"
                    >
                      Normalize %
                    </button>
                  </div>
                </div>
              </div>
            </div>

            {/* Trailing + breakeven */}
            <div className="mt-4 grid gap-4 lg:grid-cols-2">
              <div className="rounded-2xl border border-slate-800/70 bg-slate-950/60 p-3">
                <SectionTitle title="Trailing stop" hint="Lock profit as price moves. Recommended with runner." />
                <div className="mt-3 grid gap-3">
                  <Switch
                    value={!!config.exits?.trailing?.enabled}
                    onChange={(v: boolean) =>
                      setCfg({ exits: { ...(config.exits || {}), trailing: { ...(config.exits?.trailing || {}), enabled: v } } })
                    }
                    label="Enable trailing"
                    sub="If enabled, SL will trail once trade moves in your favor."
                  />
                  <Field label="Trailing kind">
                    <Select
                      value={config.exits?.trailing?.kind || "step"}
                      onChange={(e) =>
                        setCfg({
                          exits: {
                            ...(config.exits || {}),
                            trailing: { ...(config.exits?.trailing || {}), kind: (e.target as any).value },
                          },
                        })
                      }
                    >
                      <option value="step">Step</option>
                      <option value="atr">ATR</option>
                    </Select>
                  </Field>

                  {(config.exits?.trailing?.kind || "step") === "step" ? (
                    <>
                      <Field label="Step pips">
                        <Input
                          type="number"
                          value={config.exits?.trailing?.step_pips ?? 80}
                          min={1}
                          onChange={(e) =>
                            setCfg({
                              exits: {
                                ...(config.exits || {}),
                                trailing: { ...(config.exits?.trailing || {}), step_pips: Number((e.target as any).value || 0) },
                              },
                            })
                          }
                        />
                      </Field>
                      <Field label="Lock pips">
                        <Input
                          type="number"
                          value={config.exits?.trailing?.step_lock_pips ?? 40}
                          min={0}
                          onChange={(e) =>
                            setCfg({
                              exits: {
                                ...(config.exits || {}),
                                trailing: { ...(config.exits?.trailing || {}), step_lock_pips: Number((e.target as any).value || 0) },
                              },
                            })
                          }
                        />
                      </Field>
                    </>
                  ) : (
                    <Field label="ATR multiple">
                      <Input
                        type="number"
                        step="0.1"
                        value={config.exits?.trailing?.atr_mult ?? 1.0}
                        min={0.1}
                        onChange={(e) =>
                          setCfg({
                            exits: {
                              ...(config.exits || {}),
                              trailing: { ...(config.exits?.trailing || {}), atr_mult: Number((e.target as any).value || 0) },
                            },
                          })
                        }
                      />
                    </Field>
                  )}

                  <Field label="Activate after R" hint="Example: 1.0 means after 1R in profit">
                    <Input
                      type="number"
                      step="0.1"
                      value={config.exits?.trailing?.activate_after_r ?? 1.0}
                      min={0}
                      onChange={(e) =>
                        setCfg({
                          exits: {
                            ...(config.exits || {}),
                            trailing: {
                              ...(config.exits?.trailing || {}),
                              activate_after_r: Number((e.target as any).value || 0),
                            },
                          },
                        })
                      }
                    />
                  </Field>
                </div>
              </div>

              <div className="rounded-2xl border border-slate-800/70 bg-slate-950/60 p-3">
                <SectionTitle title="Break-even" hint="Optional auto move SL to entry after first milestone." />
                <div className="mt-3 grid gap-3">
                  <Switch
                    value={!!config.exits?.breakeven?.enabled}
                    onChange={(v: boolean) =>
                      setCfg({ exits: { ...(config.exits || {}), breakeven: { ...(config.exits?.breakeven || {}), enabled: v } } })
                    }
                    label="Enable break-even"
                    sub="Moves SL to entry (plus buffer) after reaching the selected R."
                  />
                  <Field label="At R">
                    <Input
                      type="number"
                      step="0.1"
                      value={config.exits?.breakeven?.at_r ?? 1.0}
                      min={0}
                      onChange={(e) =>
                        setCfg({
                          exits: {
                            ...(config.exits || {}),
                            breakeven: { ...(config.exits?.breakeven || {}), at_r: Number((e.target as any).value || 0) },
                          },
                        })
                      }
                    />
                  </Field>
                  <Field label="Buffer pips">
                    <Input
                      type="number"
                      value={config.exits?.breakeven?.buffer_pips ?? 10}
                      min={0}
                      onChange={(e) =>
                        setCfg({
                          exits: {
                            ...(config.exits || {}),
                            breakeven: {
                              ...(config.exits?.breakeven || {}),
                              buffer_pips: Number((e.target as any).value || 0),
                            },
                          },
                        })
                      }
                    />
                  </Field>
                </div>
              </div>
            </div>
          </div>

          {/* Strategy-specific fields (ONLY the selected strategy shows) */}
          <div className="rounded-2xl border border-slate-800/80 bg-slate-950/70 p-4 shadow-xl shadow-slate-950/50">
            <SectionTitle title="Entry rules" hint="Fields here change based on selected strategy." />
            <div className="mt-4">{renderStrategyFields()}</div>
          </div>

          {/* Risk sizing (shared) */}
          <div className="rounded-2xl border border-slate-800/80 bg-slate-950/70 p-4 shadow-xl shadow-slate-950/50">
            <SectionTitle title="Sizing" hint="Keep it simple. Start with Qty." />
            <div className="mt-4 grid gap-3 lg:grid-cols-2">
              <Field label="Risk mode">
                <Select
                  value={config.risk?.risk_mode || "qty"}
                  onChange={(e) => setCfg({ risk: { ...(config.risk || {}), risk_mode: (e.target as any).value } })}
                >
                  <option value="qty">Fixed Qty</option>
                  <option value="risk_pct">% Equity (later)</option>
                </Select>
              </Field>

              <Field label="Qty">
                <Input
                  type="number"
                  value={config.risk?.qty ?? 1}
                  min={0}
                  onChange={(e) => setCfg({ risk: { ...(config.risk || {}), qty: Number((e.target as any).value || 0) } })}
                />
              </Field>

              <Field label="Max open positions">
                <Input
                  type="number"
                  value={config.risk?.max_positions ?? 1}
                  min={1}
                  max={20}
                  onChange={(e) =>
                    setCfg({ risk: { ...(config.risk || {}), max_positions: clamp(Number((e.target as any).value || 1), 1, 20) } })
                  }
                />
              </Field>
            </div>
          </div>

          {/* Advanced */}
          <div className="rounded-2xl border border-slate-800/80 bg-slate-950/70 p-4 shadow-xl shadow-slate-950/50">
            <SectionTitle
              title="Advanced"
              hint="Optional raw JSON editor (power users)."
              right={
                <Switch
                  value={showAdvanced}
                  onChange={(v: boolean) => setShowAdvanced(v)}
                  label={showAdvanced ? "On" : "Off"}
                />
              }
            />
            {showAdvanced ? (
              <div className="mt-3">
                <textarea
                  className="h-64 w-full rounded-2xl border border-slate-800/80 bg-slate-950/60 p-3 font-mono text-[12px] text-slate-100 outline-none focus:border-sky-500/70 focus:ring-2 focus:ring-sky-500/15"
                  value={advancedJson}
                  onChange={(e) => setAdvancedJson((e.target as any).value)}
                />
                <p className="mt-2 text-[11px] text-slate-500">
                  Tip: Save will validate targets & SL fields. Invalid JSON will block saving.
                </p>
              </div>
            ) : (
              <p className="mt-2 text-[11px] text-slate-500">
                Keep Advanced off unless you know what you’re doing.
              </p>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
