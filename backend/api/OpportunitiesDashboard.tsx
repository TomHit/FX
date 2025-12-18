import React from "react";

type LivePrice = { price: number; lastTs: number };

function useLivePrices(refreshMs: number = 30_000) {
  const [prices, setPrices] = React.useState<Record<string, LivePrice>>({});
  const [updatedAt, setUpdatedAt] = React.useState<number | null>(null);

  const refetch = React.useCallback(async () => {
    try {
      const url = `/_api/trend/price/all?tf=M1&_=${Date.now()}`;
      const res = await fetch(url, { credentials: "include", cache: "no-store" });
      if (!res.ok) return;
      const js = await res.json();
      const map: Record<string, LivePrice> = {};
      if (Array.isArray(js?.rows)) {
        for (const r of js.rows) {
          if (r?.symbol && typeof r?.price === "number" && typeof r?.lastTs === "number") {
            map[String(r.symbol).toUpperCase()] = { price: r.price, lastTs: r.lastTs };
          }
        }
      }
      setPrices(map);
      setUpdatedAt(Date.now());
    } catch {
      // ignore
    }
  }, []);

  React.useEffect(() => {
    let t: number | null = null;
    const tick = async () => {
      await refetch();
      t = window.setTimeout(tick, refreshMs);
    };
    void tick();

    const onVis = () => { if (document.visibilityState === "visible") void refetch(); };
    const onFocus = () => void refetch();
    window.addEventListener("focus", onFocus);
    document.addEventListener("visibilitychange", onVis);

    return () => {
      if (t) window.clearTimeout(t);
      window.removeEventListener("focus", onFocus);
      document.removeEventListener("visibilitychange", onVis);
    };
  }, [refetch, refreshMs]);

  return { prices, updatedAt, refetch };
}


/**
 * OpportunitiesDashboard.tsx
 *
 * Live H1 opportunities + recent alert history.
 * Uses /trend/opportunities as the single backend source.
 */

type Direction = "up" | "down" | "flat";

/**
 * Raw API row coming from /trend/opportunities
 */

type ApiRow = {
  symbol: string;

  // Core opportunity fields
  opp_direction?: string;              // "UP" | "DOWN"
  direction?: string;                  // optional, for convenience
  opp_expected_move_pct_1h?: number;   // frozen expected move %
  expected_move_pct_1h?: number;       // legacy / alt field
  alert_price_1h?: number;             // frozen entry / basis
  basis_price_1h?: number;             // basis used for target calc
  target_price_1h?: number;            // frozen target
  alert_created_ms?: number;           // when this alert was created
  horizon_min?: number;                // usually 60
  last_status_ms?: number;             // last status update
  status?: string | null;              // "active" | "hit" | "expired" | ...

  // Prob / score
  prob_up?: number | null;
  p_up?: number | null;                // legacy prob field
  opp_score?: number | null;
  opp_confidence?: string | null;      // "high" | "medium" | ...

  // Overall nearest SR (fallback)
  sr_side?: string | null;             // "support" | "resistance" | ...
  sr_dist_pct?: number | null;         // distance in %

  // Per-timeframe SR (for richer reasons)
  sr_h1_side?: string | null;
  sr_h1_dist_pct?: number | null;
  sr_h1_level?: number | null;
  sr_h4_side?: string | null;
  sr_h4_dist_pct?: number | null;
  sr_h4_level?: number | null;

  // Text reasons from backend
  reasons?: string[] | string;
  opp_reason?: string | null;

  // Timestamps / device
  updated_broker_ms?: number | null;
  server_now_ms?: number;
  using_device?: string | null;

  // Misc / legacy fields
  decision?: string;                   // "BUY" | "SELL" | "ABSTAIN"
  opp_delta_pct?: number | null;
  opp_delta_thr?: number | null;

  // Strategy-side signal (optional; may be absent)
  signal?: string | null;             // e.g., "BUY", "SELL", "BUY@1234.5"
  signal_text?: string | null;        // UI-friendly text
  signal_price?: number | null;
  signal_ts_ms?: number | null;
};


/**
 * Raw history item from /trend/opportunities ("history" array).
 */
type ApiHistoryRow = {
  alert_time_ms?: number;
  symbol?: string;
  direction?: string;            // "UP" / "DOWN"
  decision?: string;             // "BUY" / "SELL" / "ABSTAIN"
  horizon_min?: number;
  expected_move_pct?: number;
  hit_target?: boolean | null;
  realized_move_pct?: number | null;
  max_drawdown_pct?: number | null;
  time_to_target_min?: number | null;
};

/**
 * Normalised row for UI.
 */
type OppRow = {
  symbol: string;
  direction: Direction;
  movePct: number;
  absMovePct: number;
  basisPrice: number | null;
  targetPrice: number | null;
  probUp: number | null;
  oppScore: number | null;
  oppConfidence: string | null;
  reasons: string[];
  updatedBrokerMs: number | null;
  device: string | null;
  alertTimeMs: number | null;
  horizonMin: number | null;
  srSide: string | null;
  srDistPct: number | null;
  srLabel: string | null;
  status?: "active" | "hit" | "expired" | string | null;

  signalText?: string | null;
  signalPrice?: number | null;
  signalTsMs?: number | null;
};

/**
 * Normalised history row.
 */
type HistoryRow = {
  alertTimeMs: number;
  symbol: string;
  direction: Direction;
  horizonMin: number;
  expectedMovePct: number;
  status?: "hit" | "expired" | "active" | string | null;
  hitTarget?: boolean | null;
  realizedMovePct?: number | null;
  maxDrawdownPct?: number | null;
  timeToTargetMin?: number | null;
};

const API_BASE =
  (typeof window !== "undefined" &&
    (window as any).__PUBLIC_API_BASE__) ||
  "/_api";

/* --------------------------
 * Helpers
 * -------------------------- */

function fmtPrice(sym: string, px: number | null | undefined): string {
  if (!Number.isFinite(px as any)) return "—";
  const s = sym.toUpperCase();
  const d = s === "XAUUSD" ? 2 : s.endsWith("JPY") ? 3 : 5;
  return (px as number).toFixed(d);
}

function fmtTime(ts: number | null): string {
  if (!ts) return "";
  try {
    return new Date(ts).toLocaleString(undefined, {
      year: "2-digit",
      month: "short",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
    });
  } catch {
    return "";
  }
}

function fmtDirectionLabel(direction: Direction): string {
  if (direction === "up") return "Bullish";
  if (direction === "down") return "Bearish";
  return "Flat";
}

function deriveDirection(raw: string | undefined | null): Direction {
  const v = (raw || "").toUpperCase();
  if (v === "BUY" || v === "UP" || v === "LONG") return "up";
  if (v === "SELL" || v === "DOWN" || v === "SHORT") return "down";
  return "flat";
}

/* --------------------------
 * Normalisers
 * -------------------------- */
function mapApiRow(r: ApiRow): OppRow | null {
  if (!r?.symbol) return null;

  const direction = deriveDirection(
    r.opp_direction || r.direction || r.decision
  );

  const movePctRaw =
    r.opp_expected_move_pct_1h ??
    r.expected_move_pct_1h ??
    null;
  const movePct = typeof movePctRaw === "number" ? movePctRaw : 0;
  const absMovePct = Math.abs(movePct);

  const basisPrice =
    r.alert_price_1h ??
    r.basis_price_1h ??
    null;

  const targetPrice = r.target_price_1h ?? null;

  const probUp = r.prob_up ?? r.p_up ?? null;
  const oppScore = r.opp_score ?? null;
  const oppConfidence = r.opp_confidence ?? null;

  // -------------------- reasons base --------------------
  const reasons: string[] = [];

  if (Array.isArray(r.reasons)) {
    for (const x of r.reasons) {
      if (!x) continue;
      reasons.push(String(x));
    }
  } else if (typeof r.reasons === "string" && r.reasons.trim()) {
    reasons.push(r.reasons.trim());
  }
  if (r.opp_reason && r.opp_reason.trim()) {
    reasons.push(r.opp_reason.trim());
  }

  // -------------------- SR reasons (H1 / H4) --------------------
  const srBits: string[] = [];

  const normalizeSide = (side?: string | null) => {
    if (!side) return null;
    const s = side.toLowerCase();
    if (s === "support") return "support";
    if (s === "resistance") return "resistance";
    return side;
  };

  // H1 SR
  if (
    r.sr_h1_side &&
    typeof r.sr_h1_level === "number" &&
    typeof r.sr_h1_dist_pct === "number"
  ) {
    const side = normalizeSide(r.sr_h1_side) ?? r.sr_h1_side;
    const lvlStr = fmtPrice(r.symbol, r.sr_h1_level);
    srBits.push(
      `H1 ${side} @ ${lvlStr} (${Math.abs(r.sr_h1_dist_pct).toFixed(2)}%)`
    );
  }

  // H4 SR
  if (
    r.sr_h4_side &&
    typeof r.sr_h4_level === "number" &&
    typeof r.sr_h4_dist_pct === "number"
  ) {
    const side = normalizeSide(r.sr_h4_side) ?? r.sr_h4_side;
    const lvlStr = fmtPrice(r.symbol, r.sr_h4_level);
    srBits.push(
      `H4 ${side} @ ${lvlStr} (${Math.abs(r.sr_h4_dist_pct).toFixed(2)}%)`
    );
  }

  // Fallback to overall nearest SR if per-TF not available
  if (
    srBits.length === 0 &&
    r.sr_side &&
    typeof r.sr_dist_pct === "number"
  ) {
    const side = normalizeSide(r.sr_side) ?? r.sr_side;
    srBits.push(
      `SR ${side} (${Math.abs(r.sr_dist_pct).toFixed(2)}%)`
    );
  }

  if (srBits.length > 0) {
    reasons.push(...srBits);
  }

  // -------------------- de-duplicate reasons --------------------
  const seen = new Set<string>();
  const reasonsUnique = reasons.filter((txt) => {
    const key = txt.toLowerCase();
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });

  // -------------------- SR badge (overall) --------------------
  const rawSide = normalizeSide(r.sr_side);
  const rawDist =
    typeof r.sr_dist_pct === "number" ? Math.abs(r.sr_dist_pct) : null;

  let srSide: string | null = null;
  let srDistPct: number | null = null;
  let srLabel: string | null = null;

  if (rawSide) srSide = rawSide;
  if (rawDist != null) srDistPct = rawDist;

  if (srSide && srDistPct != null) {
    const sidePretty =
      srSide === "support"
        ? "Support"
        : srSide === "resistance"
        ? "Resistance"
        : srSide.charAt(0).toUpperCase() + srSide.slice(1);

    srLabel = `${sidePretty} ~${srDistPct.toFixed(2)}% away`;
  }

  // -------------------- timestamps / misc --------------------
  const updatedBrokerMs =
    (typeof r.updated_broker_ms === "number" ? r.updated_broker_ms : undefined) ??
    (typeof r.server_now_ms === "number" ? r.server_now_ms : undefined) ??
    null;

  const device = r.using_device ?? null;
  const alertTimeMs = r.alert_created_ms ?? null;
  const horizonMin =
    typeof r.horizon_min === "number" ? r.horizon_min : 60; // default H1
  const rawStatus = (r as any).status;
  const normStatus =
    typeof rawStatus === "string" ? rawStatus.toLowerCase() : rawStatus;

  const signalText =
    (typeof (r as any).signal_text === "string" && (r as any).signal_text.trim())
      ? (r as any).signal_text.trim()
      : (typeof (r as any).signal === "string" && (r as any).signal.trim())
        ? (r as any).signal.trim()
        : null;

  const signalPrice =
    typeof (r as any).signal_price === "number"
      ? (r as any).signal_price
      : null;

  const signalTsMs =
    typeof (r as any).signal_ts_ms === "number"
      ? (r as any).signal_ts_ms
      : null;

  return {
    symbol: r.symbol,
    direction,
    movePct,
    absMovePct,
    basisPrice,
    targetPrice,
    probUp,
    oppScore,
    oppConfidence,
    reasons: reasonsUnique,
    updatedBrokerMs,
    device,
    alertTimeMs,
    horizonMin,
    srSide,
    srDistPct,
    srLabel,
    status: normStatus ?? null,
    signalText,
    signalPrice,
    signalTsMs,

  };
}


function mapHistoryRow(h: ApiHistoryRow): HistoryRow | null {
  const symbol = (h.symbol || "").toUpperCase();
  if (!symbol) return null;

  const dir = deriveDirection(h.direction || h.decision);

  const alertTimeMs = (h as any).alert_time_ms ?? (h as any).alert_created_ms;
  if (!alertTimeMs) return null;

  const horizonMin = typeof h.horizon_min === "number" ? h.horizon_min : 60;
  const expectedMovePct =
    typeof h.expected_move_pct === "number" ? h.expected_move_pct : 0;

  return {
    symbol,
    direction: dir,
    alertTimeMs,
    horizonMin,
    expectedMovePct,
    status: h.hit_target === true
      ? "hit"
      : h.hit_target === false
        ? "expired"
        : "active",

    hitTarget: h.hit_target,
    realizedMovePct: h.realized_move_pct,
    maxDrawdownPct: h.max_drawdown_pct,
    timeToTargetMin: h.time_to_target_min,
  };
}

/* --------------------------
 * Data hook
 * -------------------------- */

type ApiResponse = {
  ok: boolean;
  tf: string;
  rows: ApiRow[];
  history?: ApiHistoryRow[];
};

function useOpportunities() {
  const [rows, setRows] = React.useState<OppRow[]>([]);
  const { prices: livePrices } = useLivePrices(30_000);
  const [history, setHistory] = React.useState<HistoryRow[]>([]);
  const [lastAt, setLastAt] = React.useState<number | null>(null);
  const [error, setError] = React.useState<string | null>(null);

  // Keep first-seen opportunity per symbol frozen for this page session.
  // We show frozen items until their H1 horizon is over (~60 minutes),
  // even if backend temporarily returns rows: [].
  const frozenRef = React.useRef<Map<string, OppRow>>(new Map());

  // Optional: optimistic completions (used only if a frozen item expires client-side
  // before backend returns it in history; backend should normally be the source of truth).
  // Note: we no longer expire client-side; completion should come from backend status/history.

  const CACHE_ROWS_KEY = "xtl_opp_rows_cache_v1";
  const CACHE_HIST_KEY = "xtl_opp_history_cache_v1";
  const CACHE_AT_KEY = "xtl_opp_lastAt_cache_v1";

  async function fetchOnce() {
    const now = Date.now();

    try {
      setError(null);

      const res = await fetch(`${API_BASE}/trend/opportunities?tf=H1`, {
        credentials: "include",
      });

      if (!res.ok) {
        throw new Error("HTTP " + res.status);
      }

      const js: ApiResponse = await res.json();
      if (!js.ok) {
        throw new Error(
          (js as any).reason || "Backend reported error in /trend/opportunities"
        );
      }

      const mappedRows =
        (js.rows || [])
          .map(mapApiRow)
          .filter((x): x is OppRow => x !== null)
          .sort((a, b) => {
            if (b.absMovePct !== a.absMovePct) return b.absMovePct - a.absMovePct;
            const sa = a.oppScore ?? 0;
            const sb = b.oppScore ?? 0;
            return sb - sa;
          });

      const mappedHistory =
        (js.history || [])
          .map(mapHistoryRow)
          .filter((x): x is HistoryRow => x !== null)
          .sort((a, b) => b.alertTimeMs - a.alertTimeMs);

      // Completed alerts keyed by (symbol + alertTimeMs), NOT just symbol.
      const completedKeys = new Set(
        mappedHistory
          .filter((h) => h.hitTarget === true || h.hitTarget === false)
          .map((h) => `${h.symbol}:${h.alertTimeMs ?? 0}`)
      );

      const rowKey = (r: OppRow) => `${r.symbol}:${r.alertTimeMs ?? 0}`;

      // 1) Update / insert frozen snapshots from freshly mapped rows
      for (const r of mappedRows) {
        // If backend says it's already done, don't keep it in live.
        if (r.status && r.status !== "active") {
          frozenRef.current.delete(r.symbol);
          continue;
        }

        // Only remove if THIS SAME alert instance is completed
        if (completedKeys.has(rowKey(r))) {
          frozenRef.current.delete(r.symbol);
          continue;
        }

        const prev = frozenRef.current.get(r.symbol);
        if (prev) {
          const alertTimeMs = prev.alertTimeMs ?? r.alertTimeMs ?? now;
          frozenRef.current.set(r.symbol, { ...prev, ...r, alertTimeMs });
        } else {
          const alertTimeMs = r.alertTimeMs ?? now;
          frozenRef.current.set(r.symbol, { ...r, alertTimeMs });
        }
      }

      // 2) Prune only those already completed in backend history.
      for (const [sym, existing] of Array.from(frozenRef.current.entries())) {
        if (completedKeys.has(`${sym}:${existing.alertTimeMs ?? 0}`)) {
          frozenRef.current.delete(sym);
        }
      }

      // 3) Final rows = all frozen snapshots, sorted
      const frozenRows = Array.from(frozenRef.current.values()).sort((a, b) => {
        if (b.absMovePct !== a.absMovePct) return b.absMovePct - a.absMovePct;
        const sa = a.oppScore ?? 0;
        const sb = b.oppScore ?? 0;
        return sb - sa;
      });

      setRows(frozenRows);

      // History = backend truth.
      setHistory(mappedHistory);
      setLastAt(now);

      // Persist lightweight cache so navigating away/back doesn't look "blank"
      try {
        sessionStorage.setItem(CACHE_ROWS_KEY, JSON.stringify(frozenRows));
        sessionStorage.setItem(CACHE_HIST_KEY, JSON.stringify(mappedHistory));
        sessionStorage.setItem(CACHE_AT_KEY, String(now));
      } catch {
        // ignore storage failures
      }
    } catch (e: any) {
      console.error("[OppDashboard] fetch error", e);
      setError(e?.message || String(e));
    }
  }
  React.useEffect(() => {
    // Hydrate cached state immediately (helps when navigating away/back)
    try {
      const rawRows = sessionStorage.getItem(CACHE_ROWS_KEY);
      const rawHist = sessionStorage.getItem(CACHE_HIST_KEY);
      const rawAt = sessionStorage.getItem(CACHE_AT_KEY);

      if (rawRows) {
        const rr = JSON.parse(rawRows) as OppRow[];
        if (Array.isArray(rr) && rr.length) {
          setRows(rr);
          frozenRef.current = new Map(rr.map((r) => [r.symbol, r]));
        }
      }
      if (rawHist) {
        const hh = JSON.parse(rawHist) as HistoryRow[];
        if (Array.isArray(hh) && hh.length) setHistory(hh);
      }
      if (rawAt) {
        const n = Number(rawAt);
        if (Number.isFinite(n) && n > 0) setLastAt(n);
      }
    } catch {
      // ignore
    }

    void fetchOnce();
    const id = window.setInterval(() => void fetchOnce(), 15_000);
    return () => window.clearInterval(id);
  }, []);

  return { rows, history, lastAt, error, refetch: fetchOnce };
}

/* --------------------------
 * Small UI primitives
 * -------------------------- */

const Card: React.FC<React.HTMLAttributes<HTMLDivElement>> = ({
  className = "",
  children,
  ...rest
}) => (
  <div
    className={
       "rounded-2xl border border-slate-800/70 bg-gradient-to-b from-slate-950/90 via-slate-900/90 to-slate-950/95 " +
       "shadow-[0_18px_40px_rgba(0,0,0,0.55)] backdrop-blur-sm " +
       className
    }

    {...rest}
  >
    {children}
  </div>
);

const Pill: React.FC<{
  color: "up" | "down" | "flat";
  children: React.ReactNode;
}> = ({ color, children }) => {
  let base =
    "inline-flex items-center gap-1 rounded-full px-2.5 py-0.5 text-[11px] font-medium border";
  if (color === "up") {
    base += " border-emerald-400/60 bg-emerald-500/10 text-emerald-200";
  } else if (color === "down") {
    base += " border-rose-400/60 bg-rose-500/10 text-rose-200";
  } else {
    base += " border-slate-500/60 bg-slate-700/40 text-slate-200";
  }
  return <span className={base}>{children}</span>;
};

/* --------------------------
 * Main component
 * -------------------------- */

function OpportunitiesDashboard() {
  const { rows, history, lastAt, error, refetch } = useOpportunities();
  const livePrices = useLivePrices(30_000);
  const lastUpdatedLabel = lastAt ? fmtTime(lastAt) : "—";

  return (
    <div className="mx-auto flex max-w-6xl flex-col gap-5 px-3 pb-10 pt-4">
      {/* Page header */}
      <div className="rounded-2xl border border-slate-800/80 bg-gradient-to-r from-slate-950/95 via-slate-900/95 to-slate-950/95 px-5 py-4 shadow-[0_18px_40px_rgba(0,0,0,0.75)]">
        <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
          <div>
            <div className="flex items-center gap-2">
              <h1 className="text-xl font-semibold text-slate-50">
                Live Opportunities
              </h1>
              {rows.length > 0 && (
                <span className="rounded-full bg-emerald-500/10 px-2 py-0.5 text-[11px] font-medium text-emerald-300">
                  {rows.length} active
                </span>
              )}
            </div>
            <p className="mt-1 max-w-xl text-xs text-slate-400">
              High-conviction H1 moves filtered by room, trend, structure, SR,
              volatility and macro tilt. When a rare setup appears, it will
              show up here first.
            </p>
          </div>

          <div className="flex items-end gap-4">
            <div className="text-[11px] leading-tight text-slate-400">
              <div className="text-slate-500">Last updated</div>
              <div className="font-medium text-slate-100">
                {lastUpdatedLabel}
              </div>
            </div>
            <button
              type="button"
              onClick={() => void refetch()}
              className="inline-flex items-center gap-1 rounded-full border border-slate-500/70 bg-slate-900/80 px-3 py-1.5 text-xs font-medium text-slate-50 shadow-sm shadow-black/40 hover:border-slate-300 hover:bg-slate-800 active:scale-[0.97]"
            >
              <span className="h-1.5 w-1.5 rounded-full bg-emerald-400 shadow-[0_0_0_4px_rgba(34,197,94,0.45)]" />
              Refresh
            </button>
          </div>
        </div>
      </div>

      {error && (
        <div className="rounded-2xl border border-rose-700/80 bg-rose-950/80 px-4 py-2 text-xs text-rose-100 shadow-md shadow-black/50">
          Error: {error}
        </div>
      )}

      {/* Live opportunities */}
      <Card>
        <div className="flex items-center justify-between border-b border-slate-800/80 px-4 py-3">
          <div>
            <div className="text-[11px] font-semibold uppercase tracking-[0.12em] text-slate-400">
              H1 Opportunities
            </div>
            <div className="mt-0.5 text-[11px] text-slate-500">
              Sorted by{" "}
              <span className="font-medium text-slate-200">room</span> (|move %|)
              then{" "}
              <span className="font-medium text-slate-200">score</span>.
            </div>
          </div>
          <div className="hidden text-[11px] text-slate-500 md:block">
            Score blends trend, structure, SR distance, RVOL, ATR and macro
            tilt.
          </div>
        </div>

        <div className="overflow-x-auto">
          <table className="min-w-full border-separate border-spacing-0 text-xs">
            <thead>
              <tr className="bg-slate-950/80 text-[11px] uppercase tracking-wide text-slate-400">
                <th className="sticky left-0 z-10 bg-slate-950/90 px-4 py-2 text-left">
                  Symbol
                </th>
                <th className="px-3 py-2 text-left">Direction</th>
                <th className="px-3 py-2 text-right">Live</th>
                <th className="px-3 py-2 text-right">Basis</th>
                <th className="px-3 py-2 text-right">Target</th>
                <th className="px-3 py-2 text-right">Time left</th>
                <th className="px-3 py-2 text-left">Signal</th>
                <th className="px-3 py-2 text-right">Status</th>
                <th className="px-3 py-2 text-right">Room</th>
                <th className="px-3 py-2 text-right">Score</th>
                <th className="px-3 py-2 text-right">Prob</th>
                <th className="px-3 py-2 text-left">Reasons</th>
                <th className="px-3 py-2 text-right">Created</th>
              </tr>
            </thead>
            <tbody>
              {rows.length === 0 ? (
                <tr>
                  <td
                    colSpan={13}
                    className="px-4 py-7 text-center text-xs text-slate-400"
                  >
                    No active opportunities have passed the room & confidence
                    gates yet. When a genuinely asymmetric H1 setup appears, it
                    will land here.
                  </td>
                </tr>
              ) : (
                rows.map((r, idx) => {
                  const sym = r.symbol.toUpperCase();
                  const dirLabel = fmtDirectionLabel(r.direction);
                  const dirColor: "up" | "down" | "flat" = r.direction;

                  const horizonMin = typeof r.horizonMin === "number" ? r.horizonMin : 60;
                  const horizonMs = horizonMin * 60_000;
                  const timeLeftMs =
                    typeof r.alertTimeMs === "number" ? (r.alertTimeMs + horizonMs - Date.now()) : null;
                  const timeLeftText =
                    timeLeftMs == null
                      ? "—"
                      : (timeLeftMs <= 0
                          ? "0m"
                          : `${Math.ceil(timeLeftMs / 60_000)}m`);

                  const statusText =
                    typeof r.status === "string" && r.status
                      ? r.status.toUpperCase()
                      : "ACTIVE";
                  const lp =
                    livePrices?.prices?.[sym]?.price ??                    
                    (typeof (r as any).last_price === "number" ? (r as any).last_price : null) ??
                    (typeof (r as any).mid === "number" ? (r as any).mid : null) ??                      
                    r.basisPrice;
                      

                  const liveStr = fmtPrice(sym, typeof lp === "number" ? lp : null);

                  let distToTargetStr = "—";
                  if (
                    typeof lp === "number" &&
                    typeof r.targetPrice === "number" &&
                    typeof r.basisPrice === "number" &&
                    r.basisPrice > 0
                  ) {
                    const distPct = Math.abs((r.targetPrice - lp) / r.basisPrice) * 100.0;
                    distToTargetStr = distPct.toFixed(2) + "%";
                  }
const roomStr = r.absMovePct.toFixed(2) + "%";
                  const scoreStr =
                    r.oppScore != null ? r.oppScore.toFixed(1) : "—";
                  const probStr =
                    r.probUp != null
                      ? (r.probUp * 100).toFixed(0)+"%"
                      : "—";

                  const reasons = r.reasons.slice(0, 3);

                  // simple visual bar for score 0100
                  const scoreNorm = Math.max(
                    0,
                    Math.min(100, (r.oppScore ?? 0) * 1.0)
                  );
                  const barWidth = scoreNorm.toFixed(0) + "%";

                  const rowBg =
                    idx % 2 === 0
                      ? "bg-slate-950/50"
                      : "bg-slate-900/40";

                  return (
                    <tr
                      key={sym + "-" + (r.alertTimeMs ?? "na")}
                      className={rowBg + " border-b border-slate-900/80 hover:bg-slate-800/60"}
                    >
                      <td className="sticky left-0 z-10 bg-inherit px-4 py-2 text-sm font-semibold text-slate-50">
                        {sym}
                      </td>
                      <td className="px-3 py-2">
                        <div className="flex items-center gap-2">
                          <Pill color={dirColor}>{dirLabel}</Pill>
                          {r.oppConfidence && (
                            <span className="rounded-full bg-slate-900/80 px-2 py-0.5 text-[10px] font-medium text-slate-300">
                              {r.oppConfidence}
                            </span>
                          )}
                        </div>
                      </td>
                      <td className="px-3 py-3 text-right">
                    <div className="font-medium">{liveStr}</div>
                    <div className="mt-0.5 text-xs text-slate-400">{distToTargetStr} to target</div>
                  </td>
                      <td className="px-3 py-2 text-right tabular-nums text-slate-200">
                        {fmtPrice(sym, r.basisPrice)}
                      </td>
                      <td className="px-3 py-2 text-right tabular-nums text-slate-200">
                        {fmtPrice(sym, r.targetPrice)}
                      </td>
                      <td className="px-3 py-2 text-right tabular-nums text-slate-200">
                        {timeLeftText}
                      </td>
                      <td className="px-3 py-2">
                        {r.signalText ? (
                          <span className="inline-flex items-center rounded-full border border-slate-700/70 bg-slate-900/60 px-2 py-0.5 text-[10px] font-semibold text-slate-200">
                            {r.signalText}
                          </span>
                        ) : (
                          <span className="text-[11px] text-slate-500">—</span>
                        )}
                      </td>
                      <td className="px-3 py-2 text-right">
                        <span className={
                          statusText === "HIT"
                            ? "rounded-full bg-emerald-500/15 px-2 py-0.5 text-[10px] font-semibold text-emerald-200"
                            : statusText === "EXPIRED"
                              ? "rounded-full bg-amber-500/15 px-2 py-0.5 text-[10px] font-semibold text-amber-200"
                              : "rounded-full bg-slate-800/80 px-2 py-0.5 text-[10px] font-semibold text-slate-200"
                        }>
                          {statusText}
                        </span>
                      </td>
                      <td className="px-3 py-2 text-right tabular-nums text-slate-200">
                        {roomStr}
                      </td>
                      <td className="px-3 py-2 text-right">
                        <div className="flex flex-col items-end gap-1">
                          <span className="tabular-nums text-slate-100">
                            {scoreStr}
                          </span>
                          <div className="h-1.5 w-16 overflow-hidden rounded-full bg-slate-800">
                            <div
                              className="h-full rounded-full bg-gradient-to-r from-emerald-400 via-emerald-300 to-amber-300"
                              style={{ width: barWidth }}
                            />
                          </div>
                        </div>
                      </td>
                      <td className="px-3 py-2 text-right tabular-nums text-slate-200">
                        {probStr}
                      </td>
                      
                      <td className="px-3 py-2">
                        <div className="flex flex-col gap-1">
                          {reasons.length === 0 ? (
                            <span className="text-[11px] text-slate-500">
                              Model + macro blend
                            </span>
                          ) : (
                            <div className="flex max-w-xs flex-wrap gap-1">
                              {reasons.map((txt, i) => (
                                <span
                                  key={i}
                                  className="rounded-full bg-slate-900/90 px-2 py-0.5 text-[10px] text-slate-200"
                                >
                                  {txt}
                                </span>
                              ))}
                            </div>
                          )}

                          {r.srLabel && (
                            <span className="inline-flex items-center rounded-full border border-sky-500/60 bg-sky-500/10 px-2 py-0.5 text-[10px] font-medium text-sky-100">
                              SR: {r.srLabel}
                            </span>
                          )}
                        </div>
                      </td>

                      <td className="px-3 py-2 text-right text-[11px] text-slate-400">
                        {fmtTime(r.alertTimeMs)}
                      </td>
                    </tr>
                  );
                })
              )}
            </tbody>
          </table>
        </div>
      </Card>

      {/* History */}
      <Card>
        <div className="flex items-center justify-between border-b border-slate-800/80 px-4 py-3">
          <div>
            <div className="text-[11px] font-semibold uppercase tracking-[0.12em] text-slate-400">
              Alert History
            </div>
            <div className="mt-0.5 text-[11px] text-slate-500">
              Completed H1 opportunities  hit, missed or expired.
            </div>
          </div>
          <div className="text-[11px] text-slate-500">
            Last{" "}
            <span className="font-medium text-slate-200">
              {history.length || 0}
            </span>{" "}
            alerts
          </div>
        </div>

        <div className="overflow-x-auto">
          <table className="min-w-full border-separate border-spacing-0 text-xs">
            <thead>
              <tr className="bg-slate-950/80 text-[11px] uppercase tracking-wide text-slate-400">
                <th className="px-4 py-2 text-left">When</th>
                <th className="px-3 py-2 text-left">Symbol</th>
                <th className="px-3 py-2 text-left">Direction</th>
                <th className="px-3 py-2 text-right">Live</th>
                <th className="px-3 py-2 text-right">Result</th>
                <th className="px-3 py-2 text-right">Max DD</th>
                <th className="px-3 py-2 text-right">Time to target</th>
              </tr>
            </thead>
            <tbody>
              {history.length === 0 ? (
                <tr>
                  <td
                    colSpan={7}
                    className="px-4 py-6 text-center text-xs text-slate-400"
                  >
                    No completed opportunities tracked yet. As alerts hit target
                    or expire, they will show up here with realized move and
                    drawdown.
                  </td>
                </tr>
              ) : (
                history.map((h, idx) => {
                  const dirLabel = fmtDirectionLabel(h.direction);

                  let resultLabel = "Pending";
                  let resultClass = "text-slate-300";
                  if (h.hitTarget === true) {
                    resultLabel = "Hit target";
                    resultClass = "text-emerald-300";
                  } else if (h.hitTarget === false) {
                    resultLabel = "Expired / missed";
                    resultClass = "text-rose-300";
                  }

                  const ddStr =
                    typeof h.maxDrawdownPct === "number"
                      ? h.maxDrawdownPct.toFixed(2) + "%"
                      : "—";

                  const tttStr =
                    typeof h.timeToTargetMin === "number"
                      ? h.timeToTargetMin.toFixed(0) + " min"
                      : "—";

                  const rowBg =
                    idx % 2 === 0
                      ? "bg-slate-950/40"
                      : "bg-slate-900/40";

                  return (
                    <tr
                      key={h.symbol + "-" + h.alertTimeMs}
                      className={rowBg + " border-b border-slate-900/80 hover:bg-slate-800/60"}
                    >
                      <td className="px-4 py-2 text-[11px] text-slate-400">
                        {fmtTime(h.alertTimeMs)}
                      </td>
                      <td className="px-3 py-2 text-sm font-medium text-slate-50">
                        {h.symbol}
                      </td>
                      <td className="px-3 py-2">
                        <Pill color={h.direction}>{dirLabel}</Pill>
                      </td>
                      <td className="px-3 py-2 text-right tabular-nums text-slate-50">
                        {(typeof h.realizedMovePct === "number" ? (h.realizedMovePct > 0 ? "+" : "") + h.realizedMovePct.toFixed(2) + "%" : (h.expectedMovePct === 0 ? "0.00%" : (h.expectedMovePct > 0 ? "+" : "") + h.expectedMovePct.toFixed(2) + "%"))}
                      </td>
                      <td
                        className={"px-3 py-2 text-right text-[11px] font-medium " + resultClass}
                      >
                        {resultLabel}
                      </td>
                      <td className="px-3 py-2 text-right text-[11px] text-slate-300">
                        {ddStr}
                      </td>
                      <td className="px-3 py-2 text-right text-[11px] text-slate-300">
                        {tttStr}
                      </td>
                    </tr>
                  );
                })
              )}
            </tbody>
          </table>
        </div>
      </Card>
    </div>
  );
}

export default OpportunitiesDashboard;