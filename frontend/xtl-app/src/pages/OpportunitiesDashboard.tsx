import React from "react";

/* --------------------------
 * Small UI helpers
 * -------------------------- */

const Card: React.FC<React.HTMLAttributes<HTMLDivElement>> = ({
  className = "",
  children,
  ...rest
}) => (
  <div
    className={`rounded-2xl border border-slate-700/60 bg-slate-800/60 shadow-sm backdrop-blur ${className}`}
    {...rest}
  >
    {children}
  </div>
);

const CardContent: React.FC<React.HTMLAttributes<HTMLDivElement>> = ({
  className = "",
  children,
  ...rest
}) => (
  <div className={`p-5 ${className}`} {...rest}>
    {children}
  </div>
);

/* --------------------------
 * Types & mapping
 * -------------------------- */

type Direction = "up" | "down" | "flat";


type ApiRow = {
  symbol: string;

  // snapshot-side fields from /trend/opportunities
  opp_direction?: string;              // "BUY" | "SELL" | "FLAT"
  opp_expected_move_pct_1h?: number;   // frozen expected move %
  alert_price_1h?: number;             // frozen entry / basis
  target_price_1h?: number;            // frozen target
  alert_created_ms?: number;           // when this alert was created
  last_status_ms?: number;             // last status update for this alert
  prob_up?: number;
  opp_score?: number;  

  // NEW: extra fields that backend can send
  p_up?: number;                       // older probability field
  reasons?: string[] | string;         // reasons from backend
  updated_broker_ms?: number;          // last broker-time update
  using_device?: string | null;        // which device supplied the snap

  // fallback fields if something is missing (older shape / predict-all)
  decision?: string;
  expected_move_pct_1h?: number;
  basis_price_1h?: number;
  server_now_ms?: number;
};


type ApiHistoryRow = {
  alert_time_ms?: number;
  symbol?: string;
  direction?: string;           // "UP" / "DOWN"
  decision?: string;            // "BUY" / "SELL" / "ABSTAIN"
  horizon_min?: number;
  expected_move_pct?: number;
  hit_target?: boolean | null;
  realized_move_pct?: number | null;
  max_drawdown_pct?: number | null;
  time_to_target_min?: number | null;
};

type OppRow = {
  symbol: string;
  direction: Direction;
  movePct: number;
  absMovePct: number;
  basisPrice: number | null;     // stored basis / alert price
  targetPrice: number | null;
  probUp: number | null;
  oppScore: number | null; 
  reasons: string[];
  updatedBrokerMs: number | null;
  device: string | null;

  // For alert lifecycle / history
  alertTimeMs: number | null;    // when the opportunity was created
  horizonMin: number | null;     // e.g. 60 for H1
};

type HistoryRow = {
  alertTimeMs: number;           // broker-time in ms
  symbol: string;
  direction: Direction;
  horizonMin: number;            // e.g. 60 for H1
  expectedMovePct: number;       // signed

  // For now these are optional; backend can fill later
  hitTarget?: boolean | null;
  realizedMovePct?: number | null;
  maxDrawdownPct?: number | null;
  timeToTargetMin?: number | null;
};


const API_BASE = (window as any).__PUBLIC_API_BASE__ || "/_api";

/* --------------------------
 * Hooks: live predict + prices
 * -------------------------- */

function pipSize(sym: string): number {
  const s = sym.toUpperCase();
  if (s === "XAUUSD") return 0.1;
  if (s.endsWith("JPY")) return 0.01;
  return 0.0001;
}

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


// map API row -> OppRow (no filtering yet)

// map API row -> OppRow (no filtering yet)

// map API row -> OppRow (no filtering yet)

function mapApiRow(r: ApiRow): OppRow | null {
  if (!r?.symbol) return null;

  // 1) Direction: prefer opportunity-specific direction, fall back to decision
  const rawDir = (r.opp_direction || r.decision || "").toUpperCase();
  let direction: Direction = "flat";

  if (rawDir === "BUY" || rawDir === "UP") {
    direction = "up";
  } else if (rawDir === "SELL" || rawDir === "DOWN") {
    direction = "down";
  }

  // 2) Expected move (%): prefer frozen opportunity value, fall back to live
  const move =
    typeof r.opp_expected_move_pct_1h === "number"
      ? r.opp_expected_move_pct_1h
      : typeof r.expected_move_pct_1h === "number"
      ? r.expected_move_pct_1h
      : 0;

  // 3) Basis / alert price
  const basis =
    typeof r.alert_price_1h === "number"
      ? r.alert_price_1h
      : typeof r.basis_price_1h === "number"
      ? r.basis_price_1h
      : null;

  // 4) Target price: standard 1h target from backend
  const target =
    typeof r.target_price_1h === "number" ? r.target_price_1h : null;

  // 5) Probability: prefer prob_up (new)
  const prob =
    typeof (r as any).prob_up === "number"
      ? (r as any).prob_up
      : null;
  const oppScore =
    typeof (r as any).opp_score === "number"
      ? (r as any).opp_score
      : null;

  // 6) Reasons array (if backend sends them)
  const anyR: any = r as any;
  const reasonsRaw = anyR.reasons;
  const reasonsArray: string[] = Array.isArray(reasonsRaw)
    ? reasonsRaw.map(String)
    : reasonsRaw
    ? [String(reasonsRaw)]
    : [];

  // 7) Updated time: prefer last_status_ms, else updated_broker_ms (if present)
  const updatedMs =
    typeof (r as any).last_status_ms === "number"
      ? (r as any).last_status_ms
      : typeof (r as any).updated_broker_ms === "number"
      ? (r as any).updated_broker_ms
      : null;

  // 8) Alert time & horizon – for now assume H1 horizon
  const alertTimeMs =
    typeof r.alert_created_ms === "number" ? r.alert_created_ms : null;
  const horizonMin = 60;

  // 9) Build OppRow
  return {
    symbol: r.symbol,
    direction,
    movePct: move,
    absMovePct: Math.abs(move),
    basisPrice: basis,
    targetPrice: target,
    probUp: prob,
    oppScore,	
    reasons: reasonsArray,
    updatedBrokerMs: updatedMs,
    device: (r as any).using_device || null,
    alertTimeMs,
    horizonMin,
  };
}


function mapHistoryApiRow(r: ApiHistoryRow): HistoryRow | null {
  if (!r || !r.symbol) return null;

  const rawDir = (r.direction || r.decision || "").toUpperCase();
  let direction: Direction = "flat";
  if (rawDir === "BUY" || rawDir === "UP") direction = "up";
  else if (rawDir === "SELL" || rawDir === "DOWN") direction = "down";

  const alertTimeMs =
    typeof r.alert_time_ms === "number" ? r.alert_time_ms : Date.now();

  const horizonMin =
    typeof r.horizon_min === "number" && !Number.isNaN(r.horizon_min)
      ? r.horizon_min
      : 60;

  const expectedMovePct =
    typeof r.expected_move_pct === "number" ? r.expected_move_pct : 0;

  const hitTarget =
    typeof r.hit_target === "boolean" || r.hit_target === null
      ? r.hit_target
      : null;

  const realizedMovePct =
    typeof r.realized_move_pct === "number"
      ? r.realized_move_pct
      : undefined;

  const maxDrawdownPct =
    typeof r.max_drawdown_pct === "number"
      ? r.max_drawdown_pct
      : undefined;

  const timeToTargetMin =
    typeof r.time_to_target_min === "number"
      ? r.time_to_target_min
      : undefined;

  return {
    alertTimeMs,
    symbol: r.symbol,
    direction,
    horizonMin,
    expectedMovePct,
    hitTarget,
    realizedMovePct,
    maxDrawdownPct,
    timeToTargetMin,
  };
}

function useOpportunities() {
  const [rows, setRows] = React.useState<OppRow[]>([]);
  const [history, setHistory] = React.useState<HistoryRow[]>([]);
  const [lastAt, setLastAt] = React.useState<number | null>(null);
  const [error, setError] = React.useState<string | null>(null);

  // Keep first-seen opportunity per symbol frozen for this page session
  // (symbol -> frozen OppRow snapshot)
  const frozenRef = React.useRef<Map<string, OppRow>>(new Map());
  // Track if we already populated from backend history
  const historyFromBackendRef = React.useRef(false);

  const [toastRow, setToastRow] = React.useState<OppRow | null>(null);

  /** ------------------------------
   * Fetch opportunities once
   * ------------------------------ */
  const fetchOnce = React.useCallback(async () => {
    try {
      setError(null);

      const url = `${API_BASE}/trend/opportunities?tf=M15&_=${Date.now()}`;
      const res = await fetch(url, {
        credentials: "include",
        cache: "no-store",
      });

      if (!res.ok) {
        throw new Error(`HTTP ${res.status}`);
      }

      
      const js = await res.json();

      // Live opportunities
      const apiRows: ApiRow[] = Array.isArray(js?.rows) ? js.rows : [];

      // Backend-driven alert history
      const apiHistoryRaw: ApiHistoryRow[] = Array.isArray(js?.history)
        ? js.history
        : [];

      const mappedHistory: HistoryRow[] = apiHistoryRaw
        .map(mapHistoryApiRow)
        .filter((h): h is HistoryRow => h !== null);

      setHistory(mappedHistory);

      const mapped: OppRow[] = [];
      const now = Date.now();

     


      for (const r of apiRows) {
        const m = mapApiRow(r);
        if (!m) continue;

        // Per-symbol threshold (“room” filter)
        const sym = m.symbol.toUpperCase();
        const thr = sym === "XAUUSD" ? 0.02 : 0.02; // temp low thresholds

        if (m.absMovePct < thr) continue;

        // If you later extend OppRow with alertTimeMs / horizonMin,
        // seed them here (kept as any to avoid TS errors for now).
        (m as any).alertTimeMs = (m as any).alertTimeMs ?? now;
        (m as any).horizonMin = (m as any).horizonMin ?? 60;

        mapped.push(m);
      }

      // Dedupe per symbol: keep the strongest absolute move
      const bySymbol = new Map<string, OppRow>();
      for (const r of mapped) {
        const key = r.symbol.toUpperCase();
        const existing = bySymbol.get(key);
        if (!existing || r.absMovePct > existing.absMovePct) {
          bySymbol.set(key, r);
        }
      }

      const frozen = frozenRef.current;
      
      const newSymbols: OppRow[] = [];
      // Freeze first-seen opportunities per symbol
      for (const [sym, row] of bySymbol.entries()) {
       if (!frozen.has(sym)) {
         frozen.set(sym, row);
         newSymbols.push(row);
       } else {
         // Update existing frozen row fields but keep original alertTimeMs / basis
         const existing = frozen.get(sym)!;
         existing.movePct = row.movePct;
         existing.absMovePct = row.absMovePct;
         existing.targetPrice = row.targetPrice;
         existing.probUp = row.probUp;
         existing.oppScore = row.oppScore;
         existing.updatedBrokerMs = row.updatedBrokerMs;
         existing.reasons = row.reasons;
       }
      }

      


      

      // 3) Toast only for first new symbol this poll
      if (newSymbols.length > 0) {
        setToastRow(newSymbols[0]);
      }

      // 4) Drop entries that have passed their horizon (e.g. 60 min)
      const nowMs = Date.now();
      for (const [sym, row] of frozen.entries()) {
        if (row.alertTimeMs && row.horizonMin) {
          const expiryMs = row.alertTimeMs + row.horizonMin * 60_000;
          if (nowMs > expiryMs) {
            frozen.delete(sym);
          }
        }
      }

      // 5) Visible rows = current frozen map, sorted by abs move
      const nextRows = Array.from(frozen.values()).sort(
        (a, b) => b.absMovePct - a.absMovePct
      );

      setRows(nextRows);
      setLastAt(Date.now());
      

    } catch (e: any) {
      setError(e?.message ?? "Failed to load opportunities");
    }
  }, []);

  /** ------------------------------
   * Polling – aligned roughly to the minute
   * ------------------------------ */
  React.useEffect(() => {
    void fetchOnce();

    let t: number | null = null;

    const tick = async () => {
      await fetchOnce();

      // next call aligned to minute boundary, but at least 10s later
      const now = Date.now();
      const msToNextMinute = 60000 - (now % 60000);
      const delay = Math.max(10000, msToNextMinute);

      t = window.setTimeout(() => {
        void tick();
      }, delay);
    };

    t = window.setTimeout(() => {
      void tick();
    }, 60000);

    return () => {
      if (t !== null) {
        window.clearTimeout(t);
      }
    };
  }, [fetchOnce]);

  /** ------------------------------
   * Toast auto-hide after 8s
   * ------------------------------ */
  React.useEffect(() => {
    if (!toastRow) return;
    const id = window.setTimeout(() => setToastRow(null), 10000);
    return () => window.clearTimeout(id);
  }, [toastRow]);

  return {
    rows,
    history,
    lastAt,
    error,
    refetch: fetchOnce,
    toastRow,
    clearToast: () => setToastRow(null),
  };
}

function OpportunitiesDashboard() {
  const {
    rows,
    history,
    lastAt,
    error,
    refetch,
    toastRow,
    clearToast,
  } = useOpportunities();

  const hasRows = rows.length > 0;
  const lastUpdatedLabel = lastAt
    ? new Date(lastAt).toLocaleTimeString()
    : "—";

  return (
    <div className="mx-auto flex max-w-6xl flex-col gap-6 px-4 py-6">
      {/* Header */}
      <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <div className="text-xs font-semibold uppercase tracking-wide text-slate-400">
            Opportunities
          </div>
          <div className="mt-1 text-lg font-semibold text-slate-50">
            Live 1-hour model opportunities
          </div>
          <div className="mt-1 text-xs text-slate-400">
            Filtered by expected move and model confidence. Updated every
            minute.
          </div>
        </div>

        <div className="flex items-center gap-3">
          <div className="text-xs text-slate-400">
            Last updated:{" "}
            <span className="font-medium text-slate-200">
              {lastUpdatedLabel}
            </span>
          </div>
          <button
            type="button"
            onClick={() => void refetch()}
            className="inline-flex items-center rounded-full border border-slate-600 bg-slate-800 px-3 py-1.5 text-xs font-medium text-slate-100 hover:border-slate-400 hover:bg-slate-700"
          >
            Refresh
          </button>
        </div>
      </div>

      {error && (
        <div className="rounded-2xl border border-rose-600/60 bg-rose-900/40 px-4 py-2 text-xs text-rose-100">
          Error: {error}
        </div>
      )}

      {/* Live opportunities grid */}
      <Card className="mt-2 relative">
        <CardContent>
          <div className="mb-3 flex items-center justify-between gap-2">
            <div>
              <div className="text-xs font-semibold uppercase tracking-wide text-slate-400">
                Live Opportunities
              </div>
              <div className="mt-1 text-sm text-slate-200">
                H1 forecast filtered by minimum room and direction.
              </div>
            </div>
          </div>

          {/* Toast anchored to this card (top-right), stays 10s via useEffect */}
          {toastRow && (
            <div className="pointer-events-none absolute right-4 top-4 z-40">
              <div className="pointer-events-auto flex max-w-md items-start gap-3 rounded-2xl border border-slate-600 bg-slate-900/95 px-4 py-3 shadow-lg">
                <div className="mt-0.5 h-2 w-2 flex-shrink-0 rounded-full bg-emerald-400" />
                <div className="flex-1">
                  <div className="text-xs font-semibold uppercase tracking-wide text-emerald-300">
                    New opportunity
                  </div>
                  <div className="mt-1 text-sm text-slate-50">
                    {toastRow.symbol}{" "}
                    {toastRow.direction === "up"
                      ? "- Long room"
                      : "- Short room"}{" "}
                    {toastRow.movePct >= 0 ? "+" : ""}
                    {toastRow.movePct.toFixed(2)}% toward{" "}
                    {toastRow.targetPrice != null
                      ? fmtPrice(toastRow.symbol, toastRow.targetPrice)
                      : "target"}
                    .
                  </div>
                  {toastRow.reasons && toastRow.reasons.length > 0 && (
                    <div className="mt-1 text-xs text-slate-400">
                      {toastRow.reasons.join(" · ")}
                    </div>
                  )}
                </div>
                <button
                  type="button"
                  onClick={() => clearToast()}
                  className="ml-2 inline-flex h-6 w-6 items-center justify-center rounded-full text-slate-400 hover:bg-slate-700 hover:text-slate-100"
                >
                  ×
                </button>
              </div>
            </div>
          )}

          {!hasRows ? (
            <div className="rounded-2xl bg-slate-900/60 px-4 py-6 text-sm text-slate-400">
              No opportunities detected right now. The meter will light up when
              the model finds enough room and alignment.
            </div>
          ) : (
            <div className="overflow-x-auto">
              <table className="min-w-full text-sm text-slate-100">
                <thead className="border-b border-slate-700 text-xs text-slate-300">
                  <tr>
                    <th className="py-2 pr-4 text-left">Pair</th>
                    <th className="py-2 pr-4 text-left">Direction</th>
                    <th className="py-2 pr-4 text-left">Alert price</th>
                    <th className="py-2 pr-4 text-left">Target (1h)</th>
                    <th className="py-2 pr-4 text-left">Expected move %</th>
                    <th className="py-2 pr-4 text-left">ProbUp</th>
                    <th className="py-2 pr-4 text-left">Score</th>
                    <th className="py-2 pr-4 text-left">Last updated</th>
                  </tr>
                </thead>
                <tbody>
                  {rows.length === 0 ? (
                    <tr>
                      <td
                        colSpan={8}
                        className="py-4 pr-4 text-sm text-slate-400"
                      >
                        No current opportunities meeting the room & confidence
                        filter. When the model finds fresh room, they will
                        appear here and a toast will pop in the top-right.
                      </td>
                    </tr>
                  ) : (
                    rows.map((r) => {
                      const dirLabel =
                        r.direction === "up"
                          ? "Bullish"
                          : r.direction === "down"
                          ? "Bearish"
                          : "Flat";

                      const dirBadgeClass =
                        r.direction === "up"
                          ? "bg-emerald-500/10 text-emerald-300 border-emerald-500/40"
                          : r.direction === "down"
                          ? "bg-rose-500/10 text-rose-300 border-rose-500/40"
                          : "bg-slate-700/40 text-slate-200 border-slate-600/60";

                      return (
                        <tr
                          key={r.symbol}
                          className="border-b border-slate-800/80 last:border-0"
                        >
                          <td className="py-2 pr-4 font-medium">
                            {r.symbol}
                          </td>

                          <td className="py-2 pr-4">
                            <span
                              className={`inline-flex items-center rounded-full border px-2 py-0.5 text-xs font-medium ${dirBadgeClass}`}
                            >
                              {dirLabel}
                            </span>
                          </td>

                          <td className="py-2 pr-4">
                            {fmtPrice(r.symbol, r.basisPrice ?? null)}
                          </td>

                          <td className="py-2 pr-4">
                            {fmtPrice(r.symbol, r.targetPrice)}
                          </td>

                          <td className="py-2 pr-4">
                            {`${r.movePct >= 0 ? "+" : ""}${r.movePct.toFixed(
                              2
                            )}%`}
                          </td>

                          <td className="py-2 pr-4">
                            {r.probUp != null
                              ? `${(r.probUp * 100).toFixed(0)}%`
                              : ""}
                          </td>
                          <td className="py-2 pr-4">
                            {r.oppScore != null
                              ? r.oppScore.toFixed(1)
                              : "—"}
                          </td>

                          <td className="py-2 pr-4">
                            {fmtTime(r.updatedBrokerMs)}
                          </td>
                        </tr>
                      );
                    })
                  )}
                </tbody>
              </table>
            </div>
          )}
        </CardContent>
      </Card>

      {/* Alert History */}
      <Card className="mt-4">
        <CardContent>
          <div className="flex items-center justify-between gap-2">
            <div>
              <div className="text-xs font-semibold uppercase tracking-wide text-slate-400">
                Alert History
              </div>
              <div className="mt-1 text-sm text-slate-200">
                Past opportunities with hit / miss and realized performance.
              </div>
            </div>
          </div>

          <div className="mt-4 overflow-x-auto">
            <table className="min-w-full text-sm text-slate-100">
              <thead className="border-b border-slate-700 text-xs text-slate-300">
                <tr>
                  <th className="py-2 pr-4 text-left">Alert time</th>
                  <th className="py-2 pr-4 text-left">Pair</th>
                  <th className="py-2 pr-4 text-left">Direction</th>
                  <th className="py-2 pr-4 text-left">Horizon</th>
                  <th className="py-2 pr-4 text-left">Expected move %</th>
                  <th className="py-2 pr-4 text-left">Hit target?</th>
                  <th className="py-2 pr-4 text-left">Realized move %</th>
                  <th className="py-2 pr-4 text-left">Max drawdown %</th>
                  <th className="py-2 pr-4 text-left">Time to target</th>
                </tr>
              </thead>
              <tbody>
                {history.length === 0 ? (
                  <tr>
                    <td
                      colSpan={9}
                      className="py-4 pr-4 text-sm text-slate-400"
                    >
                      No alert history yet for this session. Once opportunities
                      hit target or expire, they will appear here.
                    </td>
                  </tr>
                ) : (
                  history.map((h) => {
                    const dirLabel =
                      h.direction === "up"
                        ? "UP"
                        : h.direction === "down"
                        ? "DOWN"
                        : "FLAT";

                    const horizonLabel =
                      h.horizonMin >= 60
                        ? `H${h.horizonMin / 60}`
                        : `${h.horizonMin}m`;

                    return (
                      <tr
                        key={`${h.alertTimeMs}-${h.symbol}-${dirLabel}`}
                        className="border-b border-slate-800/80 last:border-0"
                      >
                        <td className="py-2 pr-4 text-slate-300">
                          {fmtTime(h.alertTimeMs)}
                        </td>
                        <td className="py-2 pr-4 font-medium">
                          {h.symbol}
                        </td>
                        <td className="py-2 pr-4">{dirLabel}</td>
                        <td className="py-2 pr-4">{horizonLabel}</td>
                        <td className="py-2 pr-4">
                          {`${
                            h.expectedMovePct >= 0 ? "+" : ""
                          }${h.expectedMovePct.toFixed(2)}%`}
                        </td>
                        <td className="py-2 pr-4">
                          {h.hitTarget == null ? (
                            <span className="rounded-full bg-slate-700/60 px-2 py-0.5 text-xs text-slate-300">
                              N/A
                            </span>
                          ) : h.hitTarget ? (
                            <span className="rounded-full bg-emerald-500/10 px-2 py-0.5 text-xs text-emerald-300">
                              Yes
                            </span>
                          ) : (
                            <span className="rounded-full bg-rose-500/10 px-2 py-0.5 text-xs text-rose-300">
                              No
                            </span>
                          )}
                        </td>
                        <td className="py-2 pr-4">
                          {typeof h.realizedMovePct === "number"
                            ? `${
                                h.realizedMovePct >= 0 ? "+" : ""
                              }${h.realizedMovePct.toFixed(2)}%`
                            : "—"}
                        </td>
                        <td className="py-2 pr-4">
                          {typeof h.maxDrawdownPct === "number"
                            ? `${h.maxDrawdownPct.toFixed(2)}%`
                            : "—"}
                        </td>
                        <td className="py-2 pr-4">
                          {h.hitTarget &&
                          typeof h.timeToTargetMin === "number"
                            ? `${h.timeToTargetMin.toFixed(0)} min`
                            : "—"}
                        </td>
                      </tr>
                    );
                  })
                )}
              </tbody>
            </table>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}

export default OpportunitiesDashboard;
