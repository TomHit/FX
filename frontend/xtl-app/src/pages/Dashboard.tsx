import React from "react";
import { Link } from "react-router-dom";


/* -------------------------------------------------
 * Lightweight Card primitives (Tailwind-only)
 * ------------------------------------------------- */
const Card: React.FC<React.HTMLAttributes<HTMLDivElement>> = ({ className = "", children, ...rest }) => (
  <div className={`rounded-2xl border border-slate-700/60 bg-slate-800/60 shadow-sm backdrop-blur ${className}`} {...rest}>
    {children}
  </div>
);
const CardContent: React.FC<React.HTMLAttributes<HTMLDivElement>> = ({ className = "", children, ...rest }) => (
  <div className={`p-5 ${className}`} {...rest}>
    {children}
  </div>
);


/* -------------------------------------------------
 * Clock utilities (broker-tz aware, second-accurate)
 * ------------------------------------------------- */
function useWallClock() {
  const [now, setNow] = React.useState<Date>(() => new Date());
  React.useEffect(() => {
    let t: number;
    const tick = () => {
      const ms = 1000 - (Date.now() % 1000);
      t = window.setTimeout(() => {
        setNow(new Date());
        tick();
      }, ms);
    };
    tick();
    const onVis = () => setNow(new Date());
    document.addEventListener("visibilitychange", onVis);
    return () => {
      clearTimeout(t);
      document.removeEventListener("visibilitychange", onVis);
    };
  }, []);
  return now;
}

// --- target helpers: pip size + formatting + fallback calc ---
function pipSize(sym: string): number {
  if (!sym) return 0.0001;
  if (sym.endsWith("JPY")) return 0.01;        // USDJPY, EURJPY, ...
  if (sym === "XAUUSD") return 0.1;            // treat gold "pip" as 0.1
  return 0.0001;                               // most FX
}

// Prefer symbol rules; keep as utility when you only have a raw number
function decimalsFromPrice(px?: number): number | undefined {
  if (!Number.isFinite(px as any)) return undefined;
  const s = String(px);

  // handle scientific notation like 1e-6 ? 6
  const sci = /e-(\d+)/i.exec(s);
  if (sci) return Math.min(8, parseInt(sci[1], 10));

  const i = s.indexOf(".");
  return i >= 0 ? Math.min(8, s.length - i - 1) : 0;
}

// Symbol ? decimal places: JPY=3, XAUUSD=2, majors=5
function priceDecimals(sym: string): number {
  const s = (sym || "").toUpperCase();
  if (s === "XAUUSD") return 2;
  if (s.endsWith("JPY")) return 3;
  return 5; // most FX majors
}

function fmtPrice(sym: string, px: number | null | undefined, decimals?: number): string {
  if (!Number.isFinite(px as any)) return "—";

  const d = Number.isFinite(decimals as any)
    ? (decimals as number)
    : priceDecimals(sym);

  // Avoid floating-point artifacts before toFixed
  const k = Math.pow(10, d);
  const n = Math.round((px as number) * k) / k;

  return n.toFixed(d);
}


function calcTargetPrice(row: {
  symbol: string;
  price?: number;
  decision?: "BUY"|"SELL"|"";
  direction?: "up" | "down" | "flat"; 
  target_price_1h?: number;
  target_pips?: number;              // may be pip-count OR price-delta
  expected_move_pct_1h?: number;     // percent, e.g. 0.40 => +0.40%
}) {
  const px = Number(row.price);
  const ps = pipSize(row.symbol);                    // 0.0001 (majors), 0.01 (JPY), 0.1 (XAU)
  if (!Number.isFinite(px)) {
    // we can still return the backend target even if live price is missing
    if (Number.isFinite(row.target_price_1h as any)) return row.target_price_1h as number;
    return null;
  }

  // A) Prefer explicit backend target — this is the stabilized value we want to show.
  if (Number.isFinite(row.target_price_1h as any)) {
    return row.target_price_1h as number;
  }

  // B) Pips-based fallback (supports “pip count” OR direct price-delta)
  if (Number.isFinite(row.target_pips as any)) {
    const tp = Number(row.target_pips);
    let d = Math.abs(tp) >= 5 ? tp * ps : tp;
    if (row.decision === "SELL") d = -Math.abs(d);
    else if (row.decision === "BUY") d = +Math.abs(d);
    return px + d;
  }

  // C) Percent-based fallback
  if (Number.isFinite(row.expected_move_pct_1h as any)) {
   const pct = Number(row.expected_move_pct_1h) / 100;
   // --- patch: infer direction when missing ---
   let dir = 0;
   if (row.decision === "SELL") dir = -1;
   else if (row.decision === "BUY") dir = +1;
   else if (row.direction === "down") dir = -1;
   else if (row.direction === "up") dir = +1;
   else dir = Math.sign(pct);
   // --- end patch ---
   return px * (1 + dir * Math.abs(pct));
  }
  
  return null;
}



type TfLabel = "M1" | "M5" | "M15" | "H1" | "H4";

type PredictRow = {
  symbol: string;
  direction: "up" | "down" | "flat";
  target_price_1h: number;
  prob_up_1h?: number;             // 0..1
  expected_move_pct_1h?: number;   // optional
};

type TfSnapshot = {
  tf: TfLabel;
  rows: PredictRow[];
};

function fmtTime(ts?: number) {
  if (!ts) return "-";
  try { return new Date(ts).toLocaleTimeString(); } catch { return "-"; }
}


const TF_LIST: TfLabel[] = ["M15"];
const API_BASE = (window as any).__PUBLIC_API_BASE__ || "/_api";

type PriceRow = { price: number | null; lastTs: number | null };
type PriceMap = Record<string, PriceRow>;

// --- Broker-time helpers (offset-based; PS/TS safe) ---

// Live ticking clock using a fixed broker offset (minutes, e.g. 120)
function useOffsetClock(offsetMin: number) {
  const [now, setNow] = React.useState(() => Date.now());
  React.useEffect(() => {
    const id = window.setInterval(() => setNow(Date.now()), 1000);
    return () => window.clearInterval(id);
  }, []);
  const localOffMin = -new Date().getTimezoneOffset(); // IST => +330
  const deltaMs = (offsetMin - localOffMin) * 60_000;
  const d = new Date(now + deltaMs);
  return d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false });
}

// Format a timestamp (epoch seconds or ms) as broker time (offset minutes from UTC), invariant to viewer TZ
function fmtBrokerTime(ts: number | null | undefined, _offsetMin: number) {
  if (!ts && ts !== 0) return "—";
  const ms = ts! < 2_000_000_000 ? ts! * 1000 : ts!;     // seconds ? ms
  
  return new Date(ms).toLocaleTimeString(
    "en-GB",
    { hour: "2-digit", minute: "2-digit", hour12: false, timeZone: "UTC" }
  );
}


// Normalize epoch to ms (accepts seconds or ms)
function toMs(ts?: number | null): number | null {
  if (!ts && ts !== 0) return null;
  return ts < 2_000_000_000 ? ts * 1000 : ts; // <~2033s threshold => seconds
}

// Pretty "UTC+HH:MM" from minutes
function fmtUtcOffset(min: number) {
  const sign = min >= 0 ? "+" : "-";
  const abs = Math.abs(min);
  const hh = String(Math.floor(abs / 60)).padStart(2, "0");
  const mm = String(abs % 60).padStart(2, "0");
  return `UTC${sign}${hh}:${mm}`;
}


function useTfStripData(enabled: boolean = true): {
  data: Record<TfLabel, TfSnapshot | undefined>,
  lastAt: number | null
} {
  const [data, setData] = React.useState<Record<TfLabel, TfSnapshot | undefined>>({} as any);
  const [lastAt, setLastAt] = React.useState<number | null>(null);

  const fetchAll = React.useCallback(async () => {
    const qs = TF_LIST.map(tf =>
      fetch(`${API_BASE}/trend/predict/all?tf=${tf}`, { credentials: "include" })
        .then(r => r.json().catch(() => null))
        .then(js => {
          if (!(js && js.ok && Array.isArray(js.rows))) return undefined;

          const rows: PredictRow[] = js.rows.map((r: any) => {
            // direction from decision
            const direction: "up" | "down" | "flat" =
              r?.decision === "BUY" ? "up" : r?.decision === "SELL" ? "down" : "flat";

            // probability: prefer ML p_up, else fallback from score
            const prob_up_1h: number | undefined =
              typeof r?.p_up === "number" ? r.p_up :
              (typeof r?.score === "number" ? (r.score + 1) / 2 : undefined);

            // derive target price if not sent explicitly
            let target_price_1h: number = r?.target_price_1h;
            if (!(typeof target_price_1h === "number") &&
               typeof r?.price === "number" &&
               typeof r?.target_pips === "number") {
             const step = pipSize(r.symbol) * r.target_pips;
             target_price_1h =
               direction === "up"   ? r.price + step :
               direction === "down" ? r.price - step :
               undefined;
            }

            return {
               symbol: r.symbol,
               direction,
               prob_up_1h,
               expected_move_pct_1h:
                 typeof r?.expected_move_pct_1h === "number" ? r.expected_move_pct_1h : undefined,
               target_price_1h,
            } as PredictRow;
          });

          return { tf, rows };
        })

    );
    const res = await Promise.all(qs);
    const map: Record<TfLabel, TfSnapshot | undefined> = {} as any;
    for (const x of res) if (x) map[x.tf] = x;
    setData(map);
    setLastAt(Date.now());
  }, []);

  React.useEffect(() => {
    if (!enabled) return;                 // ? don’t poll when disabled
    let t: number | null = null;
    const tick = async () => {
      await fetchAll();
      const wait = Math.max(10000, 60000 - (Date.now() % 60000)); // align to minute
      t = window.setTimeout(tick, wait);
    };
    tick();
    return () => { if (t) window.clearTimeout(t); };
  }, [fetchAll, enabled]);

  return { data, lastAt };
}


type LivePrice = { price: number; lastTs: number };

/** Live M1 prices (last CLOSED bar) with cache-busting, visibility refetch,
 *  and dynamic interval (ms). Use 0 to disable auto refresh. */
function useLivePrices(refreshMs: number = 60_000) {
  const [prices, setPrices] = React.useState<Record<string, LivePrice>>({});
  const [updatedAt, setUpdatedAt] = React.useState<number | null>(null);

  const refetch = React.useCallback(async () => {
    try {
      // cache-bust & no-store to avoid any intermediary caching
      const url = `/_api/trend/price/all?tf=M1&_=${Date.now()}`;
      const res = await fetch(url, { credentials: "include", cache: "no-store" });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const js = await res.json();

      const map: Record<string, LivePrice> = {};
      if (Array.isArray(js?.rows)) {
        for (const r of js.rows) {
          if (r?.symbol && typeof r?.price === "number" && typeof r?.lastTs === "number") {
            map[r.symbol] = { price: r.price, lastTs: r.lastTs };
          }
        }
      }
      setPrices(map);
      setUpdatedAt(Date.now());
    } catch {
      // keep previous data on transient errors
    }
  }, []);

  React.useEffect(() => {
    // refetch immediately when mounted / interval changes
    void refetch();

    // schedule the timer if enabled
    let t: number | null = null;
    const schedule = () => {
      if (!refreshMs || refreshMs <= 0) return; // disabled
      const alignToMinute = (refreshMs % 60_000) === 0;
      const wait = alignToMinute
        ? Math.max(10_000, 60_000 - (Date.now() % 60_000))  // align to next minute boundary
        : refreshMs;
      t = window.setTimeout(async () => {
        await refetch();
        schedule();
      }, wait);
    };
    schedule();

    // refetch when tab becomes visible or window regains focus
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


function TFCard(props: {
  label: TfLabel;
  active: boolean;
  snapshot?: TfSnapshot;
  onClick: () => void;
}) {
  const r0 = props.snapshot?.rows?.[0]; // show top symbol or an aggregate; adjust if you want per-symbol cards later
  const dir = r0?.direction ?? "flat";
  const prob = r0?.prob_up_1h != null ? Math.round((r0.prob_up_1h)*100) : null;
  const target = r0?.target_price_1h;

  const ring = props.active ? "ring-1 ring-amber-400" : "ring-1 ring-slate-700";
  const bg   = props.active ? "bg-slate-800/80" : "bg-slate-900/60 hover:bg-slate-800/60";
  const chip = dir === "up" ? "text-emerald-400" : dir === "down" ? "text-rose-400" : "text-slate-400";

  return (
    <button onClick={props.onClick}
      className={`rounded-2xl px-4 py-3 ${bg} ${ring} transition-colors text-left min-w-[120px]`}
    >
      <div className="text-xs uppercase tracking-wide text-slate-400">{props.label}</div>
      <div className={`mt-1 text-sm font-medium ${chip}`}>{dir.toUpperCase()}</div>
      <div className="mt-1 text-[11px] text-slate-400">
        {prob != null ? `ProbUp: ${prob}%` : "ProbUp: —"}
      </div>
      <div className="mt-0.5 text-[11px] text-slate-400">
        {target != null ? `Target(1h): ${target}` : "Target(1h): —"}
      </div>
    </button>
  );
}

function TFStrip({ tf, setTf, enabled = true }: { tf: TfLabel; setTf: (t: TfLabel)=>void; enabled?: boolean }) {
  const { data, lastAt } = useTfStripData(enabled);
  return (
    <div className="w-full">
      <div className="flex items-center gap-2 overflow-x-auto pb-1">
        {TF_LIST.map(t => (
          <TFCard key={t} label={t} active={t===tf} snapshot={data[t]} onClick={()=>setTf(t)} />
        ))}
      </div>
      <div className="mt-2 text-[11px] text-slate-500">
        {lastAt ? `Updated: ${new Date(lastAt).toLocaleTimeString()}` : (enabled ? "Loading…" : "Paused")}
      </div>
    </div>
  );
}


function fmtTZ(ts: number, tz: string) {
  try {
    const d = new Date(ts);
    const time = new Intl.DateTimeFormat("en-GB", {
      hour: "2-digit",
      minute: "2-digit",
      hour12: false,
      timeZone: tz,
    }).format(d);
    const date = new Intl.DateTimeFormat("en-GB", {
      weekday: "short",
      day: "2-digit",
      month: "short",
      timeZone: tz,
    }).format(d);
    return { time, date };
  } catch {
    return { time: "--:--", date: "" };
  }
}

// Map common broker TZ abbreviations to IANA zones used by Intl.
// Extend as needed (EET/EEST shown here).
function brokerAbbrToIana(abbr?: string): string | undefined {
  switch ((abbr || "").toUpperCase()) {
    case "EET":  return "Europe/Helsinki";   // UTC+02:00 (winter)
    case "EEST": return "Europe/Helsinki";   // UTC+03:00 (summer)
    // add more if your broker changes label
    default:     return undefined;           // fall back to local TZ
  }
}


/* -------------------------------------------------
 
 * ------------------------------------------------- */
export type Direction = "up" | "down" | "flat";
export type Bias = "Strong Bullish" | "Bullish" | "Neutral" | "Bearish" | "Strong Bearish";

export type InstrumentRow = {
  symbol: string;
  price: number;
  bias: Bias;
  direction: Direction;
  prob_up_1h: number; // 0..1
  expected_move_pct_1h: number; // signed, +0.30 => up 0.30%
  target_price_1h: number;
  reasons: string[];
  confidence_band?: number; // ATR units
  updated_broker_ts: number; // epoch ms
  broker_tz_abbr: string; // e.g., "EET"
  using_device: string; // device id
  broker_tz_offset_min?: number;
  tz_offset_min?: number;
  decision?: "BUY" | "SELL" | "";
  target_pips?: number;
};

// Map backend /trend/predict/all row -> InstrumentRow (defensive defaults)
function mapApiRowToInstrument(r: any): InstrumentRow {
  const label: string = (r?.label || "").toString();
  const decision: string = (r?.decision || "").toString();
  const bias: Bias =
    label === "Strong Bullish" ? "Strong Bullish" :
    label === "Bullish"        ? "Bullish" :
    label === "Strong Bearish" ? "Strong Bearish" :
    label === "Bearish"        ? "Bearish" :
    "Neutral";

  const direction: Direction =
    decision === "BUY"  ? "up"   :
    decision === "SELL" ? "down" : "flat";

  // Some backends send p_up, some send prob_up_1h already
  const prob_up = typeof r?.prob_up_1h === "number" ? r.prob_up_1h
                 : typeof r?.p_up === "number"      ? r.p_up
                 : null;

  // expected_move_pct_1h: use signed score% if present; else null
  const exp_move =
    typeof r?.expected_move_pct_1h === "number" ? r.expected_move_pct_1h :
    (typeof r?.score === "number" ? r.score : null);

  // target: accept either target_price_1h or target_pips translated later
  const target_price =
    typeof r?.target_price_1h === "number" ? r.target_price_1h : null;

  // reasons: normalize to string[]
  const reasons: string[] = Array.isArray(r?.reasons)
    ? r.reasons.map(String)
    : (r?.reason ? [String(r.reason)] : []);

  // broker/device meta if present
  const tz_off =
    (typeof r?.broker_tz_offset_min === "number" ? r.broker_tz_offset_min :
     typeof r?.tz_offset_min === "number"       ? r.tz_offset_min : undefined);

  return {
    symbol: String(r?.symbol || ""),
    price: NaN as any, // priced separately from M1 feed
    bias,
    direction,
    prob_up_1h: typeof prob_up === "number" ? prob_up : NaN as any,
    expected_move_pct_1h: typeof exp_move === "number" ? exp_move : 0,
    target_price_1h: target_price ?? (typeof r?.target === "number" ? r.target : undefined),
    decision: typeof r?.decision === "string" ? (r.decision as any) : "",
    target_pips: typeof r?.target_pips === "number" ? r.target_pips : undefined,
    reasons,
    confidence_band: undefined,
    // Use server_now_ms or updated_broker_ts if provided; fallback null
    updated_broker_ts:
      (typeof r?.updated_broker_ts === "number" ? r.updated_broker_ts :
       typeof r?.server_now_ms    === "number" ? r.server_now_ms : 0),
    broker_tz_abbr: String(r?.broker_tz_abbr || ""),
    using_device: String(r?.using_device || ""),
    broker_tz_offset_min: tz_off,
    tz_offset_min: tz_off,
  };
}



// --- Live data state (replaces MOCK) ---

function usePredictRows(tf: "M1" | "M5" | "M15" | "H1" | "H4" = "M1") {
  const [rows, setRows] = React.useState<InstrumentRow[]>([]);
  const [error, setError] = React.useState<string | null>(null);
  const [lastRefreshAt, setLastRefreshAt] = React.useState<number | null>(null);

  // keep a ref to the pending timer so we can reschedule precisely
  const timerRef = React.useRef<number | null>(null);

  const scheduleNext = React.useCallback((hintMs?: number, fallbackMs?: number) => {
    // clamp between 2s and 60s to avoid long sleeps / thundering herds
    const ms = Math.max(2_000, Math.min(60_000, Math.floor(hintMs ?? fallbackMs ?? 60_000)));
    if (timerRef.current) window.clearTimeout(timerRef.current);
    timerRef.current = window.setTimeout(() => { void fetchOnce(); }, ms);
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const fetchOnce = React.useCallback(async () => {
    try {
      setError(null);
      const res = await fetch(`${API_BASE}/trend/predict/all?tf=${tf}`, { credentials: "include", cache: "no-store" });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const js = await res.json();

      // map rows safely
      const apiRows: any[] = Array.isArray(js?.rows) ? js.rows : [];
      const mapped = apiRows.map(mapApiRowToInstrument);
      setRows(mapped);
      setLastRefreshAt(Date.now());

      // refresh hint: prefer top-level poll_after_ms => else earliest per-row eta => else align to minute
      const perRowEta = (() => {
        const etas = apiRows
          .map(r => Number(r?.next_update_eta_ms))
          .filter(v => Number.isFinite(v) && v > 0);
        return etas.length ? Math.min(...etas) : undefined;
      })();

      const nowModMin = 60_000 - (Date.now() % 60_000);
      const fallback = Math.max(10_000, nowModMin); // align to minute if nothing else

      const hint = Number(js?.poll_after_ms);
      scheduleNext(Number.isFinite(hint) && hint > 0 ? hint : perRowEta, fallback);
    } catch (e: any) {
      setError(e?.message || "fetch failed");
      // retry gentle after 30s on error
      scheduleNext(30_000, 30_000);
    }
  }, [tf, scheduleNext]);

  React.useEffect(() => {
    void fetchOnce();
    return () => { if (timerRef.current) window.clearTimeout(timerRef.current); };
  }, [fetchOnce]);

  return { rows, error, lastRefreshAt, refetch: fetchOnce };
}





/* -------------------------------------------------
 * UI helpers
 * ------------------------------------------------- */
function biasToScore(b: Bias): number {
  switch (b) {
    case "Strong Bullish":
      return 2;
    case "Bullish":
      return 1;
    case "Neutral":
      return 0;
    case "Bearish":
      return -1;
    case "Strong Bearish":
      return -2;
  }
}

function biasToPill(b?: Bias): { text: string; className: string; arrow: string } {
  const base = "inline-flex items-center gap-1.5 rounded-full px-2.5 py-1 text-xs font-medium";
  switch (b) {
    case "Strong Bullish":
      return { text: "Strong Bullish", className: `${base} bg-emerald-500/15 text-emerald-300 border border-emerald-400/20`, arrow: "\u2191" };
    case "Bullish":
      return { text: "Bullish", className: `${base} bg-emerald-500/10 text-emerald-300 border border-emerald-400/15`, arrow: "\u2191" };
    case "Neutral":
      return { text: "Neutral", className: `${base} bg-slate-500/10 text-slate-300 border border-slate-400/10`, arrow: "\u2022" };
    case "Bearish":
      return { text: "Bearish", className: `${base} bg-rose-500/10 text-rose-300 border border-rose-400/15`, arrow: "\u2193" };
    case "Strong Bearish":
      return { text: "Strong Bearish", className: `${base} bg-rose-500/15 text-rose-300 border border-rose-400/20`, arrow: "\u2193" };
    default:
      return { text: "Neutral", className: `${base} bg-slate-500/10 text-slate-300 border border-slate-400/10`, arrow: "\u2022" };
  }
}


function pct(n?: number): string {
  if (typeof n !== "number" || isNaN(n)) return "—";
  const sign = n >= 0 ? "+" : "";
  return `${sign}${n.toFixed(2)}%`;
}

function pctSym(n?: number, sym?: string): string {
  if (typeof n !== "number" || isNaN(n)) return "—";
  const x = n;
  const ax = Math.abs(x);
  const isJPY = !!sym && sym.toUpperCase().endsWith("JPY");
  // JPY moves are tiny: keep more precision for very small values
  const dp = isJPY ? (ax < 0.0015 ? 3 : 2) : 2;
  const sign = x >= 0 ? "+" : "";
  return `${sign}${x.toFixed(dp)}%`;
}


/* -------------------------------------------------
 * Card & Table cells
 * ------------------------------------------------- */
const ReasonList: React.FC<{ reasons: string[] }> = ({ reasons }) => (
  <ul className="mt-2 space-y-1 text-[13px] text-slate-300">
    {reasons.slice(0, 3).map((r, i) => (
      <li key={i} className="flex items-start gap-2">
        <span className="mt-1 h-1.5 w-1.5 rounded-full bg-slate-500/70" />
        <span>{r}</span>
      </li>
    ))}
  </ul>
);

const InstCard: React.FC<{
  row: InstrumentRow; showReasons: boolean; showTarget: boolean; livePrice?: number
}> = ({ row, showReasons, showTarget, livePrice }) => {
  const pill = biasToPill(row.bias);
  const tzIANA = row.broker_tz_abbr === "EET" ? "Europe/Helsinki" : undefined; // demo mapping; replace with actual
  const { time } = fmtTZ(row.updated_broker_ts, tzIANA || Intl.DateTimeFormat().resolvedOptions().timeZone);

  // simple “room” chip logic: majors =1%, XAU=1.5%
  const absMove = Math.abs(row.expected_move_pct_1h || 0);
  const roomThreshold = row.symbol.toUpperCase() === "XAUUSD" ? 1.5 : 1.0;
  const hasRoom = absMove >= roomThreshold;

  return (
    <Card className="hover:translate-y-[1px] transition-transform">
      <CardContent>
        <div className="flex items-start justify-between">
          <div>
            <div className="text-sm text-slate-400">{row.symbol}</div>
            <div className="mt-1 flex items-center gap-2">
              <span className={pill.className}>
                <span className="tabular-nums">{pill.text}</span>
              </span>
               {row.direction === "up" ? "UP" : row.direction === "down" ? "DOWN" : "FLAT"} {pill.arrow}
              {hasRoom && (
                <span className="inline-flex items-center rounded-full bg-amber-500/15 text-amber-300 border border-amber-400/30 px-2 py-0.5 text-[11px] ml-1">
                  = {roomThreshold.toFixed(1)}% room
                </span>
              )}
            </div>
          </div>
          <div className="text-right">
            <div className="text-xl tabular-nums">
              {typeof livePrice === "number"
                ? fmtPrice(row.symbol, livePrice, decimalsFromPrice(livePrice))
                : "—"}
            </div>

            {showTarget && (
              <div className="mt-1 text-xs text-slate-400">
                1h Target{" "}
                {Number.isFinite(row.expected_move_pct_1h as any)
                  ? `${pctSym(row.expected_move_pct_1h, row.symbol)} -`
                  : "—"}{" "}
                <span className="ml-1 tabular-nums text-slate-200">
                  {(() => {
                    const t = calcTargetPrice(row as any);
                    const base = (typeof livePrice === "number" ? livePrice : row.price) as number | undefined;
                    return t != null ? fmtPrice(row.symbol, t, decimalsFromPrice(base)) : "—";
                  })()}
                </span>
              </div>
            )}

            <div className="mt-1 text-xs text-slate-400">
              ProbUp{" "}
              <span className="text-slate-200 tabular-nums">
                {typeof row.prob_up_1h === "number" ? row.prob_up_1h.toFixed(2) : "—"}
              </span>
            </div>

          </div>
        </div>
        {showReasons && Array.isArray(row.reasons) && <ReasonList reasons={row.reasons} />}
        <div className="mt-3 text-[11px] text-slate-500">Updated {time}</div>
      </CardContent>
    </Card>
  );
};

const TableView: React.FC<{
  rows: InstrumentRow[]; showReasons: boolean; showTarget: boolean;
  prices?: Record<string, { price: number; lastTs: number }>;
  brokerOffsetMin: number;
}> = ({ rows, showReasons, showTarget, prices,brokerOffsetMin}) => {
  // Build display rows: if predictions are empty, make rows from price symbols
  const displayRows: InstrumentRow[] =
    rows && rows.length
      ? rows
      : (prices ? Object.keys(prices).map(sym => ({ symbol: sym } as InstrumentRow)) : []);

  // OPTIONAL: stable sort A?Z when we’re in fallback
  displayRows.sort((a, b) => (a.symbol || "").localeCompare(b.symbol || ""));


  return (
    <div className="overflow-x-auto rounded-2xl border border-slate-700/60">
      <table className="min-w-full divide-y divide-slate-700/60">
        <thead className="bg-slate-900/60 text-slate-300 text-xs">
          <tr>
            <th className="px-4 py-3 text-left font-medium">Instrument</th>
            <th className="px-4 py-3 text-left font-medium">Status</th>
            <th className="px-4 py-3 text-left font-medium">Price (M1)</th>
            <th className="px-4 py-3 text-left font-medium">1h Target</th>
            <th className="px-4 py-3 text-left font-medium">ProbUp</th>
            <th className="px-4 py-3 text-left font-medium">Reasons</th>
            <th className="px-4 py-3 text-left font-medium">Updated (broker)</th>
            <th className="px-4 py-3 text-left font-medium">Device</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-slate-800/60 text-sm">
          {displayRows.map((r) => {
  const pill = biasToPill(r.bias);
  
  // prefer the row's broker-aligned timestamp; fallback to live price ts
  const rawPriceTs = prices?.[r.symbol]?.lastTs ?? r.updated_broker_ts;
  const priceTsMs  = toMs(rawPriceTs);

  // per-row offset if present; else page-level offset
  const rowOffsetMin = (r?.broker_tz_offset_min ?? r?.tz_offset_min ?? brokerOffsetMin);

  // format the "Updated (broker)" cell strictly from the row's broker ts
  const updatedTime = r.updated_broker_ts
    ? fmtBrokerTime(toMs(r.updated_broker_ts), rowOffsetMin)
    : "—";

  const priceNow = prices?.[r.symbol]?.price;
  
  return (
    <tr key={r.symbol} className="bg-slate-800/40 hover:bg-slate-800/60">
      {/* Instrument */}
      <td className="px-4 py-3 text-slate-200 font-medium">{r.symbol}</td>

      {/* Status (pill) */}
      <td className="px-4 py-3">
        <span className={pill.className}>{pill.text}</span>
      </td>

      {/* Price (M1) */}
      <td className="px-4 py-3">
        <div className="tabular-nums text-slate-100"> 
          {Number.isFinite(priceNow as any)
            ? fmtPrice(r.symbol, priceNow as number, decimalsFromPrice(priceNow as number))
            : "—"}
        </div>
        <div className="text-[11px] text-slate-500">
           {fmtBrokerTime(
             priceTsMs ?? toMs(r.updated_broker_ts),
             rowOffsetMin
           )}
        </div>
      </td>

      {/* 1h Target */}
      {/* 1h Target (safe/defensive) */}
<td className="px-4 py-3 text-slate-200">
  {showTarget ? (
    (() => {
      // live price (preferred) or row's own price if present
      const px = typeof (prices?.[r.symbol]?.price) === "number"
        ? (prices as any)[r.symbol].price
        : (typeof r.price === "number" ? r.price : NaN);

      // format percentage (already signed)
      const pctText = pctSym(r.expected_move_pct_1h as any, r.symbol);

      // compute target robustly (accepts explicit target, pips, or pct)
      // plus: if target equals current price (within 0.5 pip), fall back to percent
      const target = (() => {
        try {
          const pip = pipSize(r.symbol);
          const pxOk = Number.isFinite(px);

          // 1) explicit backend target
          if (typeof r.target_price_1h === "number") {
            const sameAsPx = pxOk && Math.abs(r.target_price_1h - (px as number)) < (0.5 * pip);
            if (!sameAsPx) return r.target_price_1h;
            // else fall through to compute from %/pips
          }

          // 2) pips-based
          if (pxOk && typeof r.target_pips === "number" && r.decision) {
            const step = pip * Math.abs(r.target_pips);
            return r.decision === "BUY" ? (px as number) + step : (px as number) - step;
          }

          // 3) percent-based
          if (pxOk && typeof r.expected_move_pct_1h === "number") {
            const dir =
              r.decision === "SELL" ? -1 :
              r.decision === "BUY"  ? +1 :
              Math.sign(r.expected_move_pct_1h);
            const pct = Math.abs(r.expected_move_pct_1h) / 100;
            return (px as number) * (1 + dir * pct);
          }

          return null;
        } catch {
          return null;
        }
      })();


      return (
        <span>
          {pctText} {"-"}{" "}
          <span className="tabular-nums">
            {target != null
              ? fmtPrice(r.symbol, target, decimalsFromPrice(px as number))
              : "—"}
          </span>
        </span>
      );
    })()
  ) : (
    <span className="text-slate-500">—</span>
  )}
</td>


      {/* ProbUp */}
      <td className="px-4 py-3 tabular-nums text-slate-200">
        {r.prob_up_1h != null ? r.prob_up_1h.toFixed(2) : "—"}
      </td>

      {/* Reasons */}
      <td className="px-4 py-3 text-slate-300">
        {showReasons && Array.isArray(r.reasons) ? r.reasons.slice(0,2).join("; ") : "—"}
      </td>

      {/* Updated (broker) */}
      <td className="px-4 py-3 text-slate-400">{updatedTime}
        {r.updated_broker_ts ? fmtBrokerTime(toMs(r.updated_broker_ts), rowOffsetMin) : "—"}
      </td>

      {/* Device */}
      <td className="px-4 py-3 text-slate-400">{r.using_device ? `${r.using_device.slice(0,7)}…` : "—"}</td>
    </tr>
  );
})}

        </tbody>
      </table>
    </div>
  );
};

/* -------------------------------------------------
 * Main component: AI-powered forecasts view
 * ------------------------------------------------- */
export default function PredictionMeter() {
  // State
  const [view, setView] = React.useState<"cards" | "table">("cards");
  const [showReasons, setShowReasons] = React.useState(true);
  const [showTarget, setShowTarget] = React.useState(true);
  const [tf, setTf] = React.useState<TfLabel>("M15");
  const [autoRefreshMin, setAutoRefreshMin] = React.useState<number>(15);
  const { rows, error, lastRefreshAt, refetch } = usePredictRows(tf);
  // [PATCH] Page-level broker offset in minutes (fallback 120 = UTC+02:00)
  const brokerOffsetMin =
    (rows?.[0]?.broker_tz_offset_min as number | undefined) ??
    (rows?.[0]?.tz_offset_min as number | undefined) ??
    120;

  const [sort, setSort] = React.useState<"strength" | "prob" | "move" | "az">("strength");
   
  
  // Live rows from backend (1-min auto)
  const priceRefreshMs = autoRefreshMin > 0 ? autoRefreshMin * 60_000 : 0;
  
  const { prices, updatedAt: priceUpdatedAt, refetch: refetchPrices } = useLivePrices(priceRefreshMs);
  

  // Sorted view of live rows
  const sortedRows = React.useMemo(() => {
    const arr = [...rows];
    switch (sort) {
      case "strength":
        return arr.sort((a, b) => biasToScore(b.bias) - biasToScore(a.bias));
      case "prob":
        return arr.sort((a, b) => b.prob_up_1h - a.prob_up_1h);
      case "move":
        return arr.sort((a, b) => Math.abs(b.expected_move_pct_1h) - Math.abs(a.expected_move_pct_1h));
      case "az":
        return arr.sort((a, b) => a.symbol.localeCompare(b.symbol));
    }
  }, [rows, sort]);


  const brokerAbbr = sortedRows[0]?.broker_tz_abbr || "—";
  const device = sortedRows[0]?.using_device || "—";

  // Header clock (chosen to be your local time for clarity; you can wire to broker tz later)
  const now = useWallClock();
  const londonTime = React.useMemo(() => new Intl.DateTimeFormat("en-GB", {
    hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false, timeZone: "Europe/London"
  }).format(now), [now]);
  const nyTime = React.useMemo(() => new Intl.DateTimeFormat("en-GB", {
    hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false, timeZone: "America/New_York"
  }).format(now), [now]);
  // Live Broker clock (derived from top row's broker_tz_abbr)
  const brokerTzIana = React.useMemo(
    () => brokerAbbrToIana(brokerAbbr),
    [brokerAbbr]
  );

  const brokerTime = React.useMemo(
   () =>
     new Intl.DateTimeFormat("en-GB", {
       hour: "2-digit",
       minute: "2-digit",
       second: "2-digit",
       hour12: false,
       timeZone: brokerTzIana || Intl.DateTimeFormat().resolvedOptions().timeZone,
     }).format(now),
   [now, brokerTzIana]
  );
  // [PATCH] Live broker clock based on numeric offset
  const brokerClock = useOffsetClock(brokerOffsetMin);



  return (
  <div className="mx-auto max-w-7xl px-4 py-8">
    {/* Header */}
    <div className="flex flex-wrap items-center justify-between gap-4">
      <div>
        <h1 className="text-2xl font-semibold text-slate-100">XTL • AI-powered forecasts</h1>
        <p className="mt-1 text-sm text-slate-400">
          Broker: <span className="font-medium text-slate-200">{brokerAbbr ?? "—"}</span> ·
          Device: <span className="font-mono text-slate-300">
            {device ? `${device.slice(0, 8)}…` : "—"}
          </span>
        </p>
        <p className="mt-1 text-xs text-slate-500">
          Intraday AI forecasts for FX & Gold. Use this with the main Dashboard (Opportunities = 1%) to plan entries only when there is real room to move.
        </p>
      </div>
      <div className="flex items-center gap-2 text-sm text-slate-300">
        <span className="px-2 py-1 rounded-md bg-slate-800/60 border border-slate-700/60">London {londonTime}</span>
        <span className="px-2 py-1 rounded-md bg-slate-800/60 border border-slate-700/60">New York {nyTime}</span>
        <span className="px-2 py-1 rounded-md bg-slate-800/60 border border-slate-700/60">
          Broker {fmtUtcOffset(brokerOffsetMin)} {brokerClock}
        </span>
      </div>
    </div>

    {/* TF Card Strip */}
    <section className="mt-4 mb-2">
      <TFStrip tf={tf} setTf={setTf} enabled={false} />
    </section>

    {/* Controls */}
    <Card className="mt-4">
      <CardContent className="flex flex-wrap items-center gap-4">
        {/* Toggles */}
        <div className="flex items-center gap-3">
          <label className="flex items-center gap-2 text-sm text-slate-300">
            <input
              type="checkbox"
              className="accent-emerald-400"
              checked={showReasons}
              onChange={(e) => setShowReasons(e.target.checked)}
            />
            Show reasons
          </label>
          <label className="flex items-center gap-2 text-sm text-slate-300">
            <input
              type="checkbox"
              className="accent-emerald-400"
              checked={showTarget}
              onChange={(e) => setShowTarget(e.target.checked)}
            />
            Show 1h target
          </label>
          <label className="flex items-center gap-2 text-sm text-slate-500">
            <input type="checkbox" className="accent-emerald-400" disabled />
            Confidence bands (later)
          </label>
        </div>

        {/* View */}
        <div className="flex items-center gap-2 text-sm ml-auto">
          <button
            onClick={() => setView("cards")}
            className={`px-3 py-1.5 rounded-md border ${
              view === "cards"
                ? "border-emerald-400/30 bg-emerald-400/10 text-emerald-200"
                : "border-slate-700/60 bg-slate-800/60 text-slate-300"
            }`}
          >
            Cards
          </button>
          <button
            onClick={() => setView("table")}
            className={`px-3 py-1.5 rounded-md border ${
              view === "table"
                ? "border-emerald-400/30 bg-emerald-400/10 text-emerald-200"
                : "border-slate-700/60 bg-slate-800/60 text-slate-300"
            }`}
          >
            Table
          </button>
        </div>

        {/* Sort */}
        <div className="flex items-center gap-2 text-sm">
          <span className="text-slate-400">Sort</span>
          <select
            value={sort}
            onChange={(e) => setSort(e.target.value as any)}
            className="bg-slate-900/60 border border-slate-700/60 text-slate-200 rounded-md px-2 py-1.5"
          >
            <option value="strength">Strength</option>
            <option value="prob">ProbUp</option>
            <option value="move">|ExpectedMove|</option>
            <option value="az">A?Z</option>
          </select>
        </div>

        {/* TF + Auto refresh + Manual refresh */}
        <div className="flex items-center gap-3 text-sm">
          <span className="text-slate-400">TF</span>
          <select
            value={tf}
            onChange={(e) => setTf(e.target.value as any)}
            className="bg-slate-900/60 border border-slate-700/60 text-slate-200 rounded-md px-2 py-1.5"
          >
            <option value="M1">M1</option>
            <option value="M5">M5</option>
            <option value="M15">M15</option>
            <option value="H1">H1</option>
            <option value="H4">H4</option>
          </select>

          <span className="text-slate-400">Auto-refresh</span>
          <select
            value={autoRefreshMin}
            onChange={(e) => setAutoRefreshMin(parseInt(e.target.value))}
            className="bg-slate-900/60 border border-slate-700/60 text-slate-200 rounded-md px-2 py-1.5"
          >
            <option value={0}>Off</option>
            <option value={1}>1m</option>
            <option value={5}>5m</option>
            <option value={10}>10m</option>
            <option value={15}>15m</option>
          </select>

          <button
            onClick={() => {
              void refetch();
              void refetchPrices();
            }}
            className="px-3 py-1.5 rounded-md border border-slate-700/60 bg-slate-800/60 text-slate-200 hover:bg-slate-800"
          >
            Refresh
          </button>

        </div>
      </CardContent>
    </Card>

    {/* Content */}
    {view === "cards" ? (() => {
      const baseRows: InstrumentRow[] =
        (sortedRows && sortedRows.length)
          ? sortedRows
          : (prices ? Object.keys(prices).map(sym => ({ symbol: sym } as InstrumentRow)) : []);

      return (
       <div className="mt-6 grid grid-cols-1 sm:grid-cols-2 xl:grid-cols-4 gap-5">
         {baseRows.map((r) => (
           <InstCard
             key={r.symbol}
             row={r}
             showReasons={showReasons}
             showTarget={showTarget}
             livePrice={prices?.[r.symbol]?.price}
           />
         ))}
       </div>
      );
    })() : (
      <div className="mt-6">
        <TableView
          rows={sortedRows}
          showReasons={showReasons}
          showTarget={showTarget}
          prices={prices}
          brokerOffsetMin={brokerOffsetMin}
        />
      </div>
    )}

    {/* Footer note + status */}
    <p className="mt-6 text-xs text-slate-500">
      
    </p>
    {error ? (
     <p className="mt-4 text-sm text-rose-400">Error: {error}</p>
    ) : (
      <p className="mt-4 text-xs text-slate-500">
        {priceUpdatedAt
          ? `Price updated: ${new Date(priceUpdatedAt).toLocaleTimeString()}`
          : "Waiting for live price…"}
      </p>
    )}

     </div>
);
}
