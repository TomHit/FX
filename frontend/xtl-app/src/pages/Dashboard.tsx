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
// Symbol ? decimal places: JPY=3, XAUUSD=2, majors=5
function priceDecimals(sym: string): number {
  const s = (sym || "").toUpperCase();
  if (s === "XAUUSD") return 2;
  if (s.endsWith("JPY")) return 3;
  return 5; // most FX majors
}

function fmtPrice(sym: string, px: number | null | undefined, decimals?: number): string {
  if (!Number.isFinite(px as any)) return "";

  const d = Number.isFinite(decimals as any) ? (decimals as number) : priceDecimals(sym);

  // Avoid floating-point artifacts before toFixed
  const k = Math.pow(10, d);
  const n = Math.round((px as number) * k) / k;

  return n.toFixed(d);
}


function calcTargetPrice(row: {
  symbol: string;
  price?: number;
  decision?: "BUY" | "SELL" | "" | string;
  direction?: "up" | "down" | "flat";
  target_price_1h?: number;
  target_pips?: number;              // may be pip-count OR price-delta
  expected_move_pct_1h?: number;     // percent, e.g. 0.40 => +0.40%
  st_trend_label?: Bias;
  bias?: Bias;
}) {
  const px = Number(row.price);
  const ps = pipSize(row.symbol); // 0.0001 (majors), 0.01 (JPY), 0.1 (XAU)
  if (!Number.isFinite(px)) {
    if (Number.isFinite(row.target_price_1h as any)) return row.target_price_1h as number;
    return null;
  }

  // A) Prefer explicit backend target (stable value)
  if (Number.isFinite((row as any).target_price)) {
    const t = Number((row as any).target_price);
    if (Number.isFinite(t)) return t;
  }

  const dec = String(row.decision || "").toUpperCase();

  // B) Pips-based fallback (supports pip count OR direct price-delta)
  if (Number.isFinite(row.target_pips as any)) {
    const tp = Number(row.target_pips);
    let d = Math.abs(tp) >= 5 ? tp * ps : tp;
    if (dec === "SELL") d = -Math.abs(d);
    else if (dec === "BUY") d = +Math.abs(d);
    return px + d;
  }

  // C) Percent-based fallback (1h) â€” direction should follow ST trend when available
  if (Number.isFinite(row.expected_move_pct_1h as any)) {
    const pctAbs = Math.abs(Number(row.expected_move_pct_1h)) / 100;
    const stDir = trendDirFromBiasLabel(row.st_trend_label ?? row.bias);

    const dir =
      stDir !== 0 ? stDir :
      dec === "SELL" ? -1 :
      dec === "BUY"  ? +1 :
      0;

    if (dir === 0) return null;
    return px * (1 + dir * pctAbs);
  }

  return null;
}




type TfLabel = "M1" | "M5" | "M15" | "H1" | "H4";

type PredictRow = {
  symbol: string;

  // headline decisioning (from backend)
  label?: string;
  decision?: "BUY" | "SELL" | "ABSTAIN" | string;
  confidence?: "low" | "medium" | "high" | string;
  prob_up?: number; // canonical
  p_up?: number;    // alias
  probUp?: number;  // alias

  // canonical model-driven target fields (preferred)
  basis_price?: number;
  target_price?: number;
  expected_move_pct?: number;
  model_source?: string; // "ml" | "na" etc.
  target_close_ts?: number; // epoch ms
  horizon_min?: number;      // minutes

  // backward compat / fallbacks
  expected_move_pct_1h?: number;
  target_price_1h?: number;
  basis_price_1h?: number;

  expected_move_pct_4h?: number;
  target_price_4h?: number;

  // reasons
  reasons?: string[];
  reasons_h1?: string[];
  reasons_h4?: string[];

  // raw payloads (optional)
  raw?: any;
  raw_h4?: any;

  // misc
  update_tf?: string;
  score?: number;
  reason?: string;
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
  if (!ts && ts !== 0) return "";
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
            const prob_up: number | undefined =
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
               prob_up,
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
    if (!enabled) return;                 // ? dont poll when disabled
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
  const dir = r0?.decision === "BUY" ? "up" : r0?.decision === "SELL" ? "down" : "flat";
  const prob = typeof r0?.prob_up === "number" ? Math.round(r0.prob_up * 100) : null;
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
        {prob != null ? `ProbUp: ${prob}%` : "ProbUp: "}
      </div>
      <div className="mt-0.5 text-[11px] text-slate-400">
        {target != null ? `Target(1h): ${target}` : "Target(1h): "}
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
        {lastAt ? `Updated: ${new Date(lastAt).toLocaleTimeString()}` : (enabled ? "Loading" : "Paused")}
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

type InstrumentRow = {
  symbol: string;
  price: number;
  bias: Bias;
  short_term_score?: number;
  long_term_score?: number;
  st_trend_label?: Bias;   // ST trend label from backend
  ht_trend_label?: Bias;   // HT trend label from backend
  direction: Direction;
  prob_up: number;               // 0..1
  expected_move_pct?: number;   // distance (any horizon)
  target_price?: number;        // price target (any horizon)
  horizon_min?: number;         // ML-predicted time to hit

  // --- legacy (read-only, compat) ---
  expected_move_pct_1h?: number;
  target_price_1h?: number;
  expected_move_pct_4h?: number;
  target_price_4h?: number;

  // reasons
  reasons: string[];                // legacy / generic reasons (keep for compat)
  reasons_h1?: string[];            // ST (1h) reasons
  reasons_h4?: string[];            // HT (4h) reasons

  confidence_band?: number;         // ATR units
  updated_broker_ts: number;        // epoch ms
  broker_tz_abbr: string;           // e.g., "EET"
  using_device: string;             // device id
  broker_tz_offset_min?: number;
  tz_offset_min?: number;

  // ?? newly added fields used in target calc
  decision?: "BUY" | "SELL" | "";   // backend decision for direction
  target_pips?: number;             // may be pip-count OR price-delta
  structure?: string;
};


// Map backend /trend/predict/all row -> InstrumentRow (defensive defaults)
function mapApiRowToInstrument(r: any): InstrumentRow {
  const label: string = (r?.label || "").toString();

  // --- short-term / long-term trend scores ---
  const st =
    typeof r?.st_trend_score === "number"
      ? r.st_trend_score
      : (typeof r?.score_raw_tech === "number" ? r.score_raw_tech : null);

  const ht =
    typeof r?.ht_trend_score === "number"
      ? r.ht_trend_score
      : (typeof r?.score_tech === "number" ? r.score_tech : null);

  // explicit labels (if backend sends them)
  const stLabelApi: string = (r?.st_trend_label || "").toString();
  const htLabelApi: string = (r?.ht_trend_label || "").toString();

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

  // --- probabilities (legacy + new) ---
  const prob_up =
    typeof r?.prob_up === "number"      ? r.prob_up :
    typeof r?.prob_up === "number"   ? r.prob_up :
    typeof r?.p_up === "number"         ? r.p_up :
    null;

  // --- NEW ML canonical fields ---
  const horizon_min =
    typeof r?.horizon_min === "number" ? r.horizon_min : undefined;

  const raw = (r as any)?.raw ?? null;

  // Pull pct from canonical -> raw.predMovePct -> legacy expected_move_pct_1h -> score (very old fallback)
  const pct_raw =
    typeof r?.expected_move_pct === "number"
      ? r.expected_move_pct
      : (typeof (raw as any)?.predMovePct === "number"
          ? (raw as any).predMovePct
          : (typeof r?.expected_move_pct_1h === "number"
              ? r.expected_move_pct_1h
              : (typeof r?.score === "number" ? r.score : null)));

  // Force sign to match decision (avoids "SELL but target above price")
  const dec = String(decision || "").toUpperCase();
  const basis_price =
  typeof r?.basis_price === "number"
    ? r.basis_price
    : typeof (raw as any)?.lastClose === "number"
      ? (raw as any).lastClose
      : typeof r?.basis_price_1h === "number"
        ? r.basis_price_1h
        : null;

  const sign = dec === "SELL" ? -1 : dec === "BUY" ? 1 : (typeof pct_raw === "number" ? (pct_raw >= 0 ? 1 : -1) : 1);
  const expected_move_pct = typeof pct_raw === "number" ? Math.abs(pct_raw) * sign : null;

  let target_price =
    typeof r?.target_price === "number"
      ? r.target_price
      : (typeof (raw as any)?.targetPrice === "number"
          ? (raw as any).targetPrice
          : (typeof r?.target_price_1h === "number"
              ? r.target_price_1h
              : null));

  // If target contradicts decision, recompute from basis_price + signed pct
  if (typeof basis_price === "number" && typeof expected_move_pct === "number") {
    const implied = basis_price * (1.0 + expected_move_pct / 100.0);
    if (dec === "SELL" && typeof target_price === "number" && target_price > basis_price) target_price = implied;
    if (dec === "BUY" && typeof target_price === "number" && target_price < basis_price) target_price = implied;
    if (target_price == null) target_price = implied;
  }

  // --- legacy 4h (context only) ---
  const exp_move_4h =
    typeof r?.expected_move_pct_4h === "number" ? r.expected_move_pct_4h : undefined;

  const target_price_4h =
    typeof r?.target_price_4h === "number" ? r.target_price_4h : undefined;

  // --- reasons ---
  const reasons_h1: string[] = Array.isArray(r?.reasons_h1)
    ? r.reasons_h1.map(String)
    : Array.isArray(r?.reasons)
      ? r.reasons.map(String)
      : (r?.reason ? [String(r.reason)] : []);

  const reasons_h4: string[] = Array.isArray(r?.reasons_h4)
    ? r.reasons_h4.map(String)
    : [];

  const reasons: string[] =
    reasons_h1.length ? reasons_h1 :
    (reasons_h4.length ? reasons_h4 : []);

  // --- broker / device meta ---
  const tz_off =
    (typeof r?.broker_tz_offset_min === "number" ? r.broker_tz_offset_min :
     typeof r?.tz_offset_min === "number"        ? r.tz_offset_min : undefined);

  return {
    symbol: String(r?.symbol || ""),
    price: NaN as any, // priced separately from M1 feed

    bias,
    short_term_score: st ?? undefined,
    long_term_score: ht ?? undefined,

    st_trend_label:
      stLabelApi === "Strong Bullish" ||
      stLabelApi === "Bullish" ||
      stLabelApi === "Neutral" ||
      stLabelApi === "Bearish" ||
      stLabelApi === "Strong Bearish"
        ? (stLabelApi as Bias)
        : undefined,

    ht_trend_label:
      htLabelApi === "Strong Bullish" ||
      htLabelApi === "Bullish" ||
      htLabelApi === "Neutral" ||
      htLabelApi === "Bearish" ||
      htLabelApi === "Strong Bearish"
        ? (htLabelApi as Bias)
        : undefined,

    direction,

    // --- probabilities / ML outputs ---
    prob_up: typeof prob_up === "number" ? prob_up : NaN as any,

    // canonical (ML-driven)
    expected_move_pct: typeof expected_move_pct === "number" ? expected_move_pct : undefined,
    target_price: typeof target_price === "number" ? target_price : undefined,
    horizon_min,

    // legacy compatibility (read-only)
    expected_move_pct_1h:
      typeof expected_move_pct === "number" ? expected_move_pct : 0,

    target_price_1h:
      typeof target_price === "number"
        ? target_price
        : (typeof r?.target === "number" ? r.target : undefined),

    expected_move_pct_4h: exp_move_4h,
    target_price_4h: target_price_4h,

    decision: typeof r?.decision === "string" ? (r.decision as any) : "",
    target_pips: typeof r?.target_pips === "number" ? r.target_pips : undefined,

    reasons,
    reasons_h1,
    reasons_h4,

    confidence_band: undefined,

    updated_broker_ts:
      (typeof r?.updated_broker_ts === "number" ? r.updated_broker_ts :
       typeof r?.server_now_ms === "number"      ? r.server_now_ms : 0),

    broker_tz_abbr: String(r?.broker_tz_abbr || ""),
    using_device: String(r?.using_device || ""),

    broker_tz_offset_min: tz_off,
    tz_offset_min: tz_off,

    structure: r?.structure ? String(r.structure) : undefined,
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
function trendDirFromBiasLabel(b?: Bias): number {
  if (!b) return 0;
  if (b === "Strong Bullish" || b === "Bullish") return +1;
  if (b === "Strong Bearish" || b === "Bearish") return -1;
  return 0;
}

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
function scoreToTrendLabel(s?: number): string {
  if (typeof s !== "number") return "";
  if (s >= 0.6) return "Strong Bullish";
  if (s >= 0.2) return "Bullish";
  if (s > -0.2) return "Neutral";
  if (s > -0.6) return "Bearish";
  return "Strong Bearish";
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
  if (typeof n !== "number" || isNaN(n)) return "";
  const sign = n >= 0 ? "+" : "";
  return `${sign}${n.toFixed(2)}%`;
}

function pctSym(n?: number, sym?: string): string {
  if (typeof n !== "number" || isNaN(n)) return "";
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
  row: InstrumentRow;
  showReasons: boolean;
  showTarget: boolean; // kept for API compatibility, not used directly
  livePrice?: number;
}> = ({ row, showReasons, livePrice }) => {
  // --- Trend labels as Bias ---
  const ht: Bias =
    (row.ht_trend_label as Bias | undefined) ?? row.bias;

  const stRaw =
    row.st_trend_label ||
    scoreToTrendLabel(row.short_term_score);

  const st: Bias =
    stRaw === "Strong Bullish" ||
    stRaw === "Bullish" ||
    stRaw === "Neutral" ||
    stRaw === "Bearish" ||
    stRaw === "Strong Bearish"
      ? (stRaw as Bias)
      : "Neutral";

  const htPill = biasToPill(ht);
  const stPill = biasToPill(st);

  // --- Direction chip ---
  const dirLabel =
    row.decision === "BUY" ? "UP" :
    row.decision === "SELL" ? "DOWN" : "FLAT";

  const dirArrow =
    row.decision === "BUY" ? "?" :
    row.decision === "SELL" ? "?" : "";

  const dirClass =
    row.decision === "BUY"
      ? "bg-emerald-500/10 text-emerald-300 border-emerald-400/30"
      : row.decision === "SELL"
      ? "bg-rose-500/10 text-rose-300 border-rose-400/30"
      : "bg-slate-500/10 text-slate-300 border-slate-400/30";

  // --- ST / HT reasons, with structure + RVOL extracted as chips ---
  const stReasonsAll = row.reasons_h1 || row.reasons || [];
  const htReasons = row.reasons_h4 || [];

  let structureLine: string | undefined;
  let rvolLine: string | undefined;
  const otherStReasons: string[] = [];

  for (const line of stReasonsAll) {
    const lower = line.toLowerCase();
    if (!structureLine && lower.includes("structure")) {
      structureLine = line;
      continue;
    }
    if (!rvolLine && lower.includes("rvol")) {
      rvolLine = line;
      continue;
    }
    otherStReasons.push(line);
  }

  const fallbackStructure =
    st.includes("Bullish") ? "bullish 1h structure" :
    st.includes("Bearish") ? "bearish 1h structure" :
    "neutral 1h structure";

  const structureText =
    row.structure && row.structure.trim().length > 0
      ? row.structure
      : (structureLine || fallbackStructure);

  const { time } = fmtTZ(
    row.updated_broker_ts,
    row.broker_tz_abbr === "EET"
      ? "Europe/Helsinki"
      : Intl.DateTimeFormat().resolvedOptions().timeZone
  );

  // Accent ring based on direction
  const accentRing =
    row.decision === "BUY"
      ? "ring-1 ring-emerald-500/40"
      : row.decision === "SELL"
      ? "ring-1 ring-rose-500/40"
      : "ring-1 ring-slate-500/30";

  const stP = biasToPill(row.st_trend_label as any);
  const htP = biasToPill(row.ht_trend_label as any);
  const stLabel = stP.text;
  const htLabel = htP.text;
  const stPillClass = stP.className;
  const htPillClass = htP.className;


  return (
    <Card className={`relative overflow-hidden hover:translate-y-[1px] transition-transform bg-gradient-to-b from-slate-950/80 to-slate-900/50 ${accentRing}`}>
      {/* subtle top glow */}
      <div className="pointer-events-none absolute inset-x-0 -top-10 h-16 bg-gradient-to-b from-emerald-400/10 via-transparent to-transparent" />

      <CardContent className="relative">
        {/* Header: symbol + price */}
        <div className="flex items-start justify-between">
          <div className="text-sm text-slate-300 tracking-wide">
            {row.symbol}
          </div>
          <div className="text-xl tabular-nums text-slate-50">
            {Number.isFinite(livePrice as any)
              ? fmtPrice(row.symbol, livePrice!, decimalsFromPrice(livePrice))
              : ""}
          </div>
        </div>

        
        
        {/* Trend (ST above / HT below) */}
        <div className="mt-3 overflow-hidden rounded-xl border border-slate-800/60 bg-slate-950/25">
          <div className="divide-y divide-slate-800/70">
            {/* ST */}
            <div className="flex items-center justify-between px-3 py-2">
              <div className="flex items-center gap-2">
                <span className={stPillClass}>{stLabel}</span>
                <span className="text-[12px] text-slate-400">ST Trend</span>
              </div>
            </div>

            {/* HT */}
            <div className="flex items-center justify-between px-3 py-2">
              <div className="flex items-center gap-2">
                <span className={htPillClass}>{htLabel}</span>
                <span className="text-[12px] text-slate-400">HT Trend</span>
              </div>
            </div>
          </div>
        </div>


{/* ST / HT details split: above line = ST, below line = HT */}
<div className="mt-3 overflow-hidden rounded-xl border border-slate-800/60 bg-slate-950/25">
  {/* -------- ST (top) -------- */}
  <div className="px-3 py-3">
    {/* Key chips (ST only) */}
    <div className="flex flex-wrap gap-2 text-[12px]">
      <span className="inline-flex items-center rounded-full border border-slate-600/60 bg-slate-900/60 px-2 py-1 text-slate-200">
        {structureText}
      </span>
      {rvolLine && (
        <span className="inline-flex items-center rounded-full border border-amber-400/30 bg-amber-400/5 px-2 py-1 text-amber-200">
          {rvolLine}
        </span>
      )}
    </div>

    
    {/* ST Reasons */}
    {showReasons && (
      otherStReasons.length > 0 ? (
        <div className="mt-3 text-[13px] text-slate-200">
          <div className="font-semibold mb-1">ST Reasons</div>
          <ul className="space-y-1">
            {otherStReasons.slice(0, 3).map((r, i) => (
              <li key={i} className="flex items-start gap-2">
                <span className="mt-1 h-1.5 w-1.5 rounded-full bg-emerald-400/70" />
                <span>{r}</span>
              </li>
            ))}
          </ul>
        </div>
     ) : (
        <div className="mt-3 text-[12px] text-slate-500">ST Reasons not available</div>
      )
    )}
    </div>


  {/* -------- Divider line (always) -------- */}
  <div className="border-t border-slate-800/70" />

  {/* -------- HT (bottom) -------- */}
  <div className="px-3 py-3">
    {showReasons && htReasons.length > 0 ? (
      <div className="text-[13px] text-slate-300">
        <div className="font-semibold mb-1">HT Reasons</div>
        <ul className="space-y-1">
          {htReasons.slice(0, 3).map((r, i) => (
            <li key={i} className="flex items-start gap-2">
              <span className="mt-1 h-1.5 w-1.5 rounded-full bg-slate-500/80" />
              <span>{r}</span>
            </li>
          ))}
        </ul>
      </div>
    ) : (
      <div className="text-[12px] text-slate-500">HT Reasons not available</div>
    )}
  </div>
</div>

        {/* Updated time */}
        <div className="mt-4 text-[11px] text-slate-500">
          Updated {time}
        </div>
      </CardContent>
    </Card>
  );
};


const TableView: React.FC<{
  rows: InstrumentRow[];
  showReasons: boolean;
  showTarget: boolean;
  prices?: Record<string, { price: number; lastTs: number }>;
  brokerOffsetMin: number;
}> = ({ rows, showReasons, showTarget, prices, brokerOffsetMin }) => {
  // Build display rows: if predictions are empty, make rows from price symbols
  const displayRows: InstrumentRow[] =
    rows && rows.length
      ? rows
      : prices
        ? Object.keys(prices).map((sym) => ({ symbol: sym } as InstrumentRow))
        : [];

  // Stable sort A-Z when in fallback
  displayRows.sort((a, b) => (a.symbol || "").localeCompare(b.symbol || ""));

  const [pulseOpen, setPulseOpen] = React.useState(false);
  const [pulseRow, setPulseRow] = React.useState<InstrumentRow | null>(null);

  const openPulse = (r: InstrumentRow) => {
    setPulseRow(r);
    setPulseOpen(true);
  };
  const closePulse = () => {
    setPulseOpen(false);
  };

  return (
    <>
      <div className="overflow-x-auto rounded-2xl border border-slate-700/60 bg-slate-950/60 shadow-lg shadow-black/40">
        <table className="min-w-full divide-y divide-slate-700/60">
          <thead className="bg-slate-900/60 text-xs text-slate-300">
            <tr>
              <th className="px-4 py-3 text-left font-medium">Instrument</th>
              <th className="px-4 py-3 text-left font-medium">HT Trend</th>
              <th className="px-4 py-3 text-left font-medium">ST Trend</th>
              <th className="px-4 py-3 text-left font-medium">Price (M1)</th>
              <th className="px-4 py-3 text-left font-medium">Expected move (1h)</th>
              <th className="px-4 py-3 text-left font-medium">Expected move (4h)</th>
              <th className="px-4 py-3 text-left font-medium">ProbUp</th>
              <th className="px-4 py-3 text-left font-medium">Pulse</th>
              <th className="px-4 py-3 text-left font-medium">Reasons</th>
            </tr>
          </thead>

          <tbody className="divide-y divide-slate-800/60 text-sm">
            {displayRows.map((r) => {
              const nowMs = Date.now();

              const isExpiredUI =
                typeof r.horizon_min === "number" &&
                typeof r.updated_broker_ts === "number" &&
                nowMs > r.updated_broker_ts + r.horizon_min * 60_000;

              if (isExpiredUI) return null;

              const htLabel: Bias = (r.ht_trend_label as Bias | undefined) ?? (r.bias as Bias);
              const pill = biasToPill(htLabel);

              const rawPriceTs = prices?.[r.symbol]?.lastTs ?? r.updated_broker_ts;
              const priceTsMs = toMs(rawPriceTs);

              const rowOffsetMin =
                (r as any)?.broker_tz_offset_min ??
                (r as any)?.tz_offset_min ??
                brokerOffsetMin;

              const priceNow = prices?.[r.symbol]?.price;

              return (
                <tr key={r.symbol} className="bg-slate-800/40 hover:bg-slate-800/60">
                  {/* Instrument */}
                  <td className="px-4 py-3 font-medium text-slate-200">{r.symbol}</td>

                  {/* HT Trend */}
                  <td className="px-4 py-3">
                    <span className={pill.className}>{pill.text}</span>
                  </td>

                  {/* ST Trend pill */}
                  <td className="px-4 py-3">
                    {(() => {
                      const lbl: Bias =
                        (r.st_trend_label as Bias | undefined) ??
                        (scoreToTrendLabel(r.short_term_score) as Bias);
                      const pillSt = biasToPill(lbl);
                      return <span className={pillSt.className}>{pillSt.text}</span>;
                    })()}
                  </td>

                  {/* Price (M1) */}
                  <td className="px-4 py-3">
                    <div className="tabular-nums text-slate-100">
                      {Number.isFinite(priceNow as any)
                        ? fmtPrice(
                            r.symbol,
                            priceNow as number,
                            decimalsFromPrice(priceNow as number)
                          )
                        : ""}
                    </div>
                    <div className="text-[11px] text-slate-500">
                      {fmtBrokerTime(priceTsMs ?? toMs(r.updated_broker_ts), rowOffsetMin)}
                    </div>
                  </td>

                  {/* Expected move (1h) - ST based + guaranteed side */}
                  <td className="px-4 py-3 text-slate-200">
                    {showTarget ? (
                      (() => {
                        const px =
                          typeof prices?.[r.symbol]?.price === "number"
                            ? prices[r.symbol].price
                            : typeof r.price === "number"
                              ? r.price
                              : NaN;

                        const pxOk = Number.isFinite(px);
                        if (!pxOk) return <span className="text-slate-500">—</span>;

                        // Use 1h pct only (your column is Expected move (1h))
                        const pctVal =
                          typeof r.expected_move_pct_1h === "number"
                            ? r.expected_move_pct_1h
                            : null;

                        if (pctVal == null) return <span className="text-slate-500">—</span>;

                       


                        // ST direction MUST come from the same ST label you show in the ST pill
                        const stLbl: Bias =
                          (r.st_trend_label as Bias | undefined) ??
                          (scoreToTrendLabel(r.short_term_score) as Bias) ??
                          (r.bias as Bias);

                        const stDir = trendDirFromBiasLabel(stLbl);
                        if (stDir === 0) return <span className="text-slate-500">—</span>;
                        
                        const pctText =
                          stDir > 0 ? `+${Math.abs(pctVal).toFixed(2)}%` :
                          stDir < 0 ? `-${Math.abs(pctVal).toFixed(2)}%` :
                          pctSym(pctVal, r.symbol);

                        const pct = Math.abs(pctVal) / 100;
                        let target = px * (1 + stDir * pct);

                        // hard guarantee:
                        if (stDir > 0 && target <= px) target = px * (1 + Math.abs(pct));
                        if (stDir < 0 && target >= px) target = px * (1 - Math.abs(pct));

                        return (
                          <span>
                            {pctText && <>{pctText} {"-"} </>}
                            <span className="tabular-nums">
                              {fmtPrice(r.symbol, target, decimalsFromPrice(px))}
                            </span>
                          </span>
                        );
                      })()
                    ) : (
                      <span className="text-slate-500">—</span>
                    )}
                  </td>

                  {/* Expected move (4h) - HT informational */}
                  <td className="px-4 py-3 text-slate-400">
                    {showTarget ? (
                      (() => {
                        const px =
                          typeof prices?.[r.symbol]?.price === "number"
                            ? prices[r.symbol].price
                            : typeof r.price === "number"
                              ? r.price
                              : NaN;

                        const pxOk = Number.isFinite(px);
                        if (!pxOk) return <span className="text-slate-500">—</span>;

                        const pctVal =
                          typeof r.expected_move_pct_4h === "number" ? r.expected_move_pct_4h : null;

                        if (pctVal == null) return <span className="text-slate-500">—</span>;

                        


                        const htLbl: Bias =
                          (r.ht_trend_label as Bias | undefined) ?? (r.bias as Bias);

                        const htDir = trendDirFromBiasLabel(htLbl);
                        if (htDir === 0) return <span className="text-slate-500">—</span>;

                        const pctText =
                          htDir > 0 ? `+${Math.abs(pctVal).toFixed(2)}%` :
                          htDir < 0 ? `-${Math.abs(pctVal).toFixed(2)}%` :
                          pctSym(pctVal, r.symbol);

                        const pct = Math.abs(pctVal) / 100;
                        let target = px * (1 + htDir * pct);

                        // keep on correct side for HT too
                        if (htDir > 0 && target <= px) target = px * (1 + Math.abs(pct));
                        if (htDir < 0 && target >= px) target = px * (1 - Math.abs(pct));

                        return (
                          <span>
                            {pctText && <>{pctText} {"-"} </>}
                            <span className="tabular-nums">
                              {fmtPrice(r.symbol, target, decimalsFromPrice(px))}
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
                    {r.prob_up != null ? r.prob_up.toFixed(2) : ""}
                  </td>

                  {/* Pulse */}
                  <td className="px-4 py-3">
                    <button
                      onClick={() => openPulse(r)}
                      className="inline-flex items-center gap-2 rounded-xl border border-slate-700/70 bg-slate-900/40 px-3 py-1.5 text-xs font-medium text-slate-200 hover:bg-slate-800/50"
                      title="Open pulse commentary"
                    >
                      <span className="h-2 w-2 rounded-full bg-emerald-400/70" />
                      Pulse
                    </button>
                  </td>

                  {/* Reasons */}
                  <td className="px-4 py-3 text-slate-300">
                    {showReasons ? (
                      <div className="max-w-xs space-y-1 text-xs">
                        <div
                          className="truncate text-slate-200"
                          title={
                            (r.reasons_h1 && r.reasons_h1.join("; ")) ||
                            (r.reasons && r.reasons.join("; ")) ||
                            ""
                          }
                        >
                          <span className="font-semibold">ST:</span>{" "}
                          {(r.reasons_h1 && r.reasons_h1.length ? r.reasons_h1 : (r.reasons || []))
                            .slice(0, 2)
                            .join("; ")}
                        </div>

                        <div
                          className="truncate text-slate-400"
                          title={r.reasons_h4 && r.reasons_h4.length ? r.reasons_h4.join("; ") : ""}
                        >
                          <span className="font-semibold">HT:</span>{" "}
                          {(r.reasons_h4 && r.reasons_h4.length ? r.reasons_h4 : [])
                            .slice(0, 2)
                            .join("; ")}
                        </div>
                      </div>
                    ) : (
                      ""
                    )}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      {/* Pulse modal */}
      {pulseOpen && pulseRow ? (
        <div
          className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 p-4"
          onClick={closePulse}
        >
          <div
            className="w-full max-w-2xl rounded-2xl border border-slate-700/70 bg-slate-950/95 shadow-2xl"
            onClick={(e) => e.stopPropagation()}
          >
            <div className="flex items-start justify-between gap-4 border-b border-slate-800/70 p-5">
              <div>
                <div className="text-xs uppercase tracking-wide text-slate-400">Pulse</div>
                <div className="mt-1 text-lg font-semibold text-slate-100">{pulseRow.symbol}</div>
                <div className="mt-2 flex flex-wrap gap-2">
                  {(() => {
                    const htLbl: Bias = (pulseRow.ht_trend_label as any) ?? (pulseRow.bias as any);
                    const stLbl: Bias =
                      (pulseRow.st_trend_label as any) ??
                      (scoreToTrendLabel(pulseRow.short_term_score) as any) ??
                      (pulseRow.bias as any);
                    const htP = biasToPill(htLbl);
                    const stP = biasToPill(stLbl);
                    return (
                      <>
                        <span className={htP.className}>HT: {htP.text}</span>
                        <span className={stP.className}>ST: {stP.text}</span>
                      </>
                    );
                  })()}
                </div>
              </div>
              <button
                onClick={closePulse}
                className="rounded-xl border border-slate-700/70 bg-slate-900/40 px-3 py-1.5 text-sm text-slate-200 hover:bg-slate-800/60"
              >
                Close
              </button>
            </div>

            <div className="p-5">
              <div className="grid gap-4 md:grid-cols-2">
                <div className="rounded-xl border border-slate-800/70 bg-slate-900/30 p-4">
                  <div className="text-xs font-semibold text-slate-300">ST commentary</div>
                  <ul className="mt-2 space-y-1 text-sm text-slate-200">
                    {(pulseRow.reasons_h1 && pulseRow.reasons_h1.length
                      ? pulseRow.reasons_h1
                      : pulseRow.reasons || [])
                      .slice(0, 6)
                      .map((x, i) => (
                        <li key={i} className="flex items-start gap-2">
                          <span className="mt-2 h-1.5 w-1.5 rounded-full bg-slate-500/80" />
                          <span>{x}</span>
                        </li>
                      ))}
                    {!((pulseRow.reasons_h1 && pulseRow.reasons_h1.length) || (pulseRow.reasons && pulseRow.reasons.length)) ? (
                      <li className="text-slate-500">No ST commentary available</li>
                    ) : null}
                  </ul>
                </div>

                <div className="rounded-xl border border-slate-800/70 bg-slate-900/30 p-4">
                  <div className="text-xs font-semibold text-slate-300">HT commentary</div>
                  <ul className="mt-2 space-y-1 text-sm text-slate-200">
                    {(pulseRow.reasons_h4 && pulseRow.reasons_h4.length ? pulseRow.reasons_h4 : [])
                      .slice(0, 6)
                      .map((x, i) => (
                        <li key={i} className="flex items-start gap-2">
                          <span className="mt-2 h-1.5 w-1.5 rounded-full bg-slate-500/80" />
                          <span>{x}</span>
                        </li>
                      ))}
                    {!(pulseRow.reasons_h4 && pulseRow.reasons_h4.length) ? (
                      <li className="text-slate-500">No HT commentary available</li>
                    ) : null}
                  </ul>
                </div>
              </div>
            </div>
          </div>
        </div>
      ) : null}
    </>
  );
};

/* -------------------------------------------------
 * Main component: AI-powered forecasts view
 * ------------------------------------------------- */
export default function PredictionMeter() {
  // State
  const [view, setView] = React.useState<"cards" | "table">("cards");
  const showReasons = true;
  const showTarget = true;
  // Fixed timeframe  backend is effectively M15 here
  const tf: TfLabel = "M15";
  const { rows, error } = usePredictRows(tf);

  // Page-level broker offset in minutes (fallback 120 = UTC+02:00)
  const brokerOffsetMin =
    (rows?.[0]?.broker_tz_offset_min as number | undefined) ??
    (rows?.[0]?.tz_offset_min as number | undefined) ??
    120;

  const [sort, setSort] = React.useState<"strength" | "prob" | "move" | "az">("strength");

  // Live prices auto-refresh (fixed interval, no UI control)
  const priceRefreshMs = 60_000; // 1m polling for prices
  const { prices, updatedAt: priceUpdatedAt } = useLivePrices(priceRefreshMs);

  // Sorted view of live rows
  const sortedRows = React.useMemo(() => {
    const arr = [...rows];
    switch (sort) {
      case "strength":
        return arr.sort((a, b) => biasToScore(b.bias) - biasToScore(a.bias));
      case "prob":
        return arr.sort((a, b) => b.prob_up - a.prob_up);
      case "move":
        return arr.sort(
          (a, b) =>
            Math.abs(b.expected_move_pct_1h ?? 0) -
            Math.abs(a.expected_move_pct_1h ?? 0)
        );
      case "az":
        return arr.sort((a, b) => a.symbol.localeCompare(b.symbol));
    }
  }, [rows, sort]);

  const brokerAbbr = sortedRows[0]?.broker_tz_abbr || "";
  const device = sortedRows[0]?.using_device || "";

  // Header clocks
  const now = useWallClock();
  const londonTime = React.useMemo(
    () =>
      new Intl.DateTimeFormat("en-GB", {
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
        hour12: false,
        timeZone: "Europe/London",
      }).format(now),
    [now]
  );
  const nyTime = React.useMemo(
    () =>
      new Intl.DateTimeFormat("en-GB", {
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
        hour12: false,
        timeZone: "America/New_York",
      }).format(now),
    [now]
  );

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
        timeZone:
          brokerTzIana || Intl.DateTimeFormat().resolvedOptions().timeZone,
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
          <h1 className="text-2xl font-semibold text-slate-100">
            XTL AI-powered forecasts
          </h1>
        </div>
        <div className="flex items-center gap-2 text-sm text-slate-300">
          <span className="px-2 py-1 rounded-md bg-slate-800/60 border border-slate-700/60">
            London {londonTime}
          </span>
          <span className="px-2 py-1 rounded-md bg-slate-800/60 border border-slate-700/60">
            New York {nyTime}
          </span>
          <span className="px-2 py-1 rounded-md bg-slate-800/60 border border-slate-700/60">
            Broker {fmtUtcOffset(brokerOffsetMin)} {brokerClock}
          </span>
        </div>
      </div>

      {/* Controls */}
      <Card className="mt-4">
        <CardContent className="flex flex-wrap items-center gap-4">
          {/* Toggles */}
          {/* View */}
          {/* View + Sort (right side) */}
<div className="flex items-center gap-6 text-sm ml-auto">
  {/* View toggle */}
  <div className="flex items-center gap-2">
    <span className="text-slate-400">View</span>
    <div className="inline-flex rounded-lg border border-slate-700/70 bg-slate-900/60 p-0.5">
      <button
        onClick={() => setView("cards")}
        className={`px-3 py-1.5 rounded-md text-xs sm:text-sm transition-colors ${
          view === "cards"
            ? "bg-emerald-400/15 text-emerald-200 shadow-sm"
            : "text-slate-300 hover:text-slate-100"
        }`}
      >
        Overview
      </button>
      <button
        onClick={() => setView("table")}
        className={`px-3 py-1.5 rounded-md text-xs sm:text-sm transition-colors ${
          view === "table"
            ? "bg-emerald-400/15 text-emerald-200 shadow-sm"
            : "text-slate-300 hover:text-slate-100"
        }`}
      >
        Depth
      </button>
    </div>
  </div>

  {/* Sort */}
  <div className="flex items-center gap-2">
    <span className="text-slate-400">Sort</span>
    <select
      value={sort}
      onChange={(e) => setSort(e.target.value as any)}
      className="bg-slate-900/60 border border-slate-700/60 text-slate-200 rounded-md px-2 py-1.5"
    >
      <option value="strength">Strength</option>
      <option value="prob">ProbUp</option>
      <option value="move">|ExpectedMove|</option>
      <option value="az">AZ</option>
    </select>
  </div>
</div>

          </CardContent>
          </Card>

      {/* Content */}
      {view === "cards" ? (
  <div className="mt-6 grid grid-cols-1 sm:grid-cols-2 xl:grid-cols-4 gap-5">
    {(sortedRows && sortedRows.length
      ? sortedRows
      : prices
      ? Object.keys(prices).map(
          (sym) => ({ symbol: sym } as InstrumentRow)
        )
      : []
    ).map((r) => {
      const nowMs = Date.now();
      const isExpiredUI =
        typeof r.horizon_min === "number" &&
        typeof r.updated_broker_ts === "number" &&
        nowMs > r.updated_broker_ts + r.horizon_min * 60_000;

      if (isExpiredUI) return null;

      return (
        <InstCard
          key={r.symbol}
          row={r}
          showReasons={showReasons}
          showTarget={showTarget}
          livePrice={prices?.[r.symbol]?.price}
        />
      );
    })}
  </div>
) : (

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
      <p className="mt-6 text-xs text-slate-500"></p>
      {error ? (
        <p className="mt-4 text-sm text-rose-400">Error: {error}</p>
      ) : (
        <p className="mt-4 text-xs text-slate-500">
          {priceUpdatedAt
            ? `Price updated: ${new Date(
                priceUpdatedAt
              ).toLocaleTimeString()}`
            : "Waiting for live price"}
        </p>
      )}
    </div>
  );
}