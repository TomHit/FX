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

function fmtBrokerHHMM(
  tsMs?: number | null,
  tzOffsetMin?: number | null
) {
  if (!tsMs) return "";

  const offsetMs = (tzOffsetMin ?? 0) * 60_000;

  // Shift into broker-local time, then format in UTC 
  const d = new Date(tsMs + offsetMs);
  return d.toISOString().slice(11, 16); // HH:MM
}




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


function directionPill(decisionRaw: string): { cls: string; text: string; arrow: string } {
  const d = (decisionRaw || "").toUpperCase().trim();
  if (d === "BUY" || d === "UP" || d === "LONG") {
    return {
      cls: "inline-flex items-center gap-1 rounded-full border border-emerald-600/40 bg-emerald-950/40 px-2.5 py-1 text-xs font-semibold text-emerald-200",
      text: "BUY",
      arrow: "▲",
    };
  }
  if (d === "SELL" || d === "DOWN" || d === "SHORT") {
    return {
      cls: "inline-flex items-center gap-1 rounded-full border border-rose-600/40 bg-rose-950/40 px-2.5 py-1 text-xs font-semibold text-rose-200",
      text: "SELL",
      arrow: "▼",
    };
  }
  if (d === "HOLD") {
    return {
      cls: "inline-flex items-center gap-1 rounded-full border border-slate-600/50 bg-slate-900/40 px-2.5 py-1 text-xs font-semibold text-slate-200",
      text: "HOLD",
      arrow: "•",
    };
  }
  return {
    cls: "inline-flex items-center gap-1 rounded-full border border-slate-700/70 bg-slate-900/40 px-2.5 py-1 text-xs font-semibold text-slate-200",
    text: d || "—",
    arrow: "•",
  };
}

function fmtProbPct(p: any): string {
  const n = typeof p === "number" ? p : Number(p);
  if (!Number.isFinite(n)) return "—";
  // backend sometimes returns 0..1; sometimes already 0..100
  const pct = n <= 1.0 ? n * 100.0 : n;
  return `${pct.toFixed(pct >= 10 ? 0 : 1)}%`;
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

  // C) Percent-based fallback (1h) — direction should follow ST trend when available
  if (Number.isFinite((row as any).expected_move_pct as any)) {
    const pctAbs = Math.abs(Number((row as any).expected_move_pct)) / 100;
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

// Format tick candle time as broker time (HH:MM), invariant to viewer TZ.
// We shift by broker offset, then format in UTC via ISO string slice.
function fmtBrokerTime(tsMs: number | null | undefined, tzOffsetMin?: number | null) {
  if (tsMs == null) return "";
  if (tzOffsetMin == null) return "";
  const brokerMs = tsMs + Number(tzOffsetMin) * 60_000;
  return new Date(brokerMs).toISOString().slice(11, 16); // "HH:MM"
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


type LivePrice = { price: number | null; lastTs: number | null };

/** Live M1 prices (last CLOSED bar) with cache-busting, visibility refetch,
 *  and dynamic interval (ms). Use 0 to disable auto refresh. */
function useLivePrices(refreshMs: number = 60_000) {
  const [prices, setPrices] = React.useState<Record<string, LivePrice>>({});
  const [updatedAt, setUpdatedAt] = React.useState<number | null>(null);
  const [brokerMeta, setBrokerMeta] = React.useState<{ tz_offset_min?: number; tz_name?: string; device?: string }>({});
  const lastPriceTsRef = React.useRef<number>(0);
  const refetch = React.useCallback(async () => {
    try {
      // cache-bust & no-store to avoid any intermediary caching
      const url = `/_api/trend/price/all?tf=M1&_=${Date.now()}`;
      const res = await fetch(url, { credentials: "include", cache: "no-store" });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const js = await res.json();

      const upd: Record<string, LivePrice> = {};
      if (Array.isArray(js?.rows)) {
        for (const r of js.rows) {
          if (!r?.symbol) continue;
          const sym = String(r.symbol).toUpperCase();
          upd[sym] = {
            price: (typeof r?.price === "number" ? r.price : null),
            lastTs: (typeof r?.lastTs === "number" ? r.lastTs : null),
          };
        }
      }
      setPrices(upd);

      // broker meta from backend (device + tz)
      try {
        const b = js?.broker || {};
        const tzOff = typeof b?.tz_offset_min === "number" ? b.tz_offset_min : undefined;
        const tzName = typeof b?.tz_name === "string" ? b.tz_name : undefined;
        const dev = typeof js?.device === "string" ? js.device : undefined;
        setBrokerMeta({ tz_offset_min: tzOff, tz_name: tzName, device: dev });
      } catch {
        // ignore
      }
      const respTs =
        typeof js?.resp_ts_ms === "number" ? js.resp_ts_ms : Date.now();

     
      setUpdatedAt(respTs);
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

  return { prices, updatedAt, brokerMeta, refetch };
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
  price_source?: string;
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
  macro?: string[]; 
  
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

type PulseLevel = {
  level: number;
  touches?: number;
  strength?: number;
  kind?: string;
  tf?: string;
  distance_atr?: number;
  stale?: boolean;

};

type PulseSide = {
  supports?: PulseLevel[];
  resistances?: PulseLevel[];
  // enriched views from summarize_sr_multi_tf (optional)
  supports_near?: PulseLevel[];
  resistances_near?: PulseLevel[];
  supports_major?: PulseLevel[];
  resistances_major?: PulseLevel[];
};

type PulsePayload = {
  ok: boolean;
  symbol: string;
  tf: string;

  price: number | null;
  decision: string | null;
  prob_up: number | null;
  expected_move_pct: number | null;
  target_price: number | null;
  features?: { sr?: boolean; fib?: boolean; commentary?: boolean };

  sr?: {
    symbol?: string;
    h4?: PulseSide
    h1?: PulseSide
    nearest_support?: number | null;
    nearest_resistance?: number | null;
    distance_pips?: { support?: number | null; resistance?: number | null };
    distance_atr?: { support?: number | null; resistance?: number | null };
    sr_safety?: string | null;
   
  };

  fib?: {
    range?: { hi?: number; lo?: number } | null;
    levels?: { pct: number; level: number }[];
  };

  pulse_text?: string;
};


// Map backend /trend/predict/all row -> InstrumentRow (defensive defaults)
function mapApiRowToInstrument(r: any, activeTf: TfLabel): InstrumentRow {
    // If backend provides per-timeframe payloads under `tfs`,
  // prefer the selected timeframe view (H1/H4) so UI doesn't "flip" to top-level ABSTAIN.
  const tfView =
    r?.tfs && typeof r.tfs === "object" && r.tfs?.[activeTf] && typeof r.tfs[activeTf] === "object"
      ? r.tfs[activeTf]
      : null;

  const src = tfView ? { ...r, ...tfView } : r;
  // --- NEW: macro normalization (backend may send dict | list | string) ---
  // --- Macro normalization (supports new backend contract) ---
  const macroLines: string[] = (() => {
    const srcAny = src as any;

    // 1) canonical
    if (Array.isArray(srcAny?.macro_reasons)) {
      return srcAny.macro_reasons.filter(Boolean).map(String);
    }

    // 2) sometimes nested: macro: { macro_reasons: [...] }
    if (srcAny?.macro && typeof srcAny.macro === "object" && Array.isArray(srcAny.macro?.macro_reasons)) {
      return srcAny.macro.macro_reasons.filter(Boolean).map(String);
    }

    // 3) legacy support
    const m = srcAny?.macro;
    if (!m) return [];

    if (Array.isArray(m)) return m.filter(Boolean).map(String);

    if (typeof m === "object") {
      try {
        return Object.entries(m)
          .filter(([, v]) => v != null && String(v).trim() !== "")
          .map(([k, v]) => `${String(k)}: ${String(v)}`);
      } catch {
        return [];
      }
    }

    // If macro arrives as a single string like "VIX?, RVOL?, DXY?, 10Y?"
    // split it so UI can render up to 4 chips.
    const s = String(m);
    return s
      .split(/[\n,;|]+/g)
      .map(x => x.trim())
      .filter(Boolean);

  })();



  const label: string = (src?.label || "").toString();

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

  const decision: string = (src?.decision || "").toString();

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
    typeof src?.prob_up === "number" ? src.prob_up :
   
    typeof src?.p_up === "number"    ? src.p_up :
    null;

  // --- NEW ML canonical fields ---
  const horizon_min =
    typeof r?.horizon_min === "number" ? r.horizon_min : undefined;

  const raw = (r as any)?.raw ?? null;

  // Pull pct from canonical -> raw.predMovePct -> legacy expected_move_pct_1h -> score (very old fallback)
  const pct_raw =
    typeof src?.expected_move_pct === "number"
      ? src.expected_move_pct
      : (typeof (raw as any)?.predMovePct === "number"
          ? (raw as any).predMovePct
          : (typeof src?.expected_move_pct_1h === "number"
              ? src.expected_move_pct_1h
              : (typeof src?.score === "number" ? src.score : null)));

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
    typeof src?.target_price === "number"
      ? src.target_price
      : (typeof (raw as any)?.targetPrice === "number"
          ? (raw as any).targetPrice
          : (typeof src?.target_price_1h === "number"
              ? src.target_price_1h
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
    price_source: typeof r?.price_source === "string" ? r.price_source : undefined,


    bias,
    short_term_score: st ?? undefined,
    long_term_score: ht ?? undefined,
    reasons,
    reasons_h1,
    reasons_h4,
    macro: macroLines, // NEW ?
    confidence_band: undefined,

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

    decision: typeof src?.decision === "string" ? (src.decision as any) : "",
    target_pips: typeof r?.target_pips === "number" ? r.target_pips : undefined,

    
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
  const lastRespTsRef = React.useRef<number>(0);
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
      const respTs =
        typeof js?.server_now_ms === "number"
        ? js.server_now_ms
        : Date.now();

      lastRespTsRef.current = Math.max(lastRespTsRef.current, respTs);

      // map rows safely
      const apiRows: any[] = Array.isArray(js?.rows) ? js.rows : [];
      const mapped = apiRows.map((r: any) => mapApiRowToInstrument(r, tf as TfLabel));
      setRows((prev) => {
        const prevBySym = new Map(prev.map((p) => [p.symbol, p]));
        return mapped.map((x: any) => {
          const p = prevBySym.get(x.symbol);
          if (!p) return x;

          // if new poll is missing expected move / target (or flips to WAIT), keep previous
          const newHasMove =
            Number.isFinite(Number((x as any).expected_move_pct_1h)) ||
            Number.isFinite(Number((x as any).expected_move_pct_4h)) ||
            Number.isFinite(Number((x as any).expected_move_pct));

          const newDec = String((x as any).decision || "").toUpperCase();
          const prevDec = String((p as any)?.decision || "").toUpperCase();

          const isBlankDecision = (d: string) => d === "" || d === "WAIT" || d === "ABSTAIN";

          //  blank-ish  updates: ABSTAIN/WAIT/empty should NOT erase a previous BUY/SELL
          const newLooksBlank =
            !newHasMove ||
            (p && (prevDec === "BUY" || prevDec === "SELL") && isBlankDecision(newDec));


          if (newLooksBlank) {
            return { ...x, ...p }; // keep last good values
          }
          return { ...p, ...x };   // normal update
        });
      });


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
  livePrice?: number | null;
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
            {typeof livePrice === "number" && Number.isFinite(livePrice)
              ? fmtPrice(row.symbol, livePrice!, priceDecimals(row.symbol))
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
                <span className="text-[12px] text-slate-400">Near-Term Bias</span>
              </div>
            </div>

            {/* HT */}
            <div className="flex items-center justify-between px-3 py-2">
              <div className="flex items-center gap-2">
                <span className={htPillClass}>{htLabel}</span>
                <span className="text-[12px] text-slate-400">Broader Bias</span>
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
  prices?: Record<string, LivePrice>;
  brokerOffsetMin: number;
  activetf: TfLabel;
  deviceUsed: string;
}> = ({ rows, prices, brokerOffsetMin, activetf, deviceUsed, showReasons }) => {
  // Build display rows: if predictions are empty, make rows from price symbols
  const displayRows: InstrumentRow[] =
    rows && rows.length
      ? rows
      : prices
        ? Object.keys(prices).map((sym) => ({ symbol: sym } as InstrumentRow))
        : [];

  // --- local helpers (avoid “missing name” TS errors) ---
  const fmtProbPctLocal = (v: any) => {
    const x = typeof v === "number" ? v : typeof v?.prob_up === "number" ? v.prob_up : typeof v?.prob_up === "number" ? v.prob_up : null;
    if (typeof x !== "number" || !Number.isFinite(x)) return "—";
    return `${Math.round(x * 100)}%`;
  };

  const directionPillLocal = (dec: string) => {
    const d = String(dec || "").toUpperCase();
    if (d === "BUY" || d === "LONG" || d === "UP") {
      return { text: "BUY", arrow: "▲", cls: "inline-flex items-center gap-1 rounded-full border border-emerald-700/40 bg-emerald-900/20 px-3 py-1 text-xs font-semibold text-emerald-200" };
    }
    if (d === "SELL" || d === "SHORT" || d === "DOWN") {
      return { text: "SELL", arrow: "▼", cls: "inline-flex items-center gap-1 rounded-full border border-rose-700/40 bg-rose-900/20 px-3 py-1 text-xs font-semibold text-rose-200" };
    }
    if (d === "ABSTAIN") {
      return { text: "ABSTAIN", arrow: "•", cls: "inline-flex items-center gap-1 rounded-full border border-slate-700/60 bg-slate-900/30 px-3 py-1 text-xs font-semibold text-slate-200" };
    }
    return { text: d || "—", arrow: "•", cls: "inline-flex items-center gap-1 rounded-full border border-slate-700/60 bg-slate-900/30 px-3 py-1 text-xs font-semibold text-slate-200" };
  };

  // --- Pulse modal state ---
  const [pulseOpen, setPulseOpen] = React.useState(false);
  const [pulseRow, setPulseRow] = React.useState<InstrumentRow | null>(null);
  const [pulseLoading, setPulseLoading] = React.useState(false);
  const [pulseErr, setPulseErr] = React.useState<string | null>(null);
  const [pulseData, setPulseData] = React.useState<any | null>(null);

  const closePulse = React.useCallback(() => {
    setPulseOpen(false);
    setPulseRow(null);
    setPulseErr(null);
    setPulseData(null);
  }, []);

  const fetchPulse = React.useCallback(
    async (sym: string) => {
      const symU = String(sym || "").toUpperCase().trim();
      if (!symU) return;

      setPulseLoading(true);
      setPulseErr(null);

      try {
        const q = new URLSearchParams();
        q.set("symbol", symU);
        q.set("tf", activetf); // bind to selected timeframe (M15/H1/H4)
        if (deviceUsed) q.set("device", deviceUsed);

        const url = `/_api/trend/pulse?${q.toString()}`;
        const res = await fetch(url, { credentials: "include" });
        const js = await res.json().catch(() => null);

        if (!res.ok) throw new Error(js?.detail || js?.message || `HTTP ${res.status}`);
        setPulseData(js);
      } catch (e: any) {
        setPulseErr(String(e?.message || e || "Pulse fetch failed"));
      } finally {
        setPulseLoading(false);
      }
    },
    [activetf, deviceUsed]
  );

  const openPulse = React.useCallback(
    (r: InstrumentRow) => {
      setPulseRow(r);
      setPulseOpen(true);
      setPulseData(null);
      void fetchPulse(String((r as any).symbol || "").toUpperCase());
    },
    [fetchPulse]
  );

  // ---------- SR render helpers ----------
  const srKey = String(activetf || "H1").toLowerCase(); // "h1" | "h4" | ...
  const srFrameKey = srKey === "h4" ? "h4" : "h1"; // pulse currently returns sr.h1 and sr.h4
  const getLevelNum = (x: any) => (typeof x?.level === "number" ? x.level : null);

  const filterSupport = (arr: any[], px: number) =>
    (Array.isArray(arr) ? arr : []).filter((x) => {
      const lv = getLevelNum(x);
      if (typeof lv !== "number") return false;
      if (!(lv <= px)) return false; // support must be BELOW price
      if (x?.stale === true) return false;
      if (x?.side_ok === false) return false;
      return true;
    });

  const filterResistance = (arr: any[], px: number) =>
    (Array.isArray(arr) ? arr : []).filter((x) => {
      const lv = getLevelNum(x);
      if (typeof lv !== "number") return false;
      if (!(lv >= px)) return false; // resistance must be ABOVE price
      if (x?.stale === true) return false;
      if (x?.side_ok === false) return false;
      return true;
    });
  const fallbackSupportAny = (arr: any[], px: number) =>
    (Array.isArray(arr) ? arr : [])
      .filter((x) => typeof getLevelNum(x) === "number")
      .sort((a, b) => (getLevelNum(b)! - getLevelNum(a)!))  // highest first
      .slice(0, 3);

  const fallbackResistanceAny = (arr: any[], px: number) =>
    (Array.isArray(arr) ? arr : [])
      .filter((x) => typeof getLevelNum(x) === "number")
      .sort((a, b) => (getLevelNum(a)! - getLevelNum(b)!)) // lowest first
      .slice(0, 3);

  const srChips = (items: any[], sym: string, maxN: number) => {
    const out = items.slice(0, maxN);
    if (!out.length) return <span className="text-slate-500">—</span>;
    return (
      <div className="flex flex-wrap gap-2">
        {out.map((x: any, i: number) => {
          const lv = getLevelNum(x);
          const txt =
            typeof lv === "number"
              ? fmtPrice(sym, lv, priceDecimals(sym))
              : String(x?.level ?? "—");
          const tt = `touches=${x?.touches ?? "—"} strength=${x?.strength ?? "—"} atr=${x?.distance_atr ?? "—"}`;
          return (
            <span
              key={i}
              className="rounded-full border border-slate-700/70 bg-slate-900/40 px-2.5 py-1 text-[12px] text-slate-200 tabular-nums"
              title={tt}
            >
              {txt}
            </span>
          );
        })}
      </div>
    );
  };

  return (
    <>
      <div className="overflow-x-auto rounded-2xl border border-slate-800/80 bg-slate-950/40 shadow-xl">
        <table className="min-w-full text-left text-sm">
          <thead className="bg-slate-950/70 text-xs uppercase tracking-wide text-slate-400">
            <tr>
              <th className="px-4 py-3">Symbol</th>
              <th className="px-4 py-3">Price</th>
              <th className="px-4 py-3">Direction</th>
              <th className="px-4 py-3">Expected move</th>
              <th className="px-4 py-3">Macro</th>
              <th className="px-4 py-3">Prob</th>
              <th className="px-4 py-3">Pulse</th>
            </tr>
          </thead>

          <tbody className="divide-y divide-slate-800/70">
            {displayRows.map((r) => {
              const sym = String((r as any).symbol || "").toUpperCase();
              const live = prices?.[sym];
              const priceNow = live?.price ?? null;
              const priceTsMs = live?.lastTs ?? null;

              const dec = String((r as any).decision || "").toUpperCase();
              const dpill = directionPillLocal(dec);

              const emPct =
                typeof (r as any).expected_move_pct === "number" ? (r as any).expected_move_pct : undefined;
              const tgt = typeof (r as any).target_price === "number" ? (r as any).target_price : undefined;

              // backend sends macro_reasons; keep fallback for older shapes
              // backend sends macro_reasons; accept array | string | dict/object
              const macroRaw =
                (r as any).macro ??
                (r as any).macro_reasons ??
                (r as any).macroReasons ??
                null;

              const normalizeMacro = (v: any): string[] => {
                if (!v) return [];

                // array already
                if (Array.isArray(v)) return v.filter(Boolean).map(String);

                // single string (optional: split if you ever send CSV)
                if (typeof v === "string") {
                  const s = v.trim();
                  return s ? [s] : [];
                }

                // object / dict shape: {dxy: "...", vix: "..."} or {reasons:[...]}
                if (typeof v === "object") {
                  const rr = (v as any).reasons;
                  if (Array.isArray(rr)) return rr.filter(Boolean).map(String);

                  // flatten values; if values are arrays, flatten those too
                  const vals = Object.values(v).flatMap((x: any) => (Array.isArray(x) ? x : [x]));
                  return vals.filter(Boolean).map(String);
                }

                return [];
              };

             const macro = normalizeMacro(macroRaw);
             const macroShort = macro.slice(0, 4);


              return (
                <tr key={sym} className="hover:bg-slate-900/40">
                  {/* Symbol */}
                  <td className="px-4 py-3">
                    <div className="font-semibold text-slate-100">{sym}</div>
                    <div className="text-[11px] text-slate-500">{(r as any).structure_reason || (r as any).structure || ""}</div>
                  </td>

                  {/* Price */}
                  <td className="px-4 py-3">
                    <div className="tabular-nums text-slate-100">
                      {Number.isFinite(priceNow as any)
                        ? fmtPrice(sym, priceNow as number, priceDecimals(sym))
                        : ""}
                    </div>
                    <div className="text-[11px] text-slate-500">
                      {priceNow != null && priceTsMs != null ? (
                        <>
                          <div>
                            Broker: {fmtBrokerTime(priceTsMs, brokerOffsetMin)}{" "}
                            <span className="text-slate-600">
                              ({fmtUtcOffset(Number(brokerOffsetMin || 0))})
                            </span>
                          </div>
                          <div className="text-slate-600">
                            UTC: {new Date(priceTsMs).toISOString().slice(11, 16)}
                          </div>
                        </>
                      ) : (
                        ""
                      )}
                    </div>
                  </td>

                  {/* Direction */}
                  <td className="px-4 py-3">
                    <span className={dpill.cls}>
                      <span className="opacity-80">{dpill.arrow}</span>
                      {dpill.text}
                    </span>
                  </td>

                  {/* Expected move */}
                  <td className="px-4 py-3 text-slate-200">
                    {typeof emPct === "number" && typeof tgt === "number" ? (
                      <span className="tabular-nums">
                        {pctSym(emPct, sym)} <span className="text-slate-500">•</span>{" "}
                        {fmtPrice(sym, tgt, priceDecimals(sym))}
                      </span>
                    ) : typeof emPct === "number" ? (
                      <span className="tabular-nums">{pctSym(emPct, sym)}</span>
                    ) : (
                      <span className="text-slate-500">—</span>
                    )}
                  </td>

                  {/* Macro (toggle via showReasons) */}
                  <td className="px-4 py-3">
                    {macroShort.length ? (
                      <div className="flex flex-wrap gap-1.5" title={macro.join("; ")}>
                        {macroShort.map((m: string, i: number) => (
                          <span
                            key={i}
                            className="inline-flex items-center rounded-full border border-slate-700/70 bg-slate-900/40 px-2.5 py-1 text-[11px] text-slate-200"
                          >
                            {m}
                          </span>
                        ))}
                      </div>
                    ) : (
                      <span className="text-slate-500">—</span>
                    )}
                  </td>

                  {/* Prob */}
                  <td className="px-4 py-3 tabular-nums text-slate-200">
                    {fmtProbPctLocal((r as any).prob_up ?? (r as any).prob_up ?? (r as any).p_up)}
                  </td>

                  {/* Pulse */}
                  <td className="px-4 py-3">
                    <button
                      onClick={() => openPulse(r)}
                      className="inline-flex items-center gap-2 rounded-xl border border-slate-700/70 bg-slate-900/40 px-3 py-1.5 text-xs font-medium text-slate-200 hover:bg-slate-800/50"
                      title="Open pulse"
                      type="button"
                    >
                      <span className="h-2 w-2 rounded-full bg-emerald-400/70" />
                      Pulse
                    </button>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      {/* Pulse modal */}
      {pulseOpen && pulseRow ? (
        <div className="fixed inset-0 z-50 flex items-start justify-center bg-black/60 p-4" onClick={closePulse}>
          <div
            className="my-8 w-full max-w-2xl max-h-[85vh] overflow-hidden rounded-2xl border border-slate-700/70 bg-slate-950/95 shadow-2xl"
            onClick={(e) => e.stopPropagation()}
          >
            <div className="flex items-start justify-between gap-4 border-b border-slate-800/70 p-5">
              <div>
                <div className="text-xs uppercase tracking-wide text-slate-400">Pulse</div>
                <div className="mt-1 text-lg font-semibold text-slate-100">{String((pulseRow as any).symbol || "").toUpperCase()}</div>
                <div className="mt-2 text-xs text-slate-500">
                  SR view: <span className="text-slate-300">{srFrameKey.toUpperCase()}</span>
                </div>
              </div>

              <button
                className="rounded-xl border border-slate-700/70 bg-slate-900/40 px-3 py-2 text-sm text-slate-200 hover:bg-slate-800/60"
                onClick={closePulse}
                type="button"
              >
                Close
              </button>
            </div>

            <div className="max-h-[calc(85vh-92px)] overflow-y-auto p-5 space-y-4">
              {pulseLoading ? (
                <div className="text-sm text-slate-300">Loading pulse…</div>
              ) : pulseErr ? (
                <div className="text-sm text-rose-300">Pulse error: {pulseErr}</div>
              ) : pulseData ? (
                <>
                  {/* Pulse text */}
                  <div className="rounded-xl border border-slate-800/70 bg-slate-900/30 p-4">
                    <div className="text-sm text-slate-100">{pulseData.pulse_text || "—"}</div>
                  </div>

                  {/* SR (Near + Major, correct TF + filters) */}
                  {pulseData?.sr ? (
                    (() => {
                      const sym = String((pulseRow as any)?.symbol || "").toUpperCase();
                      const px =
                        typeof pulseData?.price === "number"
                          ? pulseData.price
                          : typeof pulseData?.sr?.price === "number"
                            ? pulseData.sr.price
                            : null;

                      const srAll = pulseData?.sr || null;
                      const frameA = srAll?.[srFrameKey] || null;
                      const otherKey = srFrameKey === "h1" ? "h4" : "h1";
                      const frameB = srAll?.[otherKey] || null;

                      const frame = frameA || frameB; // render something even if preferred frame missing

                      const pickNearSup = frameA?.supports_near?.length ? frameA.supports_near : (frameB?.supports_near || []);
                      const pickNearRes = frameA?.resistances_near?.length ? frameA.resistances_near : (frameB?.resistances_near || []);

                      const nearSup = px != null ? filterSupport(pickNearSup || [], px) : [];
                      const nearRes = px != null ? filterResistance(pickNearRes || [], px) : [];

                      const pickMajorSupRaw =
                        frameA?.supports_major?.length
                          ? frameA.supports_major
                          : frameA?.supports?.length
                            ? frameA.supports
                            : frameB?.supports_major?.length
                              ? frameB.supports_major
                              : frameB?.supports || [];

                      const pickMajorResRaw =
                        frameA?.resistances_major?.length
                          ? frameA.resistances_major
                          : frameA?.resistances?.length
                            ? frameA.resistances
                            : frameB?.resistances_major?.length
                              ? frameB.resistances_major
                              : frameB?.resistances || [];

                      let majorSup = px != null ? filterSupport(pickMajorSupRaw || [], px) : (pickMajorSupRaw || []);
                      let majorRes = px != null ? filterResistance(pickMajorResRaw || [], px) : (pickMajorResRaw || []);

                      // last-resort single chip so we never render blanks when cache has a valid nearest level
                      if (majorSup.length === 0 && typeof srAll?.nearest_support === "number") {
                        majorSup = [{ level: srAll.nearest_support }];
                      }
                      if (majorRes.length === 0 && typeof srAll?.nearest_resistance === "number") {
                        majorRes = [{ level: srAll.nearest_resistance }];
                      }

                      const hasNear = nearSup.length > 0 || nearRes.length > 0;

                      return (
                        <div className="rounded-xl border border-slate-800/70 bg-slate-900/30 p-4 space-y-4">
                          <div className="text-xs font-semibold text-slate-300">Support / Resistance</div>

                          {/* Near (actionable) */}
                         {hasNear && (
                           <div className="rounded-xl border border-slate-800/60 bg-slate-950/30 p-4">
                            <div className="text-xs font-semibold text-slate-300">Near (actionable)</div>
                            <div className="mt-3 grid grid-cols-1 gap-3 md:grid-cols-2">
                              <div>
                                <div className="text-[11px] text-slate-400">Support</div>
                                <div className="mt-2">{srChips(nearSup, sym, 6)}</div>
                              </div>
                              <div>
                                <div className="text-[11px] text-slate-400">Resistance</div>
                                <div className="mt-2">{srChips(nearRes, sym, 6)}</div>
                              </div>
                            </div>
                          </div>
                         )}

                          {/* Major (strong) */}
                          <div className="rounded-xl border border-slate-800/60 bg-slate-950/30 p-4">
                            <div className="text-xs font-semibold text-slate-300">Major (strong)</div>
                            <div className="mt-3 grid grid-cols-1 gap-3 md:grid-cols-2">
                              <div>
                                <div className="text-[11px] text-slate-400">Support</div>
                                <div className="mt-2">{srChips(majorSup, sym, 8)}</div>
                              </div>
                              <div>
                                <div className="text-[11px] text-slate-400">Resistance</div>
                                <div className="mt-2">{srChips(majorRes, sym, 8)}</div>
                              </div>
                            </div>
                          </div>
                        </div>
                      );
                    })()
                  ) : null}

                  
                  {/* Fib */}
                  <div className="rounded-xl border border-slate-800/70 bg-slate-900/30 p-4">
                    <div className="text-xs font-semibold text-slate-300">
                      Fibonacci ({String(activetf || "H1").toUpperCase()})
                    </div>

                    {Array.isArray(pulseData?.fib?.levels) && pulseData.fib.levels.length > 0 ? (
                      <div className="mt-3 flex flex-wrap gap-2">
                        {pulseData.fib.levels.slice(0, 10).map((x: any, i: number) => (
                          <span
                            key={i}
                            className="rounded-full border border-slate-700/70 bg-slate-900/40 px-2.5 py-1 text-[12px] text-slate-200 tabular-nums"
                            title={`${x?.pct ?? ""}%`}
                          >
                            {typeof x?.level === "number"
                              ? fmtPrice(
                                  String((pulseRow as any)?.symbol || "").toUpperCase(),
                                  x.level,
                                  priceDecimals(String((pulseRow as any)?.symbol || "").toUpperCase())
                                )
                              : String(x?.level ?? "—")}
                          </span>
                        ))}
                      </div>
                    ) : (
                      <div className="mt-2 text-sm text-slate-500">
                        Fib not available yet.
                      </div>
                    )}
                  </div>

                </>
              ) : (
                <div className="text-sm text-slate-500">No pulse loaded.</div>
              )}
            </div>
          </div>
        </div>
      ) : null}
    </>
  );
};


export default function PredictionMeter() {
  // Timeframe selector (locked design: 15m fast, 1h & 4h frozen)
  const [tf, setTf] = React.useState<TfLabel>("M15");

  const { rows, error } = usePredictRows(tf);

  // Live prices (M1) + broker meta (tz offset + device used)
  const priceRefreshMs = 2_000;
  const { prices, brokerMeta } = useLivePrices(priceRefreshMs);

  // Prefer tz offset from price endpoint (broker time is source of truth)
  const brokerOffsetMin =
    (typeof brokerMeta?.tz_offset_min === "number" ? brokerMeta.tz_offset_min : undefined) ??
    (rows?.[0]?.broker_tz_offset_min as number | undefined) ??
    (rows?.[0]?.tz_offset_min as number | undefined) ??
    0;

  const deviceUsed = brokerMeta?.device || "auto";

  // Header clocks (nice-to-have)
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

  // Live broker clock based on numeric offset
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
{/* Timeframe toggle */}
<div className="mt-5 flex flex-wrap items-center justify-between gap-3">
  <div className="flex items-center gap-2">
    <span className="text-xs font-semibold uppercase tracking-wide text-slate-400">Timeframe</span>
    <div className="inline-flex rounded-2xl border border-slate-700/70 bg-slate-900/50 p-1">
      {(["M15", "H1", "H4"] as TfLabel[]).map((t) => (
        <button
          key={t}
          onClick={() => setTf(t)}
          className={[
            "px-4 py-2 text-sm rounded-xl transition-colors",
            tf === t
              ? "bg-emerald-400/15 text-emerald-200 shadow-sm"
              : "text-slate-300 hover:text-slate-100",
          ].join(" ")}
          title={t === "M15" ? "Fast updates" : "Frozen context"}
        >
          {t === "M15" ? "15m" : t === "H1" ? "1h" : "4h"}
        </button>
      ))}
    </div>
  </div>

  <div className="text-xs text-slate-500">
    Device: <span className="text-slate-300">{deviceUsed}</span>
  </div>
</div>

{/* Content */}
<div className="mt-6">
  <TableView
    rows={rows}
    showReasons={false}
    showTarget={true}
    prices={prices}
    brokerOffsetMin={brokerOffsetMin}
    activetf={tf}
    deviceUsed={deviceUsed}
  />
</div>

{/* Footer */}
{error ? (
  <p className="mt-4 text-sm text-rose-400">Error: {error}</p>
) : null}
    </div>
  );
}
