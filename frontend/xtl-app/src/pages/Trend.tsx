import React, { useEffect, useMemo, useRef, useState } from "react";

/**
 * XTL · Trend Page — Live (H1/H4) Final
 * - One-click: Detect Trend ? then Live auto-refresh at TF boundaries
 * - Status row: Last updated / Next bar in / Live toggle
 * - Primary button states: Detect Trend ? Stop Live ? Apply & Restart ? Resume Live
 * - Dirty settings detection with Apply & Restart or Discard Changes
 * - Devices-style cards retained; presets removed (user controls MA/Slope/Structure/Strength)
 */

// ===== Types & Defaults
export type TfLabel = "15m" | "1h" | "4h";
export type FinalLabel = "Strong Bullish" | "Bullish" | "Bearish" | "Strong Bearish";
type TrendBar = { t:number; o:number; h:number; l:number; c:number; complete?:boolean };
const REFRESH_MS = 15000; // 15s; use 5000–30000 based on taste



type TrendResp = {
  preview?: { bars?: TrendBar[] };
  nextCloseTs?: number;
  lastClosedTs?: number;
  serverNow?: number;
  warming?: boolean;
  message?: string;
  diagnostics?: any;
  stale?: boolean;
  pollAfterMs?: number;
};

type PreviewBarT = {
  t_open_ms: number;
  t_close_ms: number;
  o: number;
  h: number;
  l: number;
  c: number;
  complete?: boolean;
};



// === Broker-time formatter (UTC -> broker-local, offset-only; DST handled via agent offset) ===
// ==== Broker meta (single source of truth) ====
type BrokerMeta = {
  tz_name?: string | null;       // e.g., "EET" / "EEST" / "UTC+02:00"
  tz_abbr?: string | null;       // optional, safe to carry
  tz_offset_min?: number | null; // REQUIRED for formatting
};
function normalizeBroker(b: any): BrokerMeta | null {
  if (!b) return null;
  const off =
    Number.isFinite(Number(b?.tz_offset_min)) ? Number(b.tz_offset_min)
    : Number.isFinite(Number(b?.utc_offset_min)) ? Number(b.utc_offset_min)
    : null;

  const name = (b?.tz_name ?? b?.tz_abbr ?? "").trim() || null;
  return (name || off !== null) ? { tz_name: name, tz_offset_min: off } : null;
}

// ---- time helpers ----

// === Unified broker used across the page for all time labels ===
type AnyBar = { t_open_ms?: number; t_close_ms?: number; t?: number };

export function barOpenMs(bar: AnyBar, tfMs: number): number {
  if (typeof bar?.t_open_ms === "number") return bar.t_open_ms;          // preferred
  if (typeof bar?.t === "number")        return bar.t * 1000;            // legacy {t,o,h,l,c}
  if (typeof bar?.t_close_ms === "number" && tfMs > 0) return bar.t_close_ms - tfMs; // derive
  return 0;
}


// map TfLabel -> ms
const tfMsFrom = (tf: TfLabel) =>
  tf === "15m" ? 15 * 60 * 1000 :
  tf === "1h"  ? 60 * 60 * 1000 :
                 4  * 60 * 60 * 1000;

export function fmtBrokerTime(
  msUtc: number,
  broker?: BrokerMeta | null
) {
  const offMs = (broker?.tz_offset_min ?? 0) * 60_000;
  // Shift UTC by broker offset, then render in UTC to avoid double-shift.
  return new Intl.DateTimeFormat("en-GB", {
    timeZone: "UTC",
    year: "numeric",
    month: "short",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: true,
  }).format(new Date(msUtc + offMs));
}

// ---- MA helpers (client-side preview only) ----
function sma(values: number[], p: number) {
  const out: number[] = [];
  let s = 0;
  for (let i = 0; i < values.length; i++) {
    s += values[i];
    if (i >= p) s -= values[i - p];
    out.push(i + 1 >= p ? s / p : s / Math.max(1, i + 1));
  }
  return out;
}

function ema(values: number[], p: number) {
  const out: number[] = [];
  const k = 2 / (p + 1);
  let prev = values[0] ?? 0;
  for (let i = 0; i < values.length; i++) {
    const cur = i === 0 ? prev : values[i] * k + prev * (1 - k);
    out.push(cur);
    prev = cur;
  }
  return out;
}

function linePath(xs: number[], ys: number[]) {
  let d = "";
  for (let i = 0; i < xs.length; i++) {
    const x = xs[i], y = ys[i];
    if (Number.isFinite(x) && Number.isFinite(y)) {
      d += (d ? " L " : "M ") + x + " " + y;
    }
  }
  return d;
}


export function getBrokerOverride(): BrokerMeta | null {
  try {
    const raw = localStorage.getItem("xtl.broker.tz");
    if (!raw) return null;
    const j = JSON.parse(raw);
    const tz_offset_min = Number.isFinite(j?.tz_offset_min) ? j.tz_offset_min : undefined;
    const tz_name = typeof j?.tz_name === "string" && j.tz_name ? j.tz_name : undefined;
    if (tz_name || Number.isFinite(tz_offset_min)) return { tz_name, tz_offset_min } as any;
    return null;
  } catch {
    return null;
  }
}


function tzLabelFromBroker(b?: { tz_name?: string; tz_offset_min?: number | string }) {
  if (!b) return "UTC";
  if (b.tz_name) return b.tz_name;
  const off = Number(b.tz_offset_min ?? 0);
  const sign = off >= 0 ? "+" : "-";
  const hh = String(Math.floor(Math.abs(off) / 60)).padStart(2, "0");
  const mm = String(Math.abs(off) % 60).padStart(2, "0");
  return `UTC${sign}${hh}:${mm}`;
}


type Pivot = { t_open_ms: number; price: number; kind: "H" | "L"; confirmed: boolean };

/**
 * ZigZag-style pivots
 * @param bars preview bars (ascending)
 * @param thresholdPct % reversal required to confirm a pivot (e.g., 0.40 = 0.40%)
 * @param backstep minimum bars between pivots; nearer pivots only replace if more extreme
 */
function computePivots(
  bars: Array<{ t_open_ms: number; h: number; l: number; c: number; complete?: boolean }>,
  thresholdPct: number,
  backstep: number
): Pivot[] {
  if (!bars || bars.length < 5) return [];

  // Use CLOSED bars for stability; keep last as tentative if forming
  const last = bars[bars.length - 1];
  const useLast =
    last && last.complete === false ? bars.slice(0, -1) : bars.slice();

  const pivots: Pivot[] = [];
  const pctMove = (from: number, to: number) =>
    from === 0 ? 0 : Math.abs((to - from) / from) * 100;

  // swing state
  let dir: 1 | -1 | 0 = 0; // 1=up leg, -1=down leg, 0=unknown
  let refIdx = 0;
  let refH = useLast[0].h;
  let refL = useLast[0].l;

  // backstep bookkeeping
  let lastPivotIdx = -1;

  // helper: push or replace last pivot if within backstep and new one is more extreme
  const pushPivot = (idx: number, pv: Pivot) => {
    if (pivots.length && idx - lastPivotIdx < backstep) {
      const prev = pivots[pivots.length - 1];
      const moreExtreme =
        (pv.kind === "H" && pv.price > prev.price) ||
        (pv.kind === "L" && pv.price < prev.price);
      if (moreExtreme) {
        pivots[pivots.length - 1] = pv;
        lastPivotIdx = idx;
      }
      return;
    }
    pivots.push(pv);
    lastPivotIdx = idx;
  };

  for (let i = 1; i < useLast.length; i++) {
    const b = useLast[i];
    // expand current leg extremes
    refH = Math.max(refH, b.h);
    refL = Math.min(refL, b.l);

    // look for reversal down (confirm swing HIGH)
    if (dir >= 0) {
      if (pctMove(refH, b.l) >= thresholdPct) {
        // find exact bar that made the high since refIdx
        let hi = useLast[refIdx];
        let hiIdx = refIdx;
        for (let j = refIdx; j <= i; j++) if (useLast[j].h >= hi.h) { hi = useLast[j]; hiIdx = j; }
        pushPivot(hiIdx, { t_open_ms: hi.t_open_ms, price: hi.h, kind: "H", confirmed: true });
        // reset for down leg
        dir = -1;
        refIdx = i;
        refH = b.h;
        refL = b.l;
        continue;
      }
    }

    // look for reversal up (confirm swing LOW)
    if (dir <= 0) {
      if (pctMove(refL, b.h) >= thresholdPct) {
        let lo = useLast[refIdx];
        let loIdx = refIdx;
        for (let j = refIdx; j <= i; j++) if (useLast[j].l <= lo.l) { lo = useLast[j]; loIdx = j; }
        pushPivot(loIdx, { t_open_ms: lo.t_open_ms, price: lo.l, kind: "L", confirmed: true });
        // reset for up leg
        dir = 1;
        refIdx = i;
        refH = b.h;
        refL = b.l;
        continue;
      }
    }
  }

  // add tentative marker for current (forming) leg end
  if (last) {
    const k: "H" | "L" = dir >= 0 ? "H" : "L";
    const price = k === "H" ? refH : refL;
    pivots.push({ t_open_ms: last.t_open_ms, price, kind: k, confirmed: false });
  }

  // show only the most recent few to avoid clutter
  return pivots.slice(-8);
}


// --- Broker verify UI state ---


// ---- Broker compare: safe types & helpers (no-throw) ----
type BrokerField = "o" | "h" | "l" | "c";
type BrokerCheck =
  | { kind: "idle" }
  | { kind: "checking" }
  | { kind: "aligned" }
  | { kind: "missing"; t: number }
  | { kind: "mismatch"; t: number; fields: Array<{ field: BrokerField; app: number; broker: number }> }
  | { kind: "error"; msg: string };

// Use your existing env var; falls back to "/_api"
const API_ORIGIN: string =
  (import.meta.env?.VITE_API_ORIGIN as string) || "/_api";

/**
 * Safe helper: verify last closed bar against broker.
 * - No throws; always catches and returns.
 * - Works even if backend doesn't expose missingInAppTS.
 */



// Minimal, robust formatter: use broker UTC offset only.
// Assumes all timestamps coming from server are UTC milliseconds.


function _offsetMs(broker?: BrokerMeta | null): number {
  const offMin = Number(broker?.tz_offset_min);
  return Number.isFinite(offMin) ? offMin * 60_000 : 0;
}

export function formatInBrokerTZ(msUtc: number, broker?: BrokerMeta | null) {
  const offMs = _offsetMs(broker);
  return new Intl.DateTimeFormat("en-GB", {
    timeZone: "UTC", // we already shifted by offset
    year: "numeric",
    month: "short",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: true,
  }).format(new Date(Number(msUtc) + offMs));
}

export function floorToTfInBrokerTZ(
  msUtc: number,
  tfMs: number,
  broker?: BrokerMeta | null
) {
  const tf = Math.max(1, Number(tfMs) || 0); // guard against 0/NaN
  const offMs = _offsetMs(broker);
  const shifted = Number(msUtc) + offMs;                 // broker wall-time
  const floored = Math.floor(shifted / tf) * tf;         // floor to TF boundary
  return floored - offMs;                                 // back to UTC ms (bar OPEN)
}


const ENABLE_BROKER_VERIFY = false;

async function verifyLastBarAgainstBrokerSafely(opts: {
  symbol: string;
  tf: "M15" | "H1" | "H4";
  lastTsMs: number; // ms since epoch (OPEN time = lastClosedTs - tf)
  set: (s: BrokerCheck) => void;
}) {
  const { symbol, tf, lastTsMs, set } = opts;
  try {
    if (!ENABLE_BROKER_VERIFY) {
      console.log("[VERIFY] disabled", { symbol, tf, lastTsMs });
      // valid no-op state that satisfies BrokerCheck type
      set({ kind: "idle" });
      return;
    }

    set({ kind: "checking" });

    // If we don't have a last closed bar yet, bail out gracefully
    if (!lastTsMs || !Number.isFinite(lastTsMs)) {
      set({ kind: "idle" });
      return;
    }

    const url = `${API_ORIGIN}/devices/compare_ohlc?symbol=${encodeURIComponent(
      symbol
    )}&tf=${tf}&n=20`;

    console.log("[VERIFY] req", { symbol, tf, lastTsMs, url });

    const res = await fetch(url, { credentials: "include" });
    if (!res.ok) {
      set({ kind: "error", msg: `HTTP ${res.status}` });
      return;
    }

    const j = await res.json();
    

    console.log("[VERIFY] resp", j);

    const lastSec = Math.floor(lastTsMs / 1000);
    const diffs: any[] = Array.isArray(j?.diffs) ? j.diffs : [];
    const hit = diffs.find((d) => d?.t === lastSec);
    console.log("[VERIFY] last-sec", lastSec, "hit:", hit);

    if (hit && Array.isArray(hit.diffs) && hit.diffs.length > 0) {
      set({
        kind: "mismatch",
        t: lastSec,
        fields: hit.diffs.map((x: any) => ({
          field: x.field as BrokerField,
          app: x.app,
          broker: x.broker,
        })),
      });
      return;
    }

    // optional arrays (if backend already added them)
    const missingInAppTS: number[] = Array.isArray(j?.missingInAppTS)
      ? j.missingInAppTS
      : [];

    if (missingInAppTS.includes(lastSec)) {
      set({ kind: "missing", t: lastSec });
      return;
    }

    set({ kind: "aligned" });
  } catch (err: any) {
    console.warn("[VERIFY] error", err);
    set({ kind: "error", msg: err?.message || "compare failed" });
  }
}

// Offset-only formatter: UTC ms -> broker wall-time using tz_offset_min
export function formatInBrokerOffsetOnly(
  msUtc: number,
  broker?: BrokerMeta | null
) {
  const offMin = Number(broker?.tz_offset_min);
  const offMs  = Number.isFinite(offMin) ? offMin * 60_000 : 0;
  return new Intl.DateTimeFormat("en-GB", {
    timeZone: "UTC",
    year: "numeric",
    month: "short",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: true,
  }).format(new Date(Number(msUtc) + offMs));
}

const DEFAULT_CFG = {
  symbol: "XAUUSD",
  trendTF: "1h" as TfLabel,
  ma: { type: "EMA" as "EMA" | "SMA", fast: 50, slow: 200, slopeWin: 5, slopeThr: 0.30 },
  adx: { period: 14, min: 20, strong: 22, useDIbias: true },
  swings: {
    method: "zigzag_atr" as "zigzag_atr" | "fractal",
    // Threshold mode: "atr" uses kTrend * ATR(14); "percent" uses pivotPct (%)
    mode: "atr" as "atr" | "percent",
    kTrend: 1.5,          // ATR× when mode="atr"
    pivotPct: 0.40,       // % reversal when mode="percent" (e.g., 0.40 = 0.40%)
    minBarsTrend: 4,      // backstep: minimum bars between pivots
  },
  labels: { bullishCut: 0.25, bearishCut: -0.1, slopeStrong: 0.4 },
};

// Local persistence per user+symbol
const USER = (typeof window !== "undefined" && (window as any).__XTL_USER__?.username) || "anon";
const cfgKey = (s: string) => `xtl_trend_cfg_${USER}_${s}_v5`;
const loadCfg = (s: string) => {
  try { const raw = localStorage.getItem(cfgKey(s)); if (raw) return JSON.parse(raw); } catch {}
  return DEFAULT_CFG;
};
const saveCfg = (cfg: any) => { try { localStorage.setItem(cfgKey(cfg.symbol), JSON.stringify(cfg)); } catch {} };

// ===== Utils
const cx = (...c: (string | false | undefined)[]) => c.filter(Boolean).join(" ");

function FinalBadge({ label }: { label: FinalLabel }) {
  const tone = label.includes("Bullish") ? "emerald" : "rose";
  const strong = label.startsWith("Strong");
  return (
    <div className={cx(
      "inline-flex items-center gap-2 px-3 py-2 rounded-xl border",
      tone === "emerald"
        ? "border-emerald-700/60 bg-emerald-600/15 text-emerald-300"
        : "border-rose-700/60 bg-rose-600/15 text-rose-300"
    )}>
      <span className={cx("h-2 w-2 rounded-full bg-current", strong && "animate-pulse")} />
      <span className="text-sm font-semibold tracking-tight">{label}</span>
    </div>
  );
}

function Stat({ label, value, hint }: { label: string; value: string; hint?: string }) {
  return (
    <div className="rounded-2xl border border-slate-700/60 bg-slate-900/60 p-4">
      <div className="text-slate-400 text-xs mb-1">{label}</div>
      <div className="text-slate-100 text-xl font-semibold tracking-tight">{value}</div>
      {hint && <div className="text-slate-500 text-xs mt-1">{hint}</div>}
    </div>
  );
}

function Panel({ title, children, right, className }: { title: string; children: React.ReactNode; right?: React.ReactNode; className?: string }) {
  return (
    <section className={cx("h-full rounded-2xl bg-slate-800/60 ring-1 ring-slate-700/70 shadow-xl shadow-black/30 backdrop-blur-md p-4", className)}>
      <div className="flex items-center justify-between mb-3">
        <h3 className="text-slate-100 text-sm font-semibold tracking-tight">{title}</h3>
        {right}
      </div>
      {children}
    </section>
  );
}

function Segmented<T extends string>({ value, onChange, options }: { value: T; onChange: (v: T) => void; options: { label: string; value: T }[]; }) {
  return (
    <div className="inline-flex rounded-xl border border-slate-700/60 bg-slate-950/60 p-1">
      {options.map((o) => (
        <button key={o.value} onClick={() => onChange(o.value)} className={cx("px-3 h-8 rounded-lg text-sm", value === o.value ? "bg-indigo-600/20 text-indigo-200 border border-indigo-600/50" : "text-slate-300 hover:text-white")}>{o.label}</button>
      ))}
    </div>
  );
}


// --- Time helpers: align to TF boundaries
function nextBoundary(tf: TfLabel, from = new Date()) {
  const d = new Date(from.getTime());
  d.setMilliseconds(0); d.setSeconds(0);
  const h = d.getHours();
  if (tf === "15m") {
    // quarters: 00, 15, 30, 45 -> go to the next one
    const m = d.getMinutes();
    const nextQ = Math.ceil(m / 15) * 15;
    if (nextQ === 60) { d.setHours(h + 1); d.setMinutes(0); }
    else { d.setMinutes(nextQ); }
  } else if (tf === "1h") {
    d.setMinutes(0); d.setHours(h + 1);
  } else {
    // 4h blocks at 0/4/8/12/16/20
    const nextH = Math.ceil((h + (d.getMinutes() > 0 ? 1/60 : 0)) / 4) * 4;
    d.setMinutes(0); d.setHours(nextH);
  }
  return d;
}

function fmtTime(ts?: number | Date) {
  if (!ts) return "—";
  const d = typeof ts === "number" ? new Date(ts) : ts;
  return d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
}

function fmtCountdown(ms: number) {
  if (ms <= 0) return "00m 00s";
  const s = Math.floor(ms / 1000);
  const m = Math.floor(s / 60);
  const r = s % 60;
  return `${String(m).padStart(2, "0")}m ${String(r).padStart(2, "0")}s`;
}

// --- Validation & clamping (quiet guardrails)
function clampCfg(cfg: any) {
  const n = JSON.parse(JSON.stringify(cfg));
  n.ma.fast = Math.min(100, Math.max(5, Number(n.ma.fast)));
  n.ma.slow = Math.min(400, Math.max(50, Number(n.ma.slow)));
  if (n.ma.fast >= n.ma.slow) n.ma.slow = Math.min(400, Math.max(n.ma.fast * 2, n.ma.fast + 1));
  n.ma.slopeWin = Math.min(10, Math.max(3, Number(n.ma.slopeWin)));
  n.ma.slopeThr = Number(Math.min(0.6, Math.max(0.2, Number(n.ma.slopeThr))).toFixed(2));
  n.swings.kTrend = Number(Math.min(1.75, Math.max(1.25, Number(n.swings.kTrend))).toFixed(2));
  n.swings.minBarsTrend = Math.min(8, Math.max(2, Number(n.swings.minBarsTrend)));
  n.adx.period = Math.min(50, Math.max(5, Number(n.adx.period)));
  n.adx.min = Math.min(60, Math.max(5, Number(n.adx.min)));
  n.adx.strong = Math.min(30, Math.max(20, Number(n.adx.strong)));
  if (n.adx.strong < n.adx.min) n.adx.strong = Math.min(30, n.adx.min + 2);
  n.labels.bullishCut = Number(Math.min(0.5, Math.max(0.1, Number(n.labels.bullishCut))).toFixed(2));
  n.labels.bearishCut = Number(Math.max(-0.5, Math.min(-0.1, Number(n.labels.bearishCut))).toFixed(2));
  n.labels.slopeStrong = Number(Math.min(0.6, Math.max(0.3, Number(n.labels.slopeStrong))).toFixed(2));
  return n;
}

function deepEqual(a: any, b: any) {
  try { return JSON.stringify(a) === JSON.stringify(b); } catch { return false; }
}

function CandlesPreview({
  bars,
  forming,
  broker,
  height = 300,
  pad = 10,
}: {
  bars: { t: number; o: number; h: number; l: number; c: number; complete?: boolean }[];
  forming?: { t: number; o: number; h: number; l: number; c: number; complete?: boolean } | null;
  broker?: { tz_name?: string | null; tz_offset_min?: number | null } | null;
  height?: number;
  pad?: number;
}) {
  // Merge bars + optional forming (at the end)
  const data = React.useMemo(() => {
    const src = [...bars];
    if (forming) src.push({ ...forming, complete: false });
    // Limit to last N (keeps it light)
    const N = Math.min(120, src.length);
    return src.slice(-N);
  }, [bars, forming]);

  // Early empty state
  if (!data.length) {
    return (
      <div className="aspect-[16/9] w-full rounded-xl bg-slate-950/60 border border-slate-800/60 grid place-items-center">
        <div className="text-slate-500 text-sm">No bars yet</div>
      </div>
    );
  }

  // Dimensions
  const width = Math.max(500, data.length * 6 + pad * 2); // ~6px per candle
  const H = height;
  const W = width;

  // Scales
  const minL = Math.min(...data.map((d) => d.l));
  const maxH = Math.max(...data.map((d) => d.h));
  const y = (v: number) => {
    // invert (price high at top)
    return pad + (H - pad * 2) * (1 - (v - minL) / Math.max(1e-9, maxH - minL));
  };

  const cw = 4; // candle body width
  const gap = 2; // gap between candles
  const step = cw + gap;

  // X positions (even spacing)
  const x = (i: number) => pad + i * step + cw / 2; // wick center
  const bodyX = (i: number) => pad + i * step;

  // Simple axis labels (min / mid / max)
  const mid = (minL + maxH) / 2;
  


  return (
  <div className="w-full overflow-auto rounded-xl border border-slate-800/60 bg-slate-950/60">
    <div className="relative overflow-hidden">
      <svg width={W} height={H}>
        {/* bg */}
        <rect x={0} y={0} width={W} height={H} fill="rgba(2,6,23,.6)" />

        {/* horizontal grid lines */}
        {[maxH, mid, minL].map((val, i) => (
          <g key={i}>
            <line
              x1={0}
              x2={W}
              y1={y(val)}
              y2={y(val)}
              stroke="rgba(148,163,184,.25)"
              strokeDasharray="4 4"
            />
            <text
              x={W - 4}
              y={y(val) - 4}
              textAnchor="end"
              fontSize="10"
              fill="#94a3b8"
            >
              {val.toFixed(2)}
            </text>
          </g>
        ))}

        {/* Candles */}
        {data.map((d, i) => {
          const isUp = d.c >= d.o;
          const wickX = x(i);
          const bh = Math.max(1, Math.abs(y(d.o) - y(d.c))); // body height
          const by = Math.min(y(d.o), y(d.c));                 // body y
          // Colors — closed vs forming dim
          const alpha = d.complete === false ? 0.5 : 0.95;
          const stroke = isUp ? `rgba(34,197,94,${alpha})` : `rgba(244,63,94,${alpha})`;
          const fill = isUp ? `rgba(34,197,94,${alpha})` : `rgba(244,63,94,${alpha})`;

          return (
            <g key={i}>
              {/* wick */}
              <line
                x1={wickX}
                x2={wickX}
                y1={y(d.h)}
                y2={y(d.l)}
                stroke={stroke}
                strokeWidth={1}
              />
              {/* body */}
              <rect
                x={bodyX(i)}
                width={cw}
                y={by}
                height={bh}
                fill={fill}
                rx={1.5}
              />
            </g>
          );
        })}

        {/* time markers (start / end) */}
        <text x={pad} y={H - 4} fontSize="10" fill="#94a3b8">
          {fmtBrokerTime(((data[0] as any).t_open_ms ?? data[0].t * 1000), broker)} 
        </text>
        <text x={W - pad} y={H - 4} fontSize="10" fill="#94a3b8" textAnchor="end">
           {fmtBrokerTime(((data[data.length - 1] as any).t_open_ms ?? data[data.length - 1].t * 1000), broker)}
        </text>
      </svg>
    </div>
  </div>
);
}

function getBarOpenMs(
  bar: { t_open_ms?: number; t_close_ms?: number; t?: number },
  tfMs: number
): number {
  if (typeof bar?.t_open_ms === "number") return bar.t_open_ms;
  if (typeof bar?.t === "number") return bar.t * 1000;        // legacy preview bars {t,o,h,l,c}
  if (typeof bar?.t_close_ms === "number" && tfMs > 0) return bar.t_close_ms - tfMs;
  return 0;
}

function MiniCandleChart({
  bars,
  broker,
  tfMs,
  height = 280,
  pivots,        
  showPivots,
  maPreview,
}: {
  
  bars: { t_open_ms: number; t_close_ms: number; o: number; h: number; l: number; c: number; complete?: boolean }[];
  broker?: any;
  tfMs: number;
  height?: number;
  pivots?: { t_open_ms: number; price: number; kind: "H" | "L"; confirmed: boolean }[]; 
  showPivots?: boolean; 
  maPreview?: { fast: number[]; slow: number[]; times: number[] };

}) {
  const mp = maPreview ?? { fast: [], slow: [], times: [] };
  if (!bars?.length) {
    return (
      <div className="aspect-[16/9] w-full rounded-xl bg-slate-950/60 border border-slate-800/60 grid place-items-center">
        <div className="text-slate-500 text-sm">No preview data</div>
      </div>
    );
  }
  const dec = Math.max(
    0,
    Math.min(8, Number.isFinite(broker?.digits) ? Number(broker.digits) : 2)
  );

  const padL = 32, padR = 12, padT = 16, padB = 28;
  const W = Math.max(600, bars.length * 6) + padL + padR;
  const H = height;
  // fixed panel + gutter (so it never overlaps candles)
  const PANEL_W = 148;
  const PANEL_PAD = 8;
  const PANEL_LH = 16;
  const PANEL_MARGIN_R = 4;
  const PLOT_TO_PANEL_GAP = 16;
  // gutter must be a bit wider than the panel
  const RIGHT_GUTTER = PANEL_W + PANEL_MARGIN_R + PLOT_TO_PANEL_GAP;

  const crisp = (v: number) => Math.round(v) + 0.5;
  const ys = bars.flatMap(b => [b.h, b.l]);
  const yMin = Math.min(...ys);
  const yMax = Math.max(...ys);
  const yRange = yMax - yMin || 1;
  const plotRight = padR + RIGHT_GUTTER;

  const xw = (W - padL - plotRight) / bars.length;
  const bodyW = Math.max(2, Math.floor(xw * 0.45));

  const y = (p: number) =>
    padT + (H - padT - padB) * (1 - (p - yMin) / yRange);

  const last = bars[bars.length - 1];
  
  const fmt = (msUtc: number) => formatInBrokerTZ(msUtc, broker);




  
  // --- hover state + helpers ---
const [hover, setHover] = React.useState<null | { i: number; x: number }>(null);
// --- Tooltip positioning (clamp + flip) ---
const tipW = 200;
const tipH = 112;
const tipPad = 12;
function placeTooltip(mx: number, my: number) {
  let tx = mx + tipPad;
  let ty = my - tipH / 2;
  const rightLimit = W - padR - tipPad;
  if (tx + tipW > rightLimit) {
    tx = mx - tipW - tipPad;
    if (tx < padL + tipPad) tx = rightLimit - tipW;
  }
  const topLimit = 8;
  const botLimit = H - 8 - tipH;
  if (ty < topLimit) ty = topLimit;
  if (ty > botLimit) ty = botLimit;
  return { tx, ty };
}

// crisp strokes on high-DPI


const onMove = (e: React.MouseEvent<SVGSVGElement>) => {
  const svg = e.currentTarget as SVGSVGElement;
  const r = svg.getBoundingClientRect();
  const mx = e.clientX - r.left;

  // ignore outside the plot area
  if (mx < padL || mx > W - plotRight) {
    setHover(null);
    return;
  }

  // snap to nearest candle center
  let i = Math.round((mx - padL - xw / 2) / xw);
  i = Math.max(0, Math.min(bars.length - 1, i));
  const xc = padL + i * xw + xw / 2;
  setHover({ i, x: xc });
};

const onLeave = () => setHover(null);

return (
  <div className="w-full overflow-x-auto rounded-xl border border-slate-800/60 bg-slate-950/60">
    <svg
      width={W}
      height={H}
      onMouseMove={onMove}
      onMouseLeave={onLeave}
      style={{ cursor: "crosshair", display: "block" }}
    >
      {/* grid frame */}
      {/* grid frame */}
      <line x1={padL} y1={crisp(padT)} x2={W - plotRight} y2={crisp(padT)} stroke="#1f2937" />
      <line x1={padL} y1={crisp(H - padB)} x2={W - plotRight} y2={crisp(H - padB)} stroke="#1f2937" />

      



      {/* y labels + horizontal guides */}
      {[0, 0.5, 1].map((p, i) => {
        const v = yMin + p * yRange;
        const yv = y(v);
        return (
          <g key={i}>
            <line x1={padL} y1={yv} x2={W - plotRight} y2={yv} stroke="#111827" opacity={0.5} />
            <text x={8} y={yv + 4} fill="#64748b" fontSize="10">
              {v.toFixed(dec)}
            </text>
          </g>
        );
      })}

      {/* candles */}
      {bars.map((b, i) => {
        const xc = padL + i * xw + xw / 2;
        const bull = b.c >= b.o;
        const color = bull ? "#22c55e" : "#ef4444";
        const yH = y(b.h), yL = y(b.l), yO = y(b.o), yC = y(b.c);
        const top = Math.min(yO, yC);
        const h = Math.max(2, Math.abs(yO - yC));
        return (
          <g key={i}>
            <line x1={xc} x2={xc} y1={yH} y2={yL} stroke={color} strokeWidth={1} />
            <rect x={xc - bodyW / 2} y={top} width={bodyW} height={h} fill={color} />
          </g>
        );
      })}
      {/* --- ZigZag pivots overlay --- */}
{showPivots && Array.isArray(pivots) && pivots.length > 0 && (
  <g>
    {/* build an index for x-positions */}
    {(() => {
      const indexByOpen = new Map<number, number>();
      bars.forEach((b, i) => indexByOpen.set(b.t_open_ms, i));

      // connector polyline
      const points = pivots
        .map((p) => {
          const i = indexByOpen.get(p.t_open_ms);
          if (i == null) return null;
          const xc = padL + i * xw + xw / 2;
          const yy = y(p.price);
          return `${xc},${yy}`;
        })
        .filter(Boolean)
        .join(" ");

      return (
        <>
          <polyline
            fill="none"
            stroke="rgba(250,250,250,0.6)"
            strokeWidth={1}
            points={points}
          />
          {pivots.map((p, k) => {
            const i = indexByOpen.get(p.t_open_ms);
            if (i == null) return null;
            const xc = padL + i * xw + xw / 2;
            const yy = y(p.price);
            const sz = 5;
            const up = p.kind === "H";
            const opacity = p.confirmed ? 1 : 0.6;
            const pts = up
              ? `${xc},${yy - sz} ${xc - sz},${yy + sz} ${xc + sz},${yy + sz}`   // ?
              : `${xc},${yy + sz} ${xc - sz},${yy - sz} ${xc + sz},${yy - sz}`;   // ?
            return (
              <polygon
                key={`pv-${k}`}
                points={pts}
                fill={up ? "#10b981" : "#ef4444"}
                opacity={opacity}
              />
            );
          })}
          {/* HH/HL/LH/LL labels */}
{(() => {
  // Classify each pivot versus the previous pivot of the same type
  const labels: { k: number; tag: "HH" | "HL" | "LH" | "LL" }[] = [];
  let lastH: number | undefined;
  let lastL: number | undefined;

  pivots.forEach((p, k) => {
    if (p.kind === "H") {
      const tag = lastH == null ? "HH" : (p.price > lastH ? "HH" : "LH");
      labels.push({ k, tag });
      lastH = p.price;
    } else {
      const tag = lastL == null ? "HL" : (p.price > lastL ? "HL" : "LL");
      labels.push({ k, tag });
      lastL = p.price;
    }
  });

  // Render the small text label near each pivot
  return labels.map(({ k, tag }) => {
    const p = pivots[k];
    const i = indexByOpen.get(p.t_open_ms);
    if (i == null) return null;
    const xc = padL + i * xw + xw / 2;
    const yy = y(p.price) + (p.kind === "H" ? -8 : 12); // above highs, below lows

    return (
      <text
        key={`pv-lbl-${k}`}
        x={xc}
        y={yy}
        textAnchor="middle"
        fontSize={10}
        fill="#cbd5e1"
        style={{ pointerEvents: "none" }}
      >
        {tag}
      </text>
    );
  });
})()}

        </>
      );
    })()}
  </g>
)}

      {/* === MA preview overlays === */}
{mp.fast.length > 0 && (
  <path
    d={(() => {
      const xs: number[] = [];
      const ys: number[] = [];
      for (let i = 0; i < bars.length; i++) {
        const v = mp.fast[i];
        if (!Number.isFinite(v)) continue;
        const xc = padL + i * xw + xw / 2;
        xs.push(xc);
        ys.push(y(v));
      }
      return linePath(xs, ys);
    })()}
    stroke="#6366f1"
    strokeWidth={1.2}
    fill="none"
  />
)}

{mp.slow.length > 0 && (
  <path
    d={(() => {
      const xs: number[] = [];
      const ys: number[] = [];
      for (let i = 0; i < bars.length; i++) {
        const v = mp.slow[i];
        if (!Number.isFinite(v)) continue;
        const xc = padL + i * xw + xw / 2;
        xs.push(xc);
        ys.push(y(v));
      }
      return linePath(xs, ys);
    })()}
    stroke="#facc15"
    strokeWidth={1.2}
    fill="none"
  />
)}

      {/* footer label (first ? last) */}
      <text x={padL} y={H - 10} fill="#94a3b8" fontSize="11">
         {formatInBrokerTZ(bars[0].t_open_ms, broker)} · {formatInBrokerTZ(last.t_close_ms, broker)}
      </text>
      


      {/* hover overlay (crosshair + fixed top-right info panel) */}
      {(() => {
        // show hovered bar if any; otherwise show the last bar
        const i = hover ? hover.i : bars.length - 1;
        const b = bars[i];
        const bull = b.c >= b.o;

        // crosshair only when hovering (doesn't block pointer events)
        const cross = hover ? (
          <g pointerEvents="none">
            <line
              x1={hover.x}
              y1={padT}
              x2={hover.x}
              y2={H - padB}
              stroke="#475569"
              strokeDasharray="3 3"
            />
            <circle cx={hover.x} cy={y(b.c)} r={2} fill={bull ? "#10b981" : "#f43f5e"} />
          </g>
        ) : null;

        // fixed info panel in plot's top-right
        const panelW = 180;
        const panelPad = 8;
        const lineH = 16;
        
        const tOpenMs =
        typeof b.t_open_ms === "number" ? b.t_open_ms :
        typeof (b as any).t === "number" ? (b as any).t * 1000 :
        (typeof b.t_close_ms === "number" && tfMs > 0 ? b.t_close_ms - tfMs : 0);
        const openMs =
          typeof b.t_open_ms === "number" ? b.t_open_ms :
          (typeof b.t_close_ms === "number" ? (b.t_close_ms - tfMs) : 0);
        const timeLabel = formatInBrokerTZ(openMs, broker);

        const lines = [
            `Time: ${timeLabel}`, 
            `O: ${b.o.toFixed(dec)}`,
            `H: ${b.h.toFixed(dec)}`,
            `L: ${b.l.toFixed(dec)}`,
            `C: ${b.c.toFixed(dec)}`,
        ];

        const panelH = panelPad * 2 + lines.length * lineH;
                 
        // Fixed top-right (clamped inside chart area)
const fixedX = Math.max(padL + 8, W - padR - panelW - 12);
const fixedY = Math.max(padT + 8, 12);

const panel = (
  <g transform={`translate(${fixedX},${fixedY})`} pointerEvents="none">
    <rect
      x={0}
      y={0}
      width={panelW}
      height={panelH}
      rx={10}
      ry={10}
      fill="rgba(2,6,23,.92)"
      stroke="#334155"
    />
    {lines.map((t, k) => (
      <text
        key={k}
        x={panelPad}
        y={panelPad + (k + 1) * lineH - 4}
        fill="#cbd5e1"
        fontSize="12"
        fontFamily="ui-sans-serif,system-ui"
      >
        {t}
      </text>
    ))}
  </g>
);


        return (
          <>
            {cross}
            {panel}
          </>
        );
      })()}
    </svg>
  </div>
);

}

  


export default function Trend() {
  const [cfg, setCfg] = useState<any>(() => loadCfg(DEFAULT_CFG.symbol));
  // removed appliedCfg state;
  

  const [state, setState] = React.useState<TrendResp| null>(null);
  const [server, setServer] = useState<any | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [live, setLive] = useState(false);
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null);
  const [nextAt, setNextAt] = useState<Date | null>(null);
  const [now, setNow] = React.useState(Date.now());
  const tickRef = useRef<number | null>(null);
  const fetchingRef = useRef(false);
  const activeKeyRef = React.useRef<string>("");
  const timeoutRef = React.useRef<number | null>(null);
  const boundaryLockRef = React.useRef(0);



  // keep latest detectNow without re-creating scheduleOnce
  // keep latest detectNow without re-creating scheduleOnce
  const detectNowRef = React.useRef<null | ((src?: any, overrideCfg?: any) => Promise<void>)>(null);
 


  const scheduleOnce = React.useCallback((ms: number) => {
    // clear any previous timer
    if (timeoutRef.current) {
      window.clearTimeout(timeoutRef.current);
      timeoutRef.current = null;
    }
    // clamp 1.2s..60s to avoid hammering
    const wait = Math.max(1200, Math.min(ms || 0, 60_000));
    const key = activeKeyRef.current;          // pin symbol|tf at schedule time

    timeoutRef.current = window.setTimeout(() => {
      if (!liveRef.current) return;            // Live toggled off
      if (activeKeyRef.current !== key) return;// user switched symbol/TF
      const fn = detectNowRef.current;
      if (fn) void fn("schedule");             // call latest detectNow
    }, wait);
  }, []);

  const liveRef = React.useRef(live);
  React.useEffect(() => { liveRef.current = live; }, [live]);
  const skewRef = useRef(0);
  const [brokerCheck, setBrokerCheck] = React.useState<BrokerCheck>({ kind: "idle" });
  const [lastUpdatedLabel, setLastUpdatedLabel] = useState<string>("-");
  const [nextAtLabel, setNextAtLabel] = useState<string>("-");
  const [showPivots, setShowPivots] = React.useState(true);
  const [pivotPct, setPivotPct] = React.useState(0.25); 
  const lastKickAtRef = React.useRef<number>(0); 
  const warmRetryMsRef = React.useRef(5000); // 5s start, grows to 30s
  const [usingDevice, setUsingDevice] = React.useState<string | null>(null);
  const [broker, setBroker] =
    React.useState<{ tz_name?: string | null; tz_offset_min?: number | null } | null>(null);

 
 


  // Notice + simple apply-on-edit behavior
  const [notice, setNotice] = useState<string | null>(null);
  const applySetting = (next: any) => {
    const safe = clampCfg(next);
    setCfg(safe);
    saveCfg(safe);
    if (live) {
      if (timeoutRef.current) window.clearTimeout(timeoutRef.current);
      setLive(false);
      setNextAt(null);
    }
    setNotice("Settings updated. Press Detect Trend to apply.");
  };

  // derive badge label from API (or score/slope fallback)
  const finalLabel: FinalLabel | null = useMemo(() => {
    // 1) Prefer any textual label the API gives us (old or new shapes)
    const raw =
      server?.h1?.label ??
      server?.label ??
      server?.final ??
      null;

    if (typeof raw === "string" && raw) {
      const s = raw.toLowerCase();
      if (s.includes("strong") && s.includes("bull")) return "Strong Bullish";
      if (s.includes("bull")) return "Bullish";
      if (s.includes("strong") && s.includes("bear")) return "Strong Bearish";
      if (s.includes("bear")) return "Bearish";
    }

   // 2) Fallback: derive from numbers
   // score: prefer old (h1.score), else new (score)
    const sc: number | null =
      typeof server?.h1?.score === "number"
       ? server.h1.score
       : typeof server?.score === "number"
       ? server.score
       : null;

   // slope: prefer old (h1.slope_norm), else new (diagnostics.slopePct)
    const sl: number =
      typeof server?.h1?.slope_norm === "number"
       ? server.h1.slope_norm
       : typeof server?.diagnostics?.slopePct === "number"
       ? server.diagnostics.slopePct
       : 0;

    if (typeof sc === "number") {
      if (sc >= cfg.labels.bullishCut + 0.15 && sl >= cfg.labels.slopeStrong) return "Strong Bullish";
      if (sc >= cfg.labels.bullishCut) return "Bullish";
      if (sc <= cfg.labels.bearishCut - 0.15 && sl <= -cfg.labels.slopeStrong) return "Strong Bearish";
      if (sc <= cfg.labels.bearishCut) return "Bearish";
    }
    return null;
}, [server, cfg]);

// cache broker so first paint after load/toggle doesn't fall back to Local
const [cachedBroker, setCachedBroker] = React.useState<
  { tz_name?: string | null; tz_offset_min?: number | null } | null
>(null);

// update cache whenever server returns broker; restore on first mount
React.useEffect(() => {
  const latestBroker = (server as any)?.preview?.broker ?? server?.broker ?? null;
  if (latestBroker) {
    setCachedBroker(latestBroker);
    try { sessionStorage.setItem("xtl-broker", JSON.stringify(latestBroker)); } catch {}
  } else if (!cachedBroker) {
    try {
      const s = sessionStorage.getItem("xtl-broker");
      if (s) setCachedBroker(JSON.parse(s));
    } catch {}
  }
}, [server?.broker]);
// derived value you’ll use everywhere in the UI (prefer agent/preview broker)
const displayBroker =
  normalizeBroker((server as any)?.preview?.broker ?? server?.broker ?? cachedBroker ?? getBrokerOverride()) ?? null;


// Reset state when TF or symbol changes so old preview doesn't linger
React.useEffect(() => {
  // stop any scheduled call
  if (timeoutRef.current) window.clearTimeout(timeoutRef.current);
  fetchingRef.current = false;

  // clear stale UI from previous TF/symbol
  setServer(null);
  setNotice("Warming up - awaiting bars");
  setNextAt(null);
  setLastUpdated(null);

  // unpin until user presses Detect again
  activeKeyRef.current = "";
}, [cfg.trendTF, cfg.symbol]);


useEffect(() => { saveCfg(cfg); }, [cfg]);
// Passive preview auto-refresh (keeps last closed =60 candles up to date)
// - Runs even when Live is off
// - Uses the same detectNow("schedule") path (no overlapping calls)
// - Refreshes on mount and every REFRESH_MS while the tab is visible
useEffect(() => {
  
  return () => {};
}, [cfg.symbol, cfg.trendTF]);


// countdown ticker (for Next bar in)
useEffect(() => {
  if (!live || !nextAt) return;
  const id = window.setInterval(() => {
    const nowMs = Date.now();
    setNow(nowMs);
    const delta = nextAt.getTime() - nowMs;

    if (delta <= 0) {
      
      window.clearInterval(id);
      setNextAt(null);                               // disarm before fetch
      if (timeoutRef.current) {                      // ensure no pending one-shot
        window.clearTimeout(timeoutRef.current);
        timeoutRef.current = null;
      }
      scheduleOnce(1200);                              // single re-arm to fetch new bar
      return;
    }
  }, 1000);
  return () => window.clearInterval(id);
}, [live, nextAt, scheduleOnce]);

// whenever serverNow/nextCloseTs change, set the absolute boundary
useEffect(() => {
  const now  = Number(server?.serverNow ?? 0);
  const next = Number(server?.nextCloseTs ?? 0);
  if (Number.isFinite(now) && Number.isFinite(next) && next > now) {
    const skew = Date.now() - now;    // local - server
    setNextAt(new Date(next + skew)); // absolute Date for countdown
  }
}, [server?.serverNow, server?.nextCloseTs]);


// helper: compute safe delay until next boundary (or backend hint)
const msUntilNext = (resp: TrendResp, tfLabel: TfLabel) => {
  // Map timeframe label ? milliseconds (support a few aliases)
  const tfMap: Record<string, number> = {
    M15: 15 * 60 * 1000,  H1: 60 * 60 * 1000,  H4: 4 * 60 * 60 * 1000,
    "15M": 15 * 60 * 1000, "1H": 60 * 60 * 1000, "4H": 4 * 60 * 60 * 1000,
    "15m": 15 * 60 * 1000, "1h": 60 * 60 * 1000, "4h": 4 * 60 * 60 * 1000,
  };

  // Derive TF strictly from label; default to H1 if unknown
  const tfMs = tfMap[String(tfLabel)] || 60 * 60 * 1000;

  const now  = Number(resp?.serverNow ?? Date.now());
  const next = Number(resp?.nextCloseTs ?? 0);
  const last = Number(resp?.lastClosedTs ?? 0);

  // clamp helper: min 2s, max 1.5× TF
  const clamp = (ms: number) => Math.max(2000, Math.min(ms, Math.round(1.5 * tfMs)));

  // 1) backend-provided next boundary wins
  if (next > now) return clamp(next - now);

  // 2) otherwise, step from lastClosedTs by one TF
  if (last > 0) {
    const cand = last + tfMs;
    if (cand > now) return clamp(cand - now);
  }

  // 3) otherwise, align from "now" to the next TF slot
  const slot = Math.floor(now / tfMs) * tfMs + tfMs;
  return clamp(slot - now);
};

useEffect(() => () => {
  if (tickRef.current) cancelAnimationFrame(tickRef.current);
  if (timeoutRef.current) window.clearTimeout(timeoutRef.current);
}, []);

  const detectNow = async (
  source: "manual" | "schedule" | "boundary" = "manual",
  overrideCfg?: any
) => {
  
  // ignore non-manual calls if not live
  if (!live && source !== "manual") return;
  if (fetchingRef.current) return;           // prevent overlap
  fetchingRef.current = true;

  setLoading(true);
  setError(null);

  const base = overrideCfg ?? cfg;
  const safe = clampCfg(base);

  // On manual start, pin key and enable Live
  if (source === "manual") {
    setLive(true);
    activeKeyRef.current = `${safe.symbol}|${safe.trendTF}`;
  }

  // Ignore if selection changed mid-flight
  const currentKey = `${safe.symbol}|${safe.trendTF}`;

  try {
    const tfParam = safe.trendTF === "15m" ? "M15" : safe.trendTF === "1h" ? "H1" : "H4";
    const qp = new URLSearchParams({
      symbol: safe.symbol,
      tf: tfParam,
      source: "broker",
      n: "300",
    });
    qp.set("adxPeriod", String(safe.adx?.period ?? 14));
    qp.set("adxMin", String(safe.adx?.min ?? 20));
    qp.set("useDIbias", safe.adx?.useDIbias ? "true" : "false");
    qp.set("slopePeriod", String(safe.ma?.slopeWin ?? 5));
    qp.set("slopeThreshold", String(safe.ma?.slopeThr ?? 0.30));
    qp.set("maType", String((safe.ma?.type ?? "EMA").toLowerCase()));
    qp.set("maFast", String(safe.ma?.fast ?? 10));
    qp.set("maSlow", String(safe.ma?.slow ?? 20));

    const url = `/_api/trend/state2?${qp.toString()}`;
    const res = await fetch(url, { headers: { Accept: "application/json" }, credentials: "include" });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);

    const j = await res.json();

    // If user switched symbol/TF while we fetched, bail & schedule a gentle retry
    if (activeKeyRef.current && activeKeyRef.current !== currentKey) {
      setLoading(false);
      fetchingRef.current = false;
      if (timeoutRef.current) window.clearTimeout(timeoutRef.current);
      const hint = Number(j?.pollAfterMs ?? 0);
      scheduleOnce(Math.max(1500, Math.min(hint > 0 ? hint : warmRetryMsRef.current, 30000)));
      warmRetryMsRef.current = Math.min(warmRetryMsRef.current * 2, 30000);
      return;
    }

    // State & broker
    setState(j);
    setServer(j);
    setUsingDevice(((j as any)?.usingDevice as string) ?? ((j as any)?.preview?.broker?.device_id as string) ?? null);
    setBroker(normalizeBroker(j?.preview?.broker ?? j?.broker) ?? getBrokerOverride());
    setError(null);
    // --- re-arm next fetch from server anchors (serverNow/nextCloseTs) ---
    const now  = Number(j?.serverNow ?? 0);
    const next = Number(j?.nextCloseTs ?? 0);
    const hint = Number(j?.pollAfterMs ?? 0);

    // prefer exact server boundary; fallback to server hint
    if (Number.isFinite(now) && Number.isFinite(next) && next > now) {
      scheduleOnce(Math.max(0, next - now) + 250);
    } else if (hint > 0) {
      scheduleOnce(hint);
    }


    // compute skew (serverNow - clientNow)
    const clientNow = Date.now();
    const serverNowMs = typeof j?.serverNow === "number" ? j.serverNow : clientNow;
    skewRef.current = serverNowMs - clientNow;

    // local TF (prefer server tf_ms)
    const tfMsLocal =
      j?.tf_ms ??
      ((safe.trendTF === "15m" ? 15 : safe.trendTF === "1h" ? 60 : 240) * 60 * 1000);

    // Normalize warming if preview bars already exist
    const hasPreview =
      Array.isArray(j?.preview) ? j.preview.length > 0
      : Array.isArray(j?.preview?.bars) ? j.preview.bars.length > 0
      : false;
    if (j?.warming && hasPreview) j.warming = false;

    // Merge local override into broker meta only if server omits pieces
    // Merge local override; let override WIN (so a correct +120 beats stale +330)
    const ov = getBrokerOverride();
    if (ov && (ov.tz_name || Number.isFinite(ov.tz_offset_min))) {
      j.broker = { ...(j.broker || {}), ...ov };
    } else {
      j.broker = j.broker ?? null;
    }


    // ---------- WARMING ----------
    if (j?.warming) {
      setNotice(j.message || "Warming up - awaiting bars from agent");
      setServer(null); // avoid stale preview
      setLastUpdatedLabel(formatInBrokerTZ(serverNowMs, displayBroker));
      setUsingDevice(j?.usingDevice ?? null);

      let nextCloseMs =
        typeof j?.nextCloseTs === "number" ? j.nextCloseTs
        : (Math.floor(serverNowMs / tfMsLocal) + 1) * tfMsLocal;
      if (nextCloseMs - serverNowMs < 1000) nextCloseMs += tfMsLocal;
      setNextAtLabel(formatInBrokerTZ(nextCloseMs, displayBroker));
      setNextAt(new Date(nextCloseMs));

      // Optional verify (kept noop if disabled)
      try {
        const lastClosedCloseMs =
          typeof j?.lastClosedTs === "number" ? j.lastClosedTs
          : (typeof j?.lastClosedTS === "number" ? j.lastClosedTS : 0);
        const lastClosedOpenMs = lastClosedCloseMs ? lastClosedCloseMs - tfMsLocal : 0;
        if (ENABLE_BROKER_VERIFY && lastClosedOpenMs) {
          await verifyLastBarAgainstBrokerSafely({
            symbol: safe.symbol ?? "XAUUSD",
            tf: tfParam,
            lastTsMs: lastClosedOpenMs,
            set: setBrokerCheck,
          });
        }
      } catch { /* ignore */ }

      // schedule guarded retry (hint or backoff)
      const hint = Number(j?.pollAfterMs ?? 0);
      scheduleOnce(Math.max(1500, Math.min(hint > 0 ? hint : warmRetryMsRef.current, 30000)));
      warmRetryMsRef.current = Math.min(warmRetryMsRef.current * 2, 30000);
      return;
    }

    // ---------- NORMAL ----------
    setNotice(null);
    warmRetryMsRef.current = 5000;

    setLastUpdatedLabel(formatInBrokerTZ(serverNowMs, displayBroker));
    setUsingDevice(j?.usingDevice ?? null);
    let nextCloseMs =
      typeof j?.nextCloseTs === "number" ? j.nextCloseTs
      : (Math.floor(serverNowMs / tfMsLocal) + 1) * tfMsLocal;
    if (nextCloseMs - serverNowMs < 1000) nextCloseMs += tfMsLocal;
    setNextAtLabel(formatInBrokerTZ(nextCloseMs, displayBroker));
    setNextAt(new Date(nextCloseMs));

    // Optional verify
    try {
      const lastClosedCloseMs =
        typeof j?.lastClosedTs === "number" ? j.lastClosedTs
        : (typeof j?.lastClosedTS === "number" ? j.lastClosedTS : 0);
      const lastClosedOpenMs = lastClosedCloseMs ? lastClosedCloseMs - tfMsLocal : 0;
      if (ENABLE_BROKER_VERIFY && lastClosedOpenMs) {
        await verifyLastBarAgainstBrokerSafely({
          symbol: safe.symbol ?? "XAUUSD",
          tf: tfParam,
          lastTsMs: lastClosedOpenMs,
          set: setBrokerCheck,
        });
      }
    } catch { /* ignore */ }

    // --- schedule ONCE at the correct boundary/backoff ---
    scheduleOnce(msUntilNext(j, cfg.trendTF as TfLabel));

  } catch (e: any) {
    setError(e?.message || "Failed to detect trend");
    // backoff retry
    scheduleOnce(warmRetryMsRef.current);
    warmRetryMsRef.current = Math.min(warmRetryMsRef.current * 2, 30000);
  } finally {
    fetchingRef.current = false;
    setLoading(false);
  }
};



     
  // keep detectNow ref in sync (must be placed AFTER detectNow is declared)
  React.useEffect(() => { detectNowRef.current = detectNow; }, [detectNow]);
  const stopLive = () => {
    setLive(false);
    if (timeoutRef.current) window.clearTimeout(timeoutRef.current);
    setNextAt(null);
  };


  const resetToStandard = () => {
    const next = { ...DEFAULT_CFG, symbol: cfg.symbol };
    const safe = clampCfg(next);
    setCfg(safe);
    saveCfg(safe);
    if (live) {
      if (timeoutRef.current) window.clearTimeout(timeoutRef.current);
      setLive(false);
      setNextAt(null);
    }
    setNotice("Standard settings restored. Press Detect Trend to apply.");
  };

  const primaryLabel = useMemo(() => (live ? "Stop Live" : "Detect Trend"), [live]);

  const onPrimary = async () => {
    if (live) return stopLive();
    activeKeyRef.current = `${cfg.symbol}|${cfg.trendTF}`;
    void detectNow("manual");
  };

  const secondary = { label: "Reset to Standard", action: resetToStandard } as const;
  

// ---- Preview bars: normalize ? sort ? dedupe ? tail(60) ----
const previewBars: PreviewBarT[] = React.useMemo<PreviewBarT[]>(() => {
  // Use only the server-served ms fields; no legacy conversions
  const src = (server?.preview?.bars ?? []) as any[];

  const rows: PreviewBarT[] = (Array.isArray(src) ? src : [])
    .map((b: any): PreviewBarT => ({
      t_open_ms: Number(b?.t_open_ms ?? 0),
      t_close_ms: Number(b?.t_close_ms ?? 0),
      o: Number(b?.o),
      h: Number(b?.h),
      l: Number(b?.l),
      c: Number(b?.c),
      complete: typeof b?.complete === "boolean" ? b.complete : true,
    }))
    .filter((r) =>
      r.t_open_ms > 0 &&
      r.t_close_ms > 0 &&
      Number.isFinite(r.o) &&
      Number.isFinite(r.h) &&
      Number.isFinite(r.l) &&
      Number.isFinite(r.c)
    )
    .sort((a, b) => a.t_open_ms - b.t_open_ms);

  // Dedupe by open slot (keep the last occurrence)
  const out: PreviewBarT[] = [];
  for (const r of rows) {
    const last = out[out.length - 1];
    if (last && last.t_open_ms === r.t_open_ms) {
      out[out.length - 1] = { ...last, ...r };
    } else {
      out.push(r);
    }
  }

  return out.slice(-300);
}, [server?.preview?.bars]);

// ---- MA preview series (matches what's on the backend, for chart overlay) ----
const maPreview = React.useMemo(() => {
  if (!Array.isArray(previewBars) || previewBars.length < 3) {
    return { fast: [], slow: [], times: [] as number[] };
  }
  const closes = previewBars.map(b => b.c);
  const times  = previewBars.map(b => b.t_close_ms); // align at close

  const fastP = Math.max(2, Number(cfg.ma.fast || 10));
  const slowP = Math.max(3, Number(cfg.ma.slow || 20));
  const type  = String(cfg.ma.type || "EMA").toLowerCase();

  const fast = (type === "sma" ? sma(closes, fastP) : ema(closes, fastP));
  const slow = (type === "sma" ? sma(closes, slowP) : ema(closes, slowP));
  return { fast, slow, times };
}, [previewBars, cfg.ma.fast, cfg.ma.slow, cfg.ma.type]);




// Derive reversal threshold as a PERCENT (either from ATR× or fixed %)
// ---- ATR% × K threshold (always in percent) ----
const thresholdPct = React.useMemo(() => {
  // use ATR mode only when selected
  const mode = (cfg?.swings?.mode ?? "atr") as "atr" | "percent";
  if (mode === "percent") {
    return Number(cfg?.swings?.pivotPct ?? 0.40); // e.g., 0.40 = 0.40%
  }

  // ATR% × K
  const closes = previewBars.map(b => b.c).filter(Number.isFinite);
  if (!closes.length) return 0.40;

  const refClose = closes[closes.length - 1];
  const k = Number(cfg?.swings?.kTrend ?? 1.40);

  // quick ATR14 from preview bars
  const trs: number[] = [];
  for (let i = 1; i < previewBars.length; i++) {
    const p = previewBars[i - 1], b = previewBars[i];
    trs.push(Math.max(b.h - b.l, Math.abs(b.h - p.c), Math.abs(p.c - b.l)));
  }
  if (trs.length < 14) return 0.40;

  // RMA(14)
  let rma = trs[0], alpha = 1 / 14;
  for (let i = 1; i < trs.length; i++) rma = rma * (1 - alpha) + trs[i] * alpha;

  const atrPct = (rma / refClose) * 100;     // ATR% of price
  return atrPct * k;                          // ATR% × K (your slider)
}, [cfg?.swings?.mode, cfg?.swings?.pivotPct, cfg?.swings?.kTrend, previewBars]);

// backstep comes from UI config (min bars between pivots)
const backstep = Number(cfg?.swings?.minBarsTrend ?? 4);

// ?? Then compute pivots using those values
const pivots = React.useMemo(
  () =>
    showPivots && previewBars.length
      ? computePivots(previewBars as any, thresholdPct, backstep)
      : [],
  [showPivots, previewBars, thresholdPct, backstep]
);

// (optional) now hasBars, footer, etc.
const hasBars = previewBars.length > 0;
// Prefer detect/server broker if it has a real TZ; otherwise fall back to local override
const brokerMeta = React.useMemo(() => {
  const sb =
    (server as any)?.preview?.broker ??
    server?.broker ??
    getBrokerOverride();
  const valid =
    !!sb &&
     (
       (typeof (sb as any).tz_name === "string" && (sb as any).tz_name.trim().length > 0) ||
       Number.isFinite(Number((sb as any).tz_offset_min))
     );
  return valid ? sb : getBrokerOverride();
}, [server?.broker, (server as any)?.preview?.broker]);


const isLoadingPreview = Boolean(loading) && !hasBars;

const tfMs = React.useMemo(() => {
  if (typeof server?.tf_ms === "number" && Number.isFinite(server.tf_ms)) return server.tf_ms;
  // fallback based on UI TF
  return cfg.trendTF === "15m" ? 900_000 : cfg.trendTF === "1h" ? 3_600_000 : 14_400_000;
}, [server?.tf_ms, cfg.trendTF]);


  

  const nextInMs = nextAt ? nextAt.getTime() - now : 0;

  return (
    <>
      {/* Scoped styles: normalize + compact */}
      <style>{`
        .xtl-scope select, .xtl-scope input[type="text"], .xtl-scope input[type="number"]{ appearance:none; -webkit-appearance:none; -moz-appearance:none; height:40px; border-radius:12px; border:1px solid rgba(51,65,85,.6); background:rgba(2,6,23,.6); color:#E2E8F0; padding:0 12px; outline:none; width:100%; }
        .xtl-scope select{ background-image:linear-gradient(45deg,transparent 50%,#64748b 50%),linear-gradient(135deg,#64748b 50%,transparent 50%); background-position:calc(100% - 18px) 16px, calc(100% - 13px) 16px; background-size:6px 6px; background-repeat:no-repeat; padding-right:32px; }
        .xtl-scope select:focus, .xtl-scope input[type="text"]:focus, .xtl-scope input[type="number"]:focus{ border-color:rgba(99,102,241,.6); box-shadow:0 0 0 3px rgba(99,102,241,.25); }
        .xtl-scope input[type="range"]{ width:100%; height:4px; border-radius:999px; background:rgba(15,23,42,.6); outline:none; }
        .xtl-scope input[type="range"]::-webkit-slider-thumb{ -webkit-appearance:none; appearance:none; width:18px; height:18px; border-radius:999px; background:rgb(99,102,241); border:2px solid rgb(30,41,59); box-shadow:0 0 0 3px rgba(99,102,241,.25); cursor:pointer; }
        .xtl-scope input[type="range"]::-moz-range-thumb{ width:18px; height:18px; border-radius:999px; background:rgb(99,102,241); border:2px solid rgb(30,41,59); box-shadow:0 0 0 2px rgba(99,102,241,.25); cursor:pointer; }
        .xtl-scope .ctl-sm{ height:34px; font-size:13px; padding:0 10px; border-radius:10px; }
        .xtl-scope .ctl-range-sm{ height:3px; }
        .xtl-scope .ctl-range-sm::-webkit-slider-thumb{ width:14px; height:14px; box-shadow:0 0 0 2px rgba(99,102,241,.25); }
        .xtl-scope .ctl-range-sm::-moz-range-thumb{ width:14px; height:14px; box-shadow:0 0 0 2px rgba(99,102,241,.25); }
      `}</style>

      <main className="xtl-scope max-w-7xl mx-auto p-6 text-slate-100">
        {/* ===== Hero ===== */}
        <div className="rounded-3xl border border-slate-700/60 bg-gradient-to-br from-slate-900 via-slate-900/80 to-slate-950/80 p-6 mb-6">
          <div className="flex flex-col lg:flex-row items-start lg:items-center gap-4 justify-between">
            <div>
              <div className="inline-flex items-center gap-2 text-xs text-indigo-300/80 mb-2"><span className="h-1.5 w-1.5 rounded-full bg-indigo-400 animate-pulse"/>Live Trend Engine</div>
              <h1 className="text-3xl font-semibold tracking-tight">Trend Insight <span className="text-slate-400">·</span> {cfg.trendTF.toUpperCase()}</h1>
              <p className="text-slate-400 text-sm mt-1">Uses your settings below. Badge reflects the last closed {cfg.trendTF.toUpperCase()} bar.</p>
            </div>
            <div className="flex items-center gap-2">
              <button onClick={onPrimary} disabled={loading} className="h-10 px-4 rounded-xl bg-indigo-600 hover:bg-indigo-500 disabled:opacity-60">{loading?"Detecting…":primaryLabel}</button>
              <button onClick={secondary.action} className="h-10 px-4 rounded-xl border border-slate-600/60 bg-slate-800/40 hover:bg-slate-800/60">{secondary.label}</button>
              <label className="flex items-center gap-2 text-xs text-slate-400 pl-2">
                <input
                  type="checkbox"
                  className="accent-indigo-500"
                  checked={showPivots}
                  onChange={(e) => setShowPivots(e.target.checked)}
                />
                Zigzag pivots
              </label>

            </div>
          </div>
          {/* quick inputs */}
<div className="mt-4 flex flex-wrap items-center gap-3">
  <div className="flex items-center gap-2">
    <label className="text-xs text-slate-400">Symbol</label>
    <input
      value={cfg.symbol}
      onChange={(e) =>
        applySetting({ ...cfg, symbol: e.target.value.trim().toUpperCase() })
      }
      className="xtl-number ctl-sm w-36 tracking-wider"
      placeholder="XAUUSD"
    />
  </div>

  <div className="flex items-center gap-2">
    <label className="text-xs text-slate-400">Trend TF</label>
    <Segmented
      value={cfg.trendTF}
      onChange={(v) => applySetting({ ...cfg, trendTF: v })}
      options={[
        { label: "M15", value: "15m" as TfLabel },
        { label: "H1", value: "1h" as TfLabel },
        { label: "H4", value: "4h" as TfLabel },
      ]}
    />
  </div>

  {finalLabel && (
    <div className="ml-auto">
      <FinalBadge label={finalLabel} />
    </div>
  )}
</div>

{/* Status row (computed inline to avoid scope issues) */}
{(() => {
  // 1) Use UTC milliseconds directly from the API (no ISO parse)
  const lastUpdatedMs = Number(server?.serverNow ?? Date.now());

  // 2) Resolve broker meta (prefer server; else fallback override), normalized to { tz_abbr, utc_offset_min }
  const brokerMeta: BrokerMeta | null = (() => {
    return normalizeBroker(server?.broker) || normalizeBroker(getBrokerOverride?.()) || null;
  })();

  // 3) Format broker wall-time via offset-only helper
  const lastUpdatedLabel = brokerMeta
    ? formatInBrokerOffsetOnly(lastUpdatedMs, brokerMeta)
    : "—";

  return (
    <div className="mt-3 grid grid-cols-3 items-center text-xs">
      {/* Left: last updated (broker TZ) */}
      <div className="text-slate-400">
        Last updated:{" "}
        <span className="text-slate-200">{lastUpdatedLabel}</span>
        {usingDevice && (
          <div className="text-xs text-gray-500 mt-1">
            using device: <code>{usingDevice}</code>
          </div>
        )}
        {brokerMeta?.tz_abbr ? <span className="ml-1 text-slate-500">({brokerMeta.tz_abbr})</span> : null}
      </div>

      {/* Center: countdown */}
      <div className="text-center font-semibold tabular-nums text-slate-300">
        Next bar in:{" "}
        <span className="inline-block min-w-[5ch]">
          {live && nextAt ? fmtCountdown(nextInMs) : "—"}
        </span>
      </div>

      {/* Right: live status */}
      <div className="text-right text-slate-400">
        Live:{" "}
        <span
          className={cx("font-medium", live ? "text-emerald-300" : "text-slate-300")}
        >
          {live ? "On" : "Off"}
        </span>
      </div>
    </div>
  );
})()}


          {notice && <div className="mt-2 text-xs text-amber-300">{notice}</div>}
          {/* Broker verify status */}
{brokerCheck.kind !== "idle" && (
  <div className="mt-2 text-xs">
    {brokerCheck.kind === "checking" && (
      <span className="text-slate-400">Broker check…</span>
    )}
    {brokerCheck.kind === "aligned" && (
      <span className="text-emerald-300">Broker OHLC aligned.</span>
    )}
    {brokerCheck.kind === "missing" && (
      <span className="text-amber-300">
        Last bar {formatInBrokerTZ(brokerCheck.t * 1000, broker)} is missing in app.
      </span>
    )}
    {brokerCheck.kind === "mismatch" && (
      <span className="text-rose-300">
        Mismatch @ {formatInBrokerTZ(brokerCheck.t * 1000, broker)}
        {brokerCheck.fields.map((f) => `${f.field}:${f.app}?${f.broker}`).join(" · ")}
      </span>
    )}
    {brokerCheck.kind === "error" && (
      <span className="text-rose-300">Broker check failed: {brokerCheck.msg}</span>
    )}
  </div>
)}

        </div>

        {/* ===== KPIs ===== */}
        {/* ===== KPIs ===== */}
<div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
  <Stat
    label="Score"
    value={
      typeof server?.score === "number"
        ? server.score.toFixed(2)
        : "-"
    }
    hint="Composite trend score"
  />

  <Stat
    label={`ADX(${cfg.adx.period})`}
    value={
      Array.isArray(server?.diagnostics?.adx) &&
      server.diagnostics.adx.length > 0
        ? Number(server.diagnostics.adx[server.diagnostics.adx.length - 1]).toFixed(1)
        : "-"
    }
    hint="Strength filter"
  />

  <Stat
    label={`Slope (P=${cfg.ma.slopeWin}, Thr=${Number(cfg.ma.slopeThr).toFixed(2)})`}
    value={typeof server?.diagnostics?.slopePct === "number"
      ? server.diagnostics.slopePct.toFixed(2) + "%"
      : "-"
    }
    
  />

  <Stat
    label="Structure"
    value={
      server?.diagnostics?.structure4?.label ??
      server?.diagnostics?.structureLabel ??
      (typeof server?.diagnostics?.lastSwingDir === "number"
        ? (server.diagnostics.lastSwingDir > 0 ? "HH/HL" : "LH/LL")
        : "-")
    }
  />


</div>

        {/* ===== Preview ===== */}
<Panel title="Preview">
  {hasBars ? (
    <MiniCandleChart
      bars={previewBars}
      broker={brokerMeta}
      maPreview={maPreview}
      tfMs={tfMs}
      pivots={pivots}
      showPivots={showPivots}
    />
  ) : isLoadingPreview ? (
    <div className="aspect-[16/9] w-full rounded-xl bg-slate-950/60 border border-slate-800/60 grid place-items-center">
      <div className="text-slate-500 text-sm">Loadingâ€¦</div>
    </div>
  ) : (
    <div className="aspect-[16/9] w-full rounded-xl bg-slate-950/60 border border-slate-800/60 grid place-items-center">
      <div className="text-slate-500 text-sm">No preview yet â€” click Detect Trend</div>
    </div>
  )}

  <div className="mt-3 text-xs text-slate-500">
    {server?.stale
      ? "Snapshot is stale (market closed or agent paused)."
      : "Includes forming bar when present; updates on TF close while Live is on."}
  </div>

  <div className="text-xs text-slate-500 mt-1">
    {server?.nextCloseTs
      ? `Next ${cfg.trendTF.toUpperCase()} close: ${formatInBrokerTZ(server.nextCloseTs, brokerMeta)}`
      : "Scheduled for next closeâ€¦"}
  </div>

  <div className="mt-2 text-xs text-slate-400">
    TF: {cfg.trendTF.toUpperCase()}
    {previewBars.length > 0 ? (() => {
      const from = formatInBrokerTZ(previewBars[0].t_open_ms, brokerMeta);
      const last = previewBars[previewBars.length - 1];
      const to   = last ? formatInBrokerTZ(last.t_close_ms, brokerMeta) : "";
      return <> â€¢ Window: {from} Â· {to}</>;
    })() : ""}
  </div>
</Panel>



        {/* ===== Preview ===== */}
        {/* ===== Controls: Devices-style Cards ===== */}
        {/* ===== Controls: Devices-style Cards ===== */}
<div className="mt-6 grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 items-stretch">

  {/* Card — Regime */}
  <Panel
    title="Regime"
    right={<span className="text-xs text-slate-400">{cfg.ma.type} {cfg.ma.fast}/{cfg.ma.slow}</span>}
    className="h-full"
  >
    <div className="grid grid-cols-2 gap-3">
      <div className="col-span-2">
        <label className="text-xs text-slate-400">MA Type</label>
        <select
          value={cfg.ma.type}
          onChange={(e) => applySetting({ ...cfg, ma:{ ...cfg.ma, type:e.target.value } })}
          className="xtl-select ctl-sm"
        >
          <option>EMA</option>
          <option>SMA</option>
        </select>
      </div>
      <div>
        <label className="text-xs text-slate-400">Fast MA</label>
        <input
          type="number" min={5} max={100}
          value={cfg.ma.fast}
          onChange={(e) => applySetting({ ...cfg, ma:{ ...cfg.ma, fast:Number(e.target.value) } })}
          className="xtl-number ctl-sm"
        />
      </div>
      <div>
        <label className="text-xs text-slate-400">Slow MA</label>
        <input
          type="number" min={50} max={400}
          value={cfg.ma.slow}
          onChange={(e) => applySetting({ ...cfg, ma:{ ...cfg.ma, slow:Number(e.target.value) } })}
          className="xtl-number ctl-sm"
        />
      </div>
    </div>
  </Panel>

  {/* Card — Slope */}
  <Panel title="Slope" className="h-full">
    <div className="grid grid-cols-2 gap-3">
      <div>
        <label className="text-xs text-slate-400">Window (bars)</label>
        <input
          type="number" min={3} max={10}
          value={cfg.ma.slopeWin}
          onChange={(e) => applySetting({ ...cfg, ma:{ ...cfg.ma, slopeWin:Number(e.target.value) } })}
          className="xtl-number ctl-sm"
        />
      </div>
      <div className="col-span-2">
        <label className="text-xs text-slate-400 flex justify-between">
          <span>Slope Threshold (ATR-norm)</span>
          <span className="text-slate-300">{Number(cfg.ma.slopeThr).toFixed(2)}</span>
        </label>
        <input
          type="range" min={0.2} max={0.6} step={0.05}
          value={cfg.ma.slopeThr}
          onChange={(e) => applySetting({ ...cfg, ma:{ ...cfg.ma, slopeThr:Number(e.target.value) } })}
          className="xtl-range ctl-range-sm"
        />
      </div>
    </div>
  </Panel>

  {/* Card — Structure (Swings) */}
  <Panel title="Structure (Swings)" className="h-full">
    <div className="grid grid-cols-2 gap-3">
      {/* Detector */}
      <div className="col-span-2">
        <label className="text-xs text-slate-400">Detector</label>
        <select
          value={cfg.swings.method}
          onChange={(e) => applySetting({ ...cfg, swings: { ...cfg.swings, method: e.target.value } })}
          className="xtl-select ctl-sm"
        >
          <option value="zigzag_atr">ZigZag (ATR)</option>
          <option value="fractal">Fractal (5-bar)</option>
        </select>
      </div>

      {/* ZigZag threshold mode */}
      <div className="col-span-2 flex gap-4 text-xs">
        <label className="flex items-center gap-2">
          <input
            type="radio" name="zz-mode"
            checked={(cfg.swings.mode ?? "atr") === "atr"}
            onChange={() => applySetting({ ...cfg, swings: { ...cfg.swings, mode: "atr" as const } })}
          />
          ATR×
        </label>
        <label className="flex items-center gap-2">
          <input
            type="radio" name="zz-mode"
            checked={(cfg.swings.mode ?? "atr") === "percent"}
            onChange={() => applySetting({ ...cfg, swings: { ...cfg.swings, mode: "percent" as const } })}
          />
          % reversal
        </label>
      </div>

      {/* ZigZag ATR× (Trend TF) */}
      <div className="col-span-2">
        <label className="text-xs text-slate-400 flex justify-between">
          <span>ZigZag ATR× (Trend TF)</span>
          <span className="text-slate-300">{Number(cfg.swings.kTrend).toFixed(2)}</span>
        </label>
        <input
          type="range" min={1.25} max={1.75} step={0.05}
          value={cfg.swings.kTrend}
          onChange={(e) => applySetting({ ...cfg, swings: { ...cfg.swings, kTrend: Number(e.target.value) } })}
          className="xtl-range ctl-range-sm"
          disabled={(cfg.swings.mode ?? "atr") !== "atr"}
        />
      </div>

      {/* Pivot reversal % */}
      <div className="col-span-2">
        <label className="text-xs text-slate-400 flex justify-between">
          <span>Pivot reversal %</span>
          <span className="text-slate-300">{Number(cfg.swings.pivotPct ?? 0.40).toFixed(2)}%</span>
        </label>
        <input
          type="range" min={0.10} max={1.00} step={0.05}
          value={Number(cfg.swings.pivotPct ?? 0.40)}
          onChange={(e) => applySetting({ ...cfg, swings: { ...cfg.swings, pivotPct: Number(e.target.value) } })}
          className="xtl-range ctl-range-sm"
          disabled={(cfg.swings.mode ?? "atr") !== "percent"}
        />
      </div>

      {/* Min Bars Between Pivots */}
      <div className="col-span-2">
        <label className="text-xs text-slate-400">Min Bars Between Pivots</label>
        <input
          type="number" min={1} max={10}
          value={Number(cfg.swings.minBarsTrend ?? 4)}
          onChange={(e) =>
            applySetting({ ...cfg, swings: { ...cfg.swings, minBarsTrend: Number(e.target.value || 4) } })
          }
          className="xtl-number ctl-sm"
        />
      </div>
    </div>
  </Panel>

  {/* Card — Strength & Thresholds */}
  <Panel title="Strength & Thresholds" className="h-full">
    <div className="grid grid-cols-3 gap-3">
      {/* ADX Period */}
      <div>
        <div className="text-xs text-slate-400 mb-1">ADX Period</div>
        <input
          type="number" min={5} max={50}
          value={cfg.adx.period}
          onChange={(e) => applySetting({ ...cfg, adx: { ...cfg.adx, period: Number(e.target.value) } })}
        />
      </div>

      {/* +DI / -DI Bias */}
      <div>
        <div className="text-xs text-slate-400 mb-1">+DI/-DI Bias</div>
        <select
          value={cfg.adx.useDIbias ? "on" : "off"}
          onChange={(e) => applySetting({ ...cfg, adx: { ...cfg.adx, useDIbias: e.target.value === "on" } })}
        >
          <option value="on">On</option>
          <option value="off">Off</option>
        </select>
      </div>

      {/* ADX Min */}
      <div>
        <div className="text-xs text-slate-400 mb-1">ADX Min</div>
        <input
          type="range" min={5} max={60} step={1}
          value={cfg.adx.min}
          onChange={(e) => applySetting({ ...cfg, adx: { ...cfg.adx, min: Number(e.target.value) } })}
        />
        <div className="text-right text-xs text-slate-400 mt-1">{cfg.adx.min}</div>
      </div>
    </div>
  </Panel>

</div>



        {/* ===== Footer ===== */}
        <div className="mt-8 flex flex-wrap items-center gap-3 justify-between">
          <div className="text-slate-400 text-xs">Settings are stored per symbol. Validation auto-adjusts odd values for reliability.</div>
          {error && <div className="text-rose-300 text-sm">{error}</div>}
        </div>
      </main>
    </>
  );
}


