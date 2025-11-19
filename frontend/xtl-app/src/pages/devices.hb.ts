// src/pages/devices.hb.ts

// Normalize various time representations to epoch milliseconds.
export function toMillis(v: unknown): number {
  if (v == null) return NaN;

  // Numeric (epoch in ms or seconds)
  if (typeof v === "number") {
    // If it's clearly in seconds (e.g., 10 digits and not huge), scale up
    return v > 1e12 ? v : v * 1000;
  }

  // Numeric string (all digits)
  if (typeof v === "string" && /^\d+$/.test(v)) {
    const n = parseInt(v, 10);
    return n > 1e12 ? n : n * 1000;
  }

  // ISO or date-like string
  const t = Date.parse(String(v));
  return Number.isNaN(t) ? NaN : t;
}

/**
 * Try many possible heartbeat fields commonly seen across APIs.
 * Returns epoch milliseconds or NaN if nothing usable is found.
 */
export function hbMillis(d: any): number {
  if (!d || typeof d !== "object") return NaN;

  // 1) Direct epoch in milliseconds
  const msFields = [
    "last_heartbeat_ms",
    "heartbeat_ms",
    "last_seen_ms",
    "updated_ms",
    "hb_ms",
  ] as const;
  for (const k of msFields) {
    const v = d[k];
    if (v != null) {
      const n = Number(v);
      if (!Number.isNaN(n) && n > 0) return n;
    }
  }

  // 2) Epoch in seconds (or seconds-like counters)
  const secFields = [
    "last_heartbeat_seconds",
    "heartbeat_seconds",
    "last_seen_seconds",
    "hb",
    "hb_sec",
  ] as const;
  for (const k of secFields) {
    const v = d[k];
    if (v != null) {
      const n = Number(v);
      if (!Number.isNaN(n) && n > 0) return n * 1000;
    }
  }

  // 3) ISO / datetime strings
  const isoFields = [
    "last_heartbeat_iso",
    "heartbeat_iso",
    "last_heartbeat_at",
    "heartbeat_at",
    "last_seen_at",
    "last_seen_iso",
    "updated_at",
    "timestamp",
    "last_seen", // sometimes plain string datetime
    "updated",   // sometimes plain string datetime
  ] as const;
  for (const k of isoFields) {
    const v = d[k];
    if (v != null) {
      const n = toMillis(v);
      if (!Number.isNaN(n) && n > 0) return n;
    }
  }

  // 4) "age" fields: how long ago the last heartbeat happened
  const now = Date.now();
  const ageMsFields = [
    "last_heartbeat_age_ms",
    "heartbeat_age_ms",
    "age_ms",
  ] as const;
  for (const k of ageMsFields) {
    const v = d[k];
    const n = Number(v);
    if (!Number.isNaN(n) && n >= 0) return now - n;
  }

  const ageSecFields = [
    "last_heartbeat_age_sec",
    "heartbeat_age_sec",
    "age_sec",
    "age_seconds",
  ] as const;
  for (const k of ageSecFields) {
    const v = d[k];
    const n = Number(v);
    if (!Number.isNaN(n) && n >= 0) return now - n * 1000;
  }

  // 5) Fallback to a generic "last_heartbeat" if present
  if (d.last_heartbeat != null) {
    const n = toMillis(d.last_heartbeat);
    if (!Number.isNaN(n) && n > 0) return n;
  }

  return NaN;
}

/** Render a human-readable timestamp for the device heartbeat. */
export function hbWhen(d: any): string {
  const ms = hbMillis(d);
  return Number.isNaN(ms) ? "" : new Date(ms).toLocaleString();
}
