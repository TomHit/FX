// src/pages/Devices.tsx
import React, { useEffect, useMemo, useRef, useState } from "react";
import "./Devices.css";

const USER =
  (typeof window !== "undefined" && (window as any).__XTL_USER__?.username) || "anon";
const CACHE_KEY = `xtl_devices_${USER}_v1`;


type Device = {
  device_id: string;
  label?: string | null;
  status?: "online" | "offline";
  last_heartbeat_ms?: number | null;

  // loose/tolerant extra fields we’ll read if present
  version?: string | null;
  mt5_ok?: "0" | "1";
  api_ok?: "0" | "1";
  autostart_ok?: "0" | "1";
  last_error?: string | null;

  // possible alternates the API might send
  hostname?: string;
  device_name?: string;
  computer_name?: string;

  os?: string;
  os_name?: string;
  platform?: string;
  platform_name?: string;

  agent_version?: string;
  agent?: string;
  version_name?: string;
};

const FRESH_MS = 5 * 60 * 1000;


async function api<T = any>(path: string, init: RequestInit = {}): Promise<T> {
  const res = await fetch(`/_api${path}`, {
    credentials: "include",
    headers: { Accept: "application/json", ...(init.headers || {}) },
    cache: "no-store",
    ...init,
  });
  if (!res.ok) throw new Error(String(res.status));
  return (await res.json()) as T;
}

// tiny toast
function toast(msg: string) {
  const n = document.createElement("div");
  n.textContent = msg;
  Object.assign(n.style as Partial<CSSStyleDeclaration>, {
    position: "fixed",
    zIndex: "2147483647",
    bottom: "18px",
    left: "50%",
    transform: "translateX(-50%)",
    background: "#0b1220",
    color: "#e2e8f0",
    border: "1px solid rgba(255,255,255,.08)",
    borderRadius: "10px",
    padding: "8px 12px",
    boxShadow: "0 10px 30px rgba(0,0,0,.35)",
    fontSize: "13px",
  });
  document.body.appendChild(n);
  setTimeout(() => n.remove(), 1600);
}

// ---- tolerant helpers ----
function hbWhen(d: Device): string {
  const ms = d.last_heartbeat_ms ?? NaN;
  if (!Number.isFinite(ms)) return "-";
  try {
    return new Date(ms).toLocaleString();
  } catch {
    return "-";
  }
}
function isOnline(d: Device): boolean {
  const ms = d.last_heartbeat_ms ?? NaN;
  if (!Number.isFinite(ms)) return false;
  return Date.now() - ms <= FRESH_MS;
}
function loadCache(): Device[] | null {
  try {
    const raw = localStorage.getItem(CACHE_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    if (Array.isArray(parsed)) return parsed;
  } catch {}
  return null;
}
function saveCache(devs: Device[]) {
  try {
    localStorage.setItem(CACHE_KEY, JSON.stringify(devs));
  } catch {}
}
function pickString(d: any, keys: string[], fallback = "-") {
  for (const k of keys) {
    const v = d?.[k];
    if (v != null && String(v).trim() !== "") return String(v);
  }
  return fallback;
}
function pickMT5(d: any): string {
  // Prefer explicit flag if provided
  if (d?.mt5_ok === "1") return "Running";
  if (d?.mt5_ok === "0") return "Not running";
  // Fall back to other common shapes
  if (typeof d?.mt5_running === "boolean") return d.mt5_running ? "Running" : "Not running";
  if (typeof d?.mt5 === "string") {
    const s = d.mt5.toLowerCase();
    if (/^run|ok|on|online|active$/.test(s)) return "Running";
    if (/^stop|off|offline$/.test(s)) return "Not running";
  }
  return "-";
}

// ---- list row ----
function DeviceRow({
  d,
  active,
  onClick,
  serviceDown = false,
}: {
  d: Device;
  active: boolean;
  onClick: () => void;
  serviceDown?: boolean;
}) {
  // If service is down, force the *UI indicator* to offline.
  const online = !serviceDown && (isOnline(d) || d.status === "online");

  return (
    <div
      className="item"
      role="listitem"
      aria-selected={active}
      onClick={onClick}
      style={active ? { background: "rgba(255,255,255,.03)" } : undefined}
    >
      <span className={`dot ${online ? "ok" : "off"}`} />
      <div style={{ overflow: "hidden" }}>
        <div className="name">
          {(d as any).label?.trim() ||
           (d as any).name ||
           (d as any).hostname ||
           "Device"}
        </div>
        <div className="id">
          {(d as any).device_id ||
           (d as any).id ||
           (d as any).uid ||
           (d as any).serial ||
           "unknown"}{" "}
          · {hbWhen(d)}
        </div>
      </div>
    </div>
  );
}

// ---- right side ----
function Detail({
  d,
  onRenamed,
  onDeleted,
  serviceDown=false,
}: {
  d: Device | null;
  onRenamed: (newLabel: string) => void;
  onDeleted: () => void;
  serviceDown?: boolean;
}) {
  if (!d) {
    return (
      <div className="empty">
        <h2>Devices</h2>
        <p>Select a device on the left to see details.</p>
      </div>
    );
  }

  const online = !serviceDown && (isOnline(d) || d.status === "online");
  const id = d.device_id;
  const label = d.label?.trim() || "Device";

  // tolerant reads
  const hostname = pickString(d, ["hostname","device_name","computer_name"]);
  const os = pickString(d, ["os","os_name","platform","platform_name"]);
  const agent = pickString(d, ["agent_version","version","version_name","agent"]);
  const mt5Text = pickMT5(d);

  const copyId = async () => {
    await navigator.clipboard.writeText(String(id));
    toast("Device ID copied");
  };

  const doRename = async () => {
    const current = label === "Device" ? "" : label;
    const next = window.prompt("Rename device to:", current)?.trim();
    if (!next || next === current) return;
    try {
      // ? FastAPI: PATCH /devices/{dev_id} with {label}
      const res = await fetch(`/_api/devices/${encodeURIComponent(id)}`, {
        method: "PATCH",
        credentials: "include",
        headers: {
          "Content-Type": "application/json",
          Accept: "application/json",
        },
        body: JSON.stringify({ label: next }),
      });
      if (!res.ok) throw new Error(String(res.status));
      onRenamed(next); // optimistic + keep UI
      toast("Renamed");
    } catch (e) {
      console.warn("Rename failed:", e);
      toast("Rename failed");
    }
  };

  const doDelete = async () => {
    if (!confirm("Delete this device? This cannot be undone.")) return;
    try {
      const res = await fetch(`/_api/devices/${encodeURIComponent(id)}`, {
        method: "DELETE",
        credentials: "include",
        headers: { Accept: "application/json" },
      });
      if (!res.ok) throw new Error(String(res.status));
      onDeleted();
      toast("Deleted");
    } catch (e) {
      console.warn("Delete failed:", e);
      toast("Delete failed");
    }
  };

  return (
    <div className="detail">
      <div className="head">
        <div>
          <div style={{ fontWeight: 700, fontSize: 18 }}>{label}</div>
          <div className="id" style={{ color: "var(--muted)" }}>
            {id} • Last seen: {hbWhen(d)}
          </div>
        </div>
        <div className="badges">
          <span className={`badge ${online ? "ok" : "err"}`}>
            {online ? "online" : "offline"}
          </span>
          {d.last_error ? <span className="badge err">error</span> : null}
        </div>
      </div>

      <div className="kv">
        <div className="k">Hostname</div>
        <div>{hostname}</div>

        <div className="k">OS</div>
        <div>{os}</div>

        <div className="k">Agent</div>
        <div>{agent}</div>

        <div className="k">Last heartbeat</div>
        <div>{hbWhen(d)}</div>

        <div className="k">MT5</div>
        <div>{mt5Text}</div>

        {/* Keep your previous status items if you want them too */}
        {/* 
        <div className="k">API</div>
        <div>{d.api_ok === "1" ? "OK" : "-"}</div>

        <div className="k">Autostart</div>
        <div>{d.autostart_ok === "1" ? "OK" : "-"}</div>
        */}
        {d.last_error ? (
          <>
            <div className="k">Last error</div>
            <div>{d.last_error}</div>
          </>
        ) : null}
      </div>

      <div className="actions" style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
        <button className="btn" onClick={copyId}>Copy ID</button>
        <button className="btn" onClick={doRename}>Rename</button>
        <button className="btn danger" onClick={doDelete}>Delete</button>
      </div>
    </div>
  );
}

// ---- page ----
export default function Devices() {
  const [devices, setDevices] = useState<Device[]>([]);
  const [showOffline, setShowOffline] = useState(true);
  const [sel, setSel] = useState<Device | null>(null);
  const [loading, setLoading] = useState(false);
  const [svcError, setSvcError] = useState<string | null>(null);
  const lastLoadOk = useRef(false);

  // hydrate from cache immediately (so UI never blank)
  useEffect(() => {
    const cached = loadCache();
    if (cached && cached.length) {
      setDevices(cached);
      if (!sel) setSel(cached[0]);
    }
  }, []);

  const load = async () => {
  setLoading(true);
  try {
    const j = await api<{ devices: Device[] }>("/devices");
    const list = Array.isArray(j?.devices) ? j.devices : [];
    setDevices(list);
    saveCache(list);

    if (list.length) {
      if (!sel) {
        setSel(list[0]);
      } else {
        const updated = list.find(x => x.device_id === sel.device_id);
        setSel(updated ?? list[0]); // if previous selection vanished, pick first
      }
    } else {
      // ?? API says no devices ? clear selection AND cache so UI matches reality
      setSel(null);
      saveCache([]);
    }

    setSvcError(null);
    lastLoadOk.current = true;
  } catch (e: any) {
    // keep current UI + show banner
    const code = e?.message || "Service error";
    setSvcError(`API ${code}. Showing last known device details.`);
    lastLoadOk.current = false;
  } finally {
    setLoading(false);
  }
};

  // Initial load and gentle polling every 15s (only while tab visible).
  useEffect(() => {
    let t: number | undefined;
    const tick = () => load();
    tick();
    const schedule = () => {
      if (!document.hidden) {
        t = window.setInterval(tick, 15000);
      }
    };
    schedule();
    const onVis = () => {
      if (document.hidden && t) {
        clearInterval(t);
        t = undefined;
      } else if (!document.hidden && !t) {
        tick();
        schedule();
      }
    };
    document.addEventListener("visibilitychange", onVis);
    return () => {
      if (t) clearInterval(t);
      document.removeEventListener("visibilitychange", onVis);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const shown = useMemo(
    () =>
      devices.filter((d) => showOffline || isOnline(d) || d.status === "online"),
    [devices, showOffline]
  );

  return (
    <main className="wrap">
      <div className="layout">
        <aside className="list">
          <div className="list-tools">
            <label className="chk">
              <input
                id="toggle-offline"
                type="checkbox"
                checked={showOffline}
                onChange={(e) => setShowOffline(e.target.checked)}
              />
              <span>Show offline</span>
            </label>
          </div>

          <div id="device-list" className="list-body" role="list">
            {shown.length === 0 && (
              <div className="empty" style={{ padding: 12 }}>
                {loading ? "Loading…" : "No devices"}
              </div>
            )}
            {shown.map((d) => (
              <DeviceRow
                key={d.device_id}
                d={d}
                active={sel?.device_id === d.device_id}
                onClick={() => setSel(d)}
                serviceDown={!!svcError}
              />
            ))}
          </div>
        </aside>

        <section className="detail" id="detail">
          {svcError ? (
            <div className="svc-banner svc-err" style={{ marginBottom: 10 }}>
              {svcError}
            </div>
          ) : null}

          <Detail
            d={sel}
            serviceDown={!!svcError}
            onRenamed={(newLabel) => {
              if (!sel) return;
              const next = { ...sel, label: newLabel };
              setSel(next);
              setDevices((arr) =>
                arr.map((x) => (x.device_id === sel.device_id ? next : x))
              );
              // keep cache coherent even if service is down after rename
              saveCache(
                (devices.length
                  ? devices.map((x) =>
                      x.device_id === sel.device_id ? next : x
                    )
                  : [next]) as Device[]
              );
            }}
            onDeleted={() => {
              if (!sel) return;
              const id = sel.device_id;
              setSel(null);
              const next = devices.filter((x) => x.device_id !== id);
              setDevices(next);
              saveCache(next);
            }}
          />
        </section>
      </div>
    </main>
  );
}
