import React from "react";
import { Link } from "react-router-dom";

type Device = {
  id?: string;
  name?: string;
  label?: string;
  device_name?: string;
  hostname?: string;
  host?: string;
  status?: string;
  last_seen_ms?: number | null;
  last_heartbeat_ms?: number | null;
  last_heartbeat?: number | string | null; // epoch ms or ISO
};

function cx(...xs: (string | false | null | undefined)[]) {
  return xs.filter(Boolean).join(" ");
}
function toEpochMs(v: number | string | null | undefined): number | null {
  if (v == null) return null;
  if (typeof v === "number") return v > 2_000_000_000 ? v : v * 1000; // sec->ms
  const t = Date.parse(v);
  return Number.isFinite(t) ? t : null;
}
function hbMs(d: Device | null | undefined): number | null {
  if (!d) return null;
  return d.last_seen_ms ?? d.last_heartbeat_ms ?? toEpochMs(d.last_heartbeat) ?? null;
}
function formatAgo(ms: number | null): string {
  if (!ms) return "-";
  const diff = Date.now() - ms;
  if (diff < 0) return "just now";
  const s = Math.floor(diff / 1000);
  if (s < 60) return `${s}s ago`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m ago`;
  const h = Math.floor(m / 60);
  return `${h}h ago`;
}
function bestDeviceName(d: Device | null | undefined): string {
  if (!d) return "This PC";
  return d.name || d.label || d.device_name || d.hostname || d.host || d.id || "This PC";
}

/** Button + inline spinner while we `HEAD` the URL and kick off the download. */
function DownloadAgentRow() {
  const [busy, setBusy] = React.useState(false);
  const [err, setErr] = React.useState<string | null>(null);
  const [pct, setPct] = React.useState<number | null>(null);

  const url = "/_api/devices/user/installer.zip?cb=" + Date.now();

  function parseFilenameFromCD(cd: string | null): string | null {
    if (!cd) return null;
    // RFC 5987 first (filename*=UTF-8''name)
    let m = cd.match(/filename\*\s*=\s*(?:UTF-8''|)([^;]+)/i);
    if (m?.[1]) return decodeURIComponent(m[1].replace(/^["']|["']$/g, ""));
    // fallback: filename="name"
    m = cd.match(/filename\s*=\s*"?([^";]+)"?/i);
    return m?.[1] || null;
  }

  const start = async () => {
    setErr(null);
    setBusy(true);
    setPct(null);
    try {
      const res = await fetch(url, { credentials: "include", cache: "no-store" });
      if (!res.ok || !res.body) throw new Error(`HTTP ${res.status}`);

      const cd = res.headers.get("content-disposition");
      const suggested = parseFilenameFromCD(cd) || "xtl_agent.zip";

      const total = Number(res.headers.get("content-length") || 0);
      const reader = res.body.getReader();
      const chunks: Uint8Array[] = [];
      let received = 0;

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        chunks.push(value!);
        received += value!.length;
        if (total > 0) setPct(Math.floor((received / total) * 100));
      }

      const blob = new Blob(chunks, { type: "application/zip" });
      const href = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = href;
      a.download = suggested;               // ? uses xtl_agent_1.0.2.zip from server
      document.body.appendChild(a);
      a.click();
      a.remove();
      URL.revokeObjectURL(href);
    } catch (e) {
      console.error(e);
      setErr("Couldn't start download. Please try again.");
    } finally {
      setBusy(false);
      setPct(null);
    }
  };

  return (
    <>
      <div className="flex items-center gap-3">
        <button
          onClick={start}
          className={`inline-flex items-center rounded-full px-4 py-2 font-medium shadow-sm
                      ${busy ? "bg-blue-500 cursor-progress" : "bg-blue-600 hover:bg-blue-700"} text-white
                      focus:outline-none focus:ring-2 focus:ring-blue-400`}
          disabled={busy}
        >
          {busy ? "Downloading..." : "Download Agent (ZIP)"}
        </button>

        {busy && (
          <div className="flex items-center gap-2 text-slate-300">
            <svg width="16" height="16" viewBox="0 0 50 50" aria-hidden="true">
              <circle cx="25" cy="25" r="20" fill="none" stroke="currentColor" strokeWidth="6" opacity="0.2" />
              <path d="M45 25a20 20 0 0 1-20 20" fill="none" stroke="currentColor" strokeWidth="6">
                <animateTransform attributeName="transform" type="rotate" from="0 25 25" to="360 25 25" dur="0.8s" repeatCount="indefinite" />
              </path>
            </svg>
            <span className="text-sm">
              {pct !== null ? `Preparing... ${pct}%` : "Preparing..."}
            </span>
          </div>
        )}
      </div>

      {busy && (
        <div className="mt-2 h-1 w-48 overflow-hidden rounded bg-slate-700">
          <div
            className="h-1 bg-blue-400 transition-[width] duration-150 ease-linear"
            style={{ width: pct ? `${pct}%` : "33%" }}
          />
        </div>
      )}

      {err && <div className="mt-2 text-sm text-red-400">{err}</div>}
    </>
  );
}


const DISCORD_INVITE_URL: string =
  ((import.meta as any)?.env?.VITE_DISCORD_INVITE_URL as string) ||
  (typeof (window as any) !== "undefined" ? (window as any).__XTL_DISCORD_INVITE_URL__ : "") ||
  "";

type DiscordConnectState = "not_connected" | "connected";

function getDiscordState(): DiscordConnectState {
  try {
    const v = localStorage.getItem("xtl:discord:connected");
    return v === "1" ? "connected" : "not_connected";
  } catch {
    return "not_connected";
  }
}

function setDiscordState(s: DiscordConnectState) {
  try {
    localStorage.setItem("xtl:discord:connected", s === "connected" ? "1" : "0");
  } catch {
    // ignore
  }
}

function DiscordAlertsCard() {
  const [state, setState] = React.useState<DiscordConnectState>(() => getDiscordState());

  const inviteOk = Boolean(DISCORD_INVITE_URL && DISCORD_INVITE_URL.startsWith("http"));

  const inviteHost = React.useMemo(() => {
    if (!inviteOk) return "";
    try {
      return new URL(DISCORD_INVITE_URL).host;
    } catch {
      return "";
    }
  }, [inviteOk]);

  function openInvite() {
    if (!inviteOk) return;
    window.open(DISCORD_INVITE_URL, "_blank", "noopener,noreferrer");
  }

  const ExternalIcon = (
    <svg
      width="16"
      height="16"
      viewBox="0 0 24 24"
      fill="none"
      aria-hidden="true"
      className="opacity-90"
    >
      <path
        d="M14 3h7v7"
        stroke="currentColor"
        strokeWidth="2"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
      <path
        d="M10 14L21 3"
        stroke="currentColor"
        strokeWidth="2"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
      <path
        d="M21 14v6a1 1 0 0 1-1 1H4a1 1 0 0 1-1-1V4a1 1 0 0 1 1-1h6"
        stroke="currentColor"
        strokeWidth="2"
        strokeLinecap="round"
        strokeLinejoin="round"
        opacity="0.8"
      />
    </svg>
  );

  const CheckIcon = (
    <svg
      width="18"
      height="18"
      viewBox="0 0 24 24"
      fill="none"
      className="mt-0.5 text-emerald-300"
      aria-hidden="true"
    >
      <path
        d="M20 6L9 17l-5-5"
        stroke="currentColor"
        strokeWidth="2.5"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );

  return (
    <div className="rounded-2xl border border-slate-700 bg-slate-800/60 p-6 shadow">
      <div className="flex items-start justify-between gap-4">
        <div>
          <h2 className="text-lg font-medium text-slate-100">Real-Time Trade Alerts</h2>
          <p className="mt-1 text-sm text-slate-400">
            Get BUY/SELL entry alerts, target hits and expiry updates instantly via Discord (free).
          </p>
        </div>

        <div className="flex items-center gap-2">
          <span
            className={cx(
              "inline-flex items-center gap-2 rounded-full px-3 py-1.5 text-xs font-semibold",
              state === "connected"
                ? "bg-emerald-500/15 text-emerald-200 ring-1 ring-emerald-400/30"
                : "bg-slate-700/40 text-slate-200 ring-1 ring-slate-600/40"
            )}
          >
            <span
              className={cx(
                "h-2 w-2 rounded-full",
                state === "connected" ? "bg-emerald-400" : "bg-slate-400"
              )}
            />
            {state === "connected" ? "Connected" : "Not connected"}
          </span>
        </div>
      </div>

      <div className="mt-5 grid grid-cols-1 gap-4 lg:grid-cols-3">
        <div className="rounded-xl border border-slate-700 bg-slate-900/40 p-4">
          <div className="text-xs font-semibold uppercase tracking-wider text-slate-400">
            You&apos;ll receive
          </div>
          <ul className="mt-3 space-y-2 text-sm text-slate-200">
            <li className="flex gap-2">
              {CheckIcon}
              <span>
                Entry alert with <b>Entry / TP / SL</b>
              </span>
            </li>
            <li className="flex gap-2">
              {CheckIcon}
              <span>
                Updates: <b>Hit</b> / <b>Expired</b>
              </span>
            </li>
            <li className="flex gap-2">
              {CheckIcon}
              <span>Read-only channels (no spam)</span>
            </li>
          </ul>
          <p className="mt-3 text-xs text-slate-400">
            We never read your messages or post on your behalf.
          </p>
        </div>

        <div className="rounded-xl border border-slate-700 bg-slate-900/40 p-4">
          <div className="text-xs font-semibold uppercase tracking-wider text-slate-400">
            How to connect
          </div>
          <ol className="mt-3 list-decimal space-y-2 pl-5 text-sm text-slate-200">
            <li>
              Click <b>{state === "connected" ? "Open Discord" : "Connect Discord"}</b> to join our server.
              {inviteOk && inviteHost && (
                <span className="ml-2 inline-flex items-center rounded-full border border-slate-600/60 bg-slate-800/40 px-2 py-0.5 text-[11px] font-semibold text-slate-200">
                  {inviteHost}
                </span>
              )}
            </li>
            <li>If you don&apos;t have an account, Discord will prompt you to create one (free).</li>
            {inviteOk && <li>After joining, come back and click <b>Mark as connected</b>.</li>}
          </ol>

          {!inviteOk && (
            <div className="mt-3 rounded-lg border border-amber-500/30 bg-amber-500/10 px-3 py-2 text-xs text-amber-200">
              Discord invite URL not configured. Set{" "}
              <code className="text-amber-100">VITE_DISCORD_INVITE_URL</code> or{" "}
              <code className="text-amber-100">window.__XTL_DISCORD_INVITE_URL__</code>.
            </div>
          )}
        </div>

        <div className="rounded-xl border border-slate-700 bg-slate-900/40 p-4">
          <div className="text-xs font-semibold uppercase tracking-wider text-slate-400">Actions</div>

          <div className="mt-3 flex flex-col gap-2">
            {/* ONE primary button that always opens the invite/server */}
            <button
              onClick={openInvite}
              disabled={!inviteOk}
              className={cx(
                "inline-flex items-center justify-center gap-2 rounded-xl px-4 py-2 text-sm font-semibold shadow-sm",
                inviteOk
                  ? "bg-indigo-600 text-white hover:bg-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-400"
                  : "bg-slate-700/60 text-slate-300 cursor-not-allowed"
              )}
            >
              <span>{state === "connected" ? "Open Discord" : "Connect Discord"}</span>
              {inviteOk && ExternalIcon}
            </button>

            {/* Secondary button: local state only */}
            {state !== "connected" ? (
              inviteOk ? (
                <button
                  onClick={() => {
                    setDiscordState("connected");
                    setState("connected");
                  }}
                  className="inline-flex items-center justify-center rounded-xl border border-slate-600 bg-slate-800 px-4 py-2 text-sm font-semibold text-slate-100 hover:bg-slate-700"
                >
                  Mark as connected
                </button>
              ) : null
            ) : (
              <button
                onClick={() => {
                  setDiscordState("not_connected");
                  setState("not_connected");
                }}
                className="inline-flex items-center justify-center rounded-xl border border-slate-600 bg-slate-800 px-4 py-2 text-sm font-semibold text-slate-100 hover:bg-slate-700"
                title="Resets only the onboarding status on this browser"
              >
                Reset status
              </button>
            )}
          </div>

          <p className="mt-3 text-xs text-slate-400">
            Tip: keep Discord installed on mobile for instant push notifications.
          </p>
        </div>
      </div>
    </div>
  );
}

export default function Onboarding() {
  const [loading, setLoading] = React.useState(false);
  const [devices, setDevices] = React.useState<Device[]>([]);
  const [err, setErr] = React.useState<string | null>(null);

  async function refresh() {
    setErr(null);
    setLoading(true);
    try {
      const res = await fetch("/_api/devices", { credentials: "include", cache: "no-store" });
      if (!res.ok) throw new Error(`devices ${res.status}`);
      const j = await res.json();
      const list: Device[] = Array.isArray(j) ? j : (Array.isArray(j?.devices) ? j.devices : []);
      setDevices(list);
    } catch (e) {
      console.error(e);
      setErr("Could not load device status. Please ensure you are logged in.");
    } finally {
      setLoading(false);
    }
  }

  React.useEffect(() => {
    refresh().catch(() => {});
  }, []);

  // Show only the most recent device for this user
  const mostRecent = React.useMemo(() => {
    const sorted = [...devices].sort((a, b) => (hbMs(b) ?? 0) - (hbMs(a) ?? 0));
    return sorted[0] ?? null;
  }, [devices]);

  const mrHb = hbMs(mostRecent);
  const mrOnline = mrHb ? Date.now() - mrHb < 120_000 : false;

  return (
    <div className="mx-auto max-w-6xl px-4 py-8">
      <h1 className="text-2xl font-semibold text-slate-100">Onboarding</h1>
      <p className="mt-1 text-sm text-slate-400">
        Download the agent, run it, then verify this device shows up here.
      </p>

      <div className="mt-6 grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Card 1  Download the Agent */}
        <div className="rounded-2xl border border-slate-700 bg-slate-800/60 p-6 shadow">
          <h2 className="text-lg font-medium text-slate-100">Download the Agent</h2>

          <div className="mt-4 space-y-4 text-sm text-slate-300">
            <DownloadAgentRow />

            <div className="rounded-xl border border-slate-700 bg-slate-900/40 p-3">
              <div className="text-slate-200 font-medium">How to install</div>
              <ol className="mt-2 list-decimal space-y-1 pl-5 text-slate-300">
                <li>Right-click the ZIP {'->'} Properties {'->'} Unblock (if Windows shows a warning).</li>
                <li>Extract the ZIP to a folder you control (e.g., Desktop).</li>
                <li>Rename <code>xtl.bin</code> to <code>xtl.exe</code>.</li>
                <li>Right-click <code>xtl.exe</code> {'->'} Run as Administrator.</li>
                <li>The agent installs silently and auto-binds to your account.</li>
              </ol>
              <p className="mt-2 text-xs text-slate-400">
                We avoid scripts in the archive to reduce antivirus false-positives.
              </p>
            </div>
          </div>
        </div>

        {/* Card 2  This Device (only most recent device) */}
        <div className="rounded-2xl border border-slate-700 bg-slate-800/60 p-6 shadow">
          <div className="flex items-start justify-between gap-4">
            <h2 className="text-lg font-medium text-slate-100">This Device</h2>
            <div className="flex items-center gap-2">
              <Link
                to="/react/devices"
                className="rounded-full px-3 py-1.5 text-sm font-medium bg-slate-700 text-slate-100 hover:bg-slate-600"
              >
                Open Devices
              </Link>
              <button
                onClick={refresh}
                className={cx(
                  "rounded-full px-3 py-1.5 text-sm font-medium",
                  "bg-slate-700 text-slate-100 hover:bg-slate-600"
                )}
                disabled={loading}
              >
                {loading ? "Refreshing..." : "Refresh"}
              </button>
            </div>
          </div>

          <p className="mt-2 text-sm text-slate-400">
            After you run the agent, this panel shows the most recently seen device on your account.
          </p>

          <div className="mt-4 overflow-hidden rounded-xl border border-slate-700">
            <table className="min-w-full divide-y divide-slate-700">
              <thead className="bg-slate-900/40">
                <tr>
                  <th className="px-4 py-2 text-left text-xs font-medium uppercase tracking-wider text-slate-400">Device</th>
                  <th className="px-4 py-2 text-left text-xs font-medium uppercase tracking-wider text-slate-400">Status</th>
                  <th className="px-4 py-2 text-left text-xs font-medium uppercase tracking-wider text-slate-400">Last seen</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-700 bg-slate-900/20">
                {!mostRecent && (
                  <tr>
                    <td className="px-4 py-3 text-sm text-slate-400" colSpan={3}>
                      No device seen yet. Start the agent to bind this PC.
                    </td>
                  </tr>
                )}

                {mostRecent && (
                  <tr>
                    <td className="px-4 py-3 text-sm text-slate-200">
                      <div className="font-medium">{bestDeviceName(mostRecent)}</div>
                      {mostRecent.id && <div className="text-xs text-slate-500">{mostRecent.id}</div>}
                    </td>
                    <td className="px-4 py-3">
                      <div className="flex items-center gap-2">
                        <span
                          className={cx(
                            "inline-block h-2.5 w-2.5 rounded-full",
                            mrOnline ? "bg-emerald-400" : "bg-slate-500"
                          )}
                        />
                        <span className="text-sm text-slate-200">
                          {mrOnline ? "Online" : (mostRecent.status?.toLowerCase() || "offline")}
                        </span>
                      </div>
                    </td>
                    <td className="px-4 py-3 text-sm text-slate-300">{formatAgo(mrHb)}</td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>

          {err && <div className="mt-3 text-sm text-red-400">{err}</div>}
          <div className="mt-3 text-xs text-slate-500">
            Tip: If nothing appears after a minute, ensure the service is running and your firewall allows outbound HTTPS.
          </div>
        </div>
      </div>

      {/* Card 3  Real-Time Alerts (Discord) */}
      <div className="mt-6">
        <DiscordAlertsCard />
      </div>

    </div>
  );
}