import React, { useEffect, useMemo, useState,useRef } from "react";
import { useNavigate } from "react-router-dom";
import { useAuth } from "@/context/AuthContext";
import { api, mfaBegin, mfaVerify } from "@/lib/api";



type BeginResp = {
  ok?: boolean;
  qr_png?: string;   // base64 or data URL
  qr?: string;       // base64 or data URL
  qr_svg?: string;   // raw SVG string
  svg?: string;      // raw SVG string
  secret?: string;   // base32
  otpauth?: string;  // otpauth://totp/Label?secret=...&issuer=...
  detail?: string;
  message?: string;
  challenge_id?: string;
};

export default function MFASetup() {
  
  const nav = useNavigate();
  const { ensureMe } = useAuth();
  const [loading, setLoading] = useState(false);
  const [verifying, setVerifying] = useState(false);
  const justVerifiedRef = useRef(false);  
  const begunRef = useRef(false); 
  const [qrUrl, setQrUrl] = useState("");
  const [qrSvg, setQrSvg] = useState("");
  const [secret, setSecret] = useState("");
  const [code, setCode] = useState("");
  const [err, setErr] = useState<string | null>(null);
  const [copied, setCopied] = useState(false);
  const [challengeId, setChallengeId] = useState<string | null>(null);

  const getCsrfHeaders = () => {
    const cookie = document.cookie.split("; ").find(v => v.startsWith("csrftoken="));
    const token = cookie?.split("=")[1];
    return token ? { "X-CSRF-Token": token } : {};
  };

  // Build a data URL for inline SVG
  const svgDataUrl = useMemo(
    () => (qrSvg ? `data:image/svg+xml;utf8,${encodeURIComponent(qrSvg)}` : ""),
    [qrSvg]
  );

  const goDash = () => nav("/react/dashboard", { replace: true });

  const parseSecretFromOtpauth = (otpauth?: string) => {
    try {
      if (!otpauth || !otpauth.startsWith("otpauth://")) return "";
      const usp = new URLSearchParams(otpauth.split("?")[1] || "");
      return usp.get("secret") || "";
    } catch {
      return "";
    }
  };

 // --- keep your imports and state as-is ---

// keep your existing normalize, but you can optionally add secret_b32:
const normalize = (d: any) => {
  if (!d) return { imgSrc: "", svg: "", sec: "" };
  const pngB64 = d.qr_png_b64 || d.qr_png || d.qr || "";
  const imgSrc = pngB64
    ? (pngB64.startsWith("data:image") ? pngB64 : `data:image/png;base64,${pngB64}`)
    : "";
  const svg = d.qr_svg || d.svg || "";
  const sec =
    d.secret ||
    d.secret_b32 ||
    (d.otpauth_uri ? new URLSearchParams(String(d.otpauth_uri).split("?")[1] ?? "").get("secret") || "" : "");
  return { imgSrc, svg, sec };
};


  

  const start = async () => {
   if (verifying || justVerifiedRef.current || begunRef.current) return;

   setErr(null);
   begunRef.current = true;
   setLoading(true);
   try {
    const res = await fetch("/_api/auth/mfa/totp/begin", {
      method: "POST",
      credentials: "include",
      headers: { "Content-Type": "application/json", Accept: "application/json", "Cache-Control": "no-store" },
    });

    if (!res.ok) {
      let msg = `Start failed (${res.status})`;
      try { const j = await res.json(); msg = j?.detail || j?.message || msg; } catch {}
      throw new Error(msg);
    }

    const j = await res.json();
    setSecret(j?.secret || "");
    if (j?.qr_svg) {
      setQrSvg(j.qr_svg);
      setQrUrl("");
    } else if (j?.qr_url) {
      setQrUrl(j.qr_url);
      setQrSvg(""); // keep string type
    }
  } catch (e: any) {
    setErr(e?.message || "Unable to start MFA setup.");
  } finally {
    setLoading(false);
    begunRef.current = false;
  }
};

  const verify = async (override?: string) => {
  if (verifying) return; // prevent double-submit
  setErr(null);

  const v = (override ?? code).replace(/\D/g, "").slice(0, 6);
  if (v.length !== 6) {
    setErr("Please enter the 6-digit code.");
    return;
  }

  // block any start() (from effects/remount) during this verify cycle
  begunRef.current = true;

  setVerifying(true);
  try {
    // ?? point to the updated backend route we just fixed
    const res = await fetch("/_api/auth/mfa/totp/verify", {
      method: "POST",
      credentials: "include",
      headers: {
        "Content-Type": "application/json",
        Accept: "application/json",
        "Cache-Control": "no-store",
      },
      body: JSON.stringify({ code: v }),
    });

    if (!res.ok) {
      let msg = `Verify failed (${res.status})`;
      try {
        const ct = res.headers.get("content-type") || "";
        if (ct.includes("application/json")) {
          const j = await res.json();
          msg = j?.detail || j?.message || msg;
        } else {
          const t = await res.text();
          if (t) msg = t;
        }
      } catch {}
      throw new Error(msg);
    }

    // success — parse ONCE, then decide how to route
    let j: any = {};
    try {
      const ct = res.headers.get("content-type") || "";
      if (ct.includes("application/json")) j = await res.json();
    } catch {}

    // server may provide a redirect URL — follow it directly
    if (j?.redirect) {
      justVerifiedRef.current = true;
      // use a hard redirect to avoid any client-side routing quirks
      window.location.assign(j.redirect);
      return;
    }

    // otherwise, rely on enabled=true (or ok) and navigate SPA route
    if (j?.enabled || j?.ok) {
      justVerifiedRef.current = true;
      try { await ensureMe?.(); } catch {}
      nav("/react/dashboard", { replace: true });
      return;
    }

    // fallback to status probe if body was empty or ambiguous
    try {
      const rs = await fetch("/_api/user/mfa/status", {
        method: "GET",
        credentials: "include",
        headers: { Accept: "application/json", "Cache-Control": "no-store" },
      });
      if (rs.ok) {
        const sj = await rs.json().catch(() => ({} as any));
        if (sj?.enabled) {
          justVerifiedRef.current = true;
          try { await ensureMe?.(); } catch {}
          nav("/react/dashboard", { replace: true });
          return;
        }
      }
    } catch {}

    // if we got here, treat it as a soft failure without restarting
    setErr("Code accepted but MFA wasn’t enabled on the server. Please retry.");
    return;
  } catch (e: any) {
    setErr(e?.message || "Verification failed. Try again.");
  } finally {
    setVerifying(false);
    // release the begin guard only if we didn't mark success
    if (!justVerifiedRef.current) begunRef.current = false;
  }
};



  const clearSensitive = () => {
    setSecret("");
    setQrUrl("");
    setQrSvg("");
  };

  
   useEffect(() => {
  if (justVerifiedRef.current || begunRef.current || verifying) return;

  (async () => {
    try {
      const r = await fetch("/_api/user/mfa/status", {
        method: "GET",
        credentials: "include",
        headers: { Accept: "application/json", "Cache-Control": "no-store" },
      });
      const j = r.ok ? await r.json().catch(() => ({ enabled: false })) : { enabled: false };
      if (!j.enabled) await start();
    } catch {
      await start(); // fallback
    }
  })();

  // eslint-disable-next-line react-hooks/exhaustive-deps
}, []);





  // --------- Layout with local CSS (works even if Tailwind isn't loaded) ---------
  return (
    <main style={{ maxWidth: 1120, margin: "0 auto", padding: "24px" }}>
      <style>{`
        .mfa-grid { display: grid; grid-template-columns: 1fr; gap: 24px; }
        @media (min-width: 900px) { .mfa-grid { grid-template-columns: 1fr 1fr; } }
        .card { background: rgba(15, 23, 42, 0.5); border: 1px solid rgba(51,65,85,0.5); border-radius: 14px; padding: 20px; }
        .title { font-size: 22px; font-weight: 600; letter-spacing: -0.01em; margin: 0 0 4px; }
        .subtitle { color: #94a3b8; font-size: 13px; margin: 0 0 16px; }
        .h2 { font-size: 14px; font-weight: 600; color: #cbd5e1; margin: 0 0 10px; }
        .label { font-size: 12px; color: #94a3b8; margin-bottom: 6px; display: block; }
        .muted { font-size: 12px; color: #94a3b8; }
        .btn { display: inline-flex; align-items: center; justify-content: center; height: 36px; padding: 0 14px; border-radius: 8px; font-size: 13px; color: #fff; background: #4f46e5; border: none; cursor: pointer; }
        .btn:hover { background: #6366f1; }
        .btn:disabled { opacity: .6; cursor: not-allowed; }
        .btn-ghost { background: #334155; color: #e5e7eb; }
        .input { height: 44px; width: 100%; padding: 0 12px; border-radius: 8px; border: 1px solid #334155; background: #0f172a; color: #e5e7eb; font-size: 14px; }
        .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace; letter-spacing: 0.03em; }
        .row { display: flex; align-items: center; gap: 8px; }
        .error { margin-top: 12px; background: rgba(190,18,60,.25); border: 1px solid rgba(190,18,60,.6); color: #fee2e2; padding: 8px 12px; border-radius: 8px; font-size: 13px; }
      `}</style>

      <div className="card" style={{ marginBottom: 16 }}>
        <div className="title">Multi-Factor Authentication (TOTP)</div>
        <p className="subtitle">Use Google Authenticator</p>
      </div>

      <div className="mfa-grid">
        {/* LEFT: Scanner card */}
        <section className="card">
          <div className="h2">Scan this QR</div>

          {/* White quiet-zone + fixed size so phones scan instantly */}
          <div
            style={{
              background: "#ffffff",
              border: "1px solid rgba(15,23,42,0.1)",
              borderRadius: 12,
              width: 208,
              height: 208,
              padding: 12, // quiet zone
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              boxShadow: "0 1px 3px rgba(0,0,0,0.2)",
              margin: "0 auto",
            }}
          >
            {loading ? (
              <div style={{ width: 184, height: 184, background: "#e5e7eb", borderRadius: 8 }} />
            ) : qrSvg ? (
              <img
                src={svgDataUrl}
                alt="Authenticator QR code"
                style={{ width: 184, height: 184, objectFit: "contain", imageRendering: "pixelated" }}
              />
            ) : qrUrl ? (
              <img
                src={qrUrl}
                alt="Authenticator QR code"
                style={{ width: 184, height: 184, objectFit: "contain", imageRendering: "pixelated" }}
              />
            ) : (
              <div className="muted">QR will appear here</div>
            )}
          </div>

          <p className="muted" style={{ marginTop: 10 }}>
            Tip: If a phone struggles to lock on, ensure the white border (quiet zone) is visible.
          </p>
        </section>

        {/* RIGHT: Setup card */}
        <section className="card">
          <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 10 }}>
            <div className="h2" style={{ margin: 0 }}>
              Finish setup
            </div>
            <button
              type="button"
              onClick={() => {
                  if (verifying || justVerifiedRef.current || begunRef.current) return;    // don't re-begin while verify is in-flight
                setErr(null);
                start();
              }}
               disabled={loading || verifying || justVerifiedRef.current || begunRef.current}
              className="btn"
           >
              {loading ? "Starting…" : "Restart"}
           </button>

          </div>

          {/* Setup key (fallback) */}
          <div style={{ marginBottom: 12 }}>
            <label className="label">Setup key (use either QR scan or Key)</label>
            <div className="row">
              <input
                className="input mono"
                readOnly
                value={secret}
                placeholder="(secret will appear here)"
                onFocus={(e) => e.currentTarget.select()}
              />
              <button
                type="button"
                onClick={() => navigator.clipboard.writeText(secret || "")}
                disabled={!secret}
                className="btn btn-ghost"
                style={{ height: 36 }}
              >
                Copy
              </button>
            </div>
          </div>

          {/* Code entry */}
          <div>
            <label className="label">6-digit code</label>
            <input
              className="input"
              inputMode="numeric"
              maxLength={6}
              placeholder="******"
              value={code}
              onChange={(e) => {
                const v = e.target.value.replace(/[^0-9]/g, "").slice(0, 6);
                setCode(v);
                 
              }}
            />
            <button
              type="button"
              onClick={() => verify()}
              disabled={verifying || code.length !== 6}
              className="btn"
              style={{ width: "100%", height: 40, marginTop: 10 }}
            >
              {verifying ? "Verifying…" : "Verify & enable"}
            </button>
          </div>

          {!!err && (
            <div className="error" aria-live="polite">
              {err}
            </div>
          )}

          <ul className="muted" style={{ marginTop: 12, listStyle: "disc", paddingLeft: 18 }}>
            <li>Codes rotate every ~30s. Make sure device time is in sync.</li>
            <li>After successful authentication you will be redirected to the dashboard.</li>
          </ul>
        </section>
      </div>
    </main>
  );
}
