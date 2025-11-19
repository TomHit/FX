import React, { useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";
import { useAuth } from "@/context/AuthContext";
import { api } from "@/lib/api";
import xtlLogo from "./xtl-logo.png";

// Poll /user/me to ensure the session cookie is usable
async function waitForSession(timeoutMs = 5000) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    try {
      const r = await api.get("/user/me", { validateStatus: () => true });
      if (r.status === 200 && r.data && (r.data.id || r.data.username || r.data.email)) {
        return true;
      }
    } catch { /* ignore and retry */ }
    await new Promise((res) => setTimeout(res, 250));
  }
  throw new Error("session-wait-timeout");
}

export default function Login() {
  const nav = useNavigate();
  const { me, ready, refresh } = useAuth();

  // form state
  const [u, setU] = useState("");
  const [p, setP] = useState("");
  const [totp, setT] = useState("");

  // ui state
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [showPwd, setShowPwd] = useState(false);

  // Redirect AFTER render, when auth is actually ready and user exists
  useEffect(() => {
    if (!ready || !me) return;
    nav("/dashboard", { replace: true });
  }, [ready, me, nav]);

  async function handleSubmit(e?: React.FormEvent) {
    e?.preventDefault();
    setErr(null);
    setBusy(true);
    try {
      const body: any = { username_or_email: u.trim(), password: p };
      if (totp.trim()) body.totp = totp.trim();

      await api.post("/user/login", body, { withCredentials: true });
      await waitForSession(5000);
      await refresh();
      nav("/dashboard", { replace: true });
    } catch (ex: any) {
      const msg =
        ex?.response?.data?.detail ||
        ex?.response?.data?.error ||
        ex?.message ||
        "Sign in failed";
      if (/TOTP/i.test(msg) || /two[- ]?factor/i.test(msg)) {
        setErr("TOTP required. Open your authenticator and enter the 6-digit code.");
      } else if (/Invalid credentials/i.test(msg)) {
        setErr("Invalid credentials. Check your username/password (and TOTP if enabled).");
      } else if (msg === "session-wait-timeout") {
        setErr("Signed in, but session cookie was slow. Please try once more.");
      } else {
        setErr(msg);
      }
    } finally {
      setBusy(false);
    }
  }

  return (
    <main className="login-wrap">
      {!ready && <div className="login-boot">Checking session </div>}
      <div style={{
        position: "absolute",
        top: "1rem",
        left: "1rem",
        zIndex: 10
      }}>
        <img
          src={xtlLogo}
          alt="XTL Logo"
          style={{
            height: "96px",
            width: "auto",
            objectFit: "contain"
          }}
        />
      </div>
      <form className="login-card" onSubmit={handleSubmit} noValidate>
        <h1 className="login-title">Sign in</h1>

        {err && <div className="login-error">{err}</div>}

        <label className="login-label">Username or email</label>
        <input
          className="login-input"
          type="text"
          autoComplete="username"
          value={u}
          onChange={(e) => setU(e.target.value)}
          placeholder="you@example.com"
        />

        <label className="login-label">Password</label>
        <div className="login-pwdrow">
          <input
            className="login-input"
            type={showPwd ? "text" : "password"}
            autoComplete="current-password"
            value={p}
            onChange={(e) => setP(e.target.value)}
            placeholder="        "
          />
          <button
            type="button"
            className="login-eye"
            onClick={() => setShowPwd(v => !v)}
            aria-label={showPwd ? "Hide password" : "Show password"}
            title={showPwd ? "Hide password" : "Show password"}
          >
            <span className="sr-only">{showPwd ? "Hide password" : "Show password"}</span>
            {showPwd ? (
              <svg width="18" height="18" viewBox="0 0 24 24" fill="none"
                   stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round"
                   aria-hidden="true" focusable="false">
                <path d="M3 3l18 18" />
                <path d="M10.58 10.58a2 2 0 1 0 2.84 2.84" />
                <path d="M9.88 4.24A10.72 10.72 0 0 1 12 4c7 0 11 8 11 8a17.77 17.77 0 0 1-3.01 3.91" />
                <path d="M6.7 6.76A17.94 17.94 0 0 0 1 12s4 8 11 8c1.62 0 3.16-.38 4.53-1.06" />
              </svg>
            ) : (
              <svg width="18" height="18" viewBox="0 0 24 24" fill="none"
                   stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round"
                   aria-hidden="true" focusable="false">
                <path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z" />
                <circle cx="12" cy="12" r="3" />
              </svg>
            )}
          </button>
        </div>

        <label className="login-label">TOTP (if enabled)</label>
        <input
          className="login-input"
          type="text"
          inputMode="numeric"
          autoComplete="one-time-code"
          value={totp}
          onChange={(e) => setT(e.target.value)}
          placeholder="6-digit code"
          maxLength={6}
        />

        <button className="login-btn" type="submit" disabled={busy}>
          {busy ? "Signing in " : "Sign in"}
        </button>
      </form>
    </main>
  );
}