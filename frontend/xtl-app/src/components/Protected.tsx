// src/components/Protected.tsx
import React, { useEffect, useMemo, useState } from "react";
import { Navigate, Outlet, useLocation } from "react-router-dom";
import { useAuth } from "@/context/AuthContext";

type PreviewMeResp =
  | { ok: true; preview: true; payload?: any }
  | { ok: true; preview: false }
  | { ok: false; error?: string };

const Protected: React.FC<React.PropsWithChildren> = ({ children }) => {
  const { me, ready } = useAuth();
  const loc = useLocation();

  // --- preview mode detection ---
  const isPreviewMode = useMemo(() => {
    try {
      const q = new URLSearchParams(loc.search || "");
      return (q.get("mode") || "").toLowerCase() === "preview";
    } catch {
      return false;
    }
  }, [loc.search]);

  // --- preview auth check state ---
  const [previewChecked, setPreviewChecked] = useState(false);
  const [previewOk, setPreviewOk] = useState(false);

  useEffect(() => {
    let alive = true;

    async function run() {
      if (!ready) return;

      // Only needed when not logged-in and in preview mode
      if (me) {
        if (alive) {
          setPreviewChecked(true);
          setPreviewOk(false);
        }
        return;
      }

      try {
        const res = await fetch("/_api/preview/me", {
          method: "GET",
          credentials: "include",
          headers: { "Accept": "application/json" },
        });

        const j = (await res.json().catch(() => null)) as PreviewMeResp | null;

        const ok = !!(j && (j as any).ok && (j as any).preview === true);

        if (alive) {
          setPreviewChecked(true);
          setPreviewOk(ok);
        }
      } catch {
        if (alive) {
          setPreviewChecked(true);
          setPreviewOk(false);
        }
      }
    }

    run();
    return () => {
      alive = false;
    };
  }, [ready, me]);

  if (!ready) return null; // wait for /user/me

  // --- Allow preview access (cookie-based), even without login ---
  if (!me) {
    if (!previewChecked) return null;

    if (!previewOk) {
      // Preview cookie missing/invalid -> send to marketing pricing (or login if you prefer)
      window.location.href = "https://xautrendlab.com/pricing?err=invalid_or_expired";
      return null;
    }

    // ✅ Preview OK: allow all tabs (skip MFA requirement too)
    return children ? <>{children}</> : <Outlet />;
  }

  // --- Normal (paid) access requires login ---
  if (!me) {
    return <Navigate to="/login" replace state={{ from: loc }} />;
  }

  // --- Normal MFA gate (paid users only) ---
  const mfaEnabled =
    String(me.mfa_state || (me.mfa_enabled ? "enabled" : "")).toLowerCase() === "enabled";
  const onMfa = /\/mfa-setup\/?$/.test(loc.pathname);
  if (!mfaEnabled && !onMfa) {
    return <Navigate to="/mfa-setup" replace state={{ from: loc }} />;
  }

  return children ? <>{children}</> : <Outlet />;
};

export default Protected;
