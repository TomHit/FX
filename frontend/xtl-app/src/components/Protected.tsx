// src/components/Protected.tsx
import React from "react";
import { Navigate, Outlet, useLocation } from "react-router-dom";
import { useAuth } from "@/context/AuthContext";

const Protected: React.FC<React.PropsWithChildren> = ({ children }) => {
  const { me, ready } = useAuth();
  const loc = useLocation();

  if (!ready) return null;                 // wait for /user/me

  if (!me) {
    return <Navigate to="/login" replace state={{ from: loc }} />;
  }

  const mfaEnabled =
    String(me.mfa_state || (me.mfa_enabled ? "enabled" : "")).toLowerCase() === "enabled";
  const onMfa = /\/mfa-setup\/?$/.test(loc.pathname);
  if (!mfaEnabled && !onMfa) {
    return <Navigate to="/mfa-setup" replace state={{ from: loc }} />;
  }

  return children ? <>{children}</> : <Outlet />;
};

export default Protected;
