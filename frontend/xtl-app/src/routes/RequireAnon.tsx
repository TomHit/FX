import React from "react";
import { Navigate, useLocation } from "react-router-dom";
import { useAuth } from "@/context/AuthContext";

const RequireAnon: React.FC<React.PropsWithChildren> = ({ children }) => {
  const { me, loading } = useAuth();
  const loc = useLocation();

  if (loading) return null;
  if (me) return <Navigate to="/dashboard" replace state={{ from: loc }} />;
  return <>{children}</>;
};

export default RequireAnon;
