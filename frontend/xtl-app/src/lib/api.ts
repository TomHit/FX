// src/lib/api.ts
import axios, { AxiosError, AxiosInstance } from "axios";

function resolveApiOrigin(): string {
  const env = (import.meta as any).env?.VITE_API_ORIGIN as string | undefined;
  if (env) return env.replace(/\/+$/, "");
  if (typeof window !== "undefined") return window.location.origin.replace(/\/+$/, "") + "/_api";
  return "https://app.xautrendlab.com/_api";
}

export const API_ORIGIN = resolveApiOrigin();

// ---- Axios instance (sends session cookie) ----
export const api: AxiosInstance = axios.create({
  baseURL: API_ORIGIN,
  withCredentials: true,                 // send session cookie
  xsrfCookieName: "csrftoken",          // if you later add CSRF
  xsrfHeaderName: "X-CSRF-Token",
  headers: {
    "Content-Type": "application/json",
    Accept: "application/json",
    "Cache-Control": "no-cache",
    Pragma: "no-cache",
  },
});

// Optional: unwrap .data on success; keep full error on failure
api.interceptors.response.use(
  (res) => res,
  (err: AxiosError) => {
    if (err.response?.data) (err as any).detail = err.response.data;
    return Promise.reject(err);
  }
);

// ---------- Types ----------
export type LoginResponse = {
  ok?: boolean;
  redirect?: string;
  [k: string]: any;
};

export type TrendResp = {
  label: string;
  score: number;
  serverNow: number;
  lastClosedTs: number;
  nextCloseTs: number;
  diagnostics?: Record<string, any>;
  stale: boolean;
};

// ---------- Auth ----------
export const getMe = () => api.get("/auth/me");

export const login = async (username_or_email: string, password: string, totp?: string): Promise<LoginResponse> => {
  const res = await api.post("/user/login", { username_or_email, password, totp });
  return res.data;
};

export const logout = () => api.post("/user/logout");

export const changePassword = (payload: { current_password: string; new_password: string }) =>
  api.post("/user/change_password", payload);

// ---------- Devices ----------
export const ping = () => api.get("/devices/ping");
export const claimRecent = () => api.post("/devices/claim_recent");
export const myDevices = () => api.get("/devices", { params: { mine: 1 } });

// ---------- MFA ----------
export const mfaBegin = () => api.post("/auth/mfa/totp/begin");

export const mfaVerify = async (code: string) => {
  try { return await api.post("/user/mfa/verify", { code }); }
  catch { try { return await api.post("/auth/mfa/verify", { code }); }
  catch { return api.post("/mfa/verify", { code }); } }
};

export const mfaStart = async () => {
  try { return await api.post("/user/mfa/start"); }
  catch { try { return await api.post("/auth/mfa/start"); }
  catch { return api.post("/mfa/start"); } }
};

// ---------- Trend ----------
export const getTrendState = async (symbol: string, tf: "H1" | "H4" = "H1") => {
  const res = await api.get<TrendResp>("/trend/state2", { params: { symbol, tf } });
  return res.data;
};

export const tryGetTrendState = async (symbol: string, tf: "H1" | "H4" = "H1") => {
  try {
    return await getTrendState(symbol, tf);
  } catch (e: any) {
    if (e?.response?.status === 404) return undefined;
    throw e;
  }
};
