import React, {
  createContext,
  useContext,
  useEffect,
  useMemo,
  useState,
} from "react";
import axios from "axios";

/** Axios instance that talks to your Nginx proxy (cookie auth) */
const api = axios.create({
  baseURL: "/_api",
  withCredentials: true,
  timeout: 15000,
});

/** Extend as needed; make MFA fields optional to satisfy callers */
export type User = {
  id: string;
  username: string;
  email?: string;
  role?: string;
  mfa_enabled?: boolean;
  mfa_state?: string; // e.g. "enabled" | "disabled" | "pending"
};

export type Ctx = {
  me: User | null;
  loading: boolean;      // initial session fetch in progress
  ready: boolean;        // convenience alias: !loading
  refresh: () => Promise<void>;
  login: (usernameOrEmail: string, password: string, totp?: string) => Promise<void>;
  logout: () => Promise<void>;
  /** Ensure we have a user; throws "not_authenticated" if still missing */
  ensureMe: () => Promise<User>;
};

const AuthContext = createContext<Ctx | undefined>(undefined);

async function fetchMe(): Promise<User | null> {
  try {
    const r = await api.get("/user/me");
    // Accept either { ok, user } or the user object itself
    const u = r.data?.user ?? r.data ?? null;
    return u && typeof u === "object" && "id" in u ? (u as User) : null;
  } catch {
    return null;
  }
}

export const AuthProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const [me, setMe] = useState<User | null>(null);
  const [loading, setLoading] = useState(true);

  // Initial session probe (never throws)
  useEffect(() => {
    let alive = true;
    (async () => {
      const u = await fetchMe();
      if (!alive) return;
      setMe(u);
      setLoading(false);
    })();
    return () => {
      alive = false;
    };
  }, []);

  const refresh = async () => {
    const u = await fetchMe();
    setMe(u);
  };

  const login = async (usernameOrEmail: string, password: string, totp?: string) => {
    const body: any = { username_or_email: usernameOrEmail, password };
    const headers: any = {};
    if (totp) headers["X-TOTP"] = totp;        

    await api.post("/user/login", body, { headers });
    await refresh();
  };


  const logout = async () => {
    try {
      await api.post("/user/logout");
    } finally {
      setMe(null);
    }
  };

  const ensureMe = async (): Promise<User> => {
    if (me) return me;
    await refresh();
    if (!me) {
      // After state update tick, read the latest value:
      const latest = await fetchMe();
      if (latest) {
        setMe(latest);
        return latest;
      }
      throw new Error("not_authenticated");
    }
    return me;
  };

  const value: Ctx = useMemo(
    () => ({
      me,
      loading,
      ready: !loading,
      refresh,
      login,
      logout,
      ensureMe,
    }),
    [me, loading]
  );

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
};

export function useAuth(): Ctx {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error("useAuth must be used within <AuthProvider>");
  return ctx;
}
