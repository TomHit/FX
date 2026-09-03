import React, { useEffect, useMemo, useState } from "react";

const API_BASE =
  (
    (window as any).__PUBLIC_API_BASE__ ||
    (import.meta as any).env?.VITE_API_BASE ||
    "/_api"
  ).replace(/\/$/, "");

const apiUrl = (path: string) =>
  `${API_BASE}${path.startsWith("/") ? "" : "/"}${path}`;

type PropConfig = {
  profile_id: string;
  enabled: boolean;
  firm: string;
  phase: string;
  account_size: number;
  risk_pct: number;
  target_rr: number;
  max_open_risk_pct: number;
  max_open_positions: number;
  account_name?: string;
  account_id?: string;
  account_login?: string;
  account_server?: string;
  broker_company?: string;
  account_type?: string;
  is_demo?: boolean;
};

type ProfilesResponse = {
  ok: boolean;
  active_profile_id: string;
  profiles: PropConfig[];
  error?: string;
};

type ConnectedAccount = {
  login: string;
  server: string;
  company: string;
  account_type: string;
  is_demo: boolean;
  device_id: string;
  account_key: string;
  balance?: number;
  equity?: number;
  leverage?: number;
  snapshot_ms: number;
  snapshot_age_ms: number;
  connected: boolean;
};

type ConnectedAccountsResponse = {
  ok: boolean;
  uid?: string;
  accounts: ConnectedAccount[];
  count: number;
  generated_at_ms?: number;
  error?: string;
};

type StatusResponse = {
  ok: boolean;
  profile_id?: string;
  config?: PropConfig;
  rules?: Record<string, any>;
  limits?: {
    target_usd?: number | null;
    daily_limit_usd?: number;
    max_loss_limit_usd?: number;
  };
  risk?: Record<string, any>;
  account?: Record<string, any>;
  profile_device?: {
    ok?: boolean;
    device_id?: string;
    reason?: string;
  };
  error?: string;
};

type RiskResponse = {
  ok: boolean;
  config?: PropConfig;
  risk?: Record<string, any>;
  error?: string;
};

const money = (value: any) => {
  const numericValue = Number(value);

  if (!Number.isFinite(numericValue)) {
    return "NA";
  }

  return `$${numericValue.toLocaleString(undefined, {
    minimumFractionDigits: 0,
    maximumFractionDigits: 2,
  })}`;
};

const textOrNA = (value: any) => {
  if (value === null || value === undefined || value === "") {
    return "NA";
  }

  return String(value);
};

const normalizeBrokerIdentity = (value: any) =>
  String(value || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");

const brokerIdentityMatches = (
  configured: any,
  live: any
) => {
  const expected = normalizeBrokerIdentity(configured);
  const actual = normalizeBrokerIdentity(live);

  if (!expected || !actual) {
    return false;
  }

  return (
    expected === actual ||
    expected.includes(actual) ||
    actual.includes(expected)
  );
};

const accountMatchesProfile = (
  account: ConnectedAccount,
  profile: PropConfig
) =>
  String(account.login || "").trim() ===
    String(profile.account_login || "").trim() &&
  brokerIdentityMatches(
    profile.account_server,
    account.server
  ) &&
  brokerIdentityMatches(
    profile.broker_company,
    account.company
  );

export default function Propfirm() {
  const [profilesData, setProfilesData] =
    useState<ProfilesResponse | null>(null);

  const [connectedAccounts, setConnectedAccounts] =
    useState<ConnectedAccount[]>([]);

  const [status, setStatus] =
    useState<StatusResponse | null>(null);

  const [risk, setRisk] =
    useState<RiskResponse | null>(null);

  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [err, setErr] = useState("");
  const [viewedProfileId, setViewedProfileId] =
    useState("");

  async function fetchJson<T>(
    path: string,
    init?: RequestInit
  ): Promise<T> {
    const response = await fetch(apiUrl(path), {
      credentials: "include",
      cache: "no-store",
      ...init,
    });

    let data: any = null;

    try {
      data = await response.json();
    } catch {
      data = null;
    }

    if (!response.ok) {
      throw new Error(
        data?.detail ||
          data?.error ||
          `Request failed with status ${response.status}`
      );
    }

    return data as T;
  }

  async function load(profileIdToView?: string) {
    setErr("");

    try {
      const [profilesRes, connectedRes] = await Promise.all([
        fetchJson<ProfilesResponse>(
          "/trend/prop/profiles"
        ),
        fetchJson<ConnectedAccountsResponse>(
          "/trend/prop/connected-accounts"
        ),
      ]);

      if (!profilesRes?.ok) {
        throw new Error(
          profilesRes?.error || "Failed to load prop profiles"
        );
      }

      if (!connectedRes?.ok) {
        throw new Error(
          connectedRes?.error ||
            "Failed to load connected MT5 accounts"
        );
      }

      setConnectedAccounts(
        Array.isArray(connectedRes.accounts)
          ? connectedRes.accounts.filter(
              (account) => account.connected === true
            )
          : []
      );

      const backendActiveProfileId = String(
        profilesRes.active_profile_id || ""
      ) .trim()
        .toLowerCase();

      const availableProfiles =
        Array.isArray(profilesRes.profiles)
          ? profilesRes.profiles
          : [];

      if (availableProfiles.length === 0) {
        setProfilesData({
          ...profilesRes,
          active_profile_id: "",
          profiles: [],
        });

        setViewedProfileId("");
        setStatus(null);
        setRisk(null);
        setErr("");
        return;
      }

      const firstProfileId = String(
        availableProfiles[0]?.profile_id || ""
      )
        .trim()
        .toLowerCase();

      const requestedViewProfileId = String(
        profileIdToView ||
        viewedProfileId ||
        backendActiveProfileId ||
        firstProfileId
      )
        .trim()
        .toLowerCase();

      const profileExists = availableProfiles.some(
        (profile) =>
          String(profile?.profile_id || "")
            .trim()
            .toLowerCase() === requestedViewProfileId
      );

      const displayProfileId = profileExists
        ? requestedViewProfileId
        : (
            backendActiveProfileId ||
            firstProfileId
          );
      const encodedProfileId =
        encodeURIComponent(displayProfileId);

      const [statusRes, riskRes] = await Promise.all([
        fetchJson<StatusResponse>(
          `/trend/prop/status?profile_id=${encodedProfileId}`
        ),
        fetchJson<RiskResponse>(
          `/trend/prop/risk?profile_id=${encodedProfileId}`
        ),
      ]);

      if (!statusRes?.ok) {
        throw new Error(
          statusRes?.error ||
          "Failed to load prop status"
        );
      }

      if (!riskRes?.ok) {
        throw new Error(
          riskRes?.error ||
          "Failed to load prop risk"
        );
      }

      setViewedProfileId(displayProfileId);
      setProfilesData(profilesRes);
      setStatus(statusRes);
      setRisk(riskRes);
    } catch (error: any) {
      setErr(String(error?.message || error));
    } finally {
      setLoading(false);
    }
  }
  useEffect(() => {
    void load();
  }, []);
  

  async function activateProfile(profileId: string) {
    const requestedProfileId = String(
      profileId || ""
    ).trim();

    if (!requestedProfileId || saving) {
      return;
    }

    // Always keep the clicked account selected for inspection.
    setViewedProfileId(requestedProfileId);
    // Prevent the previous account's status from appearing
    // while the selected account is loading.
    setStatus(null);
    setRisk(null);
    setSaving(true);
    setErr("");

    try {
      const res = await fetchJson<{
        ok: boolean;
        active_profile_id?: string;
        config?: PropConfig;
        error?: string;
      }>("/trend/prop/profile/active", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          profile_id: requestedProfileId,
        }),
      });

      if (!res?.ok) {
        throw new Error(
          res?.error ||
          "Failed to activate profile"
        );
      }

      // Activation succeeded. Reload this profile as active.
      await load(requestedProfileId);
      setErr("");
    } catch (error: any) {
      // Do not restore the dropdown to the active account.
      // Keep the failed profile selected and load its status.
      setErr(String(error?.message || error));

      try {
        await load(requestedProfileId);
      } catch {
        // Preserve the activation error.
      }

      setErr(String(error?.message || error));
    } finally {
      setSaving(false);
    }
  }

 
  async function enableAndActivateProfile(
    profile: PropConfig
  ) {
    const profileId = String(
      profile?.profile_id || ""
    )
      .trim()
      .toLowerCase();

    if (!profileId || saving) {
      return;
    }

    setSaving(true);
    setErr("");
    setViewedProfileId(profileId);

    try {
      const activeRes = await fetchJson<{
        ok: boolean;
        active_profile_id?: string;
        config?: PropConfig;
        error?: string;
      }>("/trend/prop/profile/enable-and-activate", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          profile_id: profileId,
        }),
      });

      if (!activeRes?.ok) {
        throw new Error(
          activeRes?.error ||
            "Failed to enable and activate profile"
        );
      }

      await load(profileId);
      setErr("");
    } catch (error: any) {
      const message = String(
        error?.message || error
      );

      try {
        await load(profileId);
      } catch {
        // Keep the original error.
      }

      setErr(message);
    } finally {
      setSaving(false);
    }
  }
  
  async function rebindProfileAccount(
    profile: PropConfig,
    account: ConnectedAccount
  ) {
    const profileId = String(
      profile?.profile_id || ""
    )
      .trim()
      .toLowerCase();

    if (!profileId || saving) {
      return;
    }

    const confirmed = window.confirm(
      `Rebind ${profile.account_name || profileId} ` +
        `from account ${profile.account_login || "unknown"} ` +
        `to connected account ${account.login}?`
    );

    if (!confirmed) {
      return;
    }

    setSaving(true);
    setErr("");

    try {
      const result = await fetchJson<{
        ok: boolean;
        profile_id?: string;
        old_account_login?: string | null;
        new_account_login?: string;
        error?: string;
      }>(
        `/trend/prop/profile/rebind-account?profile_id=${encodeURIComponent(
          profileId
        )}`,
        {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify({
            profile_id: profileId,
            account_login: account.login,
            account_server: account.server,
            broker_company: account.company,
            account_type: account.account_type,
            device_id: account.device_id,
          }),
        }
      );

      if (!result?.ok) {
        throw new Error(
          result?.error ||
            "Failed to rebind broker account"
        );
      }

      await load(profileId);
    } catch (error: any) {
      setErr(String(error?.message || error));
    } finally {
      setSaving(false);
    }
  }

  const activeProfileId = String(
    profilesData?.active_profile_id || ""
  )
    .trim()
    .toLowerCase();

  const connectedProfiles = useMemo(() => {
    const profiles = profilesData?.profiles || [];

    return profiles.filter((profile) =>
      connectedAccounts.some((account) =>
        accountMatchesProfile(account, profile)
      )
    );
  }, [profilesData, connectedAccounts]);

  const connectedProfileIds = useMemo(
    () =>
      new Set(
        connectedProfiles.map((profile) =>
          String(profile.profile_id || "")
            .trim()
            .toLowerCase()
        )
      ),
    [connectedProfiles]
  );

  const requestedSelectValue =
    viewedProfileId || activeProfileId;

  const selectValue = connectedProfileIds.has(
    requestedSelectValue
  )
    ? requestedSelectValue
    : String(
        connectedProfiles[0]?.profile_id || ""
      )
        .trim()
        .toLowerCase();

  const selectedProfile = useMemo(() => {
    return (
      connectedProfiles.find(
        (profile) =>
          String(profile.profile_id || "")
            .trim()
            .toLowerCase() === selectValue
      ) || null
    );
  }, [connectedProfiles, selectValue]);

  const configuredActiveProfile = useMemo(() => {
    return (
      profilesData?.profiles?.find(
        (profile) =>
          String(profile.profile_id || "")
            .trim()
            .toLowerCase() === activeProfileId
      ) || null
    );
  }, [profilesData, activeProfileId]);

  const displayedProfile =
    selectedProfile || configuredActiveProfile;

  const replacementCandidates = useMemo(() => {
    const profile =
      selectedProfile || configuredActiveProfile;

    if (!profile) {
      return [];
    }

    const configuredLogin = String(
      profile.account_login || ""
    ).trim();

    return connectedAccounts.filter((account) => {
      const sameBroker =
        brokerIdentityMatches(
          profile.account_server,
          account.server
        ) &&
        brokerIdentityMatches(
          profile.broker_company,
          account.company
        );

      const differentLogin =
        String(account.login || "").trim() !==
        configuredLogin;

      return sameBroker && differentLogin;
    });
  }, [
    selectedProfile,
    configuredActiveProfile,
    connectedAccounts,
  ]);

  // When the configured active profile is disconnected, status/risk still
  // belong to it and are needed to show the rebind panel safely.
  const displayedProfileId = String(
    displayedProfile?.profile_id || ""
  )
    .trim()
    .toLowerCase();

  const statusMatchesSelection =
    String(status?.profile_id || "")
      .trim()
      .toLowerCase() === displayedProfileId;

  const riskMatchesSelection =
    String(risk?.config?.profile_id || "")
      .trim()
      .toLowerCase() === displayedProfileId;

  const cfg =
    statusMatchesSelection
      ? status?.config || displayedProfile || undefined
      : displayedProfile || undefined;

  const rs =
    riskMatchesSelection
      ? risk?.risk
      : undefined;

  const limits =
    statusMatchesSelection
      ? status?.limits
      : undefined;

  const activeTitle =
    cfg?.account_name ||
    displayedProfile?.account_name ||
    displayedProfile?.profile_id ||
    "No active profile";

  const selectedIsActive =
    Boolean(displayedProfileId) &&
    displayedProfileId === activeProfileId;

  const profileDevice =
    statusMatchesSelection
      ? status?.profile_device
      : undefined;

  /*
   * Connection truth can come from either:
   *
   * 1. /prop/status strict profile->device resolution, or
   * 2. /prop/connected-accounts fresh account matching.
   *
   * connectedProfiles already contains ONLY profiles whose configured
   * login/server/company match a fresh connected MT5 account.
   *
   * This prevents a stale/out-of-order status response from falsely
   * disabling a genuinely connected account in the UI.
   *
   * Backend enable/activate still performs its own strict resolver check,
   * so this does NOT weaken execution safety.
   */
  const deviceConnected =
     Boolean(profileDevice?.ok) ||
     (
       Boolean(displayedProfileId) &&
       connectedProfileIds.has(displayedProfileId)
     );

  const profileEnabled =
    Boolean(cfg?.enabled);

  const executionReady =
    selectedIsActive &&
    profileEnabled &&
    deviceConnected;


  return (
    <div className="min-h-screen bg-slate-950 text-slate-100 p-6">
      <div className="max-w-7xl mx-auto space-y-6">
        <div className="flex flex-col xl:flex-row xl:items-start xl:justify-between gap-4">
          <div>
            <h1 className="text-2xl font-bold">
              Prop Firm
            </h1>

            <p className="text-sm text-slate-400">
              One active execution account controls XTL
              lot size, SL/TP and risk guards.
            </p>
          </div>

          <div className="rounded-2xl border border-slate-800 bg-slate-900/70 p-4 min-w-[280px]">
            <label
              htmlFor="active-prop-profile"
              className="block text-xs text-slate-400 mb-1"
            >
              Active Execution Account
            </label>

            <select
              id="active-prop-profile"
              className="w-full bg-slate-950 border border-slate-700 rounded-lg px-3 py-2 text-sm disabled:opacity-60"
              value={selectValue}
              onChange={(event) => {
                const nextProfileId = String(
                  event.target.value || ""
                )
                  .trim()
                  .toLowerCase();

                /*
                 * Selecting a profile changes only the viewed account.
                 * Activation is performed explicitly using the button.
                 */
                setViewedProfileId(nextProfileId);
                setStatus(null);
                setRisk(null);
                setErr("");

                void load(nextProfileId);
              }}
              disabled={
                loading ||
                saving ||
                connectedProfiles.length === 0
              }
            >
              
              {connectedProfiles.length === 0 ? (
                <option value="">
                  No connected execution accounts
                </option>
              ) : null}

              {connectedProfiles.map((profile) => (
                
                <option
                  key={profile.profile_id}
                  value={profile.profile_id}
                >
                  {profile.account_name ||
                      profile.profile_id}
                  {" — "}
                  {profile.account_login || "No login"}
                  {" / "}
                  {profile.account_server || "No server"}
                  {profile.enabled ? "" : " [Disabled]"}
                </option>
                
              ))}
            </select>

            <div className="text-xs text-slate-400 mt-2">
              Only fresh, connected MT5 accounts are listed. Select one to inspect it, then explicitly enable or activate it.
            </div>

            {saving ? (
              <div className="text-xs text-amber-300 mt-1">
                Changing active profile...
              </div>
            ) : null}
          </div>
        </div>

        {configuredActiveProfile &&
         replacementCandidates.length > 0 ? (
          <div className="rounded-xl border border-amber-700 bg-amber-950/30 p-4">
            <div className="text-sm font-semibold text-amber-200">
              Replacement MT5 account detected
            </div>

            <div className="mt-1 text-xs text-slate-300">
              Configured account{" "}
              <span className="font-mono">
                {configuredActiveProfile.account_login ||
                  "unknown"}
              </span>{" "}
              does not match the connected replacement account.
              Select the correct replacement explicitly.
            </div>

            <div className="mt-3 space-y-2">
              {replacementCandidates.map((account) => (
                <div
                  key={[
                    account.device_id,
                    account.login,
                    account.account_type,
                  ].join(":")}
                  className="flex flex-col gap-3 rounded-lg border border-slate-700 bg-slate-950/60 p-3 md:flex-row md:items-center md:justify-between"
                >
                  <div>
                    <div className="font-medium text-slate-100">
                      {account.company}
                    </div>

                    <div className="text-xs text-slate-400">
                      Login {account.login} · {account.server} ·{" "}
                      {account.account_type}
                    </div>

                    <div className="text-xs text-slate-500">
                      Device {account.device_id} · snapshot{" "}
                      {Math.round(
                        Number(account.snapshot_age_ms || 0) /
                          1000
                      )}{" "}
                      sec ago
                    </div>
                  </div>

                  <button
                    type="button"
                    disabled={saving}
                    onClick={() => {
                      void rebindProfileAccount(
                        configuredActiveProfile,
                        account
                      );
                    }}
                    className={[
                      "rounded-lg border px-4 py-2 text-sm font-medium",
                      saving
                        ? "cursor-not-allowed border-slate-700 text-slate-500"
                        : "border-amber-600 bg-amber-950/50 text-amber-200 hover:bg-amber-900/50",
                    ].join(" ")}
                  >
                    {saving
                      ? "Rebinding..."
                      : `Rebind to ${account.login}`}
                  </button>
                </div>
              ))}
            </div>
          </div>
        ) : null}

        {err ? (
          <div className="rounded-xl border border-red-900 bg-red-950/40 p-3 text-sm text-red-200">
            {err}
          </div>
        ) : null}

        {loading ? (
          <div className="rounded-2xl border border-slate-800 bg-slate-900/60 p-5 text-sm text-slate-400">
            Loading prop-firm account...
          </div>
        ) : null}
        {!loading && !profilesData?.profiles?.length ? (
          <div className="rounded-2xl border border-cyan-900/60 bg-cyan-950/20 p-6">
            <div className="text-lg font-semibold text-cyan-200">
              No Prop Firm accounts configured
            </div>

            <div className="mt-2 text-sm text-slate-300">
              No prop-firm execution profiles belong to this user.
              Pair a device and configure a broker account before
              enabling XTL execution.
            </div>

            <a
              href="/react/devices"
              className="mt-4 inline-flex rounded-xl border border-cyan-700 bg-cyan-950/40 px-4 py-2 text-sm font-semibold text-cyan-200 hover:bg-cyan-900/40"
            >
              Pair a Device
            </a>
          </div>
        ) : null}

        {!loading &&
         Boolean(profilesData?.profiles?.length) ? (
          <>
            <div
              className={[
                "rounded-2xl border p-5",
                executionReady
                  ? "border-emerald-800/60 bg-emerald-950/20"
                  : "border-amber-800/60 bg-amber-950/20",
              ].join(" ")}
            >
              <div
                className={[
                  "text-xs",
                  executionReady
                    ? "text-emerald-300"
                    : "text-amber-300",
                ].join(" ")}
              >
                {executionReady
                  ? "ACTIVE FOR XTL EXECUTION"
                  : selectedIsActive
                    ? "NOT READY FOR XTL EXECUTION"
                    : "SELECTED ACCOUNT IS NOT ACTIVE"}
              </div>

              <div className="text-2xl font-bold mt-1">
                {activeTitle}
              </div>

              <div className="text-sm text-slate-300 mt-1">
                {textOrNA(
                  cfg?.firm?.toUpperCase()
                )}{" "}
                / {textOrNA(cfg?.phase)} /{" "}
                {money(cfg?.account_size)}
              </div>

              <div className="text-xs text-slate-400 mt-2">
                Every ENTRY_CAND is checked against
                this account before MT5 enqueue or
                Discord manual signal.
              </div>
              {!executionReady ? (
                <div className="mt-3 rounded-lg border border-amber-800 bg-amber-950/30 p-3 text-sm text-amber-200">
                  {!profileEnabled
                    ? deviceConnected
                      ? "This broker account is connected but its XTL execution profile is disabled. Click “Enable & Use This Account”."
                      : "This profile is disabled and its configured broker account is not connected."
                    : !deviceConnected
                      ? profileDevice?.reason ||
                        "The configured broker account is not connected."
                      : !selectedIsActive
                        ? "This profile is enabled and connected, but it is not the active execution account."
                        : "This account is not ready for XTL execution."}
                </div>
              ) : null}

              {displayedProfile ? (
                <div className="mt-4 flex flex-wrap gap-3">
                  {!profileEnabled ? (
                    <button
                      type="button"
                      disabled={
                        saving ||
                        !deviceConnected
                      }
                      onClick={() => {
                        void enableAndActivateProfile(
                          displayedProfile
                        );
                      }}
                      className={[
                        "rounded-lg border px-4 py-2 text-sm font-medium",
                        saving || !deviceConnected
                          ? "cursor-not-allowed border-slate-700 bg-slate-900 text-slate-500"
                          : "border-emerald-700 bg-emerald-950/40 text-emerald-300 hover:bg-emerald-900/50",
                      ].join(" ")}
                    >
                      {saving
                        ? "Enabling..."
                        : "Enable & Use This Account"}
                    </button>
                  ) : !selectedIsActive ? (
                    <button
                      type="button"
                      disabled={
                        saving ||
                        !deviceConnected
                      }
                      onClick={() => {
                        void activateProfile(
                          displayedProfile.profile_id
                        );
                      }}
                      className={[
                        "rounded-lg border px-4 py-2 text-sm font-medium",
                        saving || !deviceConnected
                          ? "cursor-not-allowed border-slate-700 bg-slate-900 text-slate-500"
                          : "border-cyan-700 bg-cyan-950/40 text-cyan-300 hover:bg-cyan-900/50",
                      ].join(" ")}
                    >
                      {saving
                        ? "Activating..."
                        : "Use This Account"}
                    </button>
                  ) : (
                    <div className="rounded-lg border border-emerald-700 bg-emerald-950/40 px-4 py-2 text-sm font-medium text-emerald-300">
                      Active execution account
                    </div>
                  )}
                </div>
              ) : null} 
              <div className="mt-3 flex flex-wrap gap-2 text-xs">
                <StatusBadge
                  ok={Boolean(cfg?.enabled)}
                  trueText="Profile enabled"
                  falseText="Profile disabled"
                />

                <StatusBadge
                  ok={deviceConnected}
                  trueText="Broker account connected"
                  falseText={
                    profileDevice?.reason ||
                    "Broker account not connected"
                  }
                />

                <span className="rounded-full border border-slate-700 px-2 py-1 text-slate-300">
                  Selected: {displayedProfileId || "NA"}
                </span>

                <span className="rounded-full border border-slate-700 px-2 py-1 text-slate-300">
                  Active: {activeProfileId || "NA"}
                </span>

                {profileDevice?.device_id ? (
                  <span className="rounded-full border border-slate-700 px-2 py-1 text-slate-300">
                    Device:{" "}
                    {profileDevice.device_id}
                  </span>
                ) : null}
              </div>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
              <Card
                title="Target"
                value={money(limits?.target_usd)}
                sub="Profit target"
              />

              <Card
                title="Daily Loss Limit"
                value={money(
                  limits?.daily_limit_usd ??
                    rs?.daily_loss_limit
                )}
                sub="Hard firm rule"
              />

              <Card
                title="Max Loss Limit"
                value={money(
                  limits?.max_loss_limit_usd
                )}
                sub="Overall drawdown"
              />

              <Card
                title="Open Risk"
                value={money(rs?.open_risk_usd)}
                sub={`${
                  rs?.open_positions_count ??
                  rs?.open_positions?.length ??
                  0
                } open positions`}
              />
            </div>

            <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
              <Panel title="Active Rules">
                <Row
                  label="Profile ID"
                  value={selectValue}
                />

                <Row
                  label="Firm"
                  value={cfg?.firm?.toUpperCase()}
                />

                <Row
                  label="Phase"
                  value={cfg?.phase}
                />

                <Row
                  label="Account Name"
                  value={cfg?.account_name}
                />

                <Row
                  label="Account Size"
                  value={money(cfg?.account_size)}
                />

                <Row
                  label="Risk / Trade"
                  value={
                    cfg?.risk_pct !== undefined
                      ? `${cfg.risk_pct}%`
                      : "NA"
                  }
                />

                <Row
                  label="Target RR"
                  value={
                    cfg?.target_rr !== undefined
                      ? `${cfg.target_rr}R`
                      : "NA"
                  }
                />

                <Row
                  label="Max Open Risk"
                  value={
                    cfg?.max_open_risk_pct !==
                    undefined
                      ? `${cfg.max_open_risk_pct}%`
                      : "NA"
                  }
                />

                <Row
                  label="Max Positions"
                  value={cfg?.max_open_positions}
                />

                <Row
                  label="Enabled"
                  value={cfg?.enabled ? "Yes" : "No"}
                />
              </Panel>

              <Panel title="Risk State">
                <Row
                  label="Day"
                  value={rs?.day}
                />

                <Row
                  label="Daily Result"
                  value={money(
                    rs?.current_daily_result
                  )}
                />

                <Row
                  label="Daily Loss Used"
                  value={money(
                    rs?.daily_loss_used
                  )}
                />

                <Row
                  label="Daily Loss Remaining"
                  value={money(
                    rs?.daily_loss_remaining
                  )}
                />

                <Row
                  label="Risk Reserved"
                  value={money(
                    rs?.daily_risk_reserved
                  )}
                />

                <Row
                  label="Projected Loss at All SL"
                  value={money(
                    rs?.projected_daily_loss_if_all_sl
                  )}
                />

                <Row
                  label="Max Loss Used"
                  value={money(rs?.max_loss_used)}
                />

                <Row
                  label="Wins Today"
                  value={rs?.wins_today}
                />

                <Row
                  label="Losses Today"
                  value={rs?.losses_today}
                />

                <Row
                  label="Daily R"
                  value={
                    rs?.daily_r !== undefined
                      ? `${Number(rs.daily_r).toFixed(
                          2
                        )}R`
                      : "NA"
                  }
                />

                <Row
                  label="Trading Halted"
                  value={
                    rs?.trading_halted
                      ? "Yes"
                      : "No"
                  }
                />

                {rs?.trading_halted ? (
                  <Row
                    label="Halt Reason"
                    value={rs?.halt_reason}
                  />
                ) : null}
              </Panel>
            </div>

            <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
              <Panel title="Broker Account">
                <Row
                  label="Login"
                  value={
                    status?.account?.login ??
                    cfg?.account_login
                  }
                />

                <Row
                  label="Server"
                  value={
                    status?.account?.server ??
                    cfg?.account_server
                  }
                />

                <Row
                  label="Company"
                  value={
                    status?.account?.company ??
                    cfg?.broker_company
                  }
                />

                <Row
                  label="Balance"
                  value={money(
                    status?.account?.balance ??
                    rs?.broker_balance
                  )}
                />

                <Row
                  label="Equity"
                  value={money(
                    status?.account?.equity ??
                    rs?.broker_equity
                  )}
                />

                <Row
                  label="Free Margin"
                  value={money(
                    status?.account?.free_margin ??
                    rs?.free_margin
                  )}
                />

                <Row
                  label="Floating P/L"
                  value={money(
                    status?.account?.floating_pnl ??
                    rs?.floating_pnl
                  )}
                />

                <Row
                  label="Leverage"
                  value={
                    status?.account?.leverage
                      ? `1:${status.account.leverage}`
                      : "NA"
                  }
                />
              </Panel>

              <Panel title="Broker Snapshot">
                <Row
                  label="Connected"
                  value={
                    deviceConnected ? "Yes" : "No"
                  }
                />

                <Row
                  label="Device ID"
                  value={profileDevice?.device_id}
                />

                <Row
                  label="Resolution"
                  value={profileDevice?.reason}
                />

                <Row
                  label="Snapshot Valid"
                  value={
                    rs?.snapshot_valid
                      ? "Yes"
                      : "No"
                  }
                />

                <Row
                  label="Snapshot Fresh"
                  value={
                    rs?.snapshot_fresh
                      ? "Yes"
                      : "No"
                  }
                />

                <Row
                  label="Snapshot Age"
                  value={
                    rs?.snapshot_age_ms != null
                      ? `${Math.round(Number(rs.snapshot_age_ms) / 1000)} sec`
                      : "NA"
                  }
                />

                <Row
                  label="Drawdown"
                  value={
                    rs?.drawdown_pct !== undefined
                      ? `${rs.drawdown_pct}%`
                      : "NA"
                  }
                />

                <Row
                  label="Drawdown Band"
                  value={rs?.drawdown_band}
                />
              </Panel>
            </div>

            <Panel title="Open Positions">
              {!rs?.open_positions?.length ? (
                <div className="text-sm text-slate-400">
                  No open prop risk reserved.
                </div>
              ) : (
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead className="text-slate-400">
                      <tr>
                        <th className="text-left py-2">
                          Symbol
                        </th>
                        <th className="text-left py-2">
                          Side
                        </th>
                        <th className="text-left py-2">
                          Lots
                        </th>
                        <th className="text-left py-2">
                          Risk
                        </th>
                        <th className="text-left py-2">
                          Entry
                        </th>
                        <th className="text-left py-2">
                          SL
                        </th>
                        <th className="text-left py-2">
                          TP
                        </th>
                        <th className="text-left py-2">
                          Source
                        </th>
                      </tr>
                    </thead>

                    <tbody>
                      {rs.open_positions.map(
                        (position: any, index: number) => (
                          <tr
                            key={
                              position.trade_id ||
                              position.ticket ||
                              `${position.symbol}-${index}`
                            }
                            className="border-t border-slate-800"
                          >
                            <td className="py-2">
                              {textOrNA(
                                position.symbol
                              )}
                            </td>

                            <td>
                              {textOrNA(
                                position.side
                              )}
                            </td>

                            <td>
                              {textOrNA(
                                position.lots ??
                                  position.volume
                              )}
                            </td>

                            <td>
                              {money(
                                position.risk_usd
                              )}
                            </td>

                            <td>
                              {textOrNA(
                                position.entry ??
                                  position.price_open
                              )}
                            </td>

                            <td>
                              {textOrNA(position.sl)}
                            </td>

                            <td>
                              {textOrNA(position.tp)}
                            </td>

                            <td>
                              {textOrNA(
                                position.source
                              )}
                            </td>
                          </tr>
                        )
                      )}
                    </tbody>
                  </table>
                </div>
              )}
            </Panel>
          </>
        ) : null}
      </div>
    </div>
  );
}
  


function StatusBadge({
  ok,
  trueText,
  falseText,
}: {
  ok: boolean;
  trueText: string;
  falseText: string;
}) {
  return (
    <span
      className={[
        "rounded-full border px-2 py-1",
        ok
          ? "border-emerald-700 text-emerald-300 bg-emerald-950/30"
          : "border-amber-700 text-amber-300 bg-amber-950/30",
      ].join(" ")}
    >
      {ok ? trueText : falseText}
    </span>
  );
}

function Card({
  title,
  value,
  sub,
}: {
  title: string;
  value: any;
  sub: string;
}) {
  return (
    <div className="rounded-2xl border border-slate-800 bg-slate-900/60 p-5">
      <div className="text-xs text-slate-400">
        {title}
      </div>

      <div className="text-2xl font-bold mt-1">
        {value}
      </div>

      <div className="text-xs text-slate-500 mt-1">
        {sub}
      </div>
    </div>
  );
}

function Panel({
  title,
  children,
}: {
  title: string;
  children: React.ReactNode;
}) {
  return (
    <section className="rounded-2xl border border-slate-800 bg-slate-900/60 p-5">
      <h2 className="text-lg font-semibold mb-4">
        {title}
      </h2>

      <div className="space-y-2 text-sm">
        {children}
      </div>
    </section>
  );
}

function Row({
  label,
  value,
}: {
  label: string;
  value: any;
}) {
  return (
    <div className="flex justify-between gap-4 border-b border-slate-800/60 pb-2">
      <span className="text-slate-400">
        {label}
      </span>

      <span className="font-medium text-right break-all">
        {textOrNA(value)}
      </span>
    </div>
  );
}