import Head from "next/head";
import { useEffect, useMemo, useState } from "react";
import SiteLayout from "../components/SiteLayout";

// App origin (prod / staging safe)
const APP_ORIGIN = (
  process.env.NEXT_PUBLIC_APP_ORIGIN ||
  "https://app.xautrendlab.com"
).trim();

// IMPORTANT:
// This is a lightweight gate for marketing -> app.
// It prevents casual users from hitting the app URL.
// Real authentication stays inside app.xautrendlab.com.

export default function LoginGatePage() {
  const [code, setCode] = useState("");
  const [err, setErr] = useState<string | null>(null);
  const [ok, setOk] = useState(false);

  

  async function submit(e: React.FormEvent) {
    e.preventDefault();
    setErr(null);

    const c = (code || "").trim();

    

    if (!c) {
      setErr("Please enter your access code.");
      return;
    }

    try {
      const res = await fetch("/api/access/verify", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ code: c }),
    });

    const j = await res.json();
    if (!j.ok) {
      setErr(j.error || "Invalid access code.");
      return;
    }

    if (!j.token) {
      setErr("Preview token missing. Please try again.");
      return;
    }

    setOk(true);
    window.location.href =
      `https://api.xautrendlab.com/preview/consume?token=${encodeURIComponent(j.token)}`;
  } catch {
    setErr("Unable to verify code. Please try again.");
  }
}


  return (
    <>
      <Head>
        <title>Login — XauTrendLab</title>
        <meta
          name="description"
          content="Enter your private access code to proceed to the XTL app login."
        />
      </Head>

      <SiteLayout
        title="Login"
        subtitle="Enter your private Access Code to proceed to the XTL app."
      >
        <section className="mx-auto max-w-xl px-6 pb-16">
          <div className="rounded-3xl border border-white/10 bg-white/5 p-8">
            <form onSubmit={submit}>
              <label className="text-sm font-semibold text-white/90">
                Access Code
              </label>
              <input
                value={code}
                onChange={(e) => setCode(e.target.value)}
                placeholder="Enter code"
                className="mt-2 w-full rounded-xl border border-white/15 bg-black/30 px-4 py-3 text-sm text-white outline-none placeholder:text-white/30 focus:border-white/30"
                autoComplete="off"
              />

              {err ? (
                <div className="mt-3 rounded-xl border border-red-500/30 bg-red-500/10 px-4 py-3 text-sm text-red-200">
                  {err}
                </div>
              ) : null}

              {ok ? (
                <div className="mt-3 rounded-xl border border-emerald-400/25 bg-emerald-400/10 px-4 py-3 text-sm text-emerald-200">
                  Code verified. Redirecting to app…
                </div>
              ) : null}

              <button
                type="submit"
                className="mt-5 w-full rounded-xl bg-white px-5 py-3 text-sm font-semibold text-black hover:bg-white/90"
              >
                Continue to App
              </button>

              <div className="mt-5 text-xs text-white/55">
                Don’t have a code? Purchase a plan or request access from Pricing.
              </div>
            </form>
          </div>

          <div className="mt-6 rounded-2xl border border-white/10 bg-white/5 p-6 text-sm text-white/70">
            <div className="font-semibold text-white/90">Why this gate?</div>
            <p className="mt-2">
              We keep app access private to reduce unnecessary traffic and protect the platform.
              After payment, we provide you a private Access Code.
            </p>
          </div>
        </section>
      </SiteLayout>
    </>
  );
}
