import Head from "next/head";
import SiteLayout from "../../components/SiteLayout";

export default function ProductArchitecture() {
  return (
    <>
      <Head>
        <title>Platform Architecture — XauTrendLab</title>
        <meta
          name="description"
          content="How XTL works: intelligence layer, strategy decisions, and local execution via installer/agent."
        />
      </Head>

      <SiteLayout
        title="Platform Architecture"
        subtitle="A simple, trustworthy design: intelligence in the platform, execution via your local agent."
      >
        <section className="mx-auto max-w-6xl px-6 pb-16">
          {/* Architecture overview */}
          <div className="rounded-3xl border border-white/10 bg-white/5 p-8 md:p-10">
            <h2 className="text-xl font-semibold md:text-2xl">System overview</h2>

            <div className="mt-8 grid gap-6 md:grid-cols-5">
              {[
                ["Market Inputs", "Price, structure, and session context."],
                ["Intelligence", "Models + rules convert data into signals."],
                ["Decision Layer", "Signals or bot rules decide actions."],
                ["Local Execution", "Installer/agent runs on your machine/VPS."],
                ["Trade Management", "SL/TP, trailing, exits, monitoring."],
              ].map(([t, d]) => (
                <div key={t} className="rounded-2xl border border-white/10 bg-white/5 p-5">
                  <div className="text-sm font-semibold text-white">{t}</div>
                  <div className="mt-2 text-sm text-white/70">{d}</div>
                </div>
              ))}
            </div>

            <p className="mt-8 max-w-3xl text-sm text-white/70">
              The marketing site explains the platform. The app provides the real execution workflow.
              Installer downloads are available inside the app after login.
            </p>
          </div>

          {/* Why this architecture */}
          <div className="mt-10 grid gap-6 md:grid-cols-2">
            <div className="rounded-3xl border border-white/10 bg-white/5 p-8">
              <h3 className="text-base font-semibold">Why local execution?</h3>
              <p className="mt-2 text-sm text-white/70">
                Many traders prefer keeping execution on their own machine/VPS. It improves control and
                reduces dependency on external “black-box” execution servers.
              </p>
              <ul className="mt-5 space-y-2 text-sm text-white/70">
                <li>• You control when execution runs.</li>
                <li>• You control broker connectivity and environment.</li>
                <li>• You can stop or pause anytime.</li>
              </ul>
            </div>

            <div className="rounded-3xl border border-white/10 bg-white/5 p-8">
              <h3 className="text-base font-semibold">How Signals vs MyBots differ</h3>
              <p className="mt-2 text-sm text-white/70">
                Signals are guidance-first. MyBots are rules-first. Automation is hands-free execution.
              </p>
              <ul className="mt-5 space-y-2 text-sm text-white/70">
                <li>• Signals: you decide & execute (manual/semi-assisted).</li>
                <li>• MyBots: you configure strategies; execution follows rules.</li>
                <li>• Automation: end-to-end execution + monitoring.</li>
              </ul>
            </div>
          </div>

          {/* Operational model */}
          <div className="mt-10 rounded-3xl border border-white/10 bg-white/5 p-8 md:p-10">
            <h3 className="text-base font-semibold">Operational model</h3>
            <p className="mt-2 max-w-3xl text-sm text-white/70">
              XTL is designed to be reliable: clear decisions, predictable execution behavior, and strong
              guardrails around trade management. This architecture makes scaling to more strategies easier
              while keeping user control intact.
            </p>
          </div>
        </section>
      </SiteLayout>
    </>
  );
}
