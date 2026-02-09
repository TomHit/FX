import Head from "next/head";
import SiteLayout from "../../components/SiteLayout";

export default function ProductSecurity() {
  return (
    <>
      <Head>
        <title>Security & Execution — XauTrendLab</title>
        <meta
          name="description"
          content="Security & execution model: local agent execution, control, and safe trade management."
        />
      </Head>

      <SiteLayout
        title="Security & Execution"
        subtitle="XTL emphasizes control, transparency, and safe execution behavior."
      >
        <section className="mx-auto max-w-6xl px-6 pb-16">
          {/* Core principles */}
          <div className="rounded-3xl border border-white/10 bg-white/5 p-8 md:p-10">
            <h2 className="text-xl font-semibold md:text-2xl">Security principles</h2>
            <p className="mt-3 max-w-3xl text-white/70">
              Trust matters in trading software. XTL is built to keep execution under your control and
              to make behavior predictable. Your strategy settings define what the system can and cannot do.
            </p>

            <div className="mt-8 grid gap-6 md:grid-cols-3">
              <div className="rounded-2xl border border-white/10 bg-white/5 p-6">
                <div className="text-sm font-semibold">Local execution</div>
                <p className="mt-2 text-sm text-white/70">
                  Bots and automation can run on your machine/VPS via the installer/agent. You control the runtime.
                </p>
              </div>

              <div className="rounded-2xl border border-white/10 bg-white/5 p-6">
                <div className="text-sm font-semibold">Guardrails</div>
                <p className="mt-2 text-sm text-white/70">
                  Execution respects your risk limits: SL/TP settings, trailing behavior, and strategy constraints.
                </p>
              </div>

              <div className="rounded-2xl border border-white/10 bg-white/5 p-6">
                <div className="text-sm font-semibold">Transparency</div>
                <p className="mt-2 text-sm text-white/70">
                  Clear signal/decision context so you know what the platform is doing and why.
                </p>
              </div>
            </div>
          </div>

          {/* Execution behavior */}
          <div className="mt-10 grid gap-6 md:grid-cols-2">
            <div className="rounded-3xl border border-white/10 bg-white/5 p-8">
              <h3 className="text-base font-semibold">Execution behavior</h3>
              <p className="mt-2 text-sm text-white/70">
                XTL focuses on predictable automation. When a trade is executed, the system can place and
                track orders, apply SL/TP, and manage trailing exits based on the strategy configuration.
              </p>
              <ul className="mt-5 space-y-2 text-sm text-white/70">
                <li>• Controlled entry rules (Signals / MyBots / Automation).</li>
                <li>• SL/TP placement and monitoring.</li>
                <li>• Optional trailing & exit management.</li>
                <li>• Trade tracking and reporting inside the app.</li>
              </ul>
            </div>

            <div className="rounded-3xl border border-white/10 bg-white/5 p-8">
              <h3 className="text-base font-semibold">What users control</h3>
              <p className="mt-2 text-sm text-white/70">
                You decide how much of the workflow is automated. Start manual with Signals, then increase
                automation only when you are comfortable.
              </p>
              <ul className="mt-5 space-y-2 text-sm text-white/70">
                <li>• Strategy selection and parameters (MyBots).</li>
                <li>• Risk limits: SL/TP, trailing, trade filters.</li>
                <li>• Run/pause/stop execution anytime.</li>
                <li>• Installer access only after login (inside app).</li>
              </ul>
            </div>
          </div>

          {/* Disclaimer block */}
          <div className="mt-10 rounded-3xl border border-white/10 bg-white/5 p-8">
            <h3 className="text-base font-semibold">Important note</h3>
            <p className="mt-2 text-sm text-white/70">
              Trading involves risk. XTL provides intelligence and automation tools, but outcomes depend on
              market behavior and user configuration. Always use appropriate risk management.
            </p>
          </div>
        </section>
      </SiteLayout>
    </>
  );
}
