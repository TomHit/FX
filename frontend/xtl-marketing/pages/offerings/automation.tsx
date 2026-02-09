import Head from "next/head";
import Link from "next/link";
import SiteLayout from "../../components/SiteLayout";

export default function OfferingAutomation() {
  return (
    <>
      <Head>
        <title>Automated Trading — XauTrendLab</title>
        <meta
          name="description"
          content="Automated Trading in XTL: hands-free execution with entries, SL/TP, trailing management, and monitoring."
        />
      </Head>

      <SiteLayout
        title="Automated Trading"
        subtitle="Hands-free execution: entries, SL/TP, trailing exits, and tracking—without manual intervention."
      >
        <section className="mx-auto max-w-6xl px-6 pb-16">
          <div className="rounded-3xl border border-white/10 bg-white/5 p-8 md:p-10">
            <h2 className="text-xl font-semibold md:text-2xl">What “hands-free” means in XTL</h2>
            <p className="mt-3 max-w-3xl text-white/70">
              Automated Trading is for users who want end-to-end execution once rules and guardrails are set.
              The platform executes and manages trades according to configuration, reducing manual workload.
            </p>

            <div className="mt-8 grid gap-6 md:grid-cols-3">
              <div className="rounded-2xl border border-white/10 bg-white/5 p-6">
                <div className="text-sm font-semibold">Entry execution</div>
                <p className="mt-2 text-sm text-white/70">
                  When conditions match, the system can execute entries as per the configured strategy rules.
                </p>
              </div>

              <div className="rounded-2xl border border-white/10 bg-white/5 p-6">
                <div className="text-sm font-semibold">Trade management</div>
                <p className="mt-2 text-sm text-white/70">
                  SL/TP placement, trailing logic, and exit rules are handled continuously.
                </p>
              </div>

              <div className="rounded-2xl border border-white/10 bg-white/5 p-6">
                <div className="text-sm font-semibold">Monitoring & tracking</div>
                <p className="mt-2 text-sm text-white/70">
                  Track what happened, why it happened, and how performance evolves over time.
                </p>
              </div>
            </div>
          </div>

          {/* Control and risk */}
          <div className="mt-10 grid gap-6 md:grid-cols-2">
            <div className="rounded-3xl border border-white/10 bg-white/5 p-8">
              <h3 className="text-base font-semibold">Control stays with you</h3>
              <p className="mt-2 text-sm text-white/70">
                Automated doesn’t mean uncontrolled. You define risk rails and execution preferences.
                You can pause, change settings, or stop execution anytime.
              </p>
              <ul className="mt-5 space-y-2 text-sm text-white/70">
                <li>• Limits and filters prevent unwanted trades.</li>
                <li>• SL/TP and trailing behavior follow your rules.</li>
                <li>• The agent runs under your environment (machine/VPS).</li>
              </ul>
            </div>

            <div className="rounded-3xl border border-white/10 bg-white/5 p-8">
              <h3 className="text-base font-semibold">Best for</h3>
              <ul className="mt-4 space-y-2 text-sm text-white/70">
                <li>• Traders who want minimal manual effort.</li>
                <li>• Users who already validated a strategy via Signals/MyBots.</li>
                <li>• Execution that needs consistency and reduced emotion.</li>
              </ul>
            </div>
          </div>

          {/* Next */}
          <div className="mt-12 flex flex-col gap-3 sm:flex-row">
            <Link
              href="/pricing"
              className="rounded-xl bg-white px-6 py-3 text-center text-sm font-semibold text-black hover:bg-white/90"
            >
              View Pricing →
            </Link>

            <Link
              href="/product/overview"
              className="rounded-xl border border-white/15 px-6 py-3 text-center text-sm font-semibold text-white/90 hover:bg-white/5"
            >
              Back to Product Overview →
            </Link>
          </div>
        </section>
      </SiteLayout>
    </>
  );
}
