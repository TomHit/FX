import Head from "next/head";
import Link from "next/link";
import SiteLayout from "../../components/SiteLayout";

export default function OfferingMyBots() {
  return (
    <>
      <Head>
        <title>MyBots — XauTrendLab</title>
        <meta
          name="description"
          content="MyBots in XTL: user-configured strategies executed via a local installer/agent running on your machine or VPS."
        />
      </Head>

      <SiteLayout
        title="MyBots"
        subtitle="Configure strategies you want. Run execution via installer/agent on your local machine or VPS."
      >
        <section className="mx-auto max-w-6xl px-6 pb-16">
          <div className="rounded-3xl border border-white/10 bg-white/5 p-8 md:p-10">
            <h2 className="text-xl font-semibold md:text-2xl">What are MyBots?</h2>
            <p className="mt-3 max-w-3xl text-white/70">
              MyBots lets you define how you want to trade — strategy rules, risk limits, and management
              preferences — and then execute using the XTL installer/agent running on your machine/VPS.
              It’s automation with control.
            </p>

            <div className="mt-8 grid gap-6 md:grid-cols-3">
              <div className="rounded-2xl border border-white/10 bg-white/5 p-6">
                <div className="text-sm font-semibold">Your rules</div>
                <p className="mt-2 text-sm text-white/70">
                  Choose strategy settings and constraints so execution matches your style.
                </p>
              </div>

              <div className="rounded-2xl border border-white/10 bg-white/5 p-6">
                <div className="text-sm font-semibold">Local agent execution</div>
                <p className="mt-2 text-sm text-white/70">
                  Execution runs under your environment. You can pause/stop anytime.
                </p>
              </div>

              <div className="rounded-2xl border border-white/10 bg-white/5 p-6">
                <div className="text-sm font-semibold">Repeatability</div>
                <p className="mt-2 text-sm text-white/70">
                  Once configured, bots can follow the same process consistently, reducing emotional bias.
                </p>
              </div>
            </div>
          </div>

          {/* How it works */}
          <div className="mt-10 rounded-3xl border border-white/10 bg-white/5 p-8 md:p-10">
            <h3 className="text-base font-semibold">How it works</h3>
            <div className="mt-6 grid gap-6 md:grid-cols-4">
              {[
                ["Choose strategy", "Select what you want the bot to trade and how."],
                ["Set risk rails", "Define SL/TP, trailing behavior, filters, limits."],
                ["Run installer/agent", "Start bot execution on your machine/VPS."],
                ["Monitor & refine", "Review performance and iterate settings."],
              ].map(([t, d]) => (
                <div key={t} className="rounded-2xl border border-white/10 bg-white/5 p-5">
                  <div className="text-sm font-semibold">{t}</div>
                  <div className="mt-2 text-sm text-white/70">{d}</div>
                </div>
              ))}
            </div>

            <p className="mt-6 text-sm text-white/55">
              Installer downloads are available inside the app after login.
            </p>
          </div>

          {/* Next */}
          <div className="mt-12 flex flex-col gap-3 sm:flex-row">
            <Link
              href="/offerings/automation"
              className="rounded-xl bg-white px-6 py-3 text-center text-sm font-semibold text-black hover:bg-white/90"
            >
              Next: Automated Trading →
            </Link>

            <Link
              href="/product/security"
              className="rounded-xl border border-white/15 px-6 py-3 text-center text-sm font-semibold text-white/90 hover:bg-white/5"
            >
              Security & Execution →
            </Link>
          </div>
        </section>
      </SiteLayout>
    </>
  );
}
