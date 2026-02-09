import Head from "next/head";
import Link from "next/link";
import SiteLayout from "../../components/SiteLayout";

export default function OfferingSignals() {
  return (
    <>
      <Head>
        <title>Signals — XauTrendLab</title>
        <meta
          name="description"
          content="Signals in XTL: AI-powered trade ideas with clear direction and structured context for manual or assisted execution."
        />
      </Head>

      <SiteLayout
        title="Signals"
        subtitle="AI-powered trade ideas with clear direction and structured context—built for discretionary trading."
      >
        <section className="mx-auto max-w-6xl px-6 pb-16">
          <div className="rounded-3xl border border-white/10 bg-white/5 p-8 md:p-10">
            <h2 className="text-xl font-semibold md:text-2xl">What you get</h2>
            <p className="mt-3 max-w-3xl text-white/70">
              Signals are designed for traders who want guidance but still prefer to make the final
              decision. XTL presents a clean, structured view so you can execute confidently.
            </p>

            <div className="mt-8 grid gap-6 md:grid-cols-3">
              <div className="rounded-2xl border border-white/10 bg-white/5 p-6">
                <div className="text-sm font-semibold">Direction & Context</div>
                <p className="mt-2 text-sm text-white/70">
                  A clear directional view (bullish/bearish) with context so you understand the setup.
                </p>
              </div>

              <div className="rounded-2xl border border-white/10 bg-white/5 p-6">
                <div className="text-sm font-semibold">Execution Guidance</div>
                <p className="mt-2 text-sm text-white/70">
                  Suggested entry logic and trade management framing (not blind “buy/sell” spam).
                </p>
              </div>

              <div className="rounded-2xl border border-white/10 bg-white/5 p-6">
                <div className="text-sm font-semibold">Risk-first mindset</div>
                <p className="mt-2 text-sm text-white/70">
                  Signals are built to be used with risk controls. You stay in charge of sizing and stops.
                </p>
              </div>
            </div>
          </div>

          {/* Who it's for */}
          <div className="mt-10 grid gap-6 md:grid-cols-2">
            <div className="rounded-3xl border border-white/10 bg-white/5 p-8">
              <h3 className="text-base font-semibold">Who Signals are for</h3>
              <ul className="mt-4 space-y-2 text-sm text-white/70">
                <li>• Traders who want AI guidance but prefer manual execution.</li>
                <li>• Traders who are still building confidence in automation.</li>
                <li>• Users who want to learn setups and confirm with their own analysis.</li>
              </ul>
            </div>

            <div className="rounded-3xl border border-white/10 bg-white/5 p-8">
              <h3 className="text-base font-semibold">Best way to use Signals</h3>
              <ul className="mt-4 space-y-2 text-sm text-white/70">
                <li>• Use Signals as a shortlist, not as a guaranteed outcome.</li>
                <li>• Apply your own risk rules (SL/TP/position size).</li>
                <li>• Track results and evolve toward MyBots if you want repeatability.</li>
              </ul>
            </div>
          </div>

          {/* Next */}
          <div className="mt-12 flex flex-col gap-3 sm:flex-row">
            <Link
              href="/offerings/mybots"
              className="rounded-xl bg-white px-6 py-3 text-center text-sm font-semibold text-black hover:bg-white/90"
            >
              Next: MyBots →
            </Link>

            <Link
              href="/pricing"
              className="rounded-xl border border-white/15 px-6 py-3 text-center text-sm font-semibold text-white/90 hover:bg-white/5"
            >
              View Pricing →
            </Link>
          </div>
        </section>
      </SiteLayout>
    </>
  );
}
