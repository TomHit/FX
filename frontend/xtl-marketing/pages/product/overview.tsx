import Head from "next/head";
import Link from "next/link";
import SiteLayout from "../../components/SiteLayout";

export default function ProductOverview() {
  return (
    <>
      <Head>
        <title>XTL Product Overview — XauTrendLab</title>
        <meta
          name="description"
          content="XTL is a trading intelligence platform. Use signals, configure MyBots, or run automated execution—inside one secure product."
        />
      </Head>

      <SiteLayout
        title="XTL Platform — Product Overview"
        subtitle="XTL is one product that supports multiple trading paths: Signals, MyBots, and Automated Trading."
      >
        <section className="mx-auto max-w-6xl px-6 pb-16">
          {/* What it is */}
          <div className="rounded-3xl border border-white/10 bg-white/5 p-8 md:p-10">
            <h2 className="text-xl font-semibold md:text-2xl">What is XTL?</h2>
            <p className="mt-3 max-w-3xl text-white/70">
              XTL (XauTrendLab) is a trading intelligence platform designed to help you move from
              discretionary trading to strategy-driven automation—without losing control. You can
              start with Signals, progress to MyBots, and eventually run hands-free execution.
            </p>

            <div className="mt-8 grid gap-6 md:grid-cols-3">
              <div className="rounded-2xl border border-white/10 bg-white/5 p-6">
                <div className="text-sm font-semibold text-white">Signals</div>
                <p className="mt-2 text-sm text-white/70">
                  AI-guided trade ideas with a clean presentation so you can decide and execute.
                </p>
                <Link className="mt-4 inline-block text-sm font-semibold text-white/90 hover:text-white" href="/offerings/signals">
                  Learn more →
                </Link>
              </div>

              <div className="rounded-2xl border border-white/10 bg-white/5 p-6">
                <div className="text-sm font-semibold text-white">MyBots</div>
                <p className="mt-2 text-sm text-white/70">
                  Configure strategies and run execution using an installer on your machine/VPS.
                </p>
                <Link className="mt-4 inline-block text-sm font-semibold text-white/90 hover:text-white" href="/offerings/mybots">
                  Explore MyBots →
                </Link>
              </div>

              <div className="rounded-2xl border border-white/10 bg-white/5 p-6">
                <div className="text-sm font-semibold text-white">Automated Trading</div>
                <p className="mt-2 text-sm text-white/70">
                  Fully hands-free: entries, SL/TP, trailing, and monitoring without manual work.
                </p>
                <Link className="mt-4 inline-block text-sm font-semibold text-white/90 hover:text-white" href="/offerings/automation">
                  Explore Automation →
                </Link>
              </div>
            </div>
          </div>

          {/* Core pillars */}
          <div className="mt-10 grid gap-6 md:grid-cols-3">
            <div className="rounded-3xl border border-white/10 bg-white/5 p-7">
              <h3 className="text-base font-semibold">Clarity</h3>
              <p className="mt-2 text-sm text-white/70">
                Signals and decisions are presented in a structured way. You always understand what
                the system is suggesting and why it matters.
              </p>
            </div>

            <div className="rounded-3xl border border-white/10 bg-white/5 p-7">
              <h3 className="text-base font-semibold">Control</h3>
              <p className="mt-2 text-sm text-white/70">
                MyBots lets you choose strategy parameters and execution preferences. You decide how
                aggressive or conservative the behavior should be.
              </p>
            </div>

            <div className="rounded-3xl border border-white/10 bg-white/5 p-7">
              <h3 className="text-base font-semibold">Progression</h3>
              <p className="mt-2 text-sm text-white/70">
                Start with Signals. Move to MyBots. Graduate to full Automation when you trust the
                workflow. XTL supports that journey.
              </p>
            </div>
          </div>

          {/* Next */}
          <div className="mt-12 flex flex-col gap-3 sm:flex-row">
            <Link
              href="/product/architecture"
              className="rounded-xl bg-white px-6 py-3 text-center text-sm font-semibold text-black hover:bg-white/90"
            >
              Platform Architecture →
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
