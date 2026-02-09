import Head from "next/head";
import Link from "next/link";
import SiteLayout from "../components/SiteLayout";

const APP_ORIGIN = "https://app.xautrendlab.com";

function Card({
  title,
  desc,
  href,
}: {
  title: string;
  desc: string;
  href: string;
}) {
  return (
    <Link
      href={href}
      className="group block rounded-2xl border border-white/10 bg-white/5 p-6 hover:bg-white/[0.07]"
    >
      <div className="text-lg font-semibold">{title}</div>
      <p className="mt-2 text-sm text-white/70">{desc}</p>
      <div className="mt-4 text-sm font-medium text-white/80 group-hover:text-white">
        Learn more →
      </div>
    </Link>
  );
}

export default function Home() {
  return (
    <>
      <Head>
        <title>XTL — Trading Intelligence Platform</title>
        <meta
          name="description"
          content="XauTrendLab is an institutional-grade trading intelligence platform supporting signals, strategy execution, and automation—built around defined risk and transparent outcomes."
        />
      </Head>

      <SiteLayout>
        {/* HERO */}
        <section className="mx-auto max-w-6xl px-6 pt-16 pb-14">
          {/* Text + CTAs */}
          <div className="max-w-3xl">
            <h1 className="text-4xl font-semibold tracking-tight md:text-5xl">
              Institutional-grade market intelligence for disciplined trading
            </h1>

            <p className="mt-4 text-lg text-white/70">
              XauTrendLab is a unified trading intelligence platform supporting
              discretionary signals, strategy-driven execution, and automation —
              designed around defined risk, confirmation, and transparent outcomes.
            </p>

            <div className="mt-8 flex flex-col gap-3 sm:flex-row">
              <Link
                href="/product/overview"
                className="rounded-xl bg-white px-6 py-3 text-center text-sm font-semibold text-black hover:bg-white/90"
              >
                Explore Platform
              </Link>

              <Link
                href="/pricing"
                className="rounded-xl border border-white/15 px-6 py-3 text-center text-sm font-semibold text-white/90 hover:bg-white/5"
              >
                Request Access
              </Link>

              <a
                href="/login?next=preview"
                
                className="rounded-xl border border-white/15 px-6 py-3 text-center text-sm font-semibold text-white/90 hover:bg-white/5"
              >
                Live Preview
              </a>
            </div>

            <p className="mt-4 text-sm text-white/55">
              Note: Installer downloads are available inside the app after login.
            </p>
          </div>

          {/* Home hero visual (wide) */}
          <div className="relative mt-14 overflow-hidden rounded-3xl border border-white/10">
            <div className="pointer-events-none absolute inset-0 bg-gradient-to-t from-black/60 via-black/10 to-transparent" />
            <img
              src="/images/home-intelligence.png"
              alt="Institutional market intelligence visualization"
              className="h-auto w-full"
              loading="lazy"
            />
          </div>
        </section>

        {/* OFFERINGS SUMMARY (not full details) */}
        <section className="border-t border-white/10 py-16">
          <div className="mx-auto max-w-6xl px-6">
            <h2 className="text-2xl font-semibold md:text-3xl">Offerings</h2>
            <p className="mt-3 max-w-3xl text-white/70">
              Choose how you want to trade with XTL. Each offering has its own dedicated page with full details.
            </p>

            <div className="mt-10 grid gap-6 md:grid-cols-3">
              <Card
                title="Signals"
                href="/offerings/signals"
                desc="AI-powered trade ideas with clear direction, risk context, and execution guidance."
              />
              <Card
                title="MyBots"
                href="/offerings/mybots"
                desc="Configure what you want, then run execution via installer on your local machine/VPS."
              />
              <Card
                title="Automated Trading"
                href="/offerings/automation"
                desc="Hands-free execution: entries, SL/TP, trailing, and tracking without manual intervention."
              />
            </div>
          </div>
        </section>

        {/* HOW IT WORKS (high-level only) */}
        <section className="border-t border-white/10 py-16">
          <div className="mx-auto max-w-6xl px-6">
            <h2 className="text-2xl font-semibold md:text-3xl">How XTL Works</h2>

            <div className="mt-8 grid gap-6 md:grid-cols-5">
              {[
                ["Market Data", "Price & structure signals from your trading environment."],
                ["Intelligence", "Models + rules convert data into trade context."],
                ["Decision", "Signals or strategy rules decide actions."],
                ["Execution", "Local agent places & manages orders."],
                ["Management", "SL/TP, trailing and exits monitored continuously."],
              ].map(([t, d]) => (
                <div key={t} className="rounded-2xl border border-white/10 bg-white/5 p-5">
                  <div className="text-sm font-semibold text-white">{t}</div>
                  <div className="mt-2 text-sm text-white/70">{d}</div>
                </div>
              ))}
            </div>

            <div className="mt-10">
              <Link
                href="/product/architecture"
                className="inline-flex items-center rounded-xl border border-white/15 px-5 py-3 text-sm font-semibold text-white/90 hover:bg-white/5"
              >
                View Platform Architecture →
              </Link>
            </div>
          </div>
        </section>

        {/* CTA */}
        <section className="border-t border-white/10 py-16">
          <div className="mx-auto max-w-6xl px-6">
            <div className="rounded-3xl border border-white/10 bg-white/5 p-8 md:p-10">
              <h2 className="text-2xl font-semibold md:text-3xl">
                One platform. Multiple ways to trade — depending on how much control you want.
              </h2>
              <p className="mt-3 max-w-3xl text-white/70">
                XTL supports a progression: discretionary signals → configurable bots → fully automated trading.
              </p>

              <div className="mt-6 flex flex-col gap-3 sm:flex-row">
                <Link
                  href="/offerings/signals"
                  className="rounded-xl bg-white px-6 py-3 text-center text-sm font-semibold text-black hover:bg-white/90"
                >
                  Explore Features
                </Link>

                <Link
                  href="/pricing"
                  className="rounded-xl border border-white/15 px-6 py-3 text-center text-sm font-semibold text-white/90 hover:bg-white/5"
                >
                  View Pricing
                </Link>
              </div>
            </div>
          </div>
        </section>
      </SiteLayout>
    </>
  );
}
