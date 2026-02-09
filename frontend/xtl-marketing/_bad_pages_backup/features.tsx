import SiteLayout from "../components/SiteLayout";
import Link from "next/link";

export default function FeaturesPage() {
  return (
    <SiteLayout title="XauTrendLab — Features">
      <section className="mx-auto max-w-6xl px-5 py-14 md:py-20">
        <div className="max-w-2xl">
          <div className="text-xs text-gold2/90">FEATURES</div>
          <h1 className="mt-2 text-3xl font-semibold tracking-tight md:text-4xl">
            Built to trade — not to decorate charts
          </h1>
          <p className="mt-3 text-sm text-white/70">
            XauTrendLab focuses on structured decisions: trend, entry timing, TP/SL rails, and review.
          </p>
        </div>

        <div className="mt-8 grid gap-4 md:grid-cols-2">
          {[
            ["Trend + bias labels", "Clear UP/DOWN bias with stable snapshots so targets don’t jump around."],
            ["Entry framework", "Pullback + confirmation style entries designed to reduce chasing."],
            ["Risk rails", "TP/SL + optional trailing logic so every trade has structure."],
            ["History & outcomes", "Keep opportunity history for analysis and refinement."],
            ["MT5 integration", "Agent pipeline designed for MT5 workflow and execution constraints."],
            ["Scale later", "Add alerts/Discord, payments, and strategy automation step-by-step."],
          ].map(([t, d]) => (
            <div key={t} className="rounded-2xl border border-white/10 bg-white/5 p-6 shadow-soft">
              <div className="text-sm font-semibold">{t}</div>
              <div className="mt-2 text-sm text-white/70">{d}</div>
            </div>
          ))}
        </div>

        <div className="mt-10 flex flex-col gap-3 sm:flex-row">
          <a
            href="https://app.xautrendlab.com"
            target="_blank"
            rel="noreferrer"
            className="inline-flex items-center justify-center rounded-2xl bg-white px-5 py-3 text-sm font-semibold text-black hover:opacity-95"
          >
            Open Web App
          </a>
          <Link
            href="/pricing"
            className="inline-flex items-center justify-center rounded-2xl border border-white/12 bg-white/5 px-5 py-3 text-sm font-semibold text-white/90 hover:border-white/20 hover:bg-white/7"
          >
            View Pricing
          </Link>
        </div>
      </section>
    </SiteLayout>
  );
}
