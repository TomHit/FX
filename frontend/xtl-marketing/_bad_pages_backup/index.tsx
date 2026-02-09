import Link from "next/link";
import SiteLayout from "../components/SiteLayout";

function StatCard({ label, value, sub }: { label: string; value: string; sub: string }) {
  return (
    <div className="rounded-2xl border border-white/10 bg-white/5 p-5 shadow-soft">
      <div className="text-xs text-white/60">{label}</div>
      <div className="mt-2 text-2xl font-semibold tracking-tight">{value}</div>
      <div className="mt-1 text-xs text-white/60">{sub}</div>
    </div>
  );
}

function Feature({ title, desc }: { title: string; desc: string }) {
  return (
    <div className="rounded-2xl border border-white/10 bg-[linear-gradient(180deg,rgba(255,255,255,0.06),rgba(255,255,255,0.03))] p-6 shadow-soft">
      <div className="text-sm font-semibold">{title}</div>
      <div className="mt-2 text-sm text-white/70">{desc}</div>
    </div>
  );
}

export default function Home() {
  return (
    <SiteLayout>
      {/* HERO */}
      <section className="mx-auto max-w-6xl px-5 pt-14 md:pt-20">
        <div className="grid items-center gap-10 md:grid-cols-2">
          <div>
            <div className="inline-flex items-center gap-2 rounded-full border border-white/10 bg-white/5 px-3 py-1 text-xs text-white/70">
              <span className="h-1.5 w-1.5 rounded-full bg-gold" />
              XAU-premium design • MT5-first workflow
            </div>

            <h1 className="mt-5 text-4xl font-semibold leading-tight tracking-tight md:text-5xl">
              AI-Powered <span className="text-[rgba(245,215,122,0.95)]">Intraday Trend</span> Intelligence
            </h1>

            <p className="mt-4 text-base text-white/70 md:text-lg">
              XauTrendLab helps you trade faster with clear trend direction, entry timing, and risk rails —
              designed for real intraday execution.
            </p>

            <div className="mt-7 flex flex-col gap-3 sm:flex-row">
              <a
                href="https://app.xautrendlab.com"
                target="_blank"
                rel="noreferrer"
                className="inline-flex items-center justify-center rounded-2xl bg-[linear-gradient(135deg,rgba(212,175,55,0.95),rgba(245,215,122,0.92))] px-5 py-3 text-sm font-semibold text-black shadow-glow hover:opacity-95"
              >
                Open Web App
              </a>
              <Link
                href="/features"
                className="inline-flex items-center justify-center rounded-2xl border border-white/12 bg-white/5 px-5 py-3 text-sm font-semibold text-white/90 hover:border-white/20 hover:bg-white/7"
              >
                See Features
              </Link>
            </div>

            <div className="mt-8 grid grid-cols-1 gap-3 sm:grid-cols-3">
              <StatCard label="Signals" value="Trend + Entry" sub="Direction + timing" />
              <StatCard label="Risk rails" value="TP/SL" sub="Clear trade plan" />
              <StatCard label="Workflow" value="MT5 ready" sub="Agent + Web UI" />
            </div>
          </div>

          {/* RIGHT VISUAL */}
          <div className="relative">
            <div className="absolute -inset-6 rounded-[32px] bg-[radial-gradient(600px_300px_at_70%_10%,rgba(212,175,55,0.22),transparent_60%)] blur-2xl" />
            <div className="relative overflow-hidden rounded-[28px] border border-white/10 bg-panel/70 p-6 shadow-soft">
              <div className="flex items-center justify-between">
                <div className="text-sm font-semibold">Trend Snapshot</div>
                <div className="rounded-full border border-white/10 bg-white/5 px-3 py-1 text-xs text-white/70">
                  Live-style preview
                </div>
              </div>

              <div className="mt-5 grid gap-4">
                <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
                  <div className="flex items-center justify-between">
                    <div className="text-sm font-semibold">XAUUSD</div>
                    <div className="rounded-full bg-[rgba(212,175,55,0.18)] px-3 py-1 text-xs font-semibold text-gold2">
                      Bullish bias
                    </div>
                  </div>
                  <div className="mt-3 grid grid-cols-3 gap-3 text-xs text-white/70">
                    <div className="rounded-xl border border-white/10 bg-black/30 p-3">
                      <div className="text-white/50">Entry</div>
                      <div className="mt-1 font-semibold text-white">Pullback + confirm</div>
                    </div>
                    <div className="rounded-xl border border-white/10 bg-black/30 p-3">
                      <div className="text-white/50">Target</div>
                      <div className="mt-1 font-semibold text-white">TP based on ATR</div>
                    </div>
                    <div className="rounded-xl border border-white/10 bg-black/30 p-3">
                      <div className="text-white/50">Risk</div>
                      <div className="mt-1 font-semibold text-white">SL + trail rules</div>
                    </div>
                  </div>
                </div>

                <div className="grid grid-cols-2 gap-4">
                  <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
                    <div className="text-xs text-white/60">What you see</div>
                    <div className="mt-2 text-sm font-semibold">Clean “trade plan” cards</div>
                    <div className="mt-2 text-sm text-white/70">
                      No noise. Direction + entry + TP/SL — ready for execution.
                    </div>
                  </div>
                  <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
                    <div className="text-xs text-white/60">What you avoid</div>
                    <div className="mt-2 text-sm font-semibold">Late signals & guessing</div>
                    <div className="mt-2 text-sm text-white/70">
                      Standardized rules so you can scale your process.
                    </div>
                  </div>
                </div>
              </div>

              <div className="mt-6 flex items-center justify-between rounded-2xl border border-white/10 bg-black/30 p-4">
                <div>
                  <div className="text-xs text-white/60">Next step</div>
                  <div className="mt-1 text-sm font-semibold">Build your watchlist + alerts</div>
                </div>
                <a
                  href="https://app.xautrendlab.com"
                  target="_blank"
                  rel="noreferrer"
                  className="rounded-xl bg-white px-4 py-2 text-sm font-semibold text-black hover:opacity-95"
                >
                  Launch
                </a>
              </div>
            </div>
          </div>
        </div>
      </section>

      {/* FEATURES */}
      <section className="mx-auto max-w-6xl px-5 py-16 md:py-20">
        <div className="flex items-end justify-between gap-6">
          <div>
            <div className="text-xs text-gold2/90">FEATURES</div>
            <h2 className="mt-2 text-2xl font-semibold tracking-tight md:text-3xl">
              Premium tools for real intraday trading
            </h2>
            <p className="mt-2 max-w-2xl text-sm text-white/70">
              Built to support a repeatable process: identify trend, time entries, manage risk, and review outcomes.
            </p>
          </div>
          <Link
            href="/features"
            className="hidden rounded-2xl border border-white/12 bg-white/5 px-4 py-2 text-sm font-semibold text-white/90 hover:border-white/20 hover:bg-white/7 md:inline-flex"
          >
            Explore all
          </Link>
        </div>

        <div className="mt-8 grid gap-4 md:grid-cols-3">
          <Feature title="Trend signal clarity" desc="Directional bias + structured reasons, designed for action, not theory." />
          <Feature title="Entry logic framework" desc="Pullback + confirmation style entries with clean invalidation." />
          <Feature title="Risk rails by default" desc="TP/SL + trailing options so execution stays consistent." />
          <Feature title="MT5-ready pipeline" desc="Agent ? snapshot ? web UI, aligned with real trading constraints." />
          <Feature title="History & review" desc="Keep a record of opportunities and outcomes to refine over time." />
          <Feature title="Built for expansion" desc="Static marketing + dynamic app + later alerts/community." />
        </div>
      </section>

      {/* CTA */}
      <section className="mx-auto max-w-6xl px-5 pb-20">
        <div className="rounded-[28px] border border-white/10 bg-[linear-gradient(135deg,rgba(212,175,55,0.16),rgba(255,255,255,0.04))] p-8 shadow-glow md:p-10">
          <div className="grid items-center gap-8 md:grid-cols-2">
            <div>
              <h3 className="text-2xl font-semibold tracking-tight">Ready to see it in action?</h3>
              <p className="mt-2 text-sm text-white/70">
                Open the web app and explore the live workflow. Next we’ll add: pricing page, FAQ, and a Discord join CTA.
              </p>
            </div>
            <div className="flex flex-col gap-3 sm:flex-row sm:justify-end">
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
          </div>
        </div>
      </section>
    </SiteLayout>
  );
}
