import Head from "next/head";
import Link from "next/link";
import { useState } from "react";
import SiteLayout from "../components/SiteLayout";
import RequestAccessModal from "../components/RequestAccessModal";

export default function PricingPage() {
  const [openPlan, setOpenPlan] = useState<null | "Signals" | "MyBots" | "Automation">(null);

  return (
    <>
      <Head>
        <title>Access & Plans — XauTrendLab</title>
        <meta
          name="description"
          content="XauTrendLab access model and plans. Controlled onboarding for Signals, MyBots, and Automation."
        />
      </Head>

      <SiteLayout
        title="Access & Plans"
        subtitle="XauTrendLab uses a controlled access model to ensure stability, transparency, and responsible usage."
      >
        <RequestAccessModal
          open={openPlan !== null}
          planLabel={(openPlan || "Signals") as "Signals" | "MyBots" | "Automation"}
          onClose={() => setOpenPlan(null)}
        />

        <section className="mx-auto max-w-6xl px-6 pb-16">
          {/* Intro */}
          <div className="rounded-3xl border border-white/10 bg-white/5 p-8 md:p-10">
            <div className="max-w-3xl">
              <h2 className="text-xl font-semibold md:text-2xl">A controlled access model</h2>
              <p className="mt-3 text-sm leading-relaxed text-white/70">
                XauTrendLab is not an open sign-up platform. Access is provided in stages to maintain system
                integrity, execution safety, and a consistent experience for all users.
              </p>
            </div>

            {/* Optional: screenshot block (add image later) */}
            <div className="mt-10 overflow-hidden rounded-3xl border border-white/10 bg-white/[0.02]">
              <div className="px-6 py-4 text-xs font-semibold tracking-wide text-white/55">
                PLATFORM SNAPSHOT
              </div>
              <img
                src="/images/access-snapshot.png"
                alt="XauTrendLab preview"
                className="h-auto w-full"
              />
            </div>

            {/* Access tiers */}
            <div className="mt-10 grid gap-6 md:grid-cols-3">
              {/* Signals */}
              <div className="rounded-3xl border border-white/10 bg-white/5 p-7">
                <div className="text-sm font-semibold text-white">Signals access</div>
                <p className="mt-3 text-sm text-white/70">
                  For discretionary traders who want structured trade ideas, confirmation-based entries, and defined
                  risk context.
                </p>
                <ul className="mt-5 space-y-2 text-sm text-white/70">
                  <li>• Opportunity feed with direction & context</li>
                  <li>• Confirmed BUY / SELL entries</li>
                  <li>• Stop-loss & target visibility</li>
                  <li>• Discord alerts for entry & outcome</li>
                </ul>
                <div className="mt-6 flex flex-col gap-2">
                  <button
                    onClick={() => setOpenPlan("Signals")}
                    className="rounded-xl bg-white px-5 py-3 text-center text-sm font-semibold text-black hover:bg-white/90"
                  >
                    Request access
                  </button>
                  <Link
                    href="/product/overview"
                    className="rounded-xl border border-white/15 px-5 py-3 text-center text-sm font-semibold text-white/90 hover:bg-white/5"
                  >
                    Product overview
                  </Link>
                </div>
              </div>

              {/* MyBots */}
              <div className="rounded-3xl border border-white/15 bg-white/5 p-7 ring-1 ring-white/10">
                <div className="flex items-center justify-between">
                  <div className="text-sm font-semibold text-white">MyBots access</div>
                  <span className="rounded-full border border-white/15 bg-white/5 px-3 py-1 text-xs text-white/80">
                    Most requested
                  </span>
                </div>
                <p className="mt-3 text-sm text-white/70">
                  For users who want rule-based execution with full control over strategy parameters and risk behavior.
                </p>
                <ul className="mt-5 space-y-2 text-sm text-white/70">
                  <li>• Strategy configuration & parameters</li>
                  <li>• Local installer / agent execution</li>
                  <li>• Repeatable, rules-first behavior</li>
                  <li>• Pause / resume anytime</li>
                </ul>
                <div className="mt-6 flex flex-col gap-2">
                  <button
                    onClick={() => setOpenPlan("MyBots")}
                    className="rounded-xl bg-white px-5 py-3 text-center text-sm font-semibold text-black hover:bg-white/90"
                  >
                    Request access
                  </button>
                  <Link
                    href="/product/architecture"
                    className="rounded-xl border border-white/15 px-5 py-3 text-center text-sm font-semibold text-white/90 hover:bg-white/5"
                  >
                    Architecture & execution
                  </Link>
                </div>
              </div>

              {/* Automation */}
              <div className="rounded-3xl border border-white/10 bg-white/5 p-7">
                <div className="text-sm font-semibold text-white">Automation access</div>
                <p className="mt-3 text-sm text-white/70">
                  For experienced users who are ready for hands-free execution with predefined guardrails and monitoring.
                </p>
                <ul className="mt-5 space-y-2 text-sm text-white/70">
                  <li>• End-to-end execution & monitoring</li>
                  <li>• SL / TP & exit management</li>
                  <li>• Designed for scaling strategies</li>
                </ul>
                <div className="mt-6 flex flex-col gap-2">
                  <button
                    onClick={() => setOpenPlan("Automation")}
                    className="rounded-xl bg-white px-5 py-3 text-center text-sm font-semibold text-black hover:bg-white/90"
                  >
                    Request access
                  </button>
                  <Link
                    href="/product/security"
                    className="rounded-xl border border-white/15 px-5 py-3 text-center text-sm font-semibold text-white/90 hover:bg-white/5"
                  >
                    Security & guardrails
                  </Link>
                </div>
              </div>
            </div>

            {/* Access model explanation */}
            <div className="mt-10 rounded-2xl border border-white/10 bg-white/5 p-6 text-sm text-white/70">
              <div className="font-semibold text-white/90">Why access codes?</div>
              <p className="mt-2 leading-relaxed">
                XauTrendLab uses a private access model to prevent misuse, control system load, and ensure users
                understand the risks of automated and semi-automated trading. After approval, you receive a private
                access code to log in to the app.
              </p>
            </div>

            {/* Existing users */}
            <div className="mt-8 flex flex-col items-start gap-3 sm:flex-row sm:items-center sm:justify-between">
              <p className="text-sm text-white/60">Already have an access code?</p>
              <Link
                href="/login?next=preview"
                className="rounded-xl border border-white/15 px-6 py-3 text-center text-sm font-semibold text-white/90 hover:bg-white/5"
              >
                Enter access code →
              </Link>
            </div>
          </div>
        </section>
      </SiteLayout>
    </>
  );
}
