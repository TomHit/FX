import SiteLayout from "../components/SiteLayout";

const FAQ = [
  {
    q: "What is XauTrendLab?",
    a: "XauTrendLab is an AI-powered intraday trend intelligence platform focused on high-signal setups, risk controls, and execution guidance.",
  },
  {
    q: "Is this a trading bot?",
    a: "Not by default. The web app provides signals, targets, and trade management. Automated execution can be added later (broker-dependent).",
  },
  {
    q: "Which markets are supported?",
    a: "We focus on XAUUSD first. More FX symbols and timeframes are supported in the app and can expand over time.",
  },
  {
    q: "Do you guarantee profits?",
    a: "No. Markets are risky. This is a decision-support tool; always use proper risk management.",
  },
  {
    q: "How do I access the app?",
    a: "Use the web app link and login. Onboarding helps connect your device/MT5 worker if needed.",
  },
];

export default function FaqPage() {
  return (
    <SiteLayout title="XauTrendLab — FAQ">
      <section className="mx-auto max-w-6xl px-5 py-14 md:py-20">
        <div className="max-w-3xl">
          <div className="text-xs text-gold2/90">FAQ</div>
          <h1 className="mt-2 text-3xl font-semibold tracking-tight md:text-4xl">
            Frequently asked questions
          </h1>
          <p className="mt-3 text-sm text-white/70">
            Quick answers about XauTrendLab. If you have more questions, contact us.
          </p>

          <div className="mt-10 space-y-4">
            {FAQ.map((item) => (
              <div
                key={item.q}
                className="rounded-3xl border border-white/10 bg-white/[0.03] p-6"
              >
                <div className="text-base font-semibold text-white">{item.q}</div>
                <div className="mt-2 text-sm text-white/70">{item.a}</div>
              </div>
            ))}
          </div>

          <div className="mt-10 flex flex-wrap gap-3">
            <a
              href="https://app.xautrendlab.com"
              target="_blank"
              rel="noreferrer"
              className="inline-flex items-center justify-center rounded-2xl bg-white px-4 py-2 text-sm font-semibold text-black hover:opacity-95"
            >
              Open Web App
            </a>
            <a
              href="/contact"
              className="inline-flex items-center justify-center rounded-2xl border border-white/10 bg-white/5 px-4 py-2 text-sm font-semibold text-white/90 hover:bg-white/10"
            >
              Contact
            </a>
          </div>
        </div>
      </section>
    </SiteLayout>
  );
}
