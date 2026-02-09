	import SiteLayout from "../components/SiteLayout";

const FAQ = [
  {
    q: "What is XauTrendLab?",
    a: `XauTrendLab is a trading intelligence platform designed to support
    disciplined decision-making. It provides structured opportunities,
    confirmation-based entries, defined risk levels, and deterministic
    outcome tracking. The platform is built to reduce ambiguity rather than
    to promise performance.`,
  },
  {
    q: "Is XauTrendLab a signal service?",
    a: `No. XauTrendLab is not a broadcast-style signal service.
    Opportunities appear first as setups and become actionable only after
    confirmation. This distinction is intentional and designed to avoid
    impulsive or hindsight-driven trading.`,
  },
  {
    q: "How are BUY and SELL entries generated?",
    a: `Entries are generated only after predefined confirmation conditions
    are met. The system separates the identification of an opportunity from
    the moment an entry becomes actionable. This helps avoid chasing extended
    price moves.`,
  },
  {
    q: "When are stop-loss and target levels set?",
    a: `Stop-loss and target levels are attached at the point of confirmed
    entry and are then frozen. They do not move during the lifecycle of an
    active opportunity. This ensures consistent evaluation and transparent
    outcome tracking.`,
  },
  {
    q: "Do stop-loss or targets ever change after entry?",
    a: `No. Once an opportunity becomes active, its stop-loss and target
    levels remain fixed until the opportunity resolves as HIT, SL_HIT, or
    EXPIRED.`,
  },
  {
    q: "What does HIT, SL_HIT, and EXPIRED mean?",
    a: `HIT indicates the target level was reached. SL_HIT indicates the
    stop-loss level was reached. EXPIRED indicates the opportunity was closed
    due to time or rule-based conditions without hitting either level.`,
  },
  {
    q: "Does XauTrendLab guarantee profits?",
    a: `No. XauTrendLab does not make performance guarantees.
    Trading involves risk, and losses are possible. The platform is designed
    to provide structure and clarity, not certainty.`,
  },
  {
    q: "Is this automated trading?",
    a: `XauTrendLab supports multiple workflows. Some users act manually on
    signals, while others use rule-based or automated execution with local
    agents. Automation is optional and always user-controlled.`,
  },
  {
    q: "Does XauTrendLab place trades or hold funds?",
    a: `No. XauTrendLab does not custody funds and does not place trades
    without explicit user authorization. Execution occurs locally under
    user-defined rules.`,
  },
  {
    q: "Why is access controlled?",
    a: `Access is intentionally gated to ensure system stability,
    responsible usage, and proper understanding of risk—especially for
    automated or semi-automated workflows.`,
  },
  {
    q: "How do alerts work?",
    a: `Alerts are event-driven. Users are notified when an entry is
    confirmed and when an opportunity exits. This reduces noise compared to
    constant price or signal updates.`,
  },
  {
    q: "Who is XauTrendLab designed for?",
    a: `XauTrendLab is designed for traders who value structure, defined
    risk, and repeatable processes. It is not intended for users seeking
    guaranteed returns or high-frequency scalping tools.`,
  },
];

export default function FaqPage() {
  return (
    <SiteLayout title="XauTrendLab — FAQ">
      <section className="mx-auto max-w-6xl px-6 py-14 md:py-20">
        <div className="max-w-3xl">
          <div className="text-xs font-semibold tracking-wide text-white/55">
            FAQ
          </div>
          <h1 className="mt-2 text-3xl font-semibold tracking-tight md:text-4xl">
            Frequently asked questions
          </h1>
          <p className="mt-4 text-base text-white/70">
            Answers to common questions about methodology, access, and risk.
          </p>

          <div className="mt-10 space-y-4">
            {FAQ.map((item) => (
              <div
                key={item.q}
                className="rounded-3xl border border-white/10 bg-white/[0.03] p-6"
              >
                <div className="text-base font-semibold text-white">
                  {item.q}
                </div>
                <div className="mt-2 text-sm leading-relaxed text-white/70">
                  {item.a}
                </div>
              </div>
            ))}
          </div>

          <div className="mt-10 rounded-2xl border border-white/10 bg-white/[0.02] p-6 text-xs text-white/55">
            Trading involves risk. Past outcomes do not indicate future
            performance. Use defined risk and appropriate position sizing.
          </div>
        </div>
      </section>
    </SiteLayout>
  );
}
