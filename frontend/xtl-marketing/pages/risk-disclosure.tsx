import SiteLayout from "../components/SiteLayout";

export default function RiskDisclosurePage() {
  return (
    <SiteLayout title="XauTrendLab — Risk Disclosure">
      <section className="mx-auto max-w-6xl px-6 py-14 md:py-20">
        <div className="max-w-3xl">
          <div className="text-xs font-semibold tracking-wide text-white/55">
            RISK DISCLOSURE
          </div>

          <h1 className="mt-2 text-3xl font-semibold tracking-tight md:text-4xl">
            Important risk and responsibility notice
          </h1>

          <p className="mt-4 text-base leading-relaxed text-white/70">
            Trading financial instruments involves risk and may result in losses.
            XauTrendLab is designed to provide structured information and tools,
            not to eliminate risk or guarantee outcomes.
          </p>

          {/* Section */}
          <div className="mt-10 space-y-8">
            <div>
              <h2 className="text-lg font-semibold text-white">
                No investment advice
              </h2>
              <p className="mt-2 text-sm leading-relaxed text-white/70">
                XauTrendLab does not provide investment, legal, or financial
                advice. All information presented by the platform is for
                informational and analytical purposes only. Users are solely
                responsible for their trading decisions.
              </p>
            </div>

            <div>
              <h2 className="text-lg font-semibold text-white">
                No performance guarantees
              </h2>
              <p className="mt-2 text-sm leading-relaxed text-white/70">
                Past outcomes, signal histories, or illustrative examples do not
                guarantee future results. Market conditions vary, and losses may
                exceed expectations, including in structured or automated
                strategies.
              </p>
            </div>

            <div>
              <h2 className="text-lg font-semibold text-white">
                Defined risk does not eliminate loss
              </h2>
              <p className="mt-2 text-sm leading-relaxed text-white/70">
                While XauTrendLab provides stop-loss and target levels for
                actionable opportunities, these levels do not ensure execution
                at the specified prices. Slippage, liquidity constraints, and
                market gaps may result in losses beyond defined levels.
              </p>
            </div>

            <div>
              <h2 className="text-lg font-semibold text-white">
                User-controlled execution
              </h2>
              <p className="mt-2 text-sm leading-relaxed text-white/70">
                XauTrendLab does not custody funds and does not place trades
                without explicit user authorization. Execution occurs locally
                under user-defined rules and broker conditions.
              </p>
            </div>

            <div>
              <h2 className="text-lg font-semibold text-white">
                Automation carries additional risk
              </h2>
              <p className="mt-2 text-sm leading-relaxed text-white/70">
                Automated or semi-automated trading strategies may amplify
                losses during periods of volatility, technical failure, or
                unexpected market behavior. Users should monitor automated
                systems and use conservative risk settings.
              </p>
            </div>

            <div>
              <h2 className="text-lg font-semibold text-white">
                Suitability and experience
              </h2>
              <p className="mt-2 text-sm leading-relaxed text-white/70">
                XauTrendLab is intended for users who understand trading risks
                and are capable of making independent decisions. The platform
                may not be suitable for all users, particularly those with low
                risk tolerance.
              </p>
            </div>
          </div>

          {/* Footer note */}
          <div className="mt-12 rounded-2xl border border-white/10 bg-white/[0.03] p-6 text-xs leading-relaxed text-white/55">
            By using XauTrendLab, you acknowledge that you have read and
            understood this risk disclosure and accept full responsibility for
            your trading decisions and outcomes.
          </div>
        </div>
      </section>
    </SiteLayout>
  );
}
