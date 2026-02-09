import SiteLayout from "../components/SiteLayout";

export default function TermsPage() {
  return (
    <SiteLayout title="Terms of Service">
      <section className="mx-auto max-w-6xl px-6 py-14 md:py-20">
        <div className="max-w-3xl text-white/70 text-sm leading-relaxed space-y-4">
          <p>
            These Terms of Service govern access to and use of XauTrendLab
            (“the Platform”). By accessing or using the Platform, you agree
            to these terms.
          </p>

          <p>
            XauTrendLab provides informational tools for trading analysis.
            It does not provide investment advice, brokerage services,
            or discretionary trade execution.
          </p>

          <p>
            Users are solely responsible for their trading decisions,
            execution, and risk management. Trading involves risk and
            may result in losses.
          </p>

          <p>
            Access to the Platform may be modified, suspended, or revoked
            at any time to ensure system integrity or compliance.
          </p>

          <p className="text-xs text-white/50">
            These terms may be updated periodically. Continued use of the
            Platform constitutes acceptance of the latest version.
          </p>
        </div>
      </section>
    </SiteLayout>
  );
}
