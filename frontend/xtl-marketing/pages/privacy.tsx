import SiteLayout from "../components/SiteLayout";

export default function PrivacyPage() {
  return (
    <SiteLayout title="Privacy Policy">
      <section className="mx-auto max-w-6xl px-6 py-14 md:py-20">
        <div className="max-w-3xl text-white/70 text-sm leading-relaxed space-y-4">
          <p>
            XauTrendLab respects user privacy and is designed to minimize
            unnecessary data collection.
          </p>

          <p>
            The Platform may collect basic technical information required
            for authentication, access control, and operational monitoring.
          </p>

          <p>
            Trading data, strategies, and execution details remain under
            user control. XauTrendLab does not custody funds or broker accounts.
          </p>

          <p>
            User data is not sold to third parties. Limited sharing may
            occur only where required for platform functionality or
            legal compliance.
          </p>

          <p className="text-xs text-white/50">
            This policy may be updated periodically to reflect operational
            or regulatory changes.
          </p>
        </div>
      </section>
    </SiteLayout>
  );
}
