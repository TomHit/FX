import SiteLayout from "../components/SiteLayout";

export default function CookiesPage() {
  return (
    <SiteLayout title="Cookies">
      <section className="mx-auto max-w-6xl px-6 py-14 md:py-20">
        <div className="max-w-3xl text-white/70 text-sm leading-relaxed space-y-4">
          <p>
            XauTrendLab uses limited cookies and local storage mechanisms
            to support authentication, session continuity, and basic
            analytics.
          </p>

          <p>
            Cookies are not used for advertising or third-party tracking.
            Disabling cookies may impact certain platform features.
          </p>

          <p className="text-xs text-white/50">
            By continuing to use the Platform, you consent to the use of
            cookies as described above.
          </p>
        </div>
      </section>
    </SiteLayout>
  );
}
