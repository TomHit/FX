import SiteLayout from "../components/SiteLayout";

export default function ContactPage() {
  return (
    <SiteLayout title="XauTrendLab — Contact">
      <section className="mx-auto max-w-6xl px-5 py-14 md:py-20">
        <div className="max-w-3xl">
          <div className="text-xs text-gold2/90">CONTACT</div>
          <h1 className="mt-2 text-3xl font-semibold tracking-tight md:text-4xl">
            Get in touch
          </h1>
          <p className="mt-4 text-sm text-white/70 md:text-base">
            For onboarding help or early access, contact us.
          </p>

          <div className="mt-8 rounded-3xl border border-white/10 bg-white/[0.03] p-6">
            <div className="text-sm text-white/80">Email</div>
            <div className="mt-1 text-base font-semibold text-white">
              support@xautrendlab.com
            </div>
          </div>
        </div>
      </section>
    </SiteLayout>
  );
}
