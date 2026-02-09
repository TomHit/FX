import SiteLayout from "../components/SiteLayout";

export default function ContactPage() {
  return (
    <SiteLayout title="XauTrendLab — Contact">
      <section className="mx-auto max-w-6xl px-5 py-14 md:py-20">
        <div className="max-w-2xl">
          <div className="text-xs text-gold2/90">CONTACT</div>
          <h1 className="mt-2 text-3xl font-semibold tracking-tight md:text-4xl">
            Talk to us
          </h1>
          <p className="mt-3 text-sm text-white/70">
            For onboarding, pricing, or partnership queries, reach out and we’ll respond quickly.
          </p>

          <div className="mt-8 rounded-3xl border border-white/10 bg-white/[0.03] p-6">
            <div className="text-sm text-white/80">Email</div>
            <a
              href="mailto:support@xautrendlab.com"
              className="mt-1 inline-block text-sm font-semibold text-white underline decoration-white/30 underline-offset-4 hover:decoration-white/60"
            >
              support@xautrendlab.com
            </a>

            <div className="mt-6 text-sm text-white/80">App</div>
            <a
              href="https://app.xautrendlab.com"
              target="_blank"
              rel="noreferrer"
              className="mt-1 inline-flex items-center justify-center rounded-2xl bg-white px-4 py-2 text-sm font-semibold text-black hover:opacity-95"
            >
              Open Web App
            </a>

            <div className="mt-6 text-xs text-white/50">
              (You can replace this with a form later. Static pages are best for marketing + SEO.)
            </div>
          </div>
        </div>
      </section>
    </SiteLayout>
  );
}
