import SiteLayout from "../components/SiteLayout";

export default function AboutPage() {
  return (
    <SiteLayout title="XauTrendLab — About">
      <section className="mx-auto max-w-6xl px-5 py-14 md:py-20">
        <div className="max-w-3xl">
          <div className="text-xs text-gold2/90">ABOUT</div>
          <h1 className="mt-2 text-3xl font-semibold tracking-tight md:text-4xl">
            Built for serious intraday decision-making
          </h1>
          <p className="mt-4 text-sm text-white/70 md:text-base">
            XauTrendLab focuses on clarity: signal, reason, levels, and execution guidance. No noise.
          </p>
        </div>
      </section>
    </SiteLayout>
  );
}
