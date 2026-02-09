import SiteLayout from "../components/SiteLayout";
import Link from "next/link";

export default function AboutPage() {
  return (
    <SiteLayout title="XauTrendLab — About">
      <section className="mx-auto max-w-6xl px-5 py-14 md:py-20">
        <div className="max-w-2xl">
          <div className="text-xs text-gold2/90">ABOUT</div>
          <h1 className="mt-2 text-3xl font-semibold tracking-tight md:text-4xl">
            Built for real intraday execution
          </h1>
          <p className="mt-3 text-sm text-white/70">
            XauTrendLab is a premium workflow for MT5 traders: trend direction, entry timing, and risk rails —
            designed to keep decisions consistent and repeatable.
          </p>

          <div className="mt-8 flex flex-col gap-3 sm:flex-row">
            <a
              href="https://app.xautrendlab.com"
              target="_blank"
              rel="noreferrer"
              className="inline-flex items-center justify-center rounded-2xl bg-white px-5 py-3 text-sm font-semibold text-black hover:opacity-95"
            >
              Open Web App
            </a>

            <Link
              href="/features"
              className="inline-flex items-center justify-center rounded-2xl border border-white/12 bg-white/5 px-5 py-3 text-sm font-semibold text-white/90 hover:border-white/20 hover:bg-white/7"
            >
              See Features
            </Link>
          </div>
        </div>
      </section>
    </SiteLayout>
  );
}
