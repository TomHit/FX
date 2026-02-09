import Head from "next/head";
import Link from "next/link";
import { ReactNode } from "react";

export default function SiteLayout({
  title = "XauTrendLab — AI-Powered Intraday Trend Intelligence",
  description = "Premium intraday trend detection for MT5 traders. Clear direction, entry timing, and risk rails.",
  children,
}: {
  title?: string;
  description?: string;
  children: ReactNode;
}) {
  return (
    <div className="min-h-screen bg-[radial-gradient(1200px_600px_at_70%_0%,rgba(212,175,55,0.12),transparent_60%),radial-gradient(900px_500px_at_20%_20%,rgba(116,90,255,0.10),transparent_55%)] bg-bg">
      <Head>
        <title>{title}</title>
        <meta name="description" content={description} />
      </Head>

      <header className="sticky top-0 z-50 border-b border-white/10 bg-bg/70 backdrop-blur">
        <div className="mx-auto flex max-w-6xl items-center justify-between px-5 py-4">
          <Link href="/" className="flex items-center gap-3">
            <div className="h-8 w-8 rounded-xl border border-white/10 bg-[linear-gradient(135deg,rgba(212,175,55,0.18),rgba(255,255,255,0.04))] shadow-soft" />
            <div className="leading-tight">
              <div className="text-sm font-semibold tracking-tight">XauTrendLab</div>
              <div className="text-xs text-white/60">AI intraday signals</div>
            </div>
          </Link>

          <nav className="hidden items-center gap-6 text-sm text-white/70 md:flex">
            <Link className="hover:text-white" href="/features">
              Features
            </Link>
            <Link className="hover:text-white" href="/pricing">
              Pricing
            </Link>
            <a className="hover:text-white" href="https://app.xautrendlab.com" target="_blank" rel="noreferrer">
              Web App
            </a>
          </nav>

          <div className="flex items-center gap-2">
            <a
              href="https://app.xautrendlab.com"
              target="_blank"
              rel="noreferrer"
              className="hidden rounded-xl border border-white/10 px-3 py-2 text-sm text-white/80 hover:border-white/20 hover:bg-white/5 md:inline-flex"
            >
              Login
            </a>
            <a
              href="https://app.xautrendlab.com"
              target="_blank"
              rel="noreferrer"
              className="inline-flex items-center justify-center rounded-xl bg-[linear-gradient(135deg,rgba(212,175,55,0.95),rgba(245,215,122,0.92))] px-3 py-2 text-sm font-semibold text-black shadow-glow hover:opacity-95"
            >
              Open Web App
            </a>
          </div>
        </div>
      </header>

      <main>{children}</main>

      <footer className="border-t border-white/10">
        <div className="mx-auto max-w-6xl px-5 py-10">
          <div className="flex flex-col justify-between gap-6 md:flex-row md:items-center">
            <div>
              <div className="text-sm font-semibold">XauTrendLab</div>
              <div className="mt-1 text-xs text-white/60">
                Premium intraday trend intelligence — built for MT5 traders.
              </div>
            </div>
            <div className="flex flex-wrap items-center gap-4 text-sm text-white/70">
              <Link className="hover:text-white" href="/features">Features</Link>
              <Link className="hover:text-white" href="/pricing">Pricing</Link>
              <a className="hover:text-white" href="https://app.xautrendlab.com" target="_blank" rel="noreferrer">
                App
              </a>
            </div>
          </div>

          <div className="mt-8 text-xs text-white/50">
            © {new Date().getFullYear()} XauTrendLab. All rights reserved.
          </div>
        </div>
      </footer>
    </div>
  );
}
