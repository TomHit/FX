import Link from "next/link";

export default function SiteFooter() {
  const year = new Date().getFullYear();

  return (
    <footer className="border-t border-white/10 bg-black/20">
      <div className="mx-auto max-w-6xl px-6 py-10">
        <div className="grid gap-8 md:grid-cols-4">
          {/* Brand */}
          <div className="md:col-span-1">
            <div className="text-sm font-semibold text-white">XauTrendLab</div>
            <div className="mt-2 text-sm leading-relaxed text-white/60">
              Trading intelligence with confirmation-based entries and
              deterministic lifecycle tracking.
            </div>
          </div>

          {/* Product */}
          <div>
            <div className="text-xs font-semibold tracking-wide text-white/55">
              PRODUCT
            </div>
            <ul className="mt-3 space-y-2 text-sm text-white/70">
              <li>
                <Link href="/product/overview" className="hover:text-white">
                  Overview
                </Link>
              </li>
              <li>
                <Link href="/product/architecture" className="hover:text-white">
                  Architecture
                </Link>
              </li>
              <li>
                <Link href="/product/security" className="hover:text-white">
                  Security & Execution
                </Link>
              </li>
              <li>
                <Link href="/features" className="hover:text-white">
                  Features
                </Link>
              </li>
            </ul>
          </div>

          {/* Access */}
          <div>
            <div className="text-xs font-semibold tracking-wide text-white/55">
              ACCESS
            </div>
            <ul className="mt-3 space-y-2 text-sm text-white/70">
              <li>
                <Link href="/pricing" className="hover:text-white">
                  Access & Plans
                </Link>
              </li>
              <li>
                <Link href="/access" className="hover:text-white">
                  Enter access code
                </Link>
              </li>
              <li>
                <Link href="/discord" className="hover:text-white">
                  Discord alerts
                </Link>
              </li>
              <li>
                <Link href="/faq" className="hover:text-white">
                  FAQ
                </Link>
              </li>
            </ul>
          </div>

          {/* Compliance */}
          <div>
            <div className="text-xs font-semibold tracking-wide text-white/55">
              COMPLIANCE
            </div>
            <ul className="mt-3 space-y-2 text-sm text-white/70">
              <li>
                <Link href="/risk-disclosure" className="hover:text-white">
                  Risk Disclosure
                </Link>
              </li>
              <li>
                <Link href="/terms" className="hover:text-white">
                  Terms of Service
                </Link>
              </li>
              <li>
                <Link href="/privacy" className="hover:text-white">
                  Privacy Policy
                </Link>
              </li>
              <li>
                <Link href="/cookies" className="hover:text-white">
                  Cookies
                </Link>
              </li>
            </ul>
          </div>
        </div>

        {/* Bottom bar */}
        <div className="mt-10 flex flex-col gap-3 border-t border-white/10 pt-6 md:flex-row md:items-center md:justify-between">
          <div className="text-xs text-white/55">
            © {year} XauTrendLab. All rights reserved.
          </div>
          <div className="text-xs text-white/50">
            Trading involves risk. No performance guarantees.
          </div>
        </div>
      </div>
    </footer>
  );
}
