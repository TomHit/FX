import Link from "next/link";
import { useEffect, useRef, useState } from "react";

const APP_ORIGIN = "https://app.xautrendlab.com";

type MenuKey = "product" | null;

export default function SiteHeader() {
  const [open, setOpen] = useState<MenuKey>(null);
  const ref = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    function onDocClick(e: MouseEvent) {
      if (!ref.current) return;
      if (!ref.current.contains(e.target as Node)) setOpen(null);
    }
    document.addEventListener("mousedown", onDocClick);
    return () => document.removeEventListener("mousedown", onDocClick);
  }, []);

  const itemBtn =
    "inline-flex items-center gap-1 rounded-lg px-3 py-2 text-sm font-medium text-white/80 hover:text-white hover:bg-white/5";
  const linkCls =
    "block rounded-lg px-3 py-2 text-sm text-white/80 hover:text-white hover:bg-white/5";
  const panel =
    "absolute left-0 top-full mt-2 w-64 rounded-2xl border border-white/10 bg-[#0b0b0e]/95 p-2 shadow-xl backdrop-blur";

  return (
    <header className="sticky top-0 z-50 border-b border-white/10 bg-[#0b0b0e]/85 backdrop-blur">
      <div className="mx-auto flex max-w-6xl items-center justify-between px-6 py-4" ref={ref}>
        {/* Brand */}
        <Link href="/" className="flex items-center">
          <img
            src="/images/xtl-logo.png"
            alt="XTL"
            className="h-10 w-auto"
          />      
            
          
        </Link>

        {/* Nav */}
        <nav className="hidden items-center gap-2 md:flex">
          {/* Product dropdown */}
          <div className="relative">
            <button
              className={itemBtn}
              onClick={() => setOpen(open === "product" ? null : "product")}
              aria-haspopup="menu"
              aria-expanded={open === "product"}
            >
              Product
              <span className="text-white/60">▾</span>
            </button>
            {open === "product" && (
              <div className={panel} role="menu" aria-label="Product menu">
                <Link className={linkCls} href="/product/overview" onClick={() => setOpen(null)}>
                  Overview
                </Link>
                <Link className={linkCls} href="/product/architecture" onClick={() => setOpen(null)}>
                  Platform Architecture
                </Link>
                <Link className={linkCls} href="/product/security" onClick={() => setOpen(null)}>
                  Security & Execution
                </Link>
              </div>
            )}
          </div>

          <Link className={itemBtn} href="/features">
            Features
          </Link>


          <Link className={itemBtn} href="/pricing">
            Pricing
          </Link>

          {/* Docs can be internal later; for now keep external placeholder */}
          <a className={itemBtn} href="#" onClick={(e) => e.preventDefault()}>
            Docs
          </a>

          <a
            
            href="/login?next=preview"
            className="rounded-xl bg-white px-5 py-2.5 text-sm font-semibold text-black hover:bg-white/90"
          >
            Live Preview
          </a>
        </nav>

        {/* Mobile (simple) */}
        <div className="flex items-center gap-2 md:hidden">
          <a
            className="inline-flex items-center rounded-xl bg-white px-4 py-2 text-sm font-semibold text-black hover:bg-white/90"
            href="/product/overview"
            target="_blank"
            rel="noreferrer"
          >
            Live Preview
          </a>
        </div>
      </div>
    </header>
  );
}
