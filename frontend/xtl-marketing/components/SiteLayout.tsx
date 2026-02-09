import SiteHeader from "./SiteHeader";
import SiteFooter from "./SiteFooter";

export default function SiteLayout({
  children,
  title,
  subtitle,
}: {
  children: React.ReactNode;
  title?: string;
  subtitle?: string;
}) {
  return (
    <div className="min-h-screen bg-[#0b0b0e] text-white">
      <SiteHeader />

      {(title || subtitle) && (
        <section className="mx-auto max-w-6xl px-6 pt-14 pb-10">
          {title && <h1 className="text-3xl font-semibold tracking-tight md:text-4xl">{title}</h1>}
          {subtitle && <p className="mt-3 max-w-3xl text-white/70">{subtitle}</p>}
        </section>
      )}

      <main>{children}</main>

      
      <SiteFooter />

    </div>
  );
}
