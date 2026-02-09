import SiteLayout from "../components/SiteLayout";

function PriceCard({
  name,
  price,
  desc,
  items,
  highlight,
}: {
  name: string;
  price: string;
  desc: string;
  items: string[];
  highlight?: boolean;
}) {
  return (
    <div
      className={[
        "rounded-3xl border p-7 shadow-soft",
        highlight
          ? "border-[rgba(212,175,55,0.35)] bg-[linear-gradient(135deg,rgba(212,175,55,0.12),rgba(255,255,255,0.03))] shadow-glow"
          : "border-white/10 bg-white/5",
      ].join(" ")}
    >
      <div className="flex items-center justify-between">
        <div className="text-sm font-semibold">{name}</div>
        {highlight ? (
          <div className="rounded-full bg-[rgba(212,175,55,0.18)] px-3 py-1 text-xs font-semibold text-gold2">
            Recommended
          </div>
        ) : null}
      </div>
      <div className="mt-3 text-3xl font-semibold tracking-tight">{price}</div>
      <div className="mt-2 text-sm text-white/70">{desc}</div>
      <ul className="mt-5 space-y-2 text-sm text-white/75">
        {items.map((x) => (
          <li key={x} className="flex gap-2">
            <span className="mt-1 h-1.5 w-1.5 rounded-full bg-gold" />
            <span>{x}</span>
          </li>
        ))}
      </ul>
      <a
        href="https://app.xautrendlab.com"
        target="_blank"
        rel="noreferrer"
        className={[
          "mt-6 inline-flex w-full items-center justify-center rounded-2xl px-5 py-3 text-sm font-semibold",
          highlight
            ? "bg-[linear-gradient(135deg,rgba(212,175,55,0.95),rgba(245,215,122,0.92))] text-black hover:opacity-95"
            : "border border-white/12 bg-white/5 text-white/90 hover:border-white/20 hover:bg-white/7",
        ].join(" ")}
      >
        Get started
      </a>
    </div>
  );
}

export default function PricingPage() {
  return (
    <SiteLayout title="XauTrendLab — Pricing">
      <section className="mx-auto max-w-6xl px-5 py-14 md:py-20">
        <div className="max-w-2xl">
          <div className="text-xs text-gold2/90">PRICING</div>
          <h1 className="mt-2 text-3xl font-semibold tracking-tight md:text-4xl">
            Simple pricing for a premium trading workflow
          </h1>
          <p className="mt-3 text-sm text-white/70">
            Start with the web app. Upgrade when you want more automation and alerts.
          </p>
        </div>

        <div className="mt-10 grid gap-5 md:grid-cols-3">
          <PriceCard
            name="Free"
            price="?0"
            desc="Explore the workflow"
            items={["Web app access", "Basic opportunity feed", "Manual trading workflow"]}
          />
          <PriceCard
            name="Pro"
            price="?999/mo"
            desc="For serious intraday traders"
            items={["Advanced filters", "Better signal quality gates", "Priority roadmap features"]}
            highlight
          />
          <PriceCard
            name="Elite"
            price="?1999/mo"
            desc="For power users"
            items={["More automation", "Advanced alerts", "Strategy experiments (beta)"]}
          />
        </div>

        <div className="mt-10 rounded-2xl border border-white/10 bg-white/5 p-6 text-sm text-white/70">
          Note: Pricing is placeholder — we’ll finalize after the marketing pages + onboarding are polished.
        </div>
      </section>
    </SiteLayout>
  );
}
