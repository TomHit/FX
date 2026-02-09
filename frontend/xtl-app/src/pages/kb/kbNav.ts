export type KbNavItem = {
  title: string;
  to: string;
  description?: string;
};

export type KbNavSection = {
  title: string;
  items: KbNavItem[];
  comingSoon?: boolean;
};

export const KB_NAV: KbNavSection[] = [
  {
    title: "Forecasts",
    items: [
      { title: "Overview", to: "/kb/forecasts/overview", description: "What the forecast page shows and how to use it." },
      { title: "Forecast Horizons", to: "/kb/forecasts/horizons", description: "15m vs 1h vs 4h (freeze windows)." },
      { title: "Expected Move", to: "/kb/forecasts/expected-move", description: "Magnitude, units, and why it changes." },
      { title: "Direction & Confidence", to: "/kb/forecasts/direction-confidence", description: "Up/down decision, abstain, confidence." },
      { title: "Macro Alignment", to: "/kb/forecasts/macro-alignment", description: "How macro signals influence the forecast." },
      { title: "How to Read the Forecast Page", to: "/kb/forecasts/how-to-read", description: "A step-by-step reading guide." },
      { title: "Common Misunderstandings", to: "/kb/forecasts/common-misunderstandings", description: "What users often misread (and fixes)." },
    ],
  },
  { title: "Opportunities", items: [], comingSoon: true },
  { title: "Strategy & Bots", items: [], comingSoon: true },
  { title: "Indicators & Concepts", items: [], comingSoon: true },
  { title: "Risk & Trading Basics", items: [], comingSoon: true },
  { title: "Platform & Data", items: [], comingSoon: true },
  { title: "FAQs & Glossary", items: [], comingSoon: true },
];
