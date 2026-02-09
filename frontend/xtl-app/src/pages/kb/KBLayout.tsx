import React from "react";
import { NavLink, Outlet } from "react-router-dom";
import { KB_NAV, type KbNavSection, type KbNavItem } from "./kbNav";

function cx(...xs: Array<string | false | undefined | null>) {
  return xs.filter(Boolean).join(" ");
}

export default function KBLayout() {
  const [open, setOpen] = React.useState<Record<string, boolean>>(() => {
    const init: Record<string, boolean> = {};
    for (const s of KB_NAV) init[s.title] = s.title === "Forecasts";
    return init;
  });

  return (
    <div className="mx-auto w-full max-w-6xl px-6 py-6">
      <div className="mb-6">
        <div className="text-2xl font-semibold text-slate-100">Knowledge Base</div>
        <div className="mt-1 text-sm text-slate-400">
          Learn how Forecasts, Opportunities, and Strategy features work.
        </div>
      </div>

      <div className="grid grid-cols-1 gap-6 lg:grid-cols-[280px_1fr]">
        {/* Sidebar */}
        <aside className="rounded-2xl border border-white/10 bg-white/5 p-4">
          <div className="mb-3 text-xs font-semibold uppercase tracking-wide text-slate-400">
            Browse
          </div>

          <nav className="space-y-4">
            {KB_NAV.map((section: KbNavSection) => {
              const isOpen = !!open[section.title];
              const hasItems = section.items.length > 0;

              return (
                <div key={section.title}>
                  <button
                    type="button"
                    className="flex w-full items-center justify-between rounded-xl px-2 py-2 text-left text-sm font-semibold text-slate-100 hover:bg-white/5"
                    onClick={() =>
                      setOpen((p) => ({ ...p, [section.title]: !p[section.title] }))
                    }
                  >
                    <span className="flex items-center gap-2">
                      {section.title}
                      {section.comingSoon ? (
                        <span className="rounded-full border border-white/10 bg-white/5 px-2 py-0.5 text-[11px] font-medium text-slate-300">
                          Coming soon
                        </span>
                      ) : null}
                    </span>
                    <span className="text-slate-400">{isOpen ? "–" : "+"}</span>
                  </button>

                  {isOpen && hasItems ? (
                    <div className="mt-2 space-y-1 pl-2">
                      {section.items.map((item: KbNavItem) => (
                        <NavLink
                          key={item.to}
                          to={item.to}
                          className={({ isActive }) =>
                            cx(
                              "block rounded-xl px-3 py-2 text-sm",
                              isActive
                                ? "bg-emerald-500/10 text-emerald-200 ring-1 ring-emerald-500/20"
                                : "text-slate-200 hover:bg-white/5"
                            )
                          }
                        >
                          <div className="flex items-center justify-between gap-2">
                            <span>{item.title}</span>
                            {item.description ? (
                              <span className="text-[11px] text-slate-400">{item.description}</span>
                            ) : null}
                          </div>
                        </NavLink>
                      ))}
                    </div>
                  ) : null}
                </div>
              );
            })}
          </nav>
        </aside>

        {/* Content */}
        <main className="rounded-2xl border border-white/10 bg-white/5 p-6">
          <Outlet />
        </main>
      </div>
    </div>
  );
}
