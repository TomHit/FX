// src/components/Header.tsx
import React, { useEffect, useRef, useState } from "react";
import { Link, NavLink, useLocation } from "react-router-dom";
import { useAuth } from "@/context/AuthContext";

const LINKS = [
  { to: "/dashboard",  label: "Dashboard" },
  { to: "/ai-forecasts",  label: "AI-powered forecasts" },
  { to: "/Confluence-Intelligence",  label: "Confluence Intelligence" },
  { to: "/onboarding", label: "Onboarding" },
  { to: "/devices",    label: "Devices" },
  { to: "/strategy",   label: "Strategy" },
  { to: "/trend",      label: "Trend" },
  { to: "/my-bots",    label: "My Bots" },
  { to: "/performance",label: "Daily Performance" },
  { to: "/kb",label: "Knowledge Base" },

];

export default function Header() {
  const { me, logout } = useAuth();
  const [navOpen, setNavOpen] = useState(false);        // mobile menu
  const [userOpen, setUserOpen] = useState(false);      // user dropdown
  const loc = useLocation();
  const userRef = useRef<HTMLDivElement | null>(null);

  // Close menus on route change
  useEffect(() => {
    setNavOpen(false);
    setUserOpen(false);
  }, [loc.pathname]);

  // Close user menu on ESC or outside click
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => e.key === "Escape" && setUserOpen(false);
    const onDoc = (e: MouseEvent) => {
      if (!userRef.current) return;
      if (!userRef.current.contains(e.target as Node)) setUserOpen(false);
    };
    document.addEventListener("mousedown", onDoc);
    window.addEventListener("keydown", onKey);
    return () => {
      document.removeEventListener("mousedown", onDoc);
      window.removeEventListener("keydown", onKey);
    };
  }, []);

  return (
    <header className="topbar">
      {/* Brand */}
      <Link to="/dashboard" className="logo">XTL</Link>

      {/* Mobile hamburger */}
      <button
        className="mobile-toggle"
        aria-label="Toggle menu"
        aria-expanded={navOpen}
        onClick={() => setNavOpen(v => !v)}
      >
        <span></span><span></span><span></span>
      </button>

      {/* Main navigation (kept in header, not in user menu) */}
      <nav className={`nav ${navOpen ? "open" : ""}`}>
        {LINKS.map((l) => (
         <NavLink
           key={l.to}
           to={l.to}
           className={({ isActive }) =>
             [
              "nav-link",
              isActive ? "active" : "",
             ].join(" ")
           }
         >
           {l.label}
         </NavLink>
      ))}

      </nav>

      <div className="spacer" />

      {/* User dropdown: Change password + Log out */}
      {me?.username && (
        <div ref={userRef} className={`user-menu ${userOpen ? "open" : ""}`}>
          <button
            type="button"
            className="avatar"
            onClick={() => setUserOpen(v => !v)}
            aria-haspopup="menu"
            aria-expanded={userOpen}
          >
            {me.username}
          </button>

          <div className="menu" role="menu">
            <div className="muted">Signed in as</div>
            <div className="strong">{me.username}</div>

            {/* Keep account actions here; do not move main nav here */}
            <Link to="/change-password" role="menuitem">
              Change password
            </Link>

            <hr style={{ borderColor: "var(--line)", opacity: .6, margin: "8px 0" }} />

            <button role="menuitem" onClick={logout}>
              Log out
            </button>
          </div>
        </div>
      )}
    </header>
  );
}
