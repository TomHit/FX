// src/App.tsx
import React from "react";
import { Routes, Route, Navigate, useLocation } from "react-router-dom";

import BootBoundary from "@/components/BootBoundary";
import Header from "@/components/Header";
import Protected from "@/components/Protected";

import Login from "@/pages/Login";
import Dashboard from "@/pages/Dashboard";
import Onboarding from "@/pages/Onboarding";
import Devices from "@/pages/Devices";
import Strategy from "@/pages/Strategy";
import Trend from "@/pages/Trend";
import MFASetup from "@/pages/MFASetup";
import OpportunitiesDashboard from "@/pages/OpportunitiesDashboard";
import AIForecasts from "@/pages/Dashboard";
import PerformancePages from "@/pages/PerformancePages";
import ConfluenceIntelligence from "@/pages/Confluence";

import Propfirm from "@/pages/Propfirm";


// ✅ Knowledge Base
import KBLayout from "@/pages/kb/KBLayout";
import ForecastOverview from "@/pages/kb/forecasts/ForecastOverview";
import HowToRead from "@/pages/kb/forecasts/HowToRead";
import ForecastHorizons from "@/pages/kb/forecasts/ForecastHorizons";
import ExpectedMove from "@/pages/kb/forecasts/ExpectedMove";
import DirectionConfidence from "@/pages/kb/forecasts/DirectionConfidence";
import MacroAlignment from "@/pages/kb/forecasts/MacroAlignment";
import CommonMisunderstandings from "@/pages/kb/forecasts/CommonMisunderstandings";

/** Per-route guard so a single screen error can't blank everything */
function RouteBoundary({ children }: { children: React.ReactNode }) {
  try {
    return <>{children}</>;
  } catch (e: any) {
    return (
      <div
        style={{
          maxWidth: 760,
          margin: "40px auto",
          padding: 16,
          color: "#fee2e2",
          background: "rgba(190,18,60,.15)",
          border: "1px solid rgba(190,18,60,.6)",
          borderRadius: 12,
        }}
      >
        <div style={{ fontWeight: 700, marginBottom: 8 }}>This page failed to render</div>
        <div style={{ fontSize: 13, whiteSpace: "pre-wrap" }}>{String(e?.message || e)}</div>
      </div>
    );
  }
}

function Layout({ children }: { children: React.ReactNode }) {
  const { pathname } = useLocation();
  const hideHeader = pathname === "/login";
  return (
    <>
      {!hideHeader && (
        <BootBoundary>
          <Header />
        </BootBoundary>
      )}
      {children}
    </>
  );
}

export default function App() {
  return (
    <Layout>
      {/* Wrap the whole routing tree so unexpected errors never blank the app */}
      <BootBoundary>
        <Routes>
          {/* PUBLIC */}
          <Route
            path="/login"
            element={
              <BootBoundary>
                <RouteBoundary>
                  <Login />
                </RouteBoundary>
              </BootBoundary>
            }
          />

          {/* PRIVATE */}
          <Route element={<Protected />}>
            <Route
              path="/mfa-setup"
              element={
                <RouteBoundary>
                  <MFASetup />
                </RouteBoundary>
              }
            />

            <Route
              path="/dashboard"
              element={
                <RouteBoundary>
                  <OpportunitiesDashboard />
                </RouteBoundary>
              }
            />
            <Route
              path="/prop-firm"
              element={
                <RouteBoundary>
                  <Propfirm/>
                </RouteBoundary>
              }
            />


            <Route
              path="/ai-forecasts"
              element={
                <RouteBoundary>
                  <AIForecasts />
                </RouteBoundary>
              }
            />
            <Route
              path="/Confluence-Intelligence"
              element={
                <RouteBoundary>
                  <ConfluenceIntelligence />
                </RouteBoundary>
              }
            />


            <Route
              path="/onboarding"
              element={
                <RouteBoundary>
                  <Onboarding />
                </RouteBoundary>
              }
            />

            <Route
              path="/devices"
              element={
                <RouteBoundary>
                  <Devices />
                </RouteBoundary>
              }
            />

            <Route
              path="/strategy"
              element={
                <RouteBoundary>
                  <Strategy />
                </RouteBoundary>
              }
            />

            <Route
              path="/trend"
              element={
                <RouteBoundary>
                  <Trend />
                </RouteBoundary>
              }
            />

            <Route
              path="/performance"
              element={
                <RouteBoundary>
                  <PerformancePages />
                </RouteBoundary>
              }
            />

            {/* ✅ Knowledge Base */}
            <Route
              path="/kb"
              element={
                <RouteBoundary>
                  <KBLayout />
                </RouteBoundary>
              }
            >
              {/* default KB landing */}
              <Route index element={<Navigate to="forecasts/overview" replace />} />

              {/* Forecast docs */}
              <Route path="forecasts">
                <Route index element={<Navigate to="overview" replace />} />
                <Route path="overview" element={<ForecastOverview />} />
                <Route path="how-to-read" element={<HowToRead />} />
                <Route path="horizons" element={<ForecastHorizons />} />
                <Route path="expected-move" element={<ExpectedMove />} />
                <Route path="direction-confidence" element={<DirectionConfidence />} />
                <Route path="macro-alignment" element={<MacroAlignment />} />
                <Route path="common-misunderstandings" element={<CommonMisunderstandings />} />
              </Route>
            </Route>

            <Route index element={<Navigate to="/dashboard" replace />} />
          </Route>

          {/* FALLBACK */}
          <Route path="*" element={<Navigate to="/login" replace />} />
        </Routes>
      </BootBoundary>
    </Layout>
  );
}
