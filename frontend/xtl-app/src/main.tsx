// src/main.tsx
import React from "react";
import { createRoot } from "react-dom/client";
import { BrowserRouter } from "react-router-dom";
import { AuthProvider } from "@/context/AuthContext";
import App from "./App";
import "./styles.css";
import "./index.css";
import BootBoundary from "@/components/BootBoundary";

const el = document.getElementById("root");
if (!el) throw new Error("#root not found in index.html");

window.addEventListener("error", (e) => {
  console.error("window.onerror:", e.error || e.message || e);
});
window.addEventListener("unhandledrejection", (e) => {
  console.error("unhandledrejection:", e.reason);
});

createRoot(el).render(
  <React.StrictMode>
    <BootBoundary>
      <BrowserRouter basename="/react">
        <AuthProvider>
          <App />
        </AuthProvider>
      </BrowserRouter>
    </BootBoundary>
  </React.StrictMode>
);
