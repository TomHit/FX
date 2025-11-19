// src/components/BootBoundary.tsx
import React from "react";

export default class BootBoundary extends React.Component<
  { children: React.ReactNode },
  { err: any }
> {
  constructor(props: any) {
    super(props);
    this.state = { err: null };
  }
  static getDerivedStateFromError(err: any) {
    return { err };
  }
  componentDidCatch(err: any, info: any) {
    // Keep this console so you can see stack traces server-side too
    // (Vite prod build logs to console; check browser devtools)
    // eslint-disable-next-line no-console
    console.error("BootBoundary caught:", err, info);
  }
  render() {
    if (this.state.err) {
      const msg = String(this.state.err?.message || this.state.err || "Unknown error");
      return (
        <div style={{
          maxWidth: 760, margin: "40px auto", padding: 16,
          color: "#fee2e2", background: "rgba(190,18,60,.15)",
          border: "1px solid rgba(190,18,60,.6)", borderRadius: 12,
          fontFamily: "system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif",
        }}>
          <div style={{ fontWeight: 700, marginBottom: 8 }}>App failed to start</div>
          <div style={{ fontSize: 13, whiteSpace: "pre-wrap" }}>{msg}</div>
        </div>
      );
    }
    return this.props.children as React.ReactElement;
  }
}
