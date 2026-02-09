import { useEffect, useMemo, useState } from "react";

type Props = {
  open: boolean;
  onClose: () => void;
  planLabel: "Signals" | "MyBots" | "Automation";
  toEmail?: string;
};

type FormState = {
  name: string;
  mobile: string;
  email: string;
  country: string;
  city: string;
};

export default function RequestAccessModal({
  open,
  onClose,
  planLabel,
  toEmail = "support@xautrendlab.com",
}: Props) {
  const [s, setS] = useState<FormState>({
    name: "",
    mobile: "",
    email: "",
    country: "",
    city: "",
  });
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    if (!open) return;
    setErr(null);
  }, [open]);

  const canSubmit = useMemo(() => {
    const nameOk = s.name.trim().length >= 2;
    const emailOk = /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(s.email.trim());
    const mobileOk = s.mobile.trim().length >= 7;
    const countryOk = s.country.trim().length >= 2;
    const cityOk = s.city.trim().length >= 2;
    return nameOk && emailOk && mobileOk && countryOk && cityOk;
  }, [s]);

  if (!open) return null;

  function set<K extends keyof FormState>(k: K, v: FormState[K]) {
    setS((p) => ({ ...p, [k]: v }));
  }

  function submit() {
    setErr(null);
    if (!canSubmit) {
      setErr("Please fill all fields with valid details.");
      return;
    }

    const subject = `XTL ${planLabel} Access Request`;
    const body = [
      `Access request: ${planLabel}`,
      "",
      `Name: ${s.name.trim()}`,
      `Email: ${s.email.trim()}`,
      `Mobile: ${s.mobile.trim()}`,
      `Country: ${s.country.trim()}`,
      `City: ${s.city.trim()}`,
      "",
      "Notes:",
      "- Preferred instruments (e.g., XAUUSD, FX pairs):",
      "- Trading experience level:",
      "- Preferred workflow (manual / semi-auto / automation):",
    ].join("\n");

    const href = `mailto:${encodeURIComponent(toEmail)}?subject=${encodeURIComponent(
      subject
    )}&body=${encodeURIComponent(body)}`;

    window.location.href = href;
    onClose();
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center px-4" role="dialog" aria-modal="true">
      <div className="absolute inset-0 bg-black/60" onClick={onClose} aria-hidden="true" />

      <div className="relative w-full max-w-lg rounded-3xl border border-white/10 bg-[#0b0b0e] p-6 shadow-2xl">
        <div className="flex items-start justify-between gap-4">
          <div>
            <div className="text-xs font-semibold tracking-wide text-white/55">REQUEST ACCESS</div>
            <h3 className="mt-1 text-lg font-semibold text-white">{planLabel} access request</h3>
            <p className="mt-1 text-sm text-white/70">
              Submit details for review. This opens your email client with details prefilled.
            </p>
          </div>

          <button
            onClick={onClose}
            className="rounded-xl border border-white/15 px-3 py-2 text-xs font-semibold text-white/80 hover:bg-white/5"
          >
            Close
          </button>
        </div>

        <div className="mt-6 grid gap-3">
          <div className="grid gap-3 sm:grid-cols-2">
            <Field label="Full name" value={s.name} onChange={(v) => set("name", v)} placeholder="Your name" />
            <Field label="Mobile" value={s.mobile} onChange={(v) => set("mobile", v)} placeholder="+91…" />
          </div>

          <Field label="Email" value={s.email} onChange={(v) => set("email", v)} placeholder="you@domain.com" />

          <div className="grid gap-3 sm:grid-cols-2">
            <Field label="Country" value={s.country} onChange={(v) => set("country", v)} placeholder="India" />
            <Field label="City" value={s.city} onChange={(v) => set("city", v)} placeholder="Mumbai" />
          </div>

          {err ? (
            <div className="rounded-2xl border border-red-500/20 bg-red-500/10 px-4 py-3 text-sm text-red-200">
              {err}
            </div>
          ) : null}

          <div className="mt-2 flex flex-col gap-3 sm:flex-row">
            <button
              onClick={submit}
              disabled={!canSubmit}
              className="rounded-xl bg-white px-6 py-3 text-center text-sm font-semibold text-black hover:bg-white/90 disabled:cursor-not-allowed disabled:opacity-50"
            >
              Submit request
            </button>

            <button
              onClick={onClose}
              className="rounded-xl border border-white/15 px-6 py-3 text-center text-sm font-semibold text-white/90 hover:bg-white/5"
            >
              Cancel
            </button>
          </div>

          <div className="text-xs text-white/45">
            Tip: If you prefer server-side capture (no mail client), we can add a simple API endpoint later.
          </div>
        </div>
      </div>
    </div>
  );
}

function Field({
  label,
  value,
  onChange,
  placeholder,
}: {
  label: string;
  value: string;
  onChange: (v: string) => void;
  placeholder?: string;
}) {
  return (
    <label className="grid gap-1">
      <span className="text-xs font-semibold tracking-wide text-white/55">{label}</span>
      <input
        value={value}
        onChange={(e) => onChange(e.target.value)}
        placeholder={placeholder}
        className="rounded-2xl border border-white/10 bg-white/[0.03] px-4 py-3 text-sm text-white outline-none placeholder:text-white/30 focus:border-white/20"
      />
    </label>
  );
}
