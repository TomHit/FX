import { useState } from "react";
import { changePassword } from "@/lib/api";

export default function ChangePasswordModal({ onClose }: { onClose: () => void }) {
  const [cur, setCur] = useState("");
  const [nw, setNew] = useState("");
  const [busy, setBusy] = useState(false);
  const [msg, setMsg] = useState<string | null>(null);

  async function submit(e: React.FormEvent) {
    e.preventDefault();
    setBusy(true); setMsg(null);
    try {
      await changePassword({ current_password: cur, new_password: nw });
      setMsg("Password changed.");
      setTimeout(onClose, 900);
    } catch (e: any) {
      setMsg(e?.response?.data?.detail || "Failed to change password");
    } finally {
      setBusy(false);
    }
  }

  return (
    <div style={{ position:"fixed", inset:0, background:"rgba(0,0,0,.6)", display:"grid", placeItems:"center" }}>
      <form className="card" style={{ width:380 }} onSubmit={submit}>
        <h3 style={{ marginTop:0 }}>Change password</h3>
        <div style={{ display:"grid", gap:8 }}>
          <input className="input" type="password" placeholder="Current password" value={cur} onChange={e=>setCur(e.target.value)} required />
          <input className="input" type="password" placeholder="New password" value={nw} onChange={e=>setNew(e.target.value)} required />
          <div className="row">
            <button className="btn" disabled={busy}>Save</button>
            <button type="button" className="btn secondary" onClick={onClose}>Cancel</button>
          </div>
          {msg && <div className="badge">{msg}</div>}
        </div>
      </form>
    </div>
  );
}
