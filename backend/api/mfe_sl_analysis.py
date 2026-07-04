#!/usr/bin/env python3
"""
Read-only MFE/MAE-vs-SL analysis for XTL trades.jsonl.
Touches nothing: opens the JSONL read-only, prints a report.

Answers: of SL-hit trades, how far did they run favorable (MFE) before reversing,
and what would candidate tighter stops / a break-even rule have done across the
FULL set (SL and TP together) — so you see net-R, not just the tempting half.

Usage:  python3 mfe_sl_analysis.py [/path/to/trades.jsonl]
"""
import sys, json, statistics as st

PATH = sys.argv[1] if len(sys.argv) > 1 else "/opt/xauapi/api/trend/out/trades.jsonl"

def load(path):
    rows = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except Exception:
                pass
    return rows

def pips(sym, price_dist):
    # JPY & XAU: 0.01 pip; others 0.0001
    s = (sym or "").upper()
    pip = 0.01 if ("JPY" in s or s == "XAUUSD") else 0.0001
    return price_dist / pip if pip else None

def fnum(x):
    try: return float(x)
    except Exception: return None

def main():
    try:
        rows = load(PATH)
    except FileNotFoundError:
        print(f"NOT FOUND: {PATH}"); return
    closed = [r for r in rows if r.get("_status") == "closed" or r.get("exit_reason")]
    n = len(closed)
    print(f"file: {PATH}")
    print(f"closed trades: {n}\n")

    by_reason = {}
    for r in closed:
        by_reason.setdefault(r.get("exit_reason","?"), []).append(r)
    print("exit_reason breakdown:")
    for k,v in sorted(by_reason.items()):
        print(f"  {k:8s} {len(v)}")
    print()

    sl = by_reason.get("sl", [])
    tp = by_reason.get("tp", [])
    print(f"=== SL-hit trades: MFE (how far they ran favorable BEFORE reversing) ===")
    if not sl:
        print("  (none)")
    else:
        mfes = [fnum(r.get("mfe_r")) for r in sl if fnum(r.get("mfe_r")) is not None]
        maes = [fnum(r.get("mae_r")) for r in sl if fnum(r.get("mae_r")) is not None]
        if mfes:
            print(f"  mfe_r: min {min(mfes):.2f}  median {st.median(mfes):.2f}  max {max(mfes):.2f}  mean {st.mean(mfes):.2f}")
        if maes:
            print(f"  mae_r: min {min(maes):.2f}  median {st.median(maes):.2f}  max {max(maes):.2f}  (mae<-1 => price ran past the stop level)")
        for thr in (0.3, 0.5, 0.8, 1.0):
            k = sum(1 for m in mfes if m >= thr)
            print(f"  ran >= {thr:.1f}R favorable before SL: {k}/{len(mfes)}"
                  + (f"  ({100*k/len(mfes):.0f}%)" if mfes else ""))
        # pip view per symbol
        print("\n  per-trade (SL): symbol  mfe_r  mfe_pips  mae_r  stop_dist_pips  realized_r")
        for r in sl:
            sym = r.get("symbol")
            entry = fnum(r.get("entry_price")); slp = fnum(r.get("sl_price"))
            risk = abs(entry - slp) if (entry is not None and slp is not None) else None
            mfe_r = fnum(r.get("mfe_r"))
            mfe_pips = pips(sym, mfe_r*risk) if (mfe_r is not None and risk) else None
            sd = pips(sym, risk) if risk else None
            print(f"    {sym:8s} {str(mfe_r):>5s}  "
                  f"{('%.1f'%mfe_pips) if mfe_pips is not None else '   -'}    "
                  f"{str(r.get('mae_r')):>5s}   {('%.1f'%sd) if sd else '-':>6s}      {r.get('realized_r')}")

    # Counterfactual: tighter fixed stop, across SL+TP.
    # Needs MFE/MAE in R and the realized outcome. With no TPs this is illustrative only.
    print(f"\n=== Counterfactual stop simulation (needs TPs to be meaningful) ===")
    sim = [r for r in closed if r.get("exit_reason") in ("sl","tp")
           and fnum(r.get("mae_r")) is not None]
    if len(tp) == 0:
        print("  SKIPPED: 0 TP trades in sample. A tighter-stop sim with no winners")
        print("  only ever argues for tightening. Re-run once you have TP trades.")
    else:
        for cand in (0.5, 0.75, 1.0):  # candidate stop in R
            net = 0.0
            for r in sim:
                mae = fnum(r.get("mae_r")); real = fnum(r.get("realized_r")) or 0.0
                rr = fnum(r.get("target_rr")) or 2.0
                # if the trade's worst excursion breached the candidate stop, it stops out at -cand
                if mae <= -cand:
                    net += -cand
                else:
                    net += real  # survived tighter stop -> original outcome
            print(f"  stop @ -{cand:.2f}R  -> net {net:+.2f}R over {len(sim)} trades "
                  f"(vs baseline {sum(fnum(r.get('realized_r')) or 0 for r in sim):+.2f}R)")

    print("\nNOTE: mfe_r/mae_r are H1-bar approximations (exit_confidence='medium').")
    print("Directional signal only; not pip-accurate. Verify the excursion loop is")
    print("bounded by close_timestamp before trusting mae<-1 values.")

if __name__ == "__main__":
    main()
