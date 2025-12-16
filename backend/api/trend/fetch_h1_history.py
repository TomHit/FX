# /opt/xauapi/api/trend/fetch_h1_history.py

import pathlib
import pandas as pd

from mt5_client import mt5_fetch_rates   # uses your existing MT5 wrapper

OUT = pathlib.Path("/opt/xauapi/api/trend/out")
OUT.mkdir(parents=True, exist_ok=True)

SYMS = ["XAUUSD", "EURUSD", "GBPUSD", "USDJPY", "USDCHF", "USDCAD"]

# ~1 year of H1 = 24 * 365 = 8760 bars; use small margin
BARS_H1_YEAR = 9000


def fetch_one(symbol: str) -> pd.DataFrame:
    # ask MT5 for last ~1 year of CLOSED H1 bars
    raw = mt5_fetch_rates(symbol, "H1", count=BARS_H1_YEAR, include_latest=False)
    if not raw:
        print(f"[WARN] no data for {symbol}")
        return pd.DataFrame()

    rows = []
    for r in raw:
        t_open_ms = int(r["t_open_ms"])
        rows.append(
            {
                "symbol": symbol,
                "ts_ms": t_open_ms,
                "open": float(r["o"]),
                "high": float(r["h"]),
                "low": float(r["l"]),
                "close": float(r["c"]),
                "volume": int(r.get("v", 0)),
            }
        )

    df = pd.DataFrame(rows).sort_values("ts_ms")
    print(f"[OK] {symbol}: {len(df)} H1 bars")
    return df


def main():
    all_df = []
    for sym in SYMS:
        df = fetch_one(sym)
        if not df.empty:
            all_df.append(df)

    if not all_df:
        print("[ERROR] no symbols produced data")
        return

    df_all = pd.concat(all_df, ignore_index=True)
    out_path = OUT / "train_raw_h1_1y.parquet"
    df_all.to_parquet(out_path)
    print(f"[DONE] wrote {len(df_all)} rows to {out_path}")


if __name__ == "__main__":
    main()
