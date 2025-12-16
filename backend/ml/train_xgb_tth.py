# -*- coding: utf-8 -*-
import os, json, pathlib, numpy as np, pandas as pd
from xgboost import XGBClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report

BASE = pathlib.Path("/opt/xauapi/api/trend")
OUT  = BASE / "out"

DATA_PATH = OUT / os.getenv("TTH_TRAIN_PARQUET", "train_tth_m15_8h.parquet")
MODEL_DIR = OUT / "tth_models"
MODEL_DIR.mkdir(parents=True, exist_ok=True)

def encode_hit_side(s: str) -> int:
    s = (s or "NONE").upper()
    if s == "UP": return 1
    if s == "DOWN": return 2
    return 0  # NONE

def main():
    df = pd.read_parquet(DATA_PATH)
    if df.empty:
        raise RuntimeError(f"Empty dataset: {DATA_PATH}")

    # ---- features (v1 minimal, extend later with SR/macro/etc) ----
    # Keep these stable & available everywhere
    feat_cols = ["atr14_pct", "k", "barrier_pct", "tod_min", "dow"]
    for c in feat_cols:
        if c not in df.columns:
            df[c] = np.nan
    df = df.dropna(subset=feat_cols).reset_index(drop=True)

    y_hit = df["hit_side"].map(encode_hit_side).astype(int).values
    y_t   = df["t_hit_min_bucket"].astype(int).values  # 0 for NONE, else bucket

    X = df[feat_cols].astype(float).values

    Xtr, Xte, yh_tr, yh_te, yt_tr, yt_te = train_test_split(
        X, y_hit, y_t, test_size=0.2, random_state=42, shuffle=True
    )

    # ---- Model A: hit classifier (UP/DOWN/NONE) ----
    hit_cls = XGBClassifier(
        n_estimators=600,
        max_depth=5,
        learning_rate=0.03,
        subsample=0.9,
        colsample_bytree=0.9,
        reg_lambda=1.0,
        objective="multi:softprob",
        num_class=3,
        eval_metric="mlogloss",
        n_jobs=4,
    )
    hit_cls.fit(Xtr, yh_tr)
    yh_pred = np.argmax(hit_cls.predict_proba(Xte), axis=1)
    print("\n=== hit_cls report ===")
    print(classification_report(yh_te, yh_pred))

    # ---- Model B: time bucket classifier (includes NONE=0) ----
    # Keep as multi-class; later we can condition on hit != NONE
    uniq = sorted(set(int(x) for x in np.unique(y_t)))
    bucket_to_idx = {b:i for i,b in enumerate(uniq)}
    idx_to_bucket = {i:b for b,i in bucket_to_idx.items()}

    yt_tr_idx = np.array([bucket_to_idx[int(x)] for x in yt_tr], dtype=int)
    yt_te_idx = np.array([bucket_to_idx[int(x)] for x in yt_te], dtype=int)

    t_cls = XGBClassifier(
        n_estimators=700,
        max_depth=5,
        learning_rate=0.03,
        subsample=0.9,
        colsample_bytree=0.9,
        reg_lambda=1.0,
        objective="multi:softprob",
        num_class=len(uniq),
        eval_metric="mlogloss",
        n_jobs=4,
    )
    t_cls.fit(Xtr, yt_tr_idx)
    yt_pred = np.argmax(t_cls.predict_proba(Xte), axis=1)
    print("\n=== t_bucket_cls report ===")
    print(classification_report(yt_te_idx, yt_pred))

    # ---- save ----
    hit_path = MODEL_DIR / "hit_cls.json"
    t_path   = MODEL_DIR / "t_bucket_cls.json"
    hit_cls.save_model(str(hit_path))
    t_cls.save_model(str(t_path))

    meta = {
        "feat_cols": feat_cols,
        "time_buckets": uniq,
        "bucket_to_idx": bucket_to_idx,
        "idx_to_bucket": idx_to_bucket,
        "base_tf": "M15",
        "max_hours": 8,
        "k_list": [0.5, 1.0, 1.5, 2.0],
    }
    (MODEL_DIR / "meta.json").write_text(json.dumps(meta, indent=2))
    print(f"\n[ok] saved models to {MODEL_DIR}")

if __name__ == "__main__":
    main()
