
# -*- coding: utf-8 -*-
import json, pathlib, numpy as np, pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score, log_loss, brier_score_loss, accuracy_score, precision_recall_fscore_support
import xgboost as xgb

BASE = pathlib.Path("/opt/xauapi/api/trend")
DATA = BASE / "out" / "train.parquet"
MODEL_DIR = BASE / "models"
MODEL_DIR.mkdir(parents=True, exist_ok=True)

FEATURE_COLS = [
    "atr14_h1_pct",
    "rvol_h1",
    "ret_1h",
    "usd_basket_h1_pct",
    "tod_min",
    "dow",
]
TARGET_BIN = "up_1h"
TARGET_REG = "move_1h_pct"
CALIB_PATH = MODEL_DIR / "calib.json"


def main():
    df = pd.read_parquet(DATA)

    # Clean labels
    y_cls = df[TARGET_BIN].fillna(0).astype(int)
    y_cls = y_cls.clip(0, 1).astype(np.float32)  # 0/1
    y_reg = df[TARGET_REG].astype(float)

    # Features
    X = df[FEATURE_COLS].copy()
    # Impute NaNs (tiny dataset)
    X = X.replace([np.inf, -np.inf], np.nan).fillna(0.0).astype(np.float32)

    # Split
    Xtr, Xva, ytr_cls, yva_cls, ytr_reg, yva_reg = train_test_split(
        X, y_cls, y_reg, test_size=0.25, random_state=42, stratify=y_cls if y_cls.nunique() > 1 else None
    )

    # ---- Classifier (next 1h up?) ----
    cls = xgb.XGBClassifier(
        n_estimators=200,
        max_depth=4,
        learning_rate=0.05,
        subsample=0.9,
        colsample_bytree=0.9,
        objective="binary:logistic",
        eval_metric=["auc","logloss"],
        base_score=0.5,             # <- force valid base_score in (0,1)
        tree_method="hist",
        n_jobs=2,
        random_state=42,
    )
    cls.fit(Xtr, ytr_cls, eval_set=[(Xva, yva_cls)], verbose=False)
    p_cls = cls.predict_proba(Xva)[:,1]
    auc  = roc_auc_score(yva_cls, p_cls) if yva_cls.nunique()>1 else float("nan")
    ll   = log_loss(yva_cls, p_cls, labels=[0,1])
    br   = brier_score_loss(yva_cls, p_cls)
    # threshold from config idea (0.6/0.4); use 0.6 for bull decision
    yhat = (p_cls >= 0.6).astype(int)
    acc  = accuracy_score(yva_cls, yhat)
    pr_bull = precision_recall_fscore_support(yva_cls, yhat, labels=[1], zero_division=0)
    pr_bear = precision_recall_fscore_support(yva_cls, yhat, labels=[0], zero_division=0)

    # ---- Regressor (magnitude, optional) ----
    reg = xgb.XGBRegressor(
        n_estimators=300,
        max_depth=4,
        learning_rate=0.05,
        subsample=0.9,
        colsample_bytree=0.9,
        objective="reg:squarederror",
        tree_method="hist",
        n_jobs=2,
        random_state=42,
        base_score=float(np.nanmean(ytr_reg)) if np.isfinite(np.nanmean(ytr_reg)) else 0.0,
    )
    reg.fit(Xtr, ytr_reg)
    p_reg = reg.predict(Xva)
    mae = float(np.nanmean(np.abs(p_reg - yva_reg)))
    # --- Calibration: per-symbol scaling for regressor magnitude (percent units) ---
    # --- Calibration: per-symbol scaling + data-driven caps -----------------
    # 1) Per-symbol magnitude scale: we still align median |pred| to median |true|.
    sym_va = df.loc[Xva.index, "symbol"] if "symbol" in df.columns else pd.Series(["ALL"] * len(Xva))
    scales: dict[str, float] = {}
    for s in sym_va.unique():
        mask = (sym_va == s).to_numpy()
        true_abs = np.abs(np.asarray(yva_reg)[mask])
        pred_abs = np.abs(np.asarray(p_reg)[mask])

        true_abs = true_abs[np.isfinite(true_abs)]
        pred_abs = pred_abs[np.isfinite(pred_abs)]

        if true_abs.size == 0 or pred_abs.size == 0:
            continue

        t_med = float(np.nanmedian(true_abs))
        p_med = float(np.nanmedian(pred_abs))
        if p_med > 1e-9:
            scales[str(s)] = float(np.clip(t_med / p_med, 0.5, 20.0))

    if scales:
        global_scale = float(np.clip(np.nanmedian(list(scales.values())), 0.5, 20.0))
    else:
        global_scale = 1.0

    
    # 2) Data-driven caps from validation distribution (no more magic 1.5%)
    # Use validation target distribution |yva_reg|, optionally filtered per symbol set.
    df_va = df.loc[Xva.index].copy()
    sym_va = df_va["symbol"] if "symbol" in df_va.columns else None

    majors = {"EURUSD", "GBPUSD", "USDJPY", "USDCHF", "USDCAD", "AUDUSD"}

    def _q_cap(sym_set, default_cap: float) -> float:
        # base array: absolute true 1h moves on validation
        vals_all = np.abs(yva_reg.astype(float))

        if sym_va is not None:
            mask = sym_va.isin(sym_set).to_numpy()
            vals = vals_all[mask]
        else:
            vals = vals_all

        vals = vals[np.isfinite(vals)]
        if vals.size < 100:
            # not enough history; fall back to a conservative default
            return default_cap

        # Use a high percentile as physical cap; inflate slightly (x1.2)
        q = float(np.percentile(vals, 99.0))
        return float(np.clip(q * 1.2, 0.2, 5.0))

    # --- Per-symbol caps instead of one "majors" bucket ---
    df_va = df.loc[Xva.index].copy()
    sym_va = df_va["symbol"] if "symbol" in df_va.columns else None

    clip_pct: dict[str, float] = {}

    if sym_va is not None:
        for s in sorted(sym_va.unique()):
            if not isinstance(s, str):
                continue
            s_up = s.upper()
            default_cap = 2.0 if s_up == "XAUUSD" else 1.0
            clip_pct[s_up] = _q_cap({s_up}, default_cap=default_cap)

    # Optional: keep a majors group as a generic fallback
    cap_majors = _q_cap(majors, default_cap=1.0)
    clip_pct.setdefault("majors", cap_majors)
    # also keep XAUUSD key for compatibility, but it will match per-symbol entry
    if "XAUUSD" not in clip_pct:
        clip_pct["XAUUSD"] = _q_cap({"XAUUSD"}, default_cap=2.0)


    calib = {
        "global_scale": global_scale,
        "per_symbol": scales,
        "clip_pct": clip_pct,
        "abstain": {"p_up_margin": 0.10, "min_pct": 0.03},
    }

    with open(CALIB_PATH, "w") as f:
        json.dump(calib, f)



    # Save models
    cls.save_model(str(MODEL_DIR / "xgb_cls.json"))
    reg.save_model(str(MODEL_DIR / "xgb_reg.json"))

    out = {
        "rows": {"total": int(len(df)), "train": int(len(Xtr)), "valid": int(len(Xva))},
        "metrics": {
            "classification": {
                "AUC": float(auc), "LogLoss": float(ll), "Brier": float(br),
                "Accuracy@0.6": float(acc),
                "Precision@bull": float(pr_bull[0][0] if len(pr_bull[0]) else 0.0),
                "Recall@bull": float(pr_bull[1][0] if len(pr_bull[1]) else 0.0),
                "Precision@bear": float(pr_bear[0][0] if len(pr_bear[0]) else 0.0),
                "Recall@bear": float(pr_bear[1][0] if len(pr_bear[1]) else 0.0),
            },
            "regression": {"MAE_move_pct": float(mae)},
        },
        "features": FEATURE_COLS,
        "model_paths": {
            "cls": str(MODEL_DIR / "xgb_cls.json"),
            "reg": str(MODEL_DIR / "xgb_reg.json"),
            "calib": str(CALIB_PATH),
        }
    }
    print(json.dumps(out, indent=2))

if __name__ == "__main__":
    main()

