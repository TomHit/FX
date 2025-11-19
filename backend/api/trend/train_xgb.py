
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
    "atr14_m15_pct","rvol15","ret_15m","usd_basket_d1h_pct","tod_min","dow",
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
    # We want median(|true|) ˜ scale * median(|pred|) on validation
    sym_va = df.iloc[Xva.index]["symbol"] if "symbol" in df.columns else pd.Series(["ALL"] * len(Xva))
    scales = {}
    for s in sym_va.unique():
        mask = (sym_va == s).to_numpy()
        true_abs = np.abs(np.asarray(yva_reg)[mask])
        pred_abs = np.abs(np.asarray(p_reg)[mask])
        t_med = float(np.nanmedian(true_abs)) if np.isfinite(true_abs).any() else np.nan
        p_med = float(np.nanmedian(pred_abs)) if np.isfinite(pred_abs).any() else np.nan
        if np.isfinite(t_med) and np.isfinite(p_med) and p_med > 1e-9:
           scales[str(s)] = float(np.clip(t_med / p_med, 0.5, 20.0))

    global_scale = float(np.clip(np.nanmedian(list(scales.values())) if scales else 1.0, 0.5, 20.0))
    calib = {
       "global_scale": global_scale,
       "per_symbol": scales,
       "clip_pct": {"majors": 1.5, "XAUUSD": 2.5},    # 1h horizon sane caps
       "abstain": {"p_up_margin": 0.10, "min_pct": 0.03}
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

