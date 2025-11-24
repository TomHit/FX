# -*- coding: utf-8 -*-
import json
import pathlib
from typing import Dict, Any

import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.metrics import (
    roc_auc_score,
    log_loss,
    brier_score_loss,
    accuracy_score,
    precision_recall_fscore_support,
    mean_absolute_error,
)
import xgboost as xgb

BASE = pathlib.Path("/opt/xauapi/api/trend")
DATA = BASE / "out" / "train.parquet"
MODEL_DIR = BASE / "models"
MODEL_DIR.mkdir(parents=True, exist_ok=True)


FEATURE_COLS_H4 = [
    "atr14_h4_pct",
    "rvol_h4",
    "ret_4h",
    "usd_basket_h4_pct",
    "tod_min",
    "dow",
]

TARGET_BIN = "up_4h"
TARGET_REG = "move_4h_pct"
CALIB_PATH = MODEL_DIR / "calib_h4.json"


def _build_h4_labels(df: pd.DataFrame) -> pd.DataFrame:
    """
    Starting from the H1 training dataframe, build 4-hour labels:
    - move_4h_pct: close in 4 hours vs current close (in %)
    - up_4h: 1 if move_4h_pct > 0, else 0
    """
    df = df.copy()
    if "symbol" not in df.columns or "ts_ms" not in df.columns or "close" not in df.columns:
        raise RuntimeError("train.parquet must contain 'symbol', 'ts_ms', 'close' to build 4h labels")

    df = df.sort_values(["symbol", "ts_ms"])

    def _per_symbol(g: pd.DataFrame) -> pd.DataFrame:
        g = g.sort_values("ts_ms")
        g["close_4h_ahead"] = g["close"].shift(-4)
        g["move_4h_pct"] = (g["close_4h_ahead"] - g["close"]) / g["close"] * 100.0
        g["up_4h"] = (g["move_4h_pct"] > 0).astype("int8")
        return g

    df = df.groupby("symbol", group_keys=False).apply(_per_symbol)
    df = df.dropna(subset=["move_4h_pct", "up_4h"])
    return df


def _ensure_features(df: pd.DataFrame) -> None:
    missing = [c for c in FEATURE_COLS_H4 if c not in df.columns]
    if missing:
        raise RuntimeError(f"train.parquet missing H1 feature columns for 4h training: {missing}")


def _train_models(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Train binary classifier (up/down in 4h) and regressor (move_4h_pct),
    and build a simple calibration dict.
    """
    # We keep symbol for calibration stats
    cols = FEATURE_COLS_H4 + [TARGET_BIN, TARGET_REG, "symbol"]
    # For 4h labels a lot of rows can have NaNs (due to shifting etc.).
    # Instead of dropping everything, just replace NaN/inf with 0 so we
    # keep as many samples as possible.
    dfm = df[cols].replace([np.inf, -np.inf], 0.0)
    dfm = dfm.fillna(0.0)

    if len(dfm) == 0:
        raise RuntimeError("No usable rows for 4h training (dfm length is 0 after cleaning)")

    X = dfm[FEATURE_COLS_H4].astype("float32")
    y_bin = dfm[TARGET_BIN].astype("int32")
    y_reg = dfm[TARGET_REG].astype("float32")

    X_train, X_valid, yb_train, yb_valid, yr_train, yr_valid, df_train, df_valid = train_test_split(
        X,
        y_bin,
        y_reg,
        dfm[["symbol", TARGET_REG]],
        test_size=0.25,
        random_state=42,
        stratify=y_bin,
    )

    # --- Classifier (4h up/down) ---
    cls = xgb.XGBClassifier(
        n_estimators=400,
        max_depth=4,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        objective="binary:logistic",
        eval_metric="logloss",
        n_jobs=4,
    )
    cls.fit(X_train, yb_train)

    prob_valid = cls.predict_proba(X_valid)[:, 1]
    auc = float(roc_auc_score(yb_valid, prob_valid))
    ll = float(log_loss(yb_valid, prob_valid))
    brier = float(brier_score_loss(yb_valid, prob_valid))

    # accuracy / precision / recall at fixed 0.6 threshold
    thr = 0.6
    pred_label = (prob_valid >= thr).astype("int32")
    acc = float(accuracy_score(yb_valid, pred_label))
    prec, rec, _, _ = precision_recall_fscore_support(
        yb_valid, pred_label, average=None, labels=[1, 0], zero_division=0
    )
    prec_bull = float(prec[0])
    rec_bull = float(rec[0])
    prec_bear = float(prec[1])
    rec_bear = float(rec[1])

    # --- Regressor (4h move %) ---
    reg = xgb.XGBRegressor(
        n_estimators=500,
        max_depth=4,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        objective="reg:squarederror",
        n_jobs=4,
    )
    reg.fit(X_train, yr_train)
    y_pred_reg = reg.predict(X_valid)
    mae = float(mean_absolute_error(yr_valid, y_pred_reg))

    # --- Simple calibration for 4h scale ---
    # global scale = match average absolute move
    abs_true = np.abs(yr_valid.values)
    abs_pred = np.abs(y_pred_reg)
    eps = 1e-6
    if abs_pred.mean() > eps:
        global_scale = float(np.clip(abs_true.mean() / abs_pred.mean(), 0.25, 4.0))
    else:
        global_scale = 1.0

    # per-symbol adjustments (optional, small tweak)
    per_symbol: Dict[str, float] = {}
    for sym, sub in df_valid.assign(pred_move=y_pred_reg).groupby("symbol"):
        t = sub[TARGET_REG].values
        p = sub["pred_move"].values
        if len(t) < 20:
            continue
        at = np.abs(t).mean()
        ap = np.abs(p).mean()
        if ap > eps:
            s = float(np.clip(at / ap, 0.25, 4.0))
            per_symbol[sym] = s

    calib = {
        "global_scale": global_scale,
        "per_symbol": per_symbol,
        # 4h moves can be larger; allow bigger caps than 1h
        "clip_pct": {"majors": 3.0, "XAUUSD": 6.0},
        # Abstain region for UI (if you later want to hide low-edge calls)
        "abstain": {"p_up_margin": 0.10, "min_pct": 0.10},
    }

    CALIB_PATH.write_text(json.dumps(calib))

    # Save models
    (MODEL_DIR / "xgb_cls_h4.json").unlink(missing_ok=True)
    (MODEL_DIR / "xgb_reg_h4.json").unlink(missing_ok=True)
    cls.save_model(str(MODEL_DIR / "xgb_cls_h4.json"))
    reg.save_model(str(MODEL_DIR / "xgb_reg_h4.json"))

    return {
        "rows": {
            "total": int(len(dfm)),
            "train": int(len(X_train)),
            "valid": int(len(X_valid)),
        },
        "metrics": {
            "classification": {
                "AUC": auc,
                "LogLoss": ll,
                "Brier": brier,
                "Accuracy@0.6": acc,
                "Precision@bull": prec_bull,
                "Recall@bull": rec_bull,
                "Precision@bear": prec_bear,
                "Recall@bear": rec_bear,
            },
            "regression": {"MAE_move_4h_pct": mae},
        },
        "features": FEATURE_COLS_H4,
        "model_paths": {
            "cls": str(MODEL_DIR / "xgb_cls_h4.json"),
            "reg": str(MODEL_DIR / "xgb_reg_h4.json"),
            "calib": str(CALIB_PATH),
        },
    }


def main() -> None:
    df = pd.read_parquet(DATA)
    _ensure_features(df)
    df = _build_h4_labels(df)
    out = _train_models(df)
    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
