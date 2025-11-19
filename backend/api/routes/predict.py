# -*- coding: utf-8 -*-
from fastapi import APIRouter, Query

router = APIRouter(prefix="/trend", tags=["trend"])

@router.get("/predict")
def predict(symbol: str = Query("XAUUSD")):
    # Lazy import so app can start even if ML libs missing
    try:
        from api.trend.infer_rt import predict_next_hour
    except Exception as e:
        return {"ok": False, "reason": "ml_import_error", "detail": str(e)}
    return predict_next_hour(symbol.upper())
