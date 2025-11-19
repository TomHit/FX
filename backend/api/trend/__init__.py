# -*- coding: utf-8 -*-
from fastapi import APIRouter

# This router will only aggregate the "all" endpoints from trend_endpoints.py
# /trend/predict is owned by api/predict.py to avoid duplicates.

router = APIRouter()

# Try importing the extra endpoints we just renamed.
_extra = None
_import_err = None
try:
    # Import the sibling module (api/trend_endpoints.py)
    from ..trend_endpoints import router as _extra
except Exception as e:
    _import_err = e
    import logging
    logging.getLogger(__name__).warning("trend_endpoints import failed: %r", e)

# Mount the extra endpoints under the /trend prefix.
if _extra:
    router.include_router(_extra, prefix="/trend", tags=["trend"])
else:
    # If import failed, expose stub routes so they still show in docs.
    _stub = APIRouter(prefix="/trend", tags=["trend"])

    @_stub.get("/predict/all")
    def _predict_all_stub():
        return {"ok": False, "reason": "route_init_error", "detail": repr(_import_err) if _import_err else "unknown"}

    @_stub.get("/price/all")
    def _price_all_stub():
        return {"ok": False, "reason": "route_init_error", "detail": repr(_import_err) if _import_err else "unknown"}

    router.include_router(_stub)
