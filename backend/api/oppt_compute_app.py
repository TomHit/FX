# -*- coding: utf-8 -*-
from __future__ import annotations

import logging
import os

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

#
# Mark this process before importing trend_endpoints.
# Later, trend_endpoints will use this flag to permit only
# authenticated internal opportunity refresh requests.
#
os.environ["XTL_OPPT_COMPUTE_ONLY"] = "1"

from api.trend_endpoints import router as trend_router


log = logging.getLogger("xtl.oppt_compute")


app = FastAPI(
    title="XTL Opportunity Compute Service",
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)

#
# This service listens only on 127.0.0.1.
# CORS is deliberately disabled.
#
app.include_router(
    trend_router,
    tags=["trend-compute"],
)


@app.get("/healthz")
def healthz() -> dict:
    return {
        "ok": True,
        "service": "xtl-oppt-compute",
        "compute_only": True,
        "pid": os.getpid(),
    }


@app.on_event("startup")
def _startup() -> None:
    log.warning(
        "[OPPT_COMPUTE] START "
        "pid=%s compute_only=%s",
        os.getpid(),
        os.getenv("XTL_OPPT_COMPUTE_ONLY"),
    )