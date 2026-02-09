# -*- coding: utf-8 -*-
from fastapi import APIRouter

# Keep this package router for any future trend submodules.
# IMPORTANT: do not import api.trend_endpoints here (avoids circular imports).
router = APIRouter()
