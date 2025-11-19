# -*- coding: utf-8 -*-
# Centralized Redis client (TEMP: hardcoded creds). Swap to env before pushing to git.
import os
import redis

# TEMP hardcoded URL — change before committing to git!
_HARDCODED_URL = "redis://:xau12345@127.0.0.1:6379/0"

def get_client():
    # prefer env if present so we can override without code change later
    url = os.getenv("REDIS_URL", _HARDCODED_URL)
    return redis.from_url(url)

