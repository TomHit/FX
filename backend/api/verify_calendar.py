#!/usr/bin/env python3
"""
XTL — post-deploy calendar verification.

Usage (server):
    /opt/xauapi/venv/bin/python verify_calendar.py

Exit 0 = clean, 1 = problems found.
"""
import json
import os
import sys
from datetime import datetime, timezone, timedelta

REDIS_URL = os.getenv(
    "REDIS_URL", "redis://default:xau12345@10.0.0.132:6379/0"
)
KEY = "xtl:news:calendar:daily"

import redis

R = redis.from_url(REDIS_URL, decode_responses=True)
raw = R.get(KEY)
if not raw:
    print("FAIL  calendar key missing or expired — gate is running blind")
    sys.exit(1)

data = json.loads(raw)
events = data.get("events", [])
now = datetime.now(timezone.utc)
age_min = (now.timestamp() * 1000 - int(data.get("fetched_at_ms") or 0)) / 60000

print(f"source={data.get('source')}  count={len(events)}  age={age_min:.0f}m  now={now:%Y-%m-%d %H:%M}Z")
print("=" * 96)
print(f"{'event time UTC':<18} {'ccy':<4} {'known':<6} {'block window':<16} event")
print("-" * 96)

problems = []
has_flag = False

for e in events:
    dt = datetime.fromtimestamp(e["time_ms"] / 1000, tz=timezone.utc)
    known = e.get("time_known")
    if known is not None:
        has_flag = True
    name = e.get("event", "")
    ccy = e.get("currency", "")
    pre = int(e.get("pre_block_min") or 0)
    post = int(e.get("post_block_min") or 0)
    stab = int(e.get("stabilization_min") or 0)

    if known is False:
        win = "— no gate —"
    else:
        win = (f"{dt - timedelta(minutes=pre):%H:%M}-"
               f"{dt + timedelta(minutes=post + stab):%H:%M}")

    mark = " "
    # midnight + not explicitly flagged timeless == the collapsed-group bug
    if dt.hour == 0 and dt.minute == 0 and known is not False:
        mark = "!"
        problems.append(f"{dt:%Y-%m-%d} {ccy} {name} — 00:00 with time_known={known}")
    # rate decisions must carry the 60m pre-block
    lname = name.lower()
    if any(k in lname for k in (
        "rate decision", "monetary policy statement", "monetary policy summary",
        "official bank rate", "official cash rate", "federal funds rate",
        "fomc statement", "main refinancing", "bank rate votes",
    )) and pre < 60 and known is not False:
        mark = "!"
        problems.append(f"{dt:%Y-%m-%d} {ccy} {name} — rate decision with pre={pre}m (want 60m)")

    print(f"{mark}{dt:%Y-%m-%d %H:%M}   {ccy:<4} {str(known):<6} {win:<16} {name}")

print("=" * 96)

if not has_flag:
    problems.append(
        "no event carries time_known — calendar_import.py is stripping the flag "
        "(old build, or CSV predates the patch)"
    )

if age_min > 26 * 60:
    problems.append(f"calendar is {age_min/60:.1f}h old — past TTL, re-import")

if problems:
    print(f"\n{len(problems)} PROBLEM(S):")
    for p in problems:
        print(f"  ! {p}")
    sys.exit(1)

print("\nOK — no midnight collapses, rate decisions carry 60m pre-blocks, flag present.")
sys.exit(0)
