# utils/mfa.py
from enum import Enum
from datetime import datetime, timezone
# Adjust this import to your project layout
# from models import UserMFA

class MFAState(str, Enum):
    disabled = "disabled"   # no secret stored
    pending  = "pending"    # secret created, not verified
    enabled  = "enabled"    # verified at least once

def get_totp_state(db, user_id: int) -> "MFAState":
    rec = db.query(UserMFA).filter_by(user_id=user_id).one_or_none()
    if not rec or not getattr(rec, "totp_secret_hash", None):
        return MFAState.disabled
    return MFAState.enabled if getattr(rec, "totp_verified_at", None) else MFAState.pending

def mark_pending(rec) -> None:
    rec.totp_verified_at = None

def mark_enabled(rec) -> None:
    rec.totp_verified_at = datetime.now(timezone.utc)

def clear_totp(rec) -> None:
    rec.totp_secret_hash = None
    rec.totp_verified_at = None
