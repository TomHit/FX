import pyotp

ISSUER = "XauTrendLab"

def generate_secret() -> str:
    """
    Generate a 160-bit base32 secret compatible with Google/Microsoft Authenticator.
    """
    return pyotp.random_base32()  # 32 chars, base32

def make_otpauth_uri(secret: str, username_or_email: str) -> str:
    """
    Build an otpauth:// URI for QR encoding.
    """
    label = f"{ISSUER}:{username_or_email}"
    return pyotp.totp.TOTP(secret).provisioning_uri(name=label, issuer_name=ISSUER)

def verify_totp(secret: str, code: str) -> bool:
    """
    Verify a 6–8 digit TOTP with small clock skew tolerance.
    """
    if not secret:
        return False
    code = (code or "").strip().replace(" ", "")
    if not (code.isdigit() and 6 <= len(code) <= 8):
        return False
    # valid_window=1 allows ±30s skew, interval is 30s by default
    return pyotp.TOTP(secret).verify(code, valid_window=1)


