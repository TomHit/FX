import io
import base64
import segno

def qr_svg(text: str, scale: int = 8, border: int = 1) -> str:
    """
    Return an SVG XML string (UTF-8) for the given payload.
    segno writes *bytes* to file-like objects, so use BytesIO, then decode.
    """
    q = segno.make(text, error='m')
    buf = io.BytesIO()
    # Produce compact SVG (no XML declaration); adjust as you prefer
    q.save(buf, kind='svg', scale=scale, border=border, xmldecl=False)
    return buf.getvalue().decode('utf-8')

def qr_png_b64(text: str, scale: int = 6, border: int = 1) -> str:
    """
    Return a base64-encoded PNG (no data: prefix).
    """
    q = segno.make(text, error='m')
    buf = io.BytesIO()
    q.save(buf, kind='png', scale=scale, border=border)
    return base64.b64encode(buf.getvalue()).decode('ascii')
