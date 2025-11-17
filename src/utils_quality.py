# src/utils_quality.py
import re
import math
import hashlib
from datetime import datetime

ISO_DATE_RE = re.compile(r"^\d{4}(-\d{2}){0,2}$")  # YYYY o YYYY-MM o YYYY-MM-DD
BCP47_RE = re.compile(r"^[A-Za-z]{2,3}(-[A-Za-z0-9]{2,8})*$")
CURRENCY_RE = re.compile(r"^[A-Z]{3}$")

def norm_date_iso(s: str):
    if not s:
        return None
    s = str(s).strip()
    if not ISO_DATE_RE.match(s):
        return None
    parts = s.split("-")
    try:
        if len(parts) == 1:
            return f"{int(parts[0]):04d}-01-01"
        if len(parts) == 2:
            return f"{int(parts[0]):04d}-{int(parts[1]):02d}-01"
        if len(parts) == 3:
            # validación básica de fecha
            y, m, d = map(int, parts)
            datetime(y, m, d)
            return f"{y:04d}-{m:02d}-{d:02d}"
    except Exception:
        return None
    return None

def norm_lang_bcp47(s: str):
    if not s:
        return None
    s = str(s).strip()
    return s if BCP47_RE.match(s) else None

def norm_currency_iso4217(s: str):
    if not s:
        return None
    s = str(s).strip().upper()
    return s if CURRENCY_RE.match(s) else None

def to_decimal(x):
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return None
    try:
        return float(x)
    except Exception:
        try:
            return float(str(x).replace(",", "."))
        except Exception:
            return None

def stable_id(*parts):
    base = "||".join([str(p).strip().lower() for p in parts if p is not None])
    return hashlib.sha1(base.encode("utf-8")).hexdigest()
