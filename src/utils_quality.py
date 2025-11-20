# src/utils_quality.py

# Librerías para trabajar con texto, números, hashes y fechas
import re
import math
import hashlib
from datetime import datetime

# ---------------------------------------------------------
# Patrones para validar formatos
# ---------------------------------------------------------

# Fechas ISO simples:
#   - "YYYY"
#   - "YYYY-MM"
#   - "YYYY-MM-DD"
ISO_DATE_RE = re.compile(r"^\d{4}(-\d{2}){0,2}$")  # YYYY o YYYY-MM o YYYY-MM-DD

# Idiomas en formato BCP-47, por ejemplo:
#   - "es"
#   - "en-US"
#   - "pt-BR"
BCP47_RE = re.compile(r"^[A-Za-z]{2,3}(-[A-Za-z0-9]{2,8})*$")

# Monedas en formato ISO-4217: exactamente 3 letras mayúsculas (EUR, USD, MXN...)
CURRENCY_RE = re.compile(r"^[A-Z]{3}$")


# ---------------------------------------------------------
# Normalización de fechas al formato ISO-8601 completo
# ---------------------------------------------------------
def norm_date_iso(s: str):
    """
    Recibe una fecha en forma de texto y la convierte a:
    - 'YYYY-01-01' si solo se ha dado el año
    - 'YYYY-MM-01' si se ha dado año y mes
    - 'YYYY-MM-DD' si se ha dado la fecha completa
    Si el formato no es válido o la fecha no existe, devuelve None.
    """
    if not s:
        return None
    s = str(s).strip()

    # Primero comprueba que cumple una de las formas básicas con la expresión regular
    if not ISO_DATE_RE.match(s):
        return None

    parts = s.split("-")
    try:
        if len(parts) == 1:
            # Solo año → se completa como 1 de enero
            return f"{int(parts[0]):04d}-01-01"
        if len(parts) == 2:
            # Año y mes → se completa como día 1
            return f"{int(parts[0]):04d}-{int(parts[1]):02d}-01"
        if len(parts) == 3:
            # Año, mes y día → se valida que la fecha exista
            y, m, d = map(int, parts)
            datetime(y, m, d)  # lanza error si la fecha no es real
            return f"{y:04d}-{m:02d}-{d:02d}"
    except Exception:
        # Si algo falla (fecha inválida, texto raro, etc.), devuelve None
        return None
    return None

# ---------------------------------------------------------
# Normalización de idiomas a BCP-47
# ---------------------------------------------------------
def norm_lang_bcp47(s: str):
    """
    Recibe un código de idioma (ej. 'es', 'en-US') y devuelve:
    - el mismo código si cumple el patrón BCP-47
    - None si no es válido
    """
    if not s:
        return None
    s = str(s).strip()
    return s if BCP47_RE.match(s) else None

# ---------------------------------------------------------
# Normalización de monedas a ISO-4217
# ---------------------------------------------------------
def norm_currency_iso4217(s: str):
    """
    Recibe un código de moneda y devuelve:
    - el código en mayúsculas si tiene 3 letras (ej. EUR, USD)
    - None si no cumple el patrón.
    """
    if not s:
        return None
    s = str(s).strip().upper()
    return s if CURRENCY_RE.match(s) else None

# ---------------------------------------------------------
# Conversión segura a número decimal
# ---------------------------------------------------------
def to_decimal(x):
    """
    Intenta convertir un valor a número decimal (float).
    - Devuelve None si el valor está vacío, es NaN o no se puede convertir.
    - Admite comas como separador decimal (las cambia por punto).
    """
    # Si viene None o ya es un float NaN, no sirve
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return None
    try:
        # Primer intento: conversión directa a float
        return float(x)
    except Exception:
        try:
            # Segundo intento: cambiar coma por punto y volver a intentar
            return float(str(x).replace(",", "."))
        except Exception:
            return None

# ---------------------------------------------------------
# Generación de un identificador estable a partir de varios campos
# ---------------------------------------------------------
def stable_id(*parts):
    """
    Crea un identificador único y estable en forma de hash SHA-1
    a partir de varios trozos de texto (por ejemplo, título, autor, año, editorial).

    - Une todos los trozos en minúsculas y sin espacios sobrantes.
    - Usa '||' como separador para evitar confusiones.
    - Devuelve una cadena hexadecimal (40 caracteres).
    """
    # Une solo los valores no vacíos, en minúsculas y recortados
    base = "||".join([str(p).strip().lower() for p in parts if p is not None])
    # Calcula el hash SHA-1 del texto resultante
    return hashlib.sha1(base.encode("utf-8")).hexdigest()
