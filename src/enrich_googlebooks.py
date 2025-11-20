# src/enrich_googlebooks.py

# Librerías estándar para trabajar con CSV, tiempos de espera, JSON y rutas de archivos
import csv
import time
import json
from pathlib import Path
from datetime import datetime, timezone

# Funciones auxiliares para limpiar/validar ISBN (definidas en utils_isbn.py)
from utils_isbn import *

# Librería para hacer peticiones HTTP a la API de Google Books
import requests

# Funciones de calidad/normalización (aunque en este archivo casi no se usan directamente)
from utils_quality import *


# ---------------------------------------------------------
# Definición de carpetas donde se guardan los datos
# ---------------------------------------------------------

# ROOT es la carpeta del proyecto (un nivel por encima de /src)
ROOT = Path(__file__).resolve().parents[1]

# landing: donde se guardan archivos de entrada/salida “tal cual” (JSON/CSV)
LANDING = ROOT / "landing"

# standard y docs se crean por consistencia, aunque aquí no se usen
STANDARD = ROOT / "standard"
DOCS = ROOT / "docs"

# Crea las carpetas si no existen
for p in [LANDING, STANDARD, DOCS]:
    p.mkdir(parents=True, exist_ok=True)


# URL base de la API de Google Books
GB_ENDPOINT = "https://www.googleapis.com/books/v1/volumes"


# ---------------------------------------------------------
# Funciones auxiliares internas
# ---------------------------------------------------------

def _norm_list(xs):
    """
    Recibe una lista (o un valor suelto) y devuelve:
    - una lista sin elementos vacíos
    - sin duplicados
    - con los textos limpiados de espacios.
    Se usa para autores y categorías.
    """
    if not xs:
        return []
    if isinstance(xs, list):
        return list(dict.fromkeys([str(x).strip() for x in xs if str(x).strip()]))
    return [str(xs).strip()]


def _extract_isbns(identifiers):
    """
    A partir del bloque 'industryIdentifiers' de Google Books,
    saca el isbn10 y el isbn13 si existen.
    """
    isbn10 = None
    isbn13 = None
    for it in identifiers or []:
        if it.get("type") == "ISBN_13":
            isbn13 = only_digits_x(it.get("identifier"))
        if it.get("type") == "ISBN_10":
            isbn10 = only_digits_x(it.get("identifier"))
    return isbn10, isbn13


# ---------------------------------------------------------
# Búsqueda de un libro en Google Books
# ---------------------------------------------------------
def search_book(item):
    """
    Recibe un libro (con título/autor/isbn del JSON de Goodreads)
    y busca la mejor coincidencia en la API de Google Books.
    Devuelve un diccionario con los campos necesarios para el CSV
    o None si no encuentra nada.
    """
    q = None

    # Elige el mejor ISBN disponible (valida y convierte si hace falta)
    isbn13, isbn10 = pick_best_isbn(item.get("isbn13"), item.get("isbn10"))

    # Construye la consulta 'q' para Google Books:
    # - primero prueba con isbn:...
    # - si no hay ISBN, prueba con título + autor
    if isbn13:
        q = f"isbn:{isbn13}"
    elif item.get("title") and item.get("author"):
        q = f'intitle:"{item["title"]}" inauthor:"{item["author"]}"'
    elif item.get("title"):
        q = f'intitle:"{item["title"]}"'
    if not q:
        # Si no hay nada con lo que buscar, se devuelve None
        return None

    # Parámetros para la petición: solo 1 resultado (maxResults=1)
    params = {"q": q, "maxResults": 1, "printType": "books", "langRestrict": ""}

    # Llamada HTTP a la API de Google Books
    r = requests.get(GB_ENDPOINT, params=params, timeout=20)
    if r.status_code != 200:
        # Si la respuesta no es correcta (no es 200 OK), se ignora
        return None
    data = r.json()
    if not data.get("items"):
        # Si no hay items, no se encontró nada
        return None

    # Tomamos el primer resultado devuelto
    vol = data["items"][0]
    info = vol.get("volumeInfo", {})
    sale = vol.get("saleInfo", {})

    # Google puede poner el precio en 'listPrice' o en 'retailPrice'
    list_price = (sale.get("listPrice") or {}) if isinstance(sale, dict) else {}
    retail_price = (sale.get("retailPrice") or {}) if isinstance(sale, dict) else {}
    price_amt = list_price.get("amount") or retail_price.get("amount")
    price_cur = list_price.get("currencyCode") or retail_price.get("currencyCode")

    # Saca los ISBN que vengan en la respuesta de Google Books
    isbn10_g, isbn13_g = _extract_isbns(info.get("industryIdentifiers", []))

    # Devuelve un diccionario con todos los campos que queremos en el CSV
    return {
        "gb_id": vol.get("id"),
        "title": info.get("title"),
        "subtitle": info.get("subtitle"),
        "authors": ";".join(_norm_list(info.get("authors"))),
        "publisher": info.get("publisher"),
        "pub_date": info.get("publishedDate"),
        "language": info.get("language"),
        "categories": ";".join(_norm_list(info.get("categories"))),
        # Prioriza el isbn13 que viene de Google; si no, usa el calculado
        "isbn13": isbn13_g or isbn13,
        "isbn10": isbn10_g,
        "price_amount": price_amt,
        "price_currency": price_cur
    }


# ---------------------------------------------------------
# Proceso principal: leer JSON de Goodreads y crear CSV de Google Books
# ---------------------------------------------------------
def enrich_from_goodreads():
    """
    Lee los libros obtenidos de Goodreads (JSON en landing),
    consulta la API de Google Books para cada uno y
    escribe un CSV con los datos enriquecidos.
    Además guarda un archivo de metadatos con información de la ejecución.
    """
    src = LANDING / "goodreads_books.json"
    if not src.exists():
        # Si no existe el archivo de Goodreads, se avisa de que hay que ejecutarlo primero
        raise FileNotFoundError(f"No existe {src}, ejecuta scrape_goodreads primero.")

    # Carga el contenido del JSON: metadatos + lista de libros
    payload = json.loads(src.read_text(encoding="utf-8"))
    records = payload.get("records", [])

    # Ruta de salida para el CSV de Google Books
    out = LANDING / "googlebooks_books.csv"

    # Abre el CSV para escritura
    with out.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "gb_id","title","subtitle","authors","publisher",
                "pub_date","language","categories","isbn13","isbn10",
                "price_amount","price_currency"
            ]
        )
        # Escribe la fila de cabecera (nombres de columnas)
        writer.writeheader()

        # Recorre todos los libros del JSON de Goodreads
        for r in records:
            # Busca el libro correspondiente en Google Books
            res = search_book(r)
            if res:
                # Si encuentra datos, escribe una fila en el CSV
                writer.writerow(res)
            # Pausa ligera para no hacer demasiadas peticiones seguidas
            time.sleep(0.4)

    # Prepara un pequeño JSON con metadatos sobre el CSV creado
    meta = {
        "separator": ",",
        "encoding": "UTF-8",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "source": "google_books_api",
        "input_file": str(src),
        "output_file": str(out)
    }

    # Guarda los metadatos en landing/googlebooks_meta.json
    (LANDING / "googlebooks_meta.json").write_text(
        json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    print(f"Saved {out}")


# Si ejecutas este archivo directamente (python -m src.enrich_googlebooks),
# se ejecuta el proceso principal de enriquecimiento.
if __name__ == "__main__":
    enrich_from_goodreads()
