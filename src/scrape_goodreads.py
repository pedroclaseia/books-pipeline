# src/scrape_goodreads.py

# Librerías estándar: manejar JSON, buscar texto, esperar, rutas y fechas
import json
import re
import time
from pathlib import Path
from datetime import datetime, timezone

# Librerías externas:
# - BeautifulSoup: ayuda a leer el HTML de la página
# - Selenium: abre un navegador "de verdad" y simula un usuario
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Funciones auxiliares para extraer ISBN desde la página de cada libro
from utils_isbn import *

# ---------------------------------------------------------
# Definición de carpetas donde se guardan los datos
# ---------------------------------------------------------
# ROOT es la carpeta del proyecto (un nivel por encima de /src)
ROOT = Path(__file__).resolve().parents[1]
# landing: donde se guardará el JSON con los libros de Goodreads
LANDING = ROOT / "landing"
# standard y docs se crean por consistencia, aunque aquí no se usen
STANDARD = ROOT / "standard"
DOCS = ROOT / "docs"
for p in [LANDING, STANDARD, DOCS]:
    p.mkdir(parents=True, exist_ok=True)

# URL base para buscar en Goodreads; {query} se sustituye por el texto de búsqueda
GOODREADS_SEARCH = "https://www.goodreads.com/search?q={query}"

# Texto que el navegador enviará para identificarse (parece un Chrome real)
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

# ---------------------------------------------------------
# Crea y configura el navegador controlado por Selenium
# ---------------------------------------------------------
def _build_driver(headless: bool = True):
    opts = Options()
    # headless=True hace que el navegador no se vea en pantalla
    if headless:
        opts.add_argument("--headless=new")
    # Usa el user-agent definido arriba
    opts.add_argument(f"--user-agent={USER_AGENT}")
    # Ajustes para que la página no detecte tan fácilmente que es un bot
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--no-sandbox")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)

    # Crea el navegador Chrome
    driver = webdriver.Chrome(options=opts)

    # Truco extra para ocultar la propiedad "webdriver" en JavaScript
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source": """
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined })
        """
    })
    return driver

# Limpia espacios repetidos en un texto (deja solo uno entre palabras)
def _clean_spaces(s: str):
    return re.sub(r"\s+", " ", s).strip() if s else None

# A partir del texto de rating, extrae la nota media y el número de votos
# Ejemplo de texto: "4.12 avg rating — 5,241 ratings"
def _parse_rating(text: str):
    if not text:
        return None, None
    # Busca un número como 4.12
    m = re.search(r"([0-5]\.\d+)", text)
    rating = float(m.group(1)) if m else None
    # Busca el número total de valoraciones (quitando comas)
    m2 = re.search(r"([\d,]+)\s+ratings", text)
    ratings_count = int(m2.group(1).replace(",", "")) if m2 else None
    return rating, ratings_count

# ---------------------------------------------------------
# Función principal: hace la búsqueda y recoge los libros
# ---------------------------------------------------------
def scrape_goodreads(query: str = "data science", min_items: int = 12, headless: bool = True):
    # Crea el navegador
    driver = _build_driver(headless=headless)

    # Construye la URL con la búsqueda (espacios → '+')
    url = GOODREADS_SEARCH.format(query=query.replace(" ", "+"))
    driver.get(url)

    # Pequeñas acciones para parecer más humano (mover el ratón, esperar)
    actions = ActionChains(driver)
    actions.move_by_offset(10, 10).pause(0.5).perform()
    time.sleep(1.2)

    # Espera hasta que aparezca la tabla de resultados
    WebDriverWait(driver, 15).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "table.tableList"))
    )

    # Hace varios scrolls hacia abajo para cargar más resultados
    for _ in range(3):
        driver.execute_script("window.scrollBy(0, 600);")
        time.sleep(0.8)

    # Descarga el HTML de la página y se lo pasa a BeautifulSoup
    soup = BeautifulSoup(driver.page_source, "html.parser")
    table = soup.select_one("table.tableList")
    rows = table.select("tr") if table else []

    books = []
    # Recorre cada fila de la tabla (cada libro)
    for r in rows:
        # Busca elementos HTML donde están el título, autor y rating
        title_a = r.select_one("a.bookTitle")
        author_a = r.select_one("a.authorName")
        rating_span = r.select_one("span.minirating")

        # Extrae texto limpio de esos elementos
        title = _clean_spaces(title_a.get_text(strip=True) if title_a else None)
        author = _clean_spaces(author_a.get_text(strip=True) if author_a else None)
        rating, ratings_count = _parse_rating(rating_span.get_text(strip=True) if rating_span else "")

        # Construye la URL completa del libro
        link = title_a.get("href") if title_a else None
        book_url = f"https://www.goodreads.com{link}" if link and link.startswith("/") else link

        # Intenta conseguir los ISBN entrando a la página del libro
        isbn10, isbn13 = (None, None)
        if book_url:
            # extract_isbn_from_book_page está definida en utils_isbn.py
            isbn10, isbn13 = extract_isbn_from_book_page(driver, book_url)

        # Guarda los datos de este libro en una lista
        books.append({
            "title": title,
            "author": author,
            "rating": rating,
            "ratings_count": ratings_count,
            "book_url": book_url,
            "isbn10": isbn10,
            "isbn13": isbn13
        })

        # Si ya se han recogido suficientes libros, paramos
        if len(books) >= min_items:
            break

    # Cierra el navegador
    driver.quit()

    # Prepara un envoltorio con metadatos (cuándo y cómo se hizo la extracción)
    ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
    payload = {
        "metadata": {
            "source": "goodreads_search",
            "search_query": query,
            "user_agent": USER_AGENT,
            "fetched_at_utc": ts,
            "total": len(books),
            "url": url
        },
        "records": books
    }

    # Guarda todo en un archivo JSON en la carpeta landing
    out_path = LANDING / "goodreads_books.json"
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    print(f"Saved {out_path} with {len(books)} records.")

# Si ejecutas este archivo directamente (python -m src.scrape_goodreads),
# se lanzará la función principal con los valores por defecto.
if __name__ == "__main__":
    scrape_goodreads()
