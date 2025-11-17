# src/scrape_goodreads.py
import json
import re
import time
from pathlib import Path
from datetime import datetime, timezone

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from utils_isbn import *

# Rutas base
ROOT = Path(__file__).resolve().parents[1]
LANDING = ROOT / "landing"
STANDARD = ROOT / "standard"
DOCS = ROOT / "docs"
for p in [LANDING, STANDARD, DOCS]:
    p.mkdir(parents=True, exist_ok=True)

GOODREADS_SEARCH = "https://www.goodreads.com/search?q={query}"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

def _build_driver(headless: bool = True):
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument(f"--user-agent={USER_AGENT}")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--no-sandbox")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    driver = webdriver.Chrome(options=opts)
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source": """
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined })
        """
    })
    return driver

def _clean_spaces(s: str):
    return re.sub(r"\s+", " ", s).strip() if s else None

def _parse_rating(text: str):
    # e.g. "4.12 avg rating — 5,241 ratings"
    if not text:
        return None, None
    m = re.search(r"([0-5]\.\d+)", text)
    rating = float(m.group(1)) if m else None
    m2 = re.search(r"([\d,]+)\s+ratings", text)
    ratings_count = int(m2.group(1).replace(",", "")) if m2 else None
    return rating, ratings_count



def scrape_goodreads(query: str = "data science", min_items: int = 12, headless: bool = True):
    driver = _build_driver(headless=headless)
    url = GOODREADS_SEARCH.format(query=query.replace(" ", "+"))
    driver.get(url)

    # Interacciones humanas básicas
    actions = ActionChains(driver)
    actions.move_by_offset(10, 10).pause(0.5).perform()
    time.sleep(1.2)
    WebDriverWait(driver, 15).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "table.tableList"))
    )

    # Scrolls suaves
    for _ in range(3):
        driver.execute_script("window.scrollBy(0, 600);")
        time.sleep(0.8)

    soup = BeautifulSoup(driver.page_source, "html.parser")
    table = soup.select_one("table.tableList")
    rows = table.select("tr") if table else []

    books = []
    for r in rows:
        title_a = r.select_one("a.bookTitle")
        author_a = r.select_one("a.authorName")
        rating_span = r.select_one("span.minirating")

        title = _clean_spaces(title_a.get_text(strip=True) if title_a else None)
        author = _clean_spaces(author_a.get_text(strip=True) if author_a else None)
        rating, ratings_count = _parse_rating(rating_span.get_text(strip=True) if rating_span else "")

        link = title_a.get("href") if title_a else None
        book_url = f"https://www.goodreads.com{link}" if link and link.startswith("/") else link

        isbn10, isbn13 = (None, None)
        if book_url:
            isbn10, isbn13 = extract_isbn_from_book_page(driver, book_url)

        books.append({
            "title": title,
            "author": author,
            "rating": rating,
            "ratings_count": ratings_count,
            "book_url": book_url,
            "isbn10": isbn10,
            "isbn13": isbn13
        })
        if len(books) >= min_items:
            break

    driver.quit()

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
    out_path = LANDING / "goodreads_books.json"
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print(f"Saved {out_path} with {len(books)} records.")

if __name__ == "__main__":
    scrape_goodreads()
