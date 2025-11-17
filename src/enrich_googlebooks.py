# src/enrich_googlebooks.py
import csv
import time
import json
from pathlib import Path
from datetime import datetime, timezone
from utils_isbn import *
import requests
from utils_quality import *

ROOT = Path(__file__).resolve().parents[1]
LANDING = ROOT / "landing"
STANDARD = ROOT / "standard"
DOCS = ROOT / "docs"
for p in [LANDING, STANDARD, DOCS]:
    p.mkdir(parents=True, exist_ok=True)

GB_ENDPOINT = "https://www.googleapis.com/books/v1/volumes"

def _norm_list(xs):
    if not xs:
        return []
    if isinstance(xs, list):
        return list(dict.fromkeys([str(x).strip() for x in xs if str(x).strip()]))
    return [str(xs).strip()]

def _extract_isbns(identifiers):
    isbn10 = None
    isbn13 = None
    for it in identifiers or []:
        if it.get("type") == "ISBN_13":
            isbn13 = only_digits_x(it.get("identifier"))
        if it.get("type") == "ISBN_10":
            isbn10 = only_digits_x(it.get("identifier"))
    return isbn10, isbn13

def search_book(item):
    q = None
    isbn13, isbn10 = pick_best_isbn(item.get("isbn13"), item.get("isbn10"))
    if isbn13:
        q = f"isbn:{isbn13}"
    elif item.get("title") and item.get("author"):
        q = f'intitle:"{item["title"]}" inauthor:"{item["author"]}"'
    elif item.get("title"):
        q = f'intitle:"{item["title"]}"'
    if not q:
        return None
    params = {"q": q, "maxResults": 1, "printType": "books", "langRestrict": ""}
    r = requests.get(GB_ENDPOINT, params=params, timeout=20)
    if r.status_code != 200:
        return None
    data = r.json()
    if not data.get("items"):
        return None
    vol = data["items"][0]
    info = vol.get("volumeInfo", {})
    sale = vol.get("saleInfo", {})
    list_price = (sale.get("listPrice") or {}) if isinstance(sale, dict) else {}
    retail_price = (sale.get("retailPrice") or {}) if isinstance(sale, dict) else {}
    price_amt = list_price.get("amount") or retail_price.get("amount")
    price_cur = list_price.get("currencyCode") or retail_price.get("currencyCode")
    isbn10_g, isbn13_g = _extract_isbns(info.get("industryIdentifiers", []))

    return {
        "gb_id": vol.get("id"),
        "title": info.get("title"),
        "subtitle": info.get("subtitle"),
        "authors": ";".join(_norm_list(info.get("authors"))),
        "publisher": info.get("publisher"),
        "pub_date": info.get("publishedDate"),
        "language": info.get("language"),
        "categories": ";".join(_norm_list(info.get("categories"))),
        "isbn13": isbn13_g or isbn13,
        "isbn10": isbn10_g,
        "price_amount": price_amt,
        "price_currency": price_cur
    }

def enrich_from_goodreads():
    src = LANDING / "goodreads_books.json"
    if not src.exists():
        raise FileNotFoundError(f"No existe {src}, ejecuta scrape_goodreads primero.")

    payload = json.loads(src.read_text(encoding="utf-8"))
    records = payload.get("records", [])
    out = LANDING / "googlebooks_books.csv"

    with out.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "gb_id","title","subtitle","authors","publisher",
                "pub_date","language","categories","isbn13","isbn10",
                "price_amount","price_currency"
            ]
        )
        writer.writeheader()
        for r in records:
            res = search_book(r)
            if res:
                writer.writerow(res)
            time.sleep(0.4)  # respetar rate-limit ligero

    meta = {
        "separator": ",",
        "encoding": "UTF-8",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "source": "google_books_api",
        "input_file": str(src),
        "output_file": str(out)
    }
    # Meta datos googlebooks
    (LANDING / "googlebooks_meta.json").write_text(
        json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    print(f"Saved {out}")

if __name__ == "__main__":
    enrich_from_goodreads()
