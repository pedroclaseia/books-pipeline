# src/utils_isbn.py
import re
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import pandas as pd  

def only_digits_x(s: str):
    if not s:
        return None
    s = re.sub(r"[^0-9Xx]", "", s)
    return s.upper()

def is_valid_isbn10(isbn: str) -> bool:
    s = only_digits_x(isbn)
    if not s or len(s) != 10:
        return False
    total = 0
    for i, ch in enumerate(s[:9], start=1):
        if not ch.isdigit():
            return False
        total += i * int(ch)
    check = s[9]
    check_val = 10 if check == "X" else (int(check) if check.isdigit() else -1)
    if check_val < 0:
        return False
    return (total % 11) == check_val

def is_valid_isbn13(isbn: str) -> bool:
    s = only_digits_x(isbn)
    if not s or len(s) != 13 or not s.isdigit():
        return False
    total = 0
    for i, ch in enumerate(s):
        w = 1 if i % 2 == 0 else 3
        total += w * int(ch)
    return (total % 10) == 0

def to_isbn13_from10(isbn10: str):
    s = only_digits_x(isbn10)
    if not s or len(s) != 10:
        return None
    core = "978" + s[:-1]
    total = 0
    for i, ch in enumerate(core):
        w = 1 if i % 2 == 0 else 3
        total += w * int(ch)
    check = (10 - (total % 10)) % 10
    return core + str(check)

def pick_best_isbn(isbn13: str, isbn10: str):
    if is_valid_isbn13(isbn13 or ""):
        return only_digits_x(isbn13), isbn10
    if is_valid_isbn10(isbn10 or ""):
        return to_isbn13_from10(isbn10), only_digits_x(isbn10)
    return None, None

def extract_isbn_from_book_page(driver, url: str):
    # Abre una pestaña nueva para el detalle y trata de localizar ISBN
    try:
        driver.execute_script("window.open(arguments[0], '_blank');", url)
        driver.switch_to.window(driver.window_handles[-1])
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        time.sleep(1.0)
        html = driver.page_source
        soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text(" ", strip=True)

        isbn10 = None
        isbn13 = None

        # Patrones comunes en Goodreads
        m13 = re.search(r"ISBN(?:-13)?:?\s*([\d-]{13,17})", text, re.IGNORECASE)
        if m13:
            isbn13 = re.sub(r"[^0-9Xx]", "", m13.group(1))
        m10 = re.search(r"ISBN(?:-10)?:?\s*([\dXx-]{10,13})", text, re.IGNORECASE)
        if m10:
            isbn10 = re.sub(r"[^0-9Xx]", "", m10.group(1))

        driver.close()
        driver.switch_to.window(driver.window_handles[0])
        return isbn10, isbn13
    except Exception:
        try:
            driver.close()
            driver.switch_to.window(driver.window_handles[0])
        except Exception:
            pass
        return None, None

def ensure_best_isbn_columns(df, isbn13_cols=None, isbn10_cols=None, out13="best_isbn13", out10="best_isbn10"):

    if isbn13_cols is None:
        isbn13_cols = ["isbn13", "gr_isbn13", "gb_isbn13"]
    if isbn10_cols is None:
        isbn10_cols = ["isbn10", "gr_isbn10", "gb_isbn10"]

    def first_nonnull(row, cols):
        for c in cols:
            if c in row and pd.notna(row[c]) and str(row[c]).strip() != "":
                return str(row[c]).strip()
        return None

    # Asegura existencia de columnas objetivo para evitar KeyError
    if out13 not in df.columns:
        df[out13] = None
    if out10 not in df.columns:
        df[out10] = None

    # Calcula mejores ISBN por fila
    def compute_best(row):
        raw13 = first_nonnull(row, isbn13_cols)
        raw10 = first_nonnull(row, isbn10_cols)
        b13, b10 = pick_best_isbn(raw13, raw10)
        return pd.Series({out13: b13, out10: b10})

    best = df.apply(compute_best, axis=1)
    df[out13] = best[out13]
    df[out10] = best[out10]

    return df
