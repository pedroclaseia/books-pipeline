# src/utils_isbn.py

# re: sirve para buscar texto con expresiones regulares
import re

# Estas librerías se usan para abrir páginas de libros y leer su contenido
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time

# pandas: se usa para trabajar con tablas de datos (DataFrames)
import pandas as pd


# ---------------------------------------------------------
# Funciones de limpieza y validación de ISBN
# ---------------------------------------------------------

# Deja solo dígitos y la letra X en una cadena; devuelve en mayúsculas
def only_digits_x(s: str):
    if not s:
        return None
    # Quita todo lo que no sea número o X/x
    s = re.sub(r"[^0-9Xx]", "", s)
    return s.upper()

# Comprueba si un código es un ISBN-10 válido
def is_valid_isbn10(isbn: str) -> bool:
    s = only_digits_x(isbn)
    # Debe tener exactamente 10 caracteres
    if not s or len(s) != 10:
        return False
    total = 0
    # Los 9 primeros tienen pesos 1,2,3,...,9
    for i, ch in enumerate(s[:9], start=1):
        if not ch.isdigit():
            return False
        total += i * int(ch)
    # El último carácter puede ser un número o la letra X (que vale 10)
    check = s[9]
    check_val = 10 if check == "X" else (int(check) if check.isdigit() else -1)
    if check_val < 0:
        return False
    # Regla de ISBN-10: la suma debe coincidir con el dígito de control
    return (total % 11) == check_val

# Comprueba si un código es un ISBN-13 válido
def is_valid_isbn13(isbn: str) -> bool:
    s = only_digits_x(isbn)
    # Debe tener 13 dígitos
    if not s or len(s) != 13 or not s.isdigit():
        return False
    total = 0
    # Los pesos alternan 1 y 3: 1,3,1,3,...
    for i, ch in enumerate(s):
        w = 1 if i % 2 == 0 else 3
        total += w * int(ch)
    # Regla de ISBN-13: la suma debe terminar en 0
    return (total % 10) == 0

# Convierte un ISBN-10 válido a su ISBN-13 equivalente (prefijo 978)
def to_isbn13_from10(isbn10: str):
    s = only_digits_x(isbn10)
    if not s or len(s) != 10:
        return None
    # Se añade el prefijo 978 y se recalcula el dígito de control
    core = "978" + s[:-1]
    total = 0
    for i, ch in enumerate(core):
        w = 1 if i % 2 == 0 else 3
        total += w * int(ch)
    check = (10 - (total % 10)) % 10
    return core + str(check)

# Dado un isbn13 y un isbn10, elige el “mejor” (el que sea válido)
def pick_best_isbn(isbn13: str, isbn10: str):
    # Si ya tenemos un ISBN-13 válido, lo usamos
    if is_valid_isbn13(isbn13 or ""):
        return only_digits_x(isbn13), isbn10
    # Si no, pero hay un ISBN-10 válido, lo convertimos a 13
    if is_valid_isbn10(isbn10 or ""):
        return to_isbn13_from10(isbn10), only_digits_x(isbn10)
    # Si ninguno sirve, devolvemos nada
    return None, None

# ---------------------------------------------------------
# Extraer ISBN visitando la página de un libro con Selenium
# ---------------------------------------------------------
def extract_isbn_from_book_page(driver, url: str):
    """
    Abre la página de detalle de un libro de Goodreads en una pestaña nueva
    e intenta encontrar el ISBN-10 e ISBN-13 en el texto de la página.
    """
    try:
        # Abre la URL en una nueva pestaña del navegador
        driver.execute_script("window.open(arguments[0], '_blank');", url)
        driver.switch_to.window(driver.window_handles[-1])

        # Espera a que el cuerpo de la página esté cargado
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        time.sleep(1.0)

        # Descarga el HTML y se lo pasa a BeautifulSoup
        html = driver.page_source
        soup = BeautifulSoup(html, "html.parser")
        # Convierte todo el contenido de la página a un solo texto
        text = soup.get_text(" ", strip=True)

        isbn10 = None
        isbn13 = None

        # Busca patrones de texto típicos donde aparece el ISBN-13
        m13 = re.search(r"ISBN(?:-13)?:?\s*([\d-]{13,17})", text, re.IGNORECASE)
        if m13:
            # Limpia para dejar solo dígitos o X/x
            isbn13 = re.sub(r"[^0-9Xx]", "", m13.group(1))
        # Lo mismo para ISBN-10
        m10 = re.search(r"ISBN(?:-10)?:?\s*([\dXx-]{10,13})", text, re.IGNORECASE)
        if m10:
            isbn10 = re.sub(r"[^0-9Xx]", "", m10.group(1))

        # Cierra la pestaña actual y vuelve a la principal
        driver.close()
        driver.switch_to.window(driver.window_handles[0])
        return isbn10, isbn13
    except Exception:
        # Ante cualquier error, intenta cerrar y volver a la pestaña principal
        try:
            driver.close()
            driver.switch_to.window(driver.window_handles[0])
        except Exception:
            pass
        return None, None

# ---------------------------------------------------------
# Asegurar columnas best_isbn13 / best_isbn10 en un DataFrame
# ---------------------------------------------------------
def ensure_best_isbn_columns(df, isbn13_cols=None, isbn10_cols=None, out13="best_isbn13", out10="best_isbn10"):
    """
    Dado un DataFrame (tabla) que puede tener varias columnas con ISBN,
    crea de forma segura dos columnas estándar:
      - best_isbn13
      - best_isbn10

    Busca los valores en las columnas indicadas, escoge el mejor ISBN
    para cada fila y evita errores aunque falten columnas.
    """

    # Columnas donde podría estar el isbn13
    if isbn13_cols is None:
        isbn13_cols = ["isbn13", "gr_isbn13", "gb_isbn13"]
    # Columnas donde podría estar el isbn10
    if isbn10_cols is None:
        isbn10_cols = ["isbn10", "gr_isbn10", "gb_isbn10"]

    # Devuelve el primer valor no vacío que encuentre en la fila para las columnas indicadas
    def first_nonnull(row, cols):
        for c in cols:
            if c in row and pd.notna(row[c]) and str(row[c]).strip() != "":
                return str(row[c]).strip()
        return None

    # Asegura que las columnas de salida existen, para evitar errores de clave
    if out13 not in df.columns:
        df[out13] = None
    if out10 not in df.columns:
        df[out10] = None

    # Calcula el mejor ISBN por fila usando pick_best_isbn
    def compute_best(row):
        raw13 = first_nonnull(row, isbn13_cols)
        raw10 = first_nonnull(row, isbn10_cols)
        b13, b10 = pick_best_isbn(raw13, raw10)
        # Devuelve una pequeña serie con los dos valores
        return pd.Series({out13: b13, out10: b10})

    # Aplica la función a cada fila y rellena las columnas finales
    best = df.apply(compute_best, axis=1)
    df[out13] = best[out13]
    df[out10] = best[out10]

    return df
