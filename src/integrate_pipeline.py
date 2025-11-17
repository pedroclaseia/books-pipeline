# src/integrate_pipeline.py
import json
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
import numpy as np

from utils_isbn import *
from utils_quality import *

# Rutas base
ROOT = Path(__file__).resolve().parents[1]
LANDING = ROOT / "landing"
STANDARD = ROOT / "standard"
DOCS = ROOT / "docs"
for p in [LANDING, STANDARD, DOCS]:
    p.mkdir(parents=True, exist_ok=True)

# ---------------------------
# Lectura de entradas landing
# ---------------------------
def read_inputs():
    gr_path = LANDING / "goodreads_books.json"
    gb_path = LANDING / "googlebooks_books.csv"
    if not gr_path.exists() or not gb_path.exists():
        raise FileNotFoundError("Faltan archivos en landing/. Ejecuta ETL previo.")
    gr = json.loads(gr_path.read_text(encoding="utf-8"))
    gr_df = pd.json_normalize(gr.get("records", []))
    gb_df = pd.read_csv(gb_path, dtype=str).replace({np.nan: None})
    return gr_df, gb_df

# ---------------------------
# Metadatos de ingesta
# ---------------------------
def annotate_sources(gr_df, gb_df):
    ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
    gr_df = gr_df.copy()
    gb_df = gb_df.copy()
    gr_df["source_name"] = "goodreads"
    gr_df["ingested_at"] = ts
    gb_df["source_name"] = "googlebooks"
    gb_df["ingested_at"] = ts
    return gr_df, gb_df

# -----------------------------------
# Construcción de book_source_detail
# -----------------------------------
def to_source_detail(gr_df, gb_df):
    def _add_idx(df, src):
        df = df.copy()
        df["source_id"] = [f"{src}-{i+1}" for i in range(len(df))]
        df["row_number"] = list(range(1, len(df)+1))
        df["source_file"] = "landing/goodreads_books.json" if src=="goodreads" else "landing/googlebooks_books.csv"
        df["source_name"] = src
        return df

    gr_d = _add_idx(gr_df, "goodreads")
    gb_d = _add_idx(gb_df, "googlebooks")

    # Unificar 'author' (GR) + 'authors' (GB) -> 'autor/es' y eliminar originales
    def unify_authors(df):
        df = df.copy()
        a_gr = df.get("author")
        a_gb = df.get("authors")
        s_gr = a_gr.map(lambda v: str(v).strip() if (v is not None and str(v).strip() and str(v).lower()!="nan") else None) if a_gr is not None else pd.Series([None]*len(df))
        s_gb = a_gb.map(lambda v: ";".join([t.strip() for t in str(v).split(";") if t.strip()]) if (v is not None and str(v).strip() and str(v).lower()!="nan") else None) if a_gb is not None else pd.Series([None]*len(df))

        def combine(g, b):
            vals = []
            if g:
                vals.extend([x.strip() for x in str(g).split(";") if x.strip()])
            if b:
                vals.extend([x.strip() for x in str(b).split(";") if x.strip()])
            seen = []
            for x in vals:
                if x.lower() not in [y.lower() for y in seen]:
                    seen.append(x)
            return ";".join(seen) if seen else None

        df["autor/es"] = [combine(g, b) for g, b in zip(s_gr, s_gb)]

        # Eliminar columnas originales si existen
        for c in ["author", "authors"]:
            if c in df.columns:
                df = df.drop(columns=[c])
        return df

    gr_d = unify_authors(gr_d)
    gb_d = unify_authors(gb_d)

    # Alinear columnas y concatenar
    cols = sorted(set(gr_d.columns).union(gb_d.columns))
    gr_d = gr_d.reindex(columns=cols)
    gb_d = gb_d.reindex(columns=cols)

    return pd.concat([gr_d, gb_d], ignore_index=True)

# ---------------------------
# Modelo canónico y merge
# ---------------------------
def canonicalize(gr_df, gb_df):
    # Renombrado controlado
    gr_small = gr_df.rename(columns={
        "title":"gr_title","author":"gr_author","isbn10":"gr_isbn10","isbn13":"gr_isbn13",
        "rating":"gr_rating","ratings_count":"gr_ratings_count","book_url":"gr_book_url"
    })
    gb_small = gb_df.rename(columns={
        "title":"gb_title","subtitle":"gb_subtitle","authors":"gb_authors","publisher":"gb_publisher",
        "pub_date":"gb_pub_date","language":"gb_language","categories":"gb_categories",
        "isbn13":"gb_isbn13","isbn10":"gb_isbn10","price_amount":"gb_price_amount","price_currency":"gb_price_currency",
        "gb_id":"gb_id"
    })

    # Asegurar columnas best_* de forma robusta
    gr_small = ensure_best_isbn_columns(gr_small, isbn13_cols=["gr_isbn13"], isbn10_cols=["gr_isbn10"])
    gb_small = ensure_best_isbn_columns(gb_small, isbn13_cols=["gb_isbn13"], isbn10_cols=["gb_isbn10"])

    # Merge principal por best_isbn13
    merged = pd.merge(gr_small, gb_small, how="outer", on="best_isbn13", suffixes=("_gr", "_gb"))

    # Emparejamiento de respaldo cuando no hay ISBN
    no_isbn_mask = merged["best_isbn13"].isna()
    if no_isbn_mask.any():
        def key_title(x):
            if not x:
                return None
            t = str(x).lower().strip()
            t = t.replace(":", " ").replace("-", " ")
            t = " ".join(t.split())
            return t

        gr_aux = gr_small.copy()
        gb_aux = gb_small.copy()
        gr_aux["_tkey"] = gr_aux["gr_title"].map(key_title).fillna("") + "|" + gr_aux["gr_author"].map(lambda x: str(x).lower().strip() if x else "")
        gb_aux["_tkey"] = gb_aux["gb_title"].map(key_title).fillna("") + "|" + gb_aux["gb_authors"].map(lambda x: str(x).split(";")[0].lower().strip() if x else "")

        fuzzy = pd.merge(gr_aux, gb_aux, how="inner", on="_tkey", suffixes=("_gr","_gb"))
        # Solo pares sin ISBN en ambos lados
        fuzzy = fuzzy[(fuzzy["best_isbn13_gr"].isna()) & (fuzzy["best_isbn13_gb"].isna())]

        # Alinear columnas con merged para poder concatenar
        for c in merged.columns:
            if c not in fuzzy.columns:
                fuzzy[c] = None
        fuzzy = fuzzy[merged.columns]

        merged = pd.concat([merged[~no_isbn_mask], fuzzy], ignore_index=True, sort=False)

    # Reglas de supervivencia (versiones robustas)
    def choose_title(row):
        vals = [row.get("gb_title"), row.get("gr_title")]
        cand = []
        for v in vals:
            if v is None:
                continue
            s = str(v).strip()
            if s == "" or s.lower() == "nan":
                continue
            cand.append(s)
        return max(cand, key=len) if cand else None

    def choose_authors(row):
        a = []
        gb = row.get("gb_authors")
        gr = row.get("gr_author")
        if gb is not None:
            s = str(gb)
            if s.lower() != "nan":
                a.extend([x.strip() for x in s.split(";") if x.strip()])
        if gr is not None:
            s = str(gr).strip()
            if s and s.lower() != "nan":
                a.append(s)
        seen = []
        for x in a:
            if x.lower() not in [y.lower() for y in seen]:
                seen.append(x)
        return seen

    def choose_price(row):
        amt = row.get("gb_price_amount")
        cur = row.get("gb_price_currency")
        amt = None if (amt is None or str(amt).lower()=="nan" or str(amt).strip()=="") else amt
        cur = None if (cur is None or str(cur).lower()=="nan" or str(cur).strip()=="") else cur
        return amt, cur

    # Construcción del canónico
    can = pd.DataFrame()
    can["isbn13"] = merged["best_isbn13"]

    # Blindaje de isbn10
    if "best_isbn10" in merged.columns:
        isbn10_series = merged["best_isbn10"]
    else:
        fallback_order = ["best_isbn10_gr","best_isbn10_gb","gr_isbn10","gb_isbn10"]
        cand = None
        for c in fallback_order:
            if c in merged.columns:
                cand = merged[c]
                break
        isbn10_series = cand if cand is not None else pd.Series([None]*len(merged))
    can["isbn10"] = isbn10_series
    if "gb_isbn10" in merged.columns:
        can["isbn10"] = can["isbn10"].fillna(merged["gb_isbn10"])

    can["titulo"] = merged.apply(choose_title, axis=1)
    can["titulo_normalizado"] = can["titulo"].map(
        lambda x: str(x).lower().strip() if (x is not None and str(x).lower() != "nan" and str(x).strip() != "") else None
    )
    can["autores"] = merged.apply(choose_authors, axis=1)
    can["autor_principal"] = can["autores"].map(lambda xs: xs[0] if xs else None)
    can["editorial"] = merged.get("gb_publisher")
    can["fecha_publicacion"] = merged.get("gb_pub_date")
    can["idioma"] = merged.get("gb_language")
    can["categoria"] = merged.get("gb_categories").map(
        lambda s: [x.strip() for x in str(s).split(";")] if (s is not None and str(s).lower() != "nan" and str(s).strip() != "") else []
    )

    if len(merged):
        amt, cur = zip(*merged.apply(choose_price, axis=1))
    else:
        amt, cur = [], []
    can["precio"] = list(amt)
    can["moneda"] = list(cur)

    gb_title_present = merged.get("gb_title")
    gb_title_mask = gb_title_present.notna() if gb_title_present is not None else False
    can["fuente_ganadora"] = np.where(gb_title_mask, "googlebooks", "goodreads")

    can["ts_ultima_actualizacion"] = datetime.now(timezone.utc).isoformat(timespec="seconds")

    # Normalizaciones
    can["fecha_publicacion"] = can["fecha_publicacion"].map(norm_date_iso)
    can["idioma"] = can["idioma"].map(norm_lang_bcp47)
    can["moneda"] = can["moneda"].map(norm_currency_iso4217)
    can["precio"] = can["precio"].map(to_decimal)

    # ID preferente isbn13; si falta, ID estable
    can["book_id"] = np.where(
        can["isbn13"].notna() & (can["isbn13"] != ""),
        can["isbn13"],
        [stable_id(t, a, e, f) for t, a, e, f in zip(
            can["titulo_normalizado"], can["autor_principal"], can["editorial"], can["fecha_publicacion"]
        )]
    )

    # Deduplicación por completitud
    def completeness_score(row):
        fields = ["titulo","autor_principal","editorial","fecha_publicacion","idioma","isbn13","precio","moneda"]
        return sum([1 for c in fields if row.get(c) not in [None, "", [], {}]])

    if len(can):
        can["_score"] = can.apply(completeness_score, axis=1)
        can = can.sort_values(["book_id","_score"], ascending=[True, False]).drop_duplicates("book_id", keep="first")
        can = can.drop(columns=["_score"])

    # Normalizar listas y asegurar columnas
    can["autores"] = can["autores"].map(lambda xs: list(dict.fromkeys([x for x in xs if x])) if isinstance(xs, list) else [])
    can["categoria"] = can["categoria"].map(lambda xs: list(dict.fromkeys([x for x in xs if x])) if isinstance(xs, list) else [])
    for c in ["precio","moneda","isbn10","isbn13","idioma","editorial","autor_principal","titulo","titulo_normalizado","categoria","autores","fuente_ganadora","ts_ultima_actualizacion","fecha_publicacion","book_id"]:
        if c not in can.columns:
            can[c] = None

    can = can[[
        "book_id","titulo","titulo_normalizado","autor_principal","autores",
        "editorial","fecha_publicacion","idioma","isbn10","isbn13","categoria",
        "precio","moneda","fuente_ganadora","ts_ultima_actualizacion"
    ]]
    return can

# ---------------------------
# Métricas de calidad
# ---------------------------
def compute_quality(can_df, source_detail_df):
    total = len(can_df)
    null_pct = lambda s: round(100.0 * (s.isna() | (s=="")).mean(), 2) if total else 0.0
    metrics = {
        "total_dim_book": total,
        "pct_null_titulo": null_pct(can_df["titulo"]),
        "pct_null_isbn13": null_pct(can_df["isbn13"]),
        "pct_null_price_amount": null_pct(can_df["precio"]),
        "rows_per_source": source_detail_df["source_name"].value_counts(dropna=False).to_dict(),
        "duplicates_found": int(source_detail_df["source_id"].duplicated().sum())
    }
    return metrics

# ---------------------------
# Escritura de salidas
# ---------------------------
def write_outputs(can_df, source_detail_df):
    dim_path = STANDARD / "dim_book.parquet"
    detail_path = STANDARD / "book_source_detail.parquet"
    schema_path = DOCS / "schema.md"
    qm_path = DOCS / "quality_metrics.json"

    can_df.to_parquet(dim_path, index=False)
    source_detail_df.to_parquet(detail_path, index=False)

    schema_md = """# Schema: dim_book

Campos:
- book_id (string, not null)
- titulo (string)
- titulo_normalizado (string)
- autor_principal (string)
- autores (list<string>)
- editorial (string)
- fecha_publicacion (date ISO-8601)
- idioma (BCP-47)
- isbn10 (string)
- isbn13 (string)
- categoria (list<string>)
- precio (float)
- moneda (ISO-4217)
- fuente_ganadora (string)
- ts_ultima_actualizacion (timestamp ISO-8601)

Reglas:
- ID preferente isbn13; si falta, hash estable de (titulo_normalizado, autor_principal, editorial, fecha_publicacion).
- Supervivencia: más campos completos; preferencia Google Books para título/precio.
- Listas unidas y de-duplicadas.
"""
    schema_path.write_text(schema_md, encoding="utf-8")

    metrics = compute_quality(can_df, source_detail_df)
    qm_path.write_text(json.dumps(metrics, indent=2, ensure_ascii=False), encoding="utf-8")

# ---------------------------
# Orquestación
# ---------------------------
def run_pipeline():
    gr_df, gb_df = read_inputs()
    gr_df, gb_df = annotate_sources(gr_df, gb_df)
    source_detail = to_source_detail(gr_df, gb_df)
    can = canonicalize(gr_df, gb_df)
    write_outputs(can, source_detail)
    print("Pipeline OK.")

if __name__ == "__main__":
    run_pipeline()
