# Schema: dim_book


Campos:
- book_id (string, not null)
- titulo (string)
- autor/es (string)  # autores separados por ';'
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
- ID preferente isbn13; si falta, se genera un identificador estable a partir de título, autor y otros campos clave.
- Supervivencia: más campos completos; preferencia Google Books para título/precio.
- Las listas (como categoria) se unen y de-duplican; los autores se exponen en 'autor/es' como texto.
