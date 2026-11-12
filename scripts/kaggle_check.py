"""Cuenta cuántos archivos y columnas del dataset tienen descripción en Kaggle.

`kaggle datasets metadata` no devuelve las columnas; la lista de archivos sí. Corre con el
CLI de Kaggle (no es dependencia del proyecto) y KAGGLE_API_TOKEN en el entorno:
    uvx --from kaggle==2.2.5 --with six==1.17.0 python scripts/kaggle_check.py <owner/slug>
Imprime `archivos=a/b columnas=c/d`.
"""

import sys

from kaggle.api.kaggle_api_extended import KaggleApi


def main() -> int:
    api = KaggleApi()
    api.authenticate()
    files, token = [], None
    while True:
        resp = api.dataset_list_files(sys.argv[1], page_token=token, page_size=200)
        files += resp.files or []
        token = resp.next_page_token
        if not token:
            break
    cols = [c for f in files for c in (f.columns or [])]
    print(f"archivos={sum(bool(f.description) for f in files)}/{len(files)} "
          f"columnas={sum(bool(c.description) for c in cols)}/{len(cols)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
