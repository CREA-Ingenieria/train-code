# frontend/static/download_logos.py
import os, pathlib, urllib.request

DEST = pathlib.Path(__file__).parent / "img"
DEST.mkdir(parents=True, exist_ok=True)

assets = {
    # Uniandes (archivo institucional público en dominio uniandes)
    "uniandes.svg": "https://cienciasbiologicas.uniandes.edu.co/en/file/logo-uniandessvg",
    # Metro de Bogotá (Wikimedia Commons – CC BY-SA 4.0)
    "metro_bogota.svg": "https://upload.wikimedia.org/wikipedia/commons/6/6a/LogoMetroBogot%C3%A12021.svg",
}

for name, url in assets.items():
    out = DEST / name
    print(f"Descargando {name} ...")
    urllib.request.urlretrieve(url, out)
    print(f"Guardado en {out}")

print("\nListo. Verifica que tu servidor sirva: /static/img/uniandes.svg y /static/img/metro_bogota.svg")
