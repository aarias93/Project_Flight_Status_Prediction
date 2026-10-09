from pathlib import Path
import hashlib
import json
import os

# Conexión al servidor local antes de importar Prefect.
os.environ["PREFECT_API_URL"] = "http://127.0.0.1:4200/api"

from prefect import flow
from flujo_vuelos import flujo_vuelos, MODELO

BASE = Path(__file__).resolve().parent
ENTRADAS = BASE / "entradas_prefect"
CONTROL = BASE / "control_prefect.json"


def huella(ruta):
    """Identifica el contenido sin cargar todo el archivo en memoria."""
    sha = hashlib.sha256()
    with open(ruta, "rb") as archivo:
        for bloque in iter(lambda: archivo.read(1024 * 1024), b""):
            sha.update(bloque)
    return sha.hexdigest()


@flow(name="Revisar nuevos archivos de vuelos", log_prints=True)
def revisar_entradas():
    ENTRADAS.mkdir(exist_ok=True)

    registro = (
        json.loads(CONTROL.read_text(encoding="utf-8"))
        if CONTROL.exists()
        else {}
    )

    # Reprocesar también si cambia el modelo o el código analítico.
    version = (
        huella(MODELO)
        + huella(BASE / "flujo_vuelos.py")
    )

    procesados = 0

    for archivo in sorted(ENTRADAS.glob("*.parquet")):
        firma = huella(archivo) + version
        salida = BASE / "salidas_prefect" / f"resumen_{archivo.stem}.csv"

        if registro.get(archivo.name) == firma and salida.is_file():
            print(f"Sin cambios, se omite: {archivo.name}")
            continue

        print(f"Procesando archivo nuevo o modificado: {archivo.name}")
        flujo_vuelos(str(archivo))

        # Marcar como procesado únicamente después de completar el flujo.
        registro[archivo.name] = firma
        temporal = CONTROL.with_suffix(".tmp")
        temporal.write_text(
            json.dumps(registro, indent=2),
            encoding="utf-8",
        )
        temporal.replace(CONTROL)
        procesados += 1

    print(f"Archivos procesados en esta revisión: {procesados}")


if __name__ == "__main__":
    revisar_entradas.serve(
        name="revision-automatica-vuelos",
        interval=60,
        limit=1,
        pause_on_shutdown=True,
    )
