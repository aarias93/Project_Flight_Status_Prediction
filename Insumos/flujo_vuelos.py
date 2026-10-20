from pathlib import Path
import argparse
import tempfile

import joblib
import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from prefect import flow, task
from prefect.cache_policies import NO_CACHE
from threadpoolctl import threadpool_limits

BASE = Path(__file__).resolve().parent
MODELO = BASE / "modelos_v2" / "boosting_v2_1m.joblib"
CLAVES = ["FlightDate", "Operating_Airline", "Origin"]


@task(name="Validar entrada", cache_policy=NO_CACHE)
def validar_entrada(archivo: str):
    ruta = Path(archivo)
    if not ruta.is_absolute():
        ruta = BASE / ruta

    if not ruta.is_file():
        raise FileNotFoundError(ruta)
    if not MODELO.is_file():
        raise FileNotFoundError(MODELO)

    modelo = joblib.load(MODELO)
    variables = list(modelo.feature_names_in_)
    parquet = pq.ParquetFile(ruta)
    disponibles = set(parquet.schema_arrow.names)
    faltantes = (set(variables) | set(CLAVES)) - disponibles

    if faltantes:
        raise ValueError(f"Faltan columnas: {sorted(faltantes)}")
    if parquet.metadata.num_rows == 0:
        raise ValueError("El archivo no contiene vuelos.")

    print(f"Entrada: {ruta.name}")
    print(f"Vuelos: {parquet.metadata.num_rows:,}")
    print(f"Resultado real disponible: {'ArrDel15' in disponibles}")

    return str(ruta), variables, parquet.metadata.num_rows


@task(name="Predecir y agregar por bloques", cache_policy=NO_CACHE)
def analizar_entrada(ruta: str, variables: list):
    modelo = joblib.load(MODELO)
    parquet = pq.ParquetFile(ruta)
    tiene_objetivo = "ArrDel15" in parquet.schema_arrow.names

    columnas = sorted(set(variables) | set(CLAVES))
    if tiene_objetivo:
        columnas.append("ArrDel15")

    acumulado = None
    procesados = 0

    with threadpool_limits(limits=2):
        for bloque in parquet.iter_batches(
            batch_size=32_768,
            columns=columnas,
            use_threads=False,
        ):
            datos = bloque.to_pandas()

            datos["FlightDate"] = pd.to_datetime(
                datos["FlightDate"], errors="raise"
            ).dt.normalize()

            if datos[CLAVES].isna().any().any():
                raise ValueError("Hay fechas, aerolíneas u orígenes nulos.")

            p = modelo.predict_proba(datos[variables])[:, 1]
            if not np.isfinite(p).all() or not ((p >= 0) & (p <= 1)).all():
                raise ValueError("El modelo generó probabilidades inválidas.")

            # Los resultados desconocidos permanecen como NaN.
            if tiene_objetivo:
                y = pd.to_numeric(datos["ArrDel15"], errors="raise")
                if not y.dropna().isin([0, 1]).all():
                    raise ValueError("ArrDel15 contiene valores distintos de 0 y 1.")
            else:
                y = pd.Series(np.nan, index=datos.index)

            datos["vuelos"] = 1
            datos["suma_probabilidades"] = p
            datos["vuelos_con_resultado"] = y.notna().astype("int64")
            datos["retrasos_observados"] = y

            parcial = datos.groupby(
                CLAVES, dropna=False, observed=True
            ).agg(
                vuelos=("vuelos", "sum"),
                suma_probabilidades=("suma_probabilidades", "sum"),
                vuelos_con_resultado=("vuelos_con_resultado", "sum"),
                retrasos_observados=("retrasos_observados", "sum"),
            )

            if acumulado is None:
                acumulado = parcial
            else:
                acumulado = acumulado.add(parcial, fill_value=0)

            procesados += len(datos)

    print(f"Vuelos procesados: {procesados:,}")
    return acumulado.reset_index()


@task(name="Construir indicadores", cache_policy=NO_CACHE)
def construir_indicadores(resumen: pd.DataFrame, filas_esperadas: int):
    if int(resumen["vuelos"].sum()) != filas_esperadas:
        raise ValueError("El total agregado no coincide con la entrada.")

    for columna in ["vuelos", "vuelos_con_resultado"]:
        resumen[columna] = resumen[columna].astype("int64")

    resumen["probabilidad_media_modelo"] = (
        resumen["suma_probabilidades"] / resumen["vuelos"]
    )

    # Sin resultados conocidos, no se reportan cero retrasos.
    sin_resultado = resumen["vuelos_con_resultado"].eq(0)
    resumen.loc[sin_resultado, "retrasos_observados"] = np.nan

    resumen["tasa_retraso_observada"] = (
        resumen["retrasos_observados"]
        / resumen["vuelos_con_resultado"].replace(0, np.nan)
    )

    resumen["modelo_version"] = "boosting_v2_1m"
    resumen = resumen.sort_values(CLAVES).reset_index(drop=True)

    print(f"Filas del resumen analítico: {len(resumen):,}")
    return resumen


@task(
    name="Exportar resultados analíticos",
    cache_policy=NO_CACHE,
    retries=2,
    retry_delay_seconds=3,
)
def exportar_resultados(resumen: pd.DataFrame, ruta_entrada: str):
    carpeta = BASE / "salidas_prefect"
    carpeta.mkdir(exist_ok=True)

    destino = carpeta / f"resumen_{Path(ruta_entrada).stem}.csv"

    # Escritura temporal y reemplazo: repetir la ejecución no añade duplicados.
    with tempfile.NamedTemporaryFile(
        dir=carpeta, suffix=".tmp", delete=False
    ) as temporal:
        ruta_temporal = Path(temporal.name)

    try:
        resumen.to_csv(ruta_temporal, index=False)
        ruta_temporal.replace(destino)
    finally:
        ruta_temporal.unlink(missing_ok=True)

    print(f"Resultado guardado: {destino}")
    print(resumen.head(3).to_string(index=False))
    return str(destino)


@flow(name="Analitica recurrente de vuelos", log_prints=True)
def flujo_vuelos(archivo: str):
    ruta, variables, filas = validar_entrada(archivo)
    resumen = analizar_entrada(ruta, variables)
    indicadores = construir_indicadores(resumen, filas)
    return exportar_resultados(indicadores, ruta)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--archivo",
        default="datos_modelo/vuelos_2022_01.parquet",
    )
    argumentos = parser.parse_args()
    flujo_vuelos(argumentos.archivo)
