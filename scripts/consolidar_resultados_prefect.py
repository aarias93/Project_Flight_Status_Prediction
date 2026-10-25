"""Consolidate monthly Prefect analytics for Grafana/AWS, without raw flights."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq


ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "salidas_prefect"
AWS_OUT = ROOT / "entrega_aws"
RESULTS = ROOT / "resultados_eda"
MODEL_RESULTS = ROOT / "resultados_modelo_v2"


def main() -> None:
    parts = []
    for month in range(1, 8):
        file = SOURCE / f"resumen_vuelos_2022_{month:02d}.csv"
        if not file.is_file():
            raise FileNotFoundError(file)
        data = pd.read_csv(
            file,
            parse_dates=["FlightDate"],
            dtype={"Operating_Airline": "string", "Origin": "string"},
        )
        if "hora_salida" not in data.columns:
            raise AssertionError(f"{file.name} no conserva la hora de salida.")
        if not data["FlightDate"].dt.year.eq(2022).all():
            raise AssertionError("La salida tiene años distintos de 2022.")
        if not data["FlightDate"].dt.month.eq(month).all():
            raise AssertionError(f"{file.name} contiene un mes diferente.")
        input_file = ROOT / "datos_modelo" / f"vuelos_2022_{month:02d}.parquet"
        expected = pq.ParquetFile(input_file).metadata.num_rows
        if int(data["vuelos"].sum()) != expected:
            raise AssertionError(f"Los vuelos no coinciden en 2022-{month:02d}.")
        if int(data["vuelos_con_resultado"].sum()) != expected:
            raise AssertionError(f"Los resultados no coinciden en 2022-{month:02d}.")
        parts.append(data)

    all_results = pd.concat(parts, ignore_index=True)
    keys = ["FlightDate", "Operating_Airline", "Origin", "hora_salida"]
    if all_results[keys].isna().any().any():
        raise AssertionError("Hay claves de agregación faltantes.")
    if all_results.duplicated(keys).any():
        raise AssertionError("Hay claves de agregación duplicadas.")
    if all_results["modelo_version"].nunique() != 1:
        raise AssertionError("La consolidación mezcla versiones de modelo.")
    for column in ["vuelos", "vuelos_con_resultado", "retrasos_observados"]:
        numeric = pd.to_numeric(all_results[column], errors="raise")
        if not numeric.dropna().mod(1).eq(0).all():
            raise AssertionError(f"{column} contiene conteos no enteros.")
        all_results[column] = numeric.astype("int64")
    if int(all_results["vuelos"].sum()) != 3_944_916:
        raise AssertionError("No coinciden los vuelos de enero a julio de 2022.")
    if int(all_results["retrasos_observados"].sum()) != 853_962:
        raise AssertionError("No coinciden los retrasos observados de 2022.")

    out = all_results.rename(columns={
        "FlightDate": "fecha",
        "Operating_Airline": "aerolinea",
        "Origin": "aeropuerto_origen",
    })
    out = out.sort_values(
        ["fecha", "aerolinea", "aeropuerto_origen", "hora_salida"]
    ).reset_index(drop=True)
    AWS_OUT.mkdir(exist_ok=True)
    path = AWS_OUT / "RESUMEN_ANALITICO_AWS_FINAL_2022_01-07.csv"
    out.to_csv(path, index=False, encoding="utf-8-sig")
    parquet_path = AWS_OUT / "RESUMEN_ANALITICO_AWS_FINAL_2022_01-07.parquet"
    out.to_parquet(parquet_path, index=False, compression="zstd")

    monthly = (
        out.assign(mes=out["fecha"].dt.to_period("M").astype(str))
        .groupby("mes", as_index=False)
        .agg(
            vuelos=("vuelos", "sum"),
            retrasos_observados=("retrasos_observados", "sum"),
            vuelos_con_resultado=("vuelos_con_resultado", "sum"),
            suma_probabilidades=("suma_probabilidades", "sum"),
        )
    )
    monthly["tasa_retraso_observada"] = (
        monthly["retrasos_observados"] / monthly["vuelos_con_resultado"]
    )
    monthly["probabilidad_media_modelo"] = (
        monthly["suma_probabilidades"] / monthly["vuelos"]
    )
    monthly.to_csv(
        MODEL_RESULTS / "resumen_riesgo_mensual_2022.csv",
        index=False, encoding="utf-8-sig",
    )

    hourly = (
        out.groupby("hora_salida", as_index=False)
        .agg(
            vuelos=("vuelos", "sum"),
            retrasos_observados=("retrasos_observados", "sum"),
            vuelos_con_resultado=("vuelos_con_resultado", "sum"),
            suma_probabilidades=("suma_probabilidades", "sum"),
        )
        .sort_values("hora_salida")
    )
    hourly["tasa_retraso_observada"] = (
        hourly["retrasos_observados"] / hourly["vuelos_con_resultado"]
    )
    hourly["probabilidad_media_modelo"] = (
        hourly["suma_probabilidades"] / hourly["vuelos"]
    )
    hourly.to_csv(
        MODEL_RESULTS / "resumen_riesgo_por_hora_2022.csv",
        index=False, encoding="utf-8-sig",
    )

    record = {
        "periodo": "2022-01-01 a 2022-07-31",
        "excluded_year_2020": True,
        "aggregation_keys": ["fecha", "aerolinea", "aeropuerto_origen", "hora_salida"],
        "rows_aggregated": int(len(out)),
        "flights_represented": int(out["vuelos"].sum()),
        "observed_delays": int(out["retrasos_observados"].sum()),
        "rows_with_outcome": int(out["vuelos_con_resultado"].sum()),
        "mean_model_probability": float(
            out["suma_probabilidades"].sum() / out["vuelos"].sum()
        ),
        "observed_delay_rate": float(
            out["retrasos_observados"].sum() / out["vuelos_con_resultado"].sum()
        ),
        "csv_bytes": path.stat().st_size,
        "parquet_bytes": parquet_path.stat().st_size,
        "csv_path": "entrega_aws/RESUMEN_ANALITICO_AWS_FINAL_2022_01-07.csv",
        "contains_original_flight_records": False,
        "model_version": out["modelo_version"].iloc[0],
        "status": "ready_for_database_load; no AWS credentials available in this execution",
    }
    (RESULTS / "resumen_prefect_2022.json").write_text(
        json.dumps(record, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    print(json.dumps(record, indent=2, ensure_ascii=False))
    print("\nResumen mensual:")
    print(monthly.to_string(index=False))
    print("\nResumen horario:")
    print(hourly.to_string(index=False))


if __name__ == "__main__":
    main()
