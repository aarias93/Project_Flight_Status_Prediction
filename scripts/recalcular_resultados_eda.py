"""Produce reproducible descriptive, distributional, and correlation results."""

from __future__ import annotations

import json
import time
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
FULL = ROOT / "datos_preparados" / "base_sin_2020.parquet"
MODEL_GLOB = ROOT / "datos_modelo" / "vuelos_*.parquet"
RESULTS = ROOT / "resultados_eda"
SOURCE = ROOT / "Combined_Flights_2018-2022.csv"


def main() -> None:
    if not FULL.is_file() or not MODEL_GLOB.parent.is_dir():
        raise FileNotFoundError("Ejecuta primero scripts/recalcular_base_sin_2020.py.")
    RESULTS.mkdir(exist_ok=True)
    temp = ROOT / ".duckdb_tmp"
    temp.mkdir(exist_ok=True)
    started = time.perf_counter()
    con = duckdb.connect(
        config={"memory_limit": "2GB", "threads": 2, "temp_directory": str(temp)}
    )
    full = FULL.as_posix().replace("'", "''")
    models = MODEL_GLOB.as_posix().replace("'", "''")

    summary_path = RESULTS / "eda_variables.csv"
    if summary_path.is_file():
        summary = pd.read_csv(summary_path)
        print("Usando el resumen de las 61 variables ya calculado.", flush=True)
    else:
        print("Calculando resumen estadístico de las 61 variables...", flush=True)
        summary = con.execute(f"SUMMARIZE SELECT * FROM read_parquet('{full}')").df()
    if len(summary) != 61:
        raise AssertionError(f"Se esperaban 61 variables; se obtuvieron {len(summary)}.")
    summary.to_csv(summary_path, index=False, encoding="utf-8-sig")
    print(f"Resumen de variables listo: {len(summary)}", flush=True)

    yearly = con.execute(f"""
        SELECT Year AS anio, COUNT(*) AS vuelos,
            COUNT(*) FILTER (WHERE Cancelled) AS cancelados,
            COUNT(*) FILTER (WHERE Diverted) AS desviados,
            COUNT(*) FILTER (WHERE NOT Cancelled AND NOT Diverted) AS operados,
            MIN(FlightDate) AS fecha_inicial, MAX(FlightDate) AS fecha_final
        FROM read_parquet('{full}') GROUP BY Year ORDER BY Year
    """).df()
    if yearly["anio"].eq(2020).any():
        raise AssertionError("La salida completa contiene registros de 2020.")
    yearly.to_csv(RESULTS / "cobertura_anual.csv", index=False, encoding="utf-8-sig")

    monthly = con.execute(f"""
        SELECT Year AS anio, Month AS mes, COUNT(*) AS vuelos,
            COUNT(*) FILTER (WHERE Cancelled) AS cancelados,
            COUNT(*) FILTER (WHERE Diverted) AS desviados
        FROM read_parquet('{full}') GROUP BY Year, Month ORDER BY Year, Month
    """).df()
    monthly.to_csv(RESULTS / "cobertura_mensual.csv", index=False, encoding="utf-8-sig")

    objective = con.execute(f"""
        SELECT Year AS anio, COUNT(*) AS vuelos_con_objetivo,
            COUNT(*) FILTER (WHERE ArrDel15 = 0) AS sin_retraso_15,
            COUNT(*) FILTER (WHERE ArrDel15 = 1) AS retraso_15_o_mas,
            AVG(ArrDel15) AS proporcion_retraso,
            100 * AVG(ArrDel15) AS porcentaje_retraso
        FROM read_parquet('{models}') GROUP BY Year ORDER BY Year
    """).df()
    if objective["anio"].eq(2020).any():
        raise AssertionError("La base de modelado contiene registros de 2020.")
    objective.to_csv(RESULTS / "resumen_objetivo_anual.csv", index=False, encoding="utf-8-sig")

    by_origin = con.execute(f"""
        SELECT Origin AS aeropuerto_origen, COUNT(*) AS vuelos,
            SUM(ArrDel15) AS vuelos_retrasados,
            AVG(ArrDel15) AS proporcion_retraso,
            100 * AVG(ArrDel15) AS porcentaje_retraso
        FROM read_parquet('{models}')
        GROUP BY Origin HAVING COUNT(*) >= 1000
        ORDER BY proporcion_retraso DESC, vuelos DESC
    """).df()
    by_origin.to_csv(RESULTS / "tasa_retraso_por_origen.csv", index=False, encoding="utf-8-sig")

    by_day_hour = con.execute(f"""
        SELECT DayOfWeek AS dia_semana, hora_salida,
            COUNT(*) AS vuelos, SUM(ArrDel15) AS vuelos_retrasados,
            AVG(ArrDel15) AS proporcion_retraso,
            100 * AVG(ArrDel15) AS porcentaje_retraso
        FROM read_parquet('{models}') WHERE hora_salida IS NOT NULL
        GROUP BY DayOfWeek, hora_salida ORDER BY DayOfWeek, hora_salida
    """).df()
    by_day_hour.to_csv(RESULTS / "tasa_retraso_por_dia_hora.csv", index=False, encoding="utf-8-sig")

    print("Preparando muestra común para Pearson y Spearman...", flush=True)
    sample = con.execute(f"""
        SELECT * FROM (
            SELECT DepDelay, ArrDelay, TaxiOut, TaxiIn, Distance
            FROM read_parquet('{full}')
            WHERE NOT Cancelled AND NOT Diverted
              AND DepDelay IS NOT NULL AND ArrDelay IS NOT NULL
              AND TaxiOut IS NOT NULL AND TaxiIn IS NOT NULL AND Distance IS NOT NULL
              AND isfinite(DepDelay) AND isfinite(ArrDelay)
              AND isfinite(TaxiOut) AND isfinite(TaxiIn) AND isfinite(Distance)
        ) USING SAMPLE reservoir(100000 ROWS) REPEATABLE (42)
    """).df()
    if len(sample) != 100_000:
        raise AssertionError(f"La muestra común debe tener 100.000 filas: {len(sample)}.")
    pearson = sample.corr(method="pearson")
    ranked = sample.rank(method="average", pct=False)
    spearman = ranked.corr(method="pearson")
    rows = []
    for a in sample.columns:
        for b in sample.columns:
            rows.append({
                "variable_x": a,
                "variable_y": b,
                "pearson_datos_brutos": float(pearson.loc[a, b]),
                "spearman_rangos": float(spearman.loc[a, b]),
                "n_muestra_comun": len(sample),
            })
    corr = pd.DataFrame(rows)
    corr.to_csv(RESULTS / "correlacion_pearson_spearman.csv", index=False, encoding="utf-8-sig")

    shape = sample.agg(["mean", "median", "std", "min", "max", "skew"]).T
    shape.index.name = "variable"
    shape.to_csv(RESULTS / "distribucion_muestra_correlaciones.csv", encoding="utf-8-sig")

    counts = con.execute(f"""
        SELECT COUNT(*) AS n, SUM(ArrDel15) AS retrasos,
            AVG(ArrDel15) AS tasa
        FROM read_parquet('{models}')
    """).fetchone()
    manifest_path = RESULTS / "manifest_base.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    source_rows = manifest.get("source_rows")
    source_year_rows = manifest.get("source_year_rows")
    if source_rows is None or source_year_rows is None:
        raise ValueError(
            "El manifiesto debe registrar el conteo observado del CSV antes del EDA."
        )
    source_year_rows = {int(k): int(v) for k, v in source_year_rows.items()}
    manifest.update({
        "source_rows": source_rows,
        "source_year_rows": {str(k): v for k, v in source_year_rows.items()},
        "excluded_2020_rows": source_year_rows[2020],
        "model_rows_with_valid_target": int(counts[0]),
        "model_delayed_rows": int(counts[1]),
        "model_delay_rate": float(counts[2]),
        "parquet_reduction_percent": round(
            100 * (1 - manifest["filtered_all_columns_bytes"] / manifest["source_bytes"]), 2
        ),
        "source_to_parquet_size_ratio": round(
            manifest["source_bytes"] / manifest["filtered_all_columns_bytes"], 2
        ),
        "eda_variables_rows": len(summary),
        "pearson_spearman_sample_rows": len(sample),
        "correlation_method": (
            "Same 100,000 complete observations; Pearson uses original numeric values; "
            "Spearman uses within-column average ranks of those observations."
        ),
        "eda_elapsed_minutes": round((time.perf_counter() - started) / 60, 2),
    })
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")
    con.close()

    print("\nRESULTADOS EDA", flush=True)
    print(yearly.to_string(index=False), flush=True)
    print("\nVARIABLE OBJETIVO", flush=True)
    print(objective.to_string(index=False), flush=True)
    pair = corr.query("variable_x == 'DepDelay' and variable_y == 'ArrDelay'").iloc[0]
    print(
        f"\nDepDelay-ArrDelay: Pearson={pair.pearson_datos_brutos:.4f}; "
        f"Spearman={pair.spearman_rangos:.4f}; n={len(sample):,}",
        flush=True,
    )
    print("\nMANIFEST", json.dumps(manifest, indent=2, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
