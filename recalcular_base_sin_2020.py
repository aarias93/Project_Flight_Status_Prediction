"""Regenerate project data products from the original CSV, excluding 2020.

The source CSV is read-only. Outputs are local and ignored by Git.
"""

from __future__ import annotations

import csv
import json
import shutil
import time
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
import pyarrow.parquet as pq


ROOT = Path(__file__).resolve().parent
SOURCE = ROOT / "Combined_Flights_2018-2022.csv"
FULL_DIR = ROOT / "datos_preparados"
FULL_FINAL = FULL_DIR / "base_sin_2020.parquet"
FULL_TEMP = FULL_DIR / "base_sin_2020.en_proceso.parquet"
MODEL_DIR = ROOT / "datos_modelo"
MODEL_TEMP = ROOT / "datos_modelo_en_proceso"
RESULTS = ROOT / "resultados_eda"

TEXT_COLUMNS = {
    "Airline", "Origin", "Dest", "Marketing_Airline_Network",
    "Operated_or_Branded_Code_Share_Partners",
    "IATA_Code_Marketing_Airline", "Operating_Airline",
    "IATA_Code_Operating_Airline", "Tail_Number", "OriginCityName",
    "OriginState", "OriginStateName", "DestCityName", "DestState",
    "DestStateName", "DepTimeBlk", "ArrTimeBlk",
}
INTEGER_COLUMNS = {
    "CRSDepTime", "CRSArrTime", "Year", "Quarter", "Month",
    "DayofMonth", "DayOfWeek", "DOT_ID_Marketing_Airline",
    "Flight_Number_Marketing_Airline", "DOT_ID_Operating_Airline",
    "Flight_Number_Operating_Airline", "OriginAirportID",
    "OriginAirportSeqID", "OriginCityMarketID", "OriginStateFips",
    "OriginWac", "DestAirportID", "DestAirportSeqID",
    "DestCityMarketID", "DestStateFips", "DestWac", "DistanceGroup",
}

MODEL_SCHEMA = pa.schema([
    ("FlightDate", pa.date32()),
    ("Year", pa.int16()),
    ("Month", pa.int8()),
    ("DayOfWeek", pa.int8()),
    ("Operating_Airline", pa.string()),
    ("Origin", pa.string()),
    ("Dest", pa.string()),
    ("CRSDepTime", pa.int16()),
    ("hora_salida", pa.int8()),
    ("Distance", pa.float64()),
    ("duracion_programada_min", pa.float64()),
    ("duracion_programada_faltante", pa.int8()),
    ("ArrDel15", pa.int8()),
])


def make_schema(columns: list[str]) -> pa.Schema:
    types = {}
    for name in columns:
        if name == "FlightDate":
            types[name] = pa.date32()
        elif name in {"Cancelled", "Diverted"}:
            types[name] = pa.bool_()
        elif name in TEXT_COLUMNS:
            types[name] = pa.string()
        elif name in INTEGER_COLUMNS:
            types[name] = pa.int64()
        else:
            types[name] = pa.float64()
    return pa.schema([(name, types[name]) for name in columns])


def make_feature_table(batch: pa.RecordBatch) -> pa.Table:
    frame = batch.to_pandas()
    delay = pd.to_numeric(frame["ArrDelay"], errors="coerce")
    label = pd.to_numeric(frame["ArrDel15"], errors="coerce")
    dep = pd.to_numeric(frame["CRSDepTime"], errors="coerce")
    duration = pd.to_numeric(frame["CRSElapsedTime"], errors="coerce")

    mask = (
        frame["Cancelled"].eq(False)
        & frame["Diverted"].eq(False)
        & delay.notna()
        & np.isfinite(delay)
        & label.isin([0, 1])
        & label.eq((delay >= 15).astype("int8"))
    )
    frame = frame.loc[mask].copy()
    if frame.empty:
        return pa.Table.from_batches([], schema=MODEL_SCHEMA)

    delay = delay.loc[mask]
    label = label.loc[mask]
    dep = dep.loc[mask]
    duration = duration.loc[mask]
    dep_ok = dep.notna() & dep.between(0, 2400) & (dep.mod(100) < 60)
    hour = (dep // 100).where(dep_ok)
    hour = hour.where(hour != 24, 0)
    duration_ok = duration.notna() & np.isfinite(duration) & (duration > 0)

    frame["hora_salida"] = hour
    frame["duracion_programada_min"] = duration.where(duration_ok)
    frame["duracion_programada_faltante"] = (~duration_ok).astype("int8")
    frame["ArrDel15"] = label.astype("int8")

    arrays = [
        pa.array(frame[name], type=field.type, from_pandas=True)
        for name, field in zip(MODEL_SCHEMA.names, MODEL_SCHEMA)
    ]
    return pa.Table.from_arrays(arrays, schema=MODEL_SCHEMA)


def main() -> None:
    if not SOURCE.is_file():
        raise FileNotFoundError(f"No se encontró la fuente original: {SOURCE}")
    if FULL_FINAL.exists() or FULL_TEMP.exists() or MODEL_DIR.exists() or MODEL_TEMP.exists():
        raise FileExistsError(
            "Ya existen salidas de preparación. Revisarlas antes de repetir; "
            "este proceso no sobrescribe productos analíticos."
        )
    if not SOURCE.is_file():
        raise FileNotFoundError(SOURCE)

    FULL_DIR.mkdir(parents=True, exist_ok=True)
    MODEL_TEMP.mkdir(parents=True, exist_ok=True)
    RESULTS.mkdir(parents=True, exist_ok=True)

    with SOURCE.open("r", encoding="utf-8-sig", newline="") as handle:
        columns = next(csv.reader(handle))
    schema = make_schema(columns)
    readers = pacsv.open_csv(
        SOURCE,
        read_options=pacsv.ReadOptions(block_size=8 * 1024 * 1024, use_threads=False),
        convert_options=pacsv.ConvertOptions(
            column_types=schema,
            null_values=[""],
            strings_can_be_null=True,
            true_values=["True", "true", "1"],
            false_values=["False", "false", "0"],
        ),
    )

    source_rows = 0
    retained_rows = 0
    pandemic_rows = 0
    source_years: Counter[int] = Counter()
    retained_months: Counter[tuple[int, int]] = Counter()
    target_counts: Counter[tuple[int, int, int]] = Counter()
    model_rows = 0
    next_notice = 1_000_000
    start = time.perf_counter()
    feature_writers: dict[tuple[int, int], pq.ParquetWriter] = {}

    with pq.ParquetWriter(
        FULL_TEMP,
        schema,
        compression="zstd",
        use_dictionary=True,
        write_statistics=True,
    ) as full_writer:
        try:
            for batch in readers:
                rows = batch.num_rows
                source_rows += rows
                for item in pc.value_counts(batch.column("Year")).to_pylist():
                    source_years[int(item["values"])] += int(item["counts"])

                pandemic_mask = pc.equal(batch.column("Year"), 2020)
                pandemic_mask = pc.fill_null(pandemic_mask, False)
                pandemic_rows += int(pc.sum(pandemic_mask).as_py() or 0)
                keep = pc.invert(pandemic_mask)
                filtered = pc.filter(batch, keep)
                if filtered.num_rows:
                    full_writer.write_batch(filtered)
                    retained_rows += filtered.num_rows

                    feature_batch = make_feature_table(filtered)
                    if feature_batch.num_rows:
                        feature_frame = feature_batch.to_pandas()
                        grouped = feature_frame.groupby(["Year", "Month"], sort=False)
                        for (year, month), part in grouped:
                            key = (int(year), int(month))
                            retained_months[key] += int(len(part))
                            for cls, count in part["ArrDel15"].value_counts().items():
                                target_counts[(key[0], key[1], int(cls))] += int(count)

                            arrays = [
                                pa.array(part[name], type=field.type, from_pandas=True)
                                for name, field in zip(MODEL_SCHEMA.names, MODEL_SCHEMA)
                            ]
                            table = pa.Table.from_arrays(arrays, schema=MODEL_SCHEMA)
                            output = MODEL_TEMP / f"vuelos_{year}_{month:02d}.parquet"
                            writer = feature_writers.get(key)
                            if writer is None:
                                writer = pq.ParquetWriter(
                                    output,
                                    MODEL_SCHEMA,
                                    compression="zstd",
                                    use_dictionary=True,
                                    write_statistics=True,
                                )
                                feature_writers[key] = writer
                            writer.write_table(table, row_group_size=32_768)
                            model_rows += len(part)

                if source_rows >= next_notice:
                    elapsed = (time.perf_counter() - start) / 60
                    print(
                        f"CSV leído: {source_rows:,} filas; "
                        f"retenidas sin 2020: {retained_rows:,}; "
                        f"{elapsed:.1f} min",
                        flush=True,
                    )
                    next_notice += 1_000_000
        finally:
            for writer in feature_writers.values():
                writer.close()
            readers.close()

    with pq.ParquetFile(FULL_TEMP) as parquet:
        assert parquet.metadata.num_rows == retained_rows
        assert parquet.schema_arrow.names == columns
    assert pandemic_rows == source_years.get(2020, 0)
    assert all(year != 2020 for year, _ in feature_writers)
    if not retained_rows or not model_rows:
        raise RuntimeError("La preparación terminó sin generar una base válida.")

    FULL_TEMP.replace(FULL_FINAL)
    MODEL_TEMP.replace(MODEL_DIR)

    model_size = sum(path.stat().st_size for path in MODEL_DIR.glob("*.parquet"))
    manifest = {
        "source": SOURCE.name,
        "source_bytes": SOURCE.stat().st_size,
        "source_gb_decimal": round(SOURCE.stat().st_size / 10**9, 3),
        "source_rows": source_rows,
        "source_year_rows": dict(sorted(source_years.items())),
        "excluded_year": 2020,
        "excluded_2020_rows": pandemic_rows,
        "retained_year_rows": {
            str(year): count
            for year, count in sorted(source_years.items())
            if year != 2020
        },
        "filtered_all_columns_rows": retained_rows,
        "filtered_all_columns_bytes": FULL_FINAL.stat().st_size,
        "filtered_all_columns_gb_decimal": round(FULL_FINAL.stat().st_size / 10**9, 3),
        "original_columns": len(columns),
        "model_rows_with_valid_target": model_rows,
        "model_valid_rows_by_year": {
            str(year): sum(count for (y, _), count in retained_months.items() if y == year)
            for year in sorted({year for year, _ in retained_months})
        },
        "monthly_model_files": len(feature_writers),
        "model_data_bytes": model_size,
        "model_data_gb_decimal": round(model_size / 10**9, 3),
        "target_class_counts_by_year_month": {
            f"{year}-{month:02d}-class{cls}": count
            for (year, month, cls), count in sorted(target_counts.items())
        },
        "elapsed_minutes": round((time.perf_counter() - start) / 60, 2),
        "source_modified": False,
        "method": "PyArrow CSV streaming; Year == 2020 removed before analytical Parquet write",
    }
    (RESULTS / "manifest_base.json").write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8"
    )

    print("\nPREPARACIÓN COMPLETADA", flush=True)
    print(json.dumps(manifest, indent=2, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
