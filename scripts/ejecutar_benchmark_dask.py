"""Benchmark the prepared, 2020-excluded flight data with Dask."""

from __future__ import annotations

import json
import os
import re
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DASK_SCRATCH = ROOT / ".dask_tmp"
DASK_SCRATCH.mkdir(exist_ok=True)
os.environ["DASK_TEMPORARY_DIRECTORY"] = str(DASK_SCRATCH)

import dask
import dask.dataframe as dd
import pandas as pd
import pyarrow.compute as pc
import pyarrow.parquet as pq
from distributed import Client, LocalCluster


DATA = ROOT / "datos_modelo"
OUT = ROOT / "resultados_dask"
GROUP_KEYS = ["Year", "Month", "Operating_Airline", "Origin"]


def run_one(name: str, workers: int, memory: str, repeat: int):
    files = sorted(p for p in DATA.glob("vuelos_*.parquet") if not re.search(r"(?:^|_)2020(?:_|$)", p.stem))
    if len(files) != 43 or any("2020" in p.name for p in files):
        raise AssertionError(f"Se requieren 43 Parquet mensuales sin 2020; hay {len(files)}.")
    expected_rows = sum(pq.ParquetFile(path).metadata.num_rows for path in files)
    expected_delays = sum(
        int(pc.sum(pq.read_table(path, columns=["ArrDel15"])["ArrDel15"]).as_py())
        for path in files
    )
    dask_scratch = ROOT / ".dask_tmp"
    dask_scratch.mkdir(exist_ok=True)
    dask.config.set({"temporary-directory": str(dask_scratch)})
    config = {"configuracion": name, "repeticion": repeat, "workers": workers,
              "memoria_por_worker": memory, "hilos_por_worker": 1}
    with LocalCluster(
        n_workers=workers, threads_per_worker=1, processes=True,
        memory_limit=memory, dashboard_address=None,
        local_directory=str(dask_scratch),
    ) as cluster:
        with Client(cluster) as client:
            client.wait_for_workers(workers)
            worker_limits = sorted(
                round(info["memory_limit"] / 1024**2)
                for info in client.scheduler_info()["workers"].values()
            )
            if len(worker_limits) != workers:
                raise AssertionError(f"Se esperaban {workers} workers: {worker_limits}")
            start = time.perf_counter()
            flights = dd.read_parquet(
                [str(p) for p in files], engine="pyarrow",
                columns=GROUP_KEYS + ["ArrDel15"], split_row_groups=True,
                aggregate_files=False,
            )
            grouped = (
                flights.groupby(GROUP_KEYS)["ArrDel15"]
                .agg(["count", "sum"], split_every=4, split_out=1)
                .compute()
            )
            seconds = time.perf_counter() - start
    result = (
        grouped.rename(columns={"count": "vuelos", "sum": "vuelos_retrasados"})
        .reset_index().sort_values(GROUP_KEYS).reset_index(drop=True)
    )
    observed_rows = int(result["vuelos"].sum())
    observed_delays = int(result["vuelos_retrasados"].sum())
    if observed_rows != expected_rows or observed_delays != expected_delays:
        raise AssertionError(
            f"Conteos inconsistentes: {observed_rows}/{expected_rows}; "
            f"{observed_delays}/{expected_delays}"
        )
    config.update({
        "limites_worker_mib": worker_limits,
        "particiones_lectura": int(flights.npartitions),
        "segundos": seconds,
        "filas_procesadas": observed_rows,
        "retrasos_observados": observed_delays,
        "grupos_resultado": len(result),
    })
    print(
        f"{name} repetición {repeat}: {workers} worker(s), {worker_limits} MiB, "
        f"{seconds:.2f} s, {observed_rows:,} vuelos, {len(result):,} grupos",
        flush=True,
    )
    return config, result


def main() -> None:
    OUT.mkdir(exist_ok=True)
    prior = OUT / "puntualidad_mensual_aerolinea_origen.parquet"
    archive = OUT / "obsoletos_con_2020"
    archive.mkdir(exist_ok=True)
    if prior.is_file() and not (archive / prior.name).exists():
        prior.replace(archive / prior.name)

    configs = {"A": (1, "1GiB"), "B": (2, "512MiB")}
    order = [(1, "A"), (1, "B"), (2, "B"), (2, "A"), (3, "A"), (3, "B")]
    measurements = []
    reference = None
    for repeat, name in order:
        workers, memory = configs[name]
        row, result = run_one(name, workers, memory, repeat)
        if reference is None:
            reference = result
        else:
            pd.testing.assert_frame_equal(reference, result, check_exact=True)
        measurements.append(row)

    benchmark = pd.DataFrame(measurements)
    benchmark.to_csv(OUT / "benchmark_dask.csv", index=False, encoding="utf-8-sig")
    comparison = (
        benchmark.groupby(["configuracion", "workers", "memoria_por_worker"], as_index=False)
        .agg(
            mediana_segundos=("segundos", "median"),
            minimo_segundos=("segundos", "min"),
            maximo_segundos=("segundos", "max"),
            filas=("filas_procesadas", "first"),
            grupos=("grupos_resultado", "first"),
            retrasos=("retrasos_observados", "first"),
        )
    )
    comparison.to_csv(OUT / "comparacion_dask.csv", index=False, encoding="utf-8-sig")

    result = reference.copy()
    result["tasa_retraso"] = result["vuelos_retrasados"] / result["vuelos"]
    result.to_parquet(OUT / "puntualidad_mensual_aerolinea_origen.parquet", index=False)

    n = int(result["vuelos"].sum())
    delayed = int(result["vuelos_retrasados"].sum())
    temporal = (
        result.groupby("Year", as_index=False)
        .agg(vuelos=("vuelos", "sum"), retrasados=("vuelos_retrasados", "sum"))
    )
    temporal["tasa_retraso"] = temporal["retrasados"] / temporal["vuelos"]
    temporal.to_csv(OUT / "division_temporal.csv", index=False, encoding="utf-8-sig")

    manifest = {
        "dask_version": dask.__version__,
        "periodo": "2018, 2019, 2021 y enero-julio de 2022",
        "year_2020_excluded": True,
        "rows": n,
        "delays": delayed,
        "delay_rate": delayed / n,
        "groups": len(result),
        "configurations": comparison.to_dict(orient="records"),
        "repetitions_per_configuration": 3,
        "same_output_all_runs": True,
        "scope": "Read prepared monthly Parquet files, aggregate delay counts by year, month, airline, and origin.",
    }
    (OUT / "benchmark_dask.json").write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False, default=float), encoding="utf-8"
    )
    print("\nDASK MEDIAN COMPARISON", flush=True)
    print(comparison.to_string(index=False), flush=True)
    print(f"\nValidated totals: {n:,} flights, {delayed:,} delays", flush=True)


if __name__ == "__main__":
    main()
