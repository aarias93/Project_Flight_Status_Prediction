"""Re-evaluate existing V2 models on regenerated, 2020-excluded Parquet data."""

from __future__ import annotations

import json
import time
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from sklearn.metrics import (
    average_precision_score,
    brier_score_loss,
    roc_auc_score,
)
from threadpoolctl import threadpool_limits


ROOT = Path(__file__).resolve().parent
DATA = ROOT / "datos_modelo"
OUT = ROOT / "resultados_modelo_v2"
MODELS = {
    "Logística 400k": ROOT / "Insumos" / "modelos_v2" / "logistica_v2.joblib",
    "Boosting 400k": ROOT / "Insumos" / "modelos_v2" / "boosting_v2.joblib",
    "Boosting 1M": ROOT / "modelos_v2" / "boosting_v2_1m.joblib",
}


def metric_row(model_name: str, period: str, y: np.ndarray, p: np.ndarray, months: np.ndarray) -> dict:
    n = len(y)
    positives = int(y.sum())
    k = int(np.ceil(n * 0.10))
    tie_order = np.random.default_rng(42).permutation(n)
    selected = np.lexsort((tie_order, -p))[:k]
    found = int(y[selected].sum())
    baseline = positives / n
    return {
        "periodo": period,
        "modelo": model_name,
        "vuelos": n,
        "retrasos_observados": positives,
        "tasa_retraso": baseline,
        "roc_auc": roc_auc_score(y, p),
        "average_precision": average_precision_score(y, p),
        "brier": brier_score_loss(y, p),
        "vuelos_priorizados_10pct": k,
        "precision_top10": found / k,
        "recall_top10": found / positives,
        "lift_top10": (found / k) / baseline,
        "retrasos_detectados_top10": found,
        "probabilidad_media": float(p.mean()),
    }


def evaluate(period: str, files: list[Path], models: dict[str, object]) -> tuple[list[dict], list[dict]]:
    n = sum(pq.ParquetFile(path).metadata.num_rows for path in files)
    y = np.empty(n, dtype=np.int8)
    months = np.empty(n, dtype=np.int8)
    predictions = {name: np.empty(n, dtype=np.float64) for name in models}
    position = 0
    start = time.perf_counter()

    with threadpool_limits(limits=2):
        for path in files:
            with pq.ParquetFile(path) as parquet:
                for batch in parquet.iter_batches(
                    batch_size=32_768,
                    columns=sorted(set(
                        col for model in models.values()
                        for col in model.feature_names_in_
                    ) | {"ArrDel15", "Month", "Year"}),
                    use_threads=False,
                ):
                    frame = batch.to_pandas()
                    end = position + len(frame)
                    if frame["Year"].eq(2020).any():
                        raise AssertionError("La evaluación contiene año 2020.")
                    y[position:end] = frame["ArrDel15"].to_numpy(dtype=np.int8)
                    months[position:end] = frame["Month"].to_numpy(dtype=np.int8)
                    for name, model in models.items():
                        features = list(model.feature_names_in_)
                        predictions[name][position:end] = model.predict_proba(
                            frame[features]
                        )[:, 1]
                    position = end
            print(f"{period}: {path.name}, {position:,}/{n:,} vuelos", flush=True)

    if position != n or not set(np.unique(y)).issubset({0, 1}):
        raise AssertionError("La evaluación quedó incompleta o la etiqueta no es binaria.")
    for p in predictions.values():
        if not np.isfinite(p).all() or ((p < 0) | (p > 1)).any():
            raise AssertionError("Se encontraron probabilidades inválidas.")

    overall = [metric_row(name, period, y, pred, months) for name, pred in predictions.items()]
    monthly = []
    for month in sorted(np.unique(months)):
        mask = months == month
        for name, pred in predictions.items():
            monthly.append(metric_row(name, f"{period}-{month:02d}", y[mask], pred[mask], months[mask]))
    print(f"{period}: evaluación completa en {(time.perf_counter()-start)/60:.2f} min", flush=True)
    return overall, monthly


def main() -> None:
    OUT.mkdir(exist_ok=True)
    missing = [path for path in MODELS.values() if not path.is_file()]
    if missing:
        raise FileNotFoundError("Faltan modelos: " + ", ".join(map(str, missing)))
    models = {name: joblib.load(path) for name, path in MODELS.items()}
    feature_sets = {tuple(model.feature_names_in_) for model in models.values()}
    if len(feature_sets) != 1:
        raise AssertionError("Los modelos no reciben el mismo conjunto de variables.")

    val_files = [DATA / f"vuelos_2021_{month:02d}.parquet" for month in [10, 11, 12]]
    test_files = [DATA / f"vuelos_2022_{month:02d}.parquet" for month in range(1, 8)]
    if not all(path.is_file() for path in val_files + test_files):
        raise FileNotFoundError("Faltan archivos de validación o evaluación.")

    val_overall, val_monthly = evaluate("validacion_2021_Q4", val_files, models)
    test_overall, test_monthly = evaluate("evaluacion_2022_enero_julio", test_files, models)
    pd.DataFrame(val_overall).to_csv(
        OUT / "comparacion_modelos_validacion_2021_Q4_recalculada.csv",
        index=False, encoding="utf-8-sig",
    )
    pd.DataFrame(test_overall).to_csv(
        OUT / "comparacion_modelos_2022_recalculada.csv",
        index=False, encoding="utf-8-sig",
    )
    pd.DataFrame(val_monthly + test_monthly).to_csv(
        OUT / "comparacion_mensual_modelos_recalculada.csv",
        index=False, encoding="utf-8-sig",
    )
    record = {
        "version": "V2",
        "train_period": "2021-01 a 2021-09",
        "common_validation": "2021-10 a 2021-12",
        "additional_evaluation": "2022-01 a 2022-07",
        "year_2020_excluded": True,
        "matched_algorithm_comparison": "Logística 400k vs Boosting 400k; same training sample and periods, each pipeline keeps its appropriate preprocessing.",
        "larger_boosting_candidate": "Boosting 1M uses a larger training sample and is reported separately from the matched 400k algorithm comparison.",
        "predictor_columns": list(next(iter(models.values())).feature_names_in_),
        "probabilities_stored_as": "float64",
        "top10_is_retrospective_only": True,
        "conclusion_limit": "These retrospective ranking metrics do not demonstrate that operational actions prevent delays or save money.",
        "validation": val_overall,
        "evaluation": test_overall,
    }
    (OUT / "evaluacion_recalculada.json").write_text(
        json.dumps(record, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    print("\nVALIDACIÓN 2021 Q4", pd.DataFrame(val_overall).round(4).to_string(index=False), flush=True)
    print("\nEVALUACIÓN ADICIONAL 2022", pd.DataFrame(test_overall).round(4).to_string(index=False), flush=True)


if __name__ == "__main__":
    main()
