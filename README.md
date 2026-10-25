# Priorización de vuelos con riesgo de retraso

Proyecto grupal de Herramientas de Big Data, Maestría en Analítica Aplicada, Universidad de La Sabana.
Equipo: Angela Liseth Arias Robles, Andres Jose Triana Olivera y Luis Felipe Cangrejo Motta.

## Objetivo y alcance
Analizamos vuelos históricos de Estados Unidos para describir retrasos y evaluar cómo ordenar todos los vuelos programados por riesgo de llegar con 15 minutos o más de retraso. El equipo de planificación de una aerolínea es el usuario propuesto. El cálculo a 24 horas de la salida es un escenario académico: la fuente no documenta cuándo estuvo disponible cada variable.

Colombia es una posible aplicación futura, no una validación local. Antes de usar el modelo allí se requieren datos colombianos, revisión de RAC y procedimientos operativos, comprobación del horizonte, calibración y evaluación prospectiva.

## Datos y exclusión de 2020
Fuente: [Flight Delay Dataset 2018–2022 en Kaggle](https://www.kaggle.com/datasets/robikscube/flight-delay-dataset-20182022).

El CSV original contiene 29.193.782 filas, 61 columnas y pesa 10.214.365.394 bytes (10,214 GB decimales). Excluimos 2020 de todos los análisis, modelos, evaluaciones y visualizaciones por la falta de comparabilidad del periodo pandémico. No presentamos métricas de ese año y conservamos el CSV original intacto. Periodo analítico: 2018, 2019, 2021 y enero–julio de 2022.

La base Parquet analítica, con las 61 columnas y sin 2020, contiene 24.171.385 filas y pesa 678.550.973 bytes (0,679 GB decimales). La reducción respecto del CSV original es 93,36 % (15,05 veces menor); combina el filtro de 2020 y la conversión a Parquet. Los 43 Parquet mensuales de modelado ocupan 89.234.974 bytes (0,089 GB). La etiqueta supervisada válida contiene 23.634.669 vuelos, con 4.537.818 retrasos (19,20 %).

## Resultados reproducidos
- Dask: Configuración A: 1 proceso *worker*, 1 hilo por *worker* y 1 GiB de memoria por *worker* (1 GiB total); mediana 21,656 s. Configuración B: 2 procesos *worker*, 1 hilo por *worker* y 512 MiB por *worker* (1 GiB total); mediana 10,568 s. Se hicieron tres ejecuciones por configuración y las seis produjeron los mismos 23.634.669 vuelos y 81.136 grupos. Los límites de memoria son asignaciones por proceso, no una medición de la RAM máxima consumida. Es una comparación de una tarea en un equipo, no una afirmación de escalamiento universal.
- Modelos: la comparación justa entre Logística 400k y Boosting 400k usa la misma muestra y periodos, con preprocesamiento adecuado para cada algoritmo. Boosting 1M se reporta como candidato con más datos de entrenamiento.
- Evaluación enero–julio 2022: Boosting 1M alcanzó ROC AUC 0,6455 y precisión de 41,52 % en el 10 % de mayor puntuación. Es retrospectiva, no independiente, y no demuestra retrasos evitados ni ahorro. Se deben calibrar las probabilidades.
- Prefect: ejecución mensual enero–julio de 2022, 3.944.916 vuelos y 1.726.512 filas agregadas por fecha, aerolínea, origen y hora. El flujo comienza con Parquet preparado, no automatiza el proceso entero desde CSV.

## Versiones de documentos

Los tres entregables vigentes están agrupados en `ENTREGA_FINAL/`: informe, presentación y notebook de preparación del modelo. `README.md`, `INSTRUCCIONES.md` y `PLAN_DE_TRABAJO_FINAL.md` orientan la navegación y el trabajo. Las versiones anteriores se conservan identificadas en `versiones_anteriores/`.

## Archivos principales
| Archivo o carpeta | Uso |
| --- | --- |
| ENTREGA_FINAL/Informe final.docx | Informe final del proyecto |
| ENTREGA_FINAL/Presentación final.pptx | Diapositivas finales para la exposición |
| ENTREGA_FINAL/Preparacion modelo vuelos.ipynb | Notebook final de preparación del modelo |
| notebooks/ | Notebooks de exploración y desarrollo |
| scripts/ | Scripts de procesamiento, evaluación y Prefect |
| referencias/ | Instrucciones originales y enlace a los datos |
| PLAN_DE_TRABAJO_FINAL.md | Estado y tareas siguientes |
| DECISION_EXCLUSION_2020.md | Decisión metodológica sobre 2020 |
| scripts/recalcular_base_sin_2020.py | Produce Parquet analítico y particiones de modelado |
| scripts/recalcular_resultados_eda.py | Recalcula EDA descriptivo y tablas |
| scripts/ejecutar_benchmark_dask.py | Compara configuraciones Dask |
| scripts/evaluar_modelos_v2.py | Reevalúa modelos sin 2020 |
| scripts/flujo_vuelos.py | Predicción y agregación con Prefect |
| scripts/consolidar_resultados_prefect.py | Consolida las salidas mensuales |
| resultados_eda/ | Estadísticos y resúmenes; los archivos grandes quedan locales |
| resultados_dask/ | Tiempos y resultados de Dask |
| resultados_modelo_v2/ | Métricas recalculadas por periodo y modelo |
| entrega_aws/ | Resultado agregado preparado para AWS |
| grafana/DASHBOARD_GRAFANA_FINAL_RIESGO_VUELOS.json | Definición local del tablero |
| aws/AWS_TABLA_ANALITICA_FINAL.sql | Esquema agregado para PostgreSQL/RDS |
| aws/AWS_CARGA_RESUMEN_FINAL.py | Cargador idempotente; requiere credenciales locales seguras |

El CSV fuente, los Parquet grandes y los temporales quedan fuera de GitHub. Los pipelines pequeños requeridos por el flujo sí están incluidos; los archivos de datos se generan o descargan localmente.

## Integración de resultados y visualización
La salida analítica agregada para el periodo enero–julio de 2022 está en `entrega_aws/RESUMEN_ANALITICO_AWS_FINAL_2022_01-07.csv` y en su copia Parquet. El esquema de `aws/AWS_TABLA_ANALITICA_FINAL.sql` organiza las métricas en la tabla `analitica.resumen_vuelos`. El cargador de `aws/AWS_CARGA_RESUMEN_FINAL.py` valida el esquema, utiliza conexión PostgreSQL con TLS y carga únicamente los agregados. La definición de `grafana/DASHBOARD_GRAFANA_FINAL_RIESGO_VUELOS.json` consulta esa tabla para presentar métricas y desgloses por periodo, aerolínea, aeropuerto y hora.

Para reproducir la integración, configure la conexión y el certificado TLS en el entorno local, ejecute el cargador con el archivo agregado, y en Grafana configure la fuente PostgreSQL e importe la definición del tablero. La base analítica excluye los registros originales de vuelos.

## Ejecución
Consulte `Requirements.txt` y abra la raíz del repositorio en VS Code. Ejecute los notebooks manteniendo esa raíz como carpeta de trabajo para resolver las rutas de datos. Para recalcular los resultados, ejecute `python scripts/recalcular_base_sin_2020.py`, `python scripts/recalcular_resultados_eda.py`, `python scripts/ejecutar_benchmark_dask.py`, `python scripts/evaluar_modelos_v2.py` y `python scripts/consolidar_resultados_prefect.py`, en ese orden, con el CSV original y los modelos requeridos. Para el flujo de Prefect, use `python scripts/flujo_vuelos.py`. Configure `AWS_DATABASE_URL` y el certificado TLS fuera del repositorio; cargue solo el resumen agregado.

Consulte `INSTRUCCIONES.md` y `PLAN_DE_TRABAJO_FINAL.md`. Para identificar documentos históricos, consulte `versiones_anteriores/LEEME_VERSIONES_ANTERIORES.md`.
