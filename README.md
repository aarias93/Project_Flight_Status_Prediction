# Project_Flight_Status_Prediction
261009 - Espacio de trabajo para el proyecto grupal de Big Data 
**Elaborado por:**

* Angela Liseth Arias Robles
* Andres Jose Triana Olivera
* Luis Felipe Cangrejo Motta

## Fuente de datos
[Flight Delay Dataset (2018–2022) en Kaggle](https://www.kaggle.com/datasets/robikscube/flight-delay-dataset-20182022)

## Instrucciones del trabajo

[Ver las instrucciones completas](INSTRUCCIONES.md)

---

# Priorización de vuelos con riesgo de retraso

Proyecto grupal de Herramientas de Big Data.
Maestría en Analítica Aplicada — Universidad de La Sabana.

## 1. Objetivo de negocio

Priorizar, antes de la salida programada, los vuelos con mayor riesgo
de llegar con al menos 15 minutos de retraso.

El usuario propuesto es el equipo de operaciones de una aerolínea.
Se evalúa una capacidad ilustrativa de seguimiento del 10 % de los vuelos.

La unidad de análisis es un vuelo individual.

## 2. Fuente y cobertura

Dataset: Flight Delay Dataset 2018–2022, publicado por robikscube.

https://www.kaggle.com/datasets/robikscube/flight-delay-dataset-20182022

- Archivo original: Combined_Flights_2018-2022.csv.
- Tamaño registrado en el notebook: 10,21 GB.
- Registros originales: 29.193.782.
- Columnas originales: 61.
- Cobertura observada: enero de 2018 a julio de 2022.
- Base preparada: 28.347.597 vuelos elegibles.
- Exportación: 55 archivos Parquet mensuales.

El CSV original debe descargarse por separado. No se incluye en GitHub
ni se carga a AWS.

## 3. Variable objetivo

- Clase 1: ArrDelay >= 15 minutos.
- Clase 0: ArrDelay < 15 minutos.

La clasificación se contrasta con ArrDel15. Se excluyen vuelos cancelados,
desviados y registros sin resultado conocido de la base supervisada.
Los resultados desconocidos no se convierten en clase 0.

## 4. Archivos principales

| Archivo o carpeta | Función |
| --- | --- |
| Preparacion_modelo_vuelos.ipynb | Preparación, controles, Dask, modelos y carga analítica |
| flujo_vuelos.py | Predicción y agregación recurrente con Prefect |
| programar_vuelos.py | Revisión periódica de entradas nuevas o modificadas |
| Requirements.txt | Versiones de las dependencias principales |
| entorno_ejecucion.txt | Información del entorno utilizado |
| datos_modelo/ | Parquet mensuales preparados |
| modelos_v2/boosting_v2_1m.joblib | Pipeline final entrenado |
| entradas_prefect/ | Parquet preparados para procesamiento recurrente |
| salidas_prefect/ | Resúmenes analíticos exportados |
| control_prefect.json | Registro de entradas procesadas |
| grafana/dashboard_vuelos.json | Exportación del tablero de Grafana |

El nombre o la ubicación actual del JSON de Grafana puede diferir.
Las carpetas de datos, modelos y salidas se generan localmente.

## 5. Entorno y ejecución del notebook

El entorno utilizado quedó registrado en entorno_ejecucion.txt.
Las versiones principales se encuentran en Requirements.txt.

Para preparar otro entorno, instalar sus dependencias desde la terminal:

    python -m pip install -r Requirements.txt

Esta instalación debe comprobarse en el equipo de destino.
El registro de versiones no demuestra, por sí solo, una reproducción
completa desde cero.

Abrir la carpeta del proyecto en VS Code y seleccionar el kernel
del entorno correspondiente. Colocar el CSV original en la ubicación
esperada por el notebook y revisar las rutas antes de ejecutar.

Seguir este orden:
1. Diagnóstico del entorno y localización del CSV.
2. Conversión por bloques a Parquet.
3. Validación de cobertura, calidad y variable objetivo.
4. Construcción de características y exportación mensual.
5. Comparación de configuraciones de Dask.
6. Entrenamiento y evaluación de los modelos.
7. Ejecución del flujo Prefect.
8. Consolidación de resultados y carga separada a AWS.
9. Conexión e importación del tablero de Grafana.

El notebook contiene operaciones que generan archivos y realizan cargas.
Revisar cada sección antes de ejecutar todo nuevamente.

## 6. Evaluación con Dask

Se compara la misma agregación sobre la base preparada:

| Configuración | Workers | Memoria por worker | Tiempo mediano |
| --- | --- | --- | --- |
| A | 1 | 1 GiB | 4,190 segundos |
| B | 2 | 512 MiB | 2,712 segundos |

Se realizaron tres ejecuciones por configuración y se verificaron
los mismos 102.792 grupos en las seis ejecuciones.

La reducción del tiempo mediano fue aproximadamente 35,3 %.
Los resultados dependen de las condiciones locales y de la caché.
La memoria configurada no equivale a una medición del consumo máximo.

## 7. Modelo final

Modelo: HistGradientBoostingClassifier.

Variables:
- Operating_Airline.
- Origin.
- Dest.
- DayOfWeek.
- hora_salida, derivada de CRSDepTime.
- Distance.
- duracion_programada_min.
- duracion_programada_faltante.

No se utilizan como predictores los retrasos observados, las horas reales
de salida o llegada ni los tiempos reales de rodaje.

Entrenamiento: muestra de 1.000.000 de vuelos de enero a septiembre de 2021.
Validación: octubre a diciembre de 2021.
Evaluación adicional: enero a julio de 2022.

2022 ya había sido consultado durante el desarrollo; por tanto,
no constituye una prueba final completamente independiente.

En 2022, el modelo final identificó 163.780 retrasos entre los 394.492
vuelos priorizados: precisión en el grupo seleccionado de 41,52 %.
La primera versión identificó 131.777: diferencia de 32.003 retrasos.

Esta comparación no demuestra reducción causal de retrasos ni ahorro.
Las versiones también difieren en sus condiciones de entrenamiento.

## 8. Prefect

El flujo recibe un Parquet preparado con las características que espera
el modelo. No recibe directamente el CSV original.

Desde una terminal situada en la carpeta del proyecto:

    python flujo_vuelos.py --archivo datos_modelo/vuelos_2022_01.parquet

Para la revisión recurrente, iniciar el servidor local de Prefect:

    prefect server start

Mantener esa terminal abierta. En otra terminal, con el mismo entorno
y situada en la carpeta del proyecto, ejecutar:

    python programar_vuelos.py

El programador utiliza http://127.0.0.1:4200/api y revisa
entradas_prefect/ cada 60 segundos.

Procesa archivos nuevos o modificados y vuelve a procesarlos si cambia
el modelo o el código analítico. Las entradas sin cambios se omiten
cuando su salida sigue disponible.

El flujo predice por bloques, agrega por fecha, aerolínea y origen,
y exporta un CSV a salidas_prefect/.

Puede generar puntuaciones sin ArrDel15. En ese caso, no presenta
resultados desconocidos como cero retrasos.

El servidor y el proceso de programación deben permanecer activos.
La automatización implementada llega hasta la exportación del CSV.

## 9. AWS y Grafana

La consolidación y carga a Amazon RDS PostgreSQL se ejecutan por separado
en el notebook. Solo se almacenan resultados analíticos agregados.

La carga histórica documentada contiene:
- 358.665 filas agregadas.
- 3.944.916 vuelos representados.
- 853.962 retrasos observados.
- Período: enero a julio de 2022.

Las comprobaciones de esa carga están fijadas al período histórico.
Para incorporar otros períodos se deben adaptar y validar.

Grafana utiliza una conexión PostgreSQL con un usuario de lectura.
Las credenciales se configuran en el entorno de ejecución y no se
incluyen en el repositorio.

Importar el JSON del tablero y asignar la fuente de datos correspondiente.
Configurar la conexión TLS y el certificado de confianza utilizados
para RDS. El certificado debe ser accesible desde el contenedor de Grafana.

El tablero permite filtrar por aerolínea y revisar tasas, riesgo estimado
y aeropuertos de origen. El período del tablero exportado está fijado
a enero–julio de 2022.

## 10. Limitaciones

- El modelo supervisado se evalúa sobre vuelos completados sin desvío.
- La selección del 10 % se calculó sobre todo el período, no por día.
- Se requiere una evaluación en un período nuevo.
- Las probabilidades requieren revisar su calibración.
- No se incorporaron variables meteorológicas.
- No se demostró transferencia directa a vuelos de Colombia.
- La carga a AWS no forma parte del flujo automático actual.

## 11. Soportes de entrega

El repositorio debe acompañarse del EDA del grupo, el documento
colaborativo con las fases CRISP-DM y las evidencias de ejecución.

La presentación ejecutiva resume el proyecto y sus resultados.
