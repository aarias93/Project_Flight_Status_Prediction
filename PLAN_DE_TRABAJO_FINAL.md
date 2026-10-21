# Plan de trabajo actualizado

## Propósito
Analizamos vuelos históricos de Estados Unidos para describir retrasos y evaluar una puntuación de riesgo para todos los vuelos. El 10 % de mayor puntuación es solo una métrica retrospectiva. El escenario a 24 horas es un supuesto académico. Colombia es un contexto futuro propuesto, pendiente de datos y validación local.

## Avances completados
1. Excluimos 2020 de todas las salidas analíticas y mantenemos intacto el CSV original.
2. Generamos una base Parquet con 24.171.385 filas, 61 columnas y 678.550.973 bytes (0,679 GB).
3. Recalculamos EDA de las 61 variables y métricas por periodo, origen, día y hora.
4. Comparamos Pearson y Spearman sobre la misma muestra completa, usando rangos para Spearman.
5. Comparamos Logística 400k y Boosting 400k con la misma muestra y predictores; escalamos para logística y no para boosting. Reportamos Boosting 1M por separado.
6. Ejecutamos Dask con 1 worker/1 GiB y 2 workers/512 MiB por worker; procesaron las mismas filas y grupos.
7. Ejecutamos Prefect para los siete meses disponibles de 2022 y validamos el total mensual.
8. Preparamos una salida analítica agregada para AWS; el CSV original no forma parte de esa salida.
9. Actualizamos el borrador del informe y los resultados principales.

## Pendientes
1. Verificar el horizonte de 24 horas con marcas de disponibilidad por predictor y con el proceso de planificación.
2. Revisar RAC y procedimientos de una aerolínea. El umbral estadounidense de 15 minutos no se presenta como norma colombiana.
3. Probar calibración y estabilidad con un periodo nuevo, independiente del desarrollo, y acordar umbrales con el área usuaria.
4. Completar AWS con solo el agregado; verificar conteos, permisos y consulta. Guardar credenciales fuera del repositorio.
5. Conectar Grafana a la base y adjuntar evidencia de consultas, filtros y paneles.
6. Revisar el Word/Google Docs final y compartirlo con rodolfo.meza@gmail.com con edición.
7. Confirmar notebooks/scripts vigentes en GitHub y que datos fuente, Parquet, modelos privados y temporales no se publiquen.

## Límites
Las métricas de enero–julio de 2022 son retrospectivas y no independientes; la probabilidad media estimada queda por debajo de la tasa observada, por lo que se requiere calibración. El 10 % superior deja fuera la mayoría de retrasos y no es una cuota operativa: la propuesta asigna puntuación a todos los vuelos. Ningún resultado demuestra retrasos evitados, causalidad o ahorro. Las mediciones Dask corresponden a una tarea y un equipo; no justifican una regla universal de escalamiento.
