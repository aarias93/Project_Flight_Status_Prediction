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
8. Preparamos una salida analítica agregada para la integración en AWS; el CSV original no forma parte de esa salida.
9. Actualizamos el borrador del informe y los resultados principales.

## Siguiente etapa de implementación
1. Contrastar el horizonte de 24 horas con las marcas de disponibilidad por predictor y con el proceso de planificación.
2. Revisar RAC y procedimientos de una aerolínea; documentar el contexto de aplicación colombiano.
3. Probar calibración y estabilidad con un periodo nuevo e independiente del desarrollo, y acordar umbrales con el área usuaria.
4. Aplicar el esquema agregado en PostgreSQL, ejecutar el cargador y validar conteos, permisos y consultas. Mantener las credenciales fuera del repositorio.
5. Configurar la fuente PostgreSQL en Grafana, importar el tablero y revisar sus consultas, filtros y paneles.
6. Actualizar el informe de procesos y compartir el enlace de trabajo con el docente.
7. Revisar notebooks y scripts vigentes en GitHub y confirmar que datos fuente, Parquet, modelos privados y temporales no se publiquen.

## Consideraciones para interpretar los resultados
Las métricas de enero–julio de 2022 son retrospectivas y no independientes; la probabilidad media estimada queda por debajo de la tasa observada, por lo que se requiere calibración. El 10 % superior deja fuera la mayoría de retrasos y no es una cuota operativa: la propuesta asigna puntuación a todos los vuelos. Ningún resultado demuestra retrasos evitados, causalidad o ahorro. Las mediciones Dask corresponden a una tarea y un equipo; no justifican una regla universal de escalamiento.
