# Plan de trabajo actualizado

## Propósito

Nosotros analizamos vuelos históricos de Estados Unidos para estimar cuáles tendrían mayor riesgo de llegar con al menos 15 minutos de retraso. Proponemos que un equipo de planificación de una aerolínea consulte el riesgo de todos sus vuelos y reciba alertas sobre los de mayor riesgo. El horizonte de 24 horas antes de la salida es un supuesto académico: los datos históricos no muestran cuándo estuvo disponible cada variable.

La aplicación a Colombia es una propuesta. Los datos usados no son colombianos y el modelo aún no ha sido validado en Colombia.

## Pasos de ajuste

1. **Cerrar el alcance del proyecto.** Mantener como población analizada los vuelos de Estados Unidos y describir Colombia como escenario propuesto, pendiente de datos y validación local. Definir un vuelo programado como unidad de análisis.
2. **Aclarar el resultado.** Mantener ArrDel15 (llegada con retraso de 15 minutos o más) como etiqueta histórica del conjunto. Explicar que el umbral proviene de los datos/reportes de EE. UU. y no se afirma que sea un umbral normativo colombiano.
3. **Revisar la anticipación de 24 horas.** Documentarla como supuesto; verificar con el área operacional qué campos se conocerían realmente en ese momento. La tabla histórica no permite reconstruir su disponibilidad temporal.
4. **Ajustar la propuesta operacional.** Calcular una puntuación para cada vuelo programado y usar alertas configurables para resaltar riesgos altos. Conservar la selección del 10 % solamente como escenario retrospectivo de comparación, no como límite para vigilar vuelos.
5. **Excluir 2020 de todo análisis.** Mantener el archivo fuente intacto, pero filtrar 2020 antes de EDA comparativo, transformaciones, Dask, modelos, validación, evaluación y visualizaciones. No presentar métricas ni conclusiones de ese año. Recalcular todas las salidas previas y registrar el periodo analítico: 2018, 2019, 2021 y enero–julio de 2022. Seguir [la decisión metodológica](DECISION_EXCLUSION_2020.md).
6. **Verificar el contexto colombiano.** Consultar las versiones vigentes de los Reglamentos Aeronáuticos de Colombia (RAC), las obligaciones aplicables y los procedimientos de la aerolínea antes de proponer uso operacional. La Aeronáutica Civil publica la normativa en [su portal oficial](https://www.aerocivil.gov.co/normatividad/NormatividadAeronautica/Pages/RAC.aspx). No presentar el umbral estadounidense de 15 minutos como una regla colombiana.
7. **Respaldar la viabilidad con mediciones.** Reportar cantidad de vuelos, tiempo de ejecución, memoria configurada y limitaciones del equipo. Aclarar que el crecimiento observado es lineal si los resultados muestran una tendencia lineal; no afirmar escalabilidad exponencial sin evidencia.
8. **Revisar los métodos por separado.** Preparar las entradas según los supuestos matemáticos de cada técnica de correlación o modelo, según la comparación que el grupo defina. Documentar escalamiento, variables, periodo y métricas para que la comparación sea justa. Evitar una conclusión de mejor modelo sin resultados comparables.
9. **Actualizar los entregables.** Alinear informe, notebook, presentación, tablero Grafana y README con la exclusión de 2020. Los resultados previos de EDA, Dask, conteos y archivos Parquet preparados son obsoletos. Recalcular la base y sus tamaños después del filtro antes de informar cifras.
10. **Sincronizar el repositorio.** Mantener el CSV y Parquet originales fuera de GitHub; publicar código, notebooks, documentación y resultados analíticos permitidos. Revisar cambios antes de confirmar la sincronización.

## Uso de las métricas retrospectivas

Las cifras del escenario del 10 % describen cómo se ordenaron históricamente los vuelos evaluados. No prueban que se hayan evitado retrasos, que exista una cuota real de revisión ni que se produzca ahorro. Una operación que puntúe todos los vuelos debe definir con sus responsables qué umbral de alerta es útil y cómo se verifican las alertas.
