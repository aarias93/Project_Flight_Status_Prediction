CREATE SCHEMA IF NOT EXISTS analitica;

CREATE TABLE IF NOT EXISTS analitica.resumen_vuelos (
    fecha DATE NOT NULL,
    aerolinea TEXT NOT NULL,
    aeropuerto_origen TEXT NOT NULL,
    hora_salida SMALLINT NOT NULL CHECK (hora_salida BETWEEN 0 AND 23),
    vuelos BIGINT NOT NULL,
    suma_probabilidades DOUBLE PRECISION NOT NULL,
    vuelos_con_resultado BIGINT NOT NULL,
    retrasos_observados BIGINT NOT NULL,
    probabilidad_media_modelo DOUBLE PRECISION NOT NULL,
    tasa_retraso_observada DOUBLE PRECISION NOT NULL,
    modelo_version TEXT NOT NULL,
    PRIMARY KEY (fecha, aerolinea, aeropuerto_origen, hora_salida, modelo_version)
);
