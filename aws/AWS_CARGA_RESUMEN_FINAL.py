"""Carga de agregados analíticos a PostgreSQL/RDS.
Requiere AWS_DATABASE_URL con sslmode=verify-full y sslrootcert.
Valida el encabezado antes de conectar y rechaza el CSV original.
"""
import argparse, csv, os
from pathlib import Path
import psycopg
from psycopg.conninfo import conninfo_to_dict
ROOT=Path(__file__).resolve().parent.parent
DEFAULT_CSV=ROOT/"entrega_aws"/"RESUMEN_ANALITICO_AWS_FINAL_2022_01-07.csv"
COLUMNS=["fecha","aerolinea","aeropuerto_origen","hora_salida","vuelos","suma_probabilidades","vuelos_con_resultado","retrasos_observados","probabilidad_media_modelo","tasa_retraso_observada","modelo_version"]
CREATE_SQL=(Path(__file__).parent/"AWS_TABLA_ANALITICA_FINAL.sql").read_text(encoding="utf-8")
COPY_SQL="""COPY stage_resumen (fecha,aerolinea,aeropuerto_origen,hora_salida,vuelos,suma_probabilidades,vuelos_con_resultado,retrasos_observados,probabilidad_media_modelo,tasa_retraso_observada,modelo_version) FROM STDIN WITH (FORMAT CSV, HEADER TRUE)"""
UPSERT_SQL="""INSERT INTO analitica.resumen_vuelos SELECT * FROM stage_resumen ON CONFLICT (fecha,aerolinea,aeropuerto_origen,hora_salida,modelo_version) DO UPDATE SET vuelos=EXCLUDED.vuelos,suma_probabilidades=EXCLUDED.suma_probabilidades,vuelos_con_resultado=EXCLUDED.vuelos_con_resultado,retrasos_observados=EXCLUDED.retrasos_observados,probabilidad_media_modelo=EXCLUDED.probabilidad_media_modelo,tasa_retraso_observada=EXCLUDED.tasa_retraso_observada"""
def validate(path):
 if not path.is_file(): raise FileNotFoundError(f"No existe: {path}")
 with path.open(encoding="utf-8-sig",newline="") as f:
  r=csv.reader(f)
  if next(r,[])!=COLUMNS: raise ValueError("Esquema no reconocido; no se conectó a la base. Debe ser el resumen agregado, no el CSV original.")
  return sum(1 for _ in r)
def main():
 ap=argparse.ArgumentParser(description="Carga solo el resumen agregado de vuelos.")
 ap.add_argument("--csv",type=Path,default=DEFAULT_CSV); a=ap.parse_args(); n=validate(a.csv)
 dsn=os.environ.get("AWS_DATABASE_URL","")
 if not dsn: raise SystemExit("Falta AWS_DATABASE_URL; configure credenciales fuera del repositorio.")
 cfg=conninfo_to_dict(dsn)
 if cfg.get("sslmode")!="verify-full" or not cfg.get("sslrootcert"): raise SystemExit("Se requiere sslmode=verify-full y sslrootcert.")
 if not Path(cfg["sslrootcert"]).is_file(): raise SystemExit("No se encontró el certificado sslrootcert.")
 with psycopg.connect(dsn) as conn:
  with conn.cursor() as cur:
   cur.execute(CREATE_SQL)
   cur.execute("CREATE TEMP TABLE stage_resumen (LIKE analitica.resumen_vuelos) ON COMMIT DROP")
   with a.csv.open("rb") as f,cur.copy(COPY_SQL) as copy:
    while chunk:=f.read(1024*1024): copy.write(chunk)
   cur.execute("SELECT COUNT(*) FROM stage_resumen"); loaded=cur.fetchone()[0]
   if loaded!=n: raise RuntimeError(f"Conteo distinto: entrada={n}, staging={loaded}")
   cur.execute(UPSERT_SQL)
   cur.execute("SELECT COUNT(*),COALESCE(SUM(vuelos),0),COALESCE(SUM(retrasos_observados),0) FROM stage_resumen")
   result=cur.fetchone()
 print(f"Filas agregadas: {result[0]:,}; vuelos representados: {result[1]:,}; retrasos: {result[2]:,}.")
 print("El archivo de vuelos original no se cargó.")
if __name__=="__main__": main()

