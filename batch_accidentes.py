"""
Análisis en Batch con Apache Spark
Dataset: Accidentes de Tránsito - Municipio de Bucaramanga (2012-2023)
Fuente: datos.gov.co (ID: 7cci-nqqb)
Autor: Edwin Pantoja Bustos
Curso: Big Data - UNAD
"""

# Este script está organizado en 5 secciones:
# SECCIÓN 1 - Crear la sesión de Spark
# SECCIÓN 2 - Cargar y limpiar el dataset CSV
# SECCIÓN 3 - Análisis con DataFrames (groupBy, agg, filter, orderBy)
# SECCIÓN 4 - Operaciones con RDD (flatMap, reduceByKey, reduce, take)
# SECCIÓN 5 - Resumen final del análisis

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, round as spark_round, sum as spark_sum
from pyspark.sql.types import IntegerType

# ─────────────────────────────────────────────
# SECCIÓN 1 - CREAR SESIÓN DE SPARK
# Se crea la sesión principal con el nombre de la aplicación.
# SparkSession es el punto de entrada para todas las funciones de Spark.
# ─────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("Accidentes_Bucaramanga_Batch") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("\n" + "="*60)
print(" ANÁLISIS BATCH - ACCIDENTES DE TRÁNSITO BUCARAMANGA")
print("="*60)

# ─────────────────────────────────────────────
# SECCIÓN 2 - CARGAR Y LIMPIAR DATOS
# Se carga el archivo CSV con header=True para usar los nombres de columna
# e inferSchema=True para detectar automáticamente los tipos de datos.
# Luego se renombran columnas con caracteres especiales (AÑO, DIURNIO/NOCTURNO)
# y se eliminan filas con valores nulos en columnas clave.
# ─────────────────────────────────────────────
df = spark.read.csv(
    "accidentes_bucaramanga.csv",
    header=True,
    inferSchema=True
)

# Renombrar columnas con caracteres especiales
df = df.withColumnRenamed("AÑO", "ANIO") \
       .withColumnRenamed("DÍA", "DIA") \
       .withColumnRenamed("DIURNIO/NOCTURNO", "JORNADA")

# Eliminar filas con valores nulos en columnas clave
df = df.dropna(subset=["ANIO", "GRAVEDAD", "BARRIO", "COMUNA", "JORNADA"])

# Convertir columnas de vehículos a entero
vehicle_cols = ["AUTOMOVIL", "CAMPERO", "CAMIONETA", "MICRO",
                "BUSETA", "BUS", "CAMION", "VOLQUETA", "MOTO",
                "BICICLETA", "OTRO"]

for col_name in vehicle_cols:
    df = df.withColumn(col_name, col(col_name).cast(IntegerType()))

df = df.withColumn("PEATON", col("PEATON").cast(IntegerType()))

total_registros = df.count()
print(f"\nTotal registros cargados: {total_registros}")
print(f"Total columnas: {len(df.columns)}")

# ─────────────────────────────────────────────
# SECCIÓN 3 - ANÁLISIS CON DataFrames
# Los DataFrames permiten usar operaciones SQL-like de alto nivel.
# groupBy agrupa los datos, agg aplica funciones de agregación (count, sum),
# filter filtra filas por condición y orderBy ordena los resultados.
# ─────────────────────────────────────────────

# --- 1. Accidentes por año ---
print("\n--- 1. Accidentes por año ---")
df.groupBy("ANIO") \
  .agg(count("*").alias("total_accidentes")) \
  .orderBy("ANIO") \
  .show(20)

# --- 2. Distribución por gravedad ---
print("--- 2. Distribución por gravedad ---")
total = df.count()
df.groupBy("GRAVEDAD") \
  .agg(count("*").alias("total")) \
  .withColumn("porcentaje", spark_round(col("total") / total * 100, 2)) \
  .orderBy(col("total").desc()) \
  .show()

# --- 3. Accidentes por tipo de vía (comunas) – Top 10 ---
print("--- 3. Top 10 comunas con más accidentes ---")
df.groupBy("COMUNA") \
  .agg(count("*").alias("total_accidentes")) \
  .orderBy(col("total_accidentes").desc()) \
  .show(10)

# --- 4. Accidentes por jornada ---
print("--- 4. Accidentes por jornada ---")
df.groupBy("JORNADA") \
  .agg(count("*").alias("total_accidentes")) \
  .orderBy(col("total_accidentes").desc()) \
  .show()

# --- 5. Top 10 barrios con más accidentes ---
print("--- 5. Top 10 barrios con más accidentes ---")
df.groupBy("BARRIO") \
  .agg(count("*").alias("total_accidentes")) \
  .orderBy(col("total_accidentes").desc()) \
  .show(10)

# --- 6. Accidentes fatales por año ---
print("--- 6. Accidentes fatales (Con muertos) por año ---")
df.filter(col("GRAVEDAD").isin(["Con muertos", "Con Muertos"])) \
  .groupBy("ANIO") \
  .agg(count("*").alias("accidentes_fatales")) \
  .orderBy("ANIO") \
  .show()

# --- 7. Total de vehículos involucrados por tipo (DataFrame) ---
print("--- 7. Total de vehículos involucrados por tipo ---")
vehicle_totals = {v: df.agg(spark_sum(col(v)).alias(v)).collect()[0][v] for v in vehicle_cols}
# Mostrar de forma ordenada
from pyspark.sql import Row
rows = [Row(tipo=k, total=int(v or 0)) for k, v in vehicle_totals.items()]
spark.createDataFrame(rows) \
     .orderBy(col("total").desc()) \
     .show()

# ─────────────────────────────────────────────
# SECCIÓN 4 - OPERACIONES CON RDDs
# Se convierte el DataFrame a RDD para aplicar operaciones de bajo nivel.
# flatMap expande cada fila en múltiples pares clave-valor.
# reduceByKey combina los valores por clave (suma totales por tipo de vehículo).
# reduce aplica una función acumulativa sobre todo el RDD (suma peatones).
# take retorna los primeros N elementos del RDD.
# ─────────────────────────────────────────────

# Convertir DataFrame a RDD (cada fila como Row)
rdd = df.rdd

# --- 8. FLATMAP + REDUCEBYKEY: Vehículos involucrados ---
print("--- FLATMAP + REDUCEBYKEY: Vehículos involucrados ---")
print("Total de vehículos involucrados por tipo (vía RDD):")

vehicle_rdd = rdd.flatMap(lambda row: [
    (veh, int(row[veh]) if row[veh] else 0) for veh in vehicle_cols
]).reduceByKey(lambda a, b: a + b) \
  .sortBy(lambda x: x[1], ascending=False)

for veh, total in vehicle_rdd.collect():
    print(f"  {veh}: {total}")

# --- 9. REDUCEBYKEY: Accidentes por gravedad vía RDD ---
print("\n--- REDUCEBYKEY: Accidentes por gravedad (vía RDD) ---")
gravedad_rdd = rdd.map(lambda row: (str(row["GRAVEDAD"]), 1)) \
                  .reduceByKey(lambda a, b: a + b) \
                  .sortBy(lambda x: x[1], ascending=False)

for gravedad, total in gravedad_rdd.collect():
    print(f"  {gravedad}: {total}")

# --- 10. REDUCE: Total de peatones involucrados ---
print("\n--- REDUCE: Total de peatones involucrados ---")
total_peatones = rdd.map(lambda row: int(row["PEATON"]) if row["PEATON"] else 0) \
                    .reduce(lambda a, b: a + b)
print(f"Total peatones involucrados en accidentes: {total_peatones}")

# --- 11. TAKE: Primeros 3 registros del RDD ---
print("\n--- TAKE: Primeros 3 registros del RDD ---")
primeros = rdd.take(3)
for r in primeros:
    print(f"  Fecha: {r['FECHA']}, Gravedad: {r['GRAVEDAD']}, "
          f"Comuna: {r['COMUNA']}, Barrio: {r['BARRIO']}")

# ─────────────────────────────────────────────
# SECCIÓN 5 - RESUMEN FINAL
# Se consolidan los datos más importantes del análisis.
# ─────────────────────────────────────────────
print("\n" + "="*60)
print("  RESUMEN DEL ANÁLISIS")
print("="*60)
fatales = df.filter(col("GRAVEDAD").isin(["Con muertos", "Con Muertos"])).count()
print(f"  Dataset: Accidentes de Tránsito - Bucaramanga (2012-2023)")
print(f"  Fuente: datos.gov.co (ID: 7cci-nqqb)")
print(f"  Total registros analizados: {total_registros}")
print(f"  Total accidentes fatales: {fatales}")
print(f"  Total peatones involucrados: {total_peatones}")
print(f"  Operaciones DataFrame: groupBy, agg, filter, orderBy, select")
print(f"  Operaciones RDD: map, filter, flatMap, reduceByKey, reduce, take, count")
print("="*60)

spark.stop()
