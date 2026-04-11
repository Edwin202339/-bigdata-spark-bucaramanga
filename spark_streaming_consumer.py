from kafka import KafkaConsumer
from pyspark.sql import SparkSession
from pyspark.sql import Row
import json

# =============================================
# INICIALIZAR SPARK SESSION
# =============================================
spark = SparkSession.builder \
    .appName("SensorDataAnalysis") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# =============================================
# CONFIGURAR CONSUMER DE KAFKA
# =============================================
consumer = KafkaConsumer(
    'sensor_data',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='latest',
    consumer_timeout_ms=5000
)

print("\n========================================")
print("  SPARK STREAMING - SENSOR DATA ANALYSIS")
print("========================================\n")

batch_num = 1

while True:
    # =============================================
    # LEER MENSAJES DE KAFKA EN MICRO-BATCH
    # =============================================
    messages = []
    print(f"--- Micro-Batch {batch_num} ---")
    
    try:
        for msg in consumer:
            messages.append(msg.value)
            if len(messages) >= 10:
                break
    except Exception:
        pass

    if messages:
        # =============================================
        # PROCESAR CON PYSPARK
        # =============================================
        rows = [Row(
            sensor_id=m['sensor_id'],
            temperature=float(m['temperature']),
            humidity=float(m['humidity'])
        ) for m in messages]

        df = spark.createDataFrame(rows)

        print(f"Mensajes recibidos: {len(messages)}")
        print("\nPromedio por sensor:")
        
        df.groupBy("sensor_id") \
          .avg("temperature", "humidity") \
          .withColumnRenamed("avg(temperature)", "avg_temperature") \
          .withColumnRenamed("avg(humidity)", "avg_humidity") \
          .orderBy("sensor_id") \
          .show()

        batch_num += 1
    else:
        print("Esperando datos de Kafka...\n")
