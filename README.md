BigData - Spark y Kafka - Bucaramanga
Tarea 3 - Procesamiento de datos con Apache Spark
UNAD | Grupo 202016911_72 | Edwin Pantoja Bustos

Descripción
Este repositorio contiene los scripts desarrollados para la Tarea 3 del curso Big Data de la UNAD.

Archivos
Video 1 - Procesamiento por lotes
batch_accidentes.py - Análisis de accidentes de tránsito en Bucaramanga usando PySpark. Procesa 39.193 registros con operaciones DataFrame y RDD.
Dataset: Accidentes de Tránsito - Municipio de Bucaramanga (2012-2023)

Video 2 - Procesamiento en streaming
kafka_producer.py - Productor que genera datos de sensores IoT en tiempo real y los envía al topic sensor_data de Kafka cada segundo.

spark_streaming_consumer.py - Consumidor que procesa los datos en micro-lotes con Spark y calcula promedios de temperatura y humedad por sensor.

Tecnologías usadas
Apache Spark 4.1.1

Apache Kafka 3.9.2

Python 3.10.3

Ubuntu 22.04

Instrucciones de ejecución
Clona el repositorio: git clone https://github.com/Edwin202339/-bigdata-spark-bucaramanga.git

Para Video 1: spark-submit batch_accidentes.py

Para Video 2: Inicia Kafka, ejecuta kafka_producer.py y spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 spark_streaming_consumer.py
