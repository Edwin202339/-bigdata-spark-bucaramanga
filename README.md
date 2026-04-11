# BigData - Spark y Kafka - Bucaramanga
## Tarea 3 - Procesamiento de Datos con Apache Spark
### UNAD | Grupo 202016911_72 | Edwin Pantoja Bustos

## Descripcion
Este repositorio contiene los scripts desarrollados para la Tarea 3 del curso Big Data de la UNAD.

## Archivos

### Video 1 - Procesamiento Batch
- **batch_accidentes.py** - Analisis de accidentes de transito en Bucaramanga usando PySpark.
  Procesa 39.193 registros con operaciones DataFrame y RDD.
  Dataset: Accidentes de Transito - Municipio de Bucaramanga (2012-2023)

### Video 2 - Procesamiento Streaming
- **kafka_producer.py** - Producer que genera datos de sensores IoT en tiempo real y los envia al topic sensor_data de Kafka cada segundo.
- **spark_streaming_consumer.py** - Consumer que procesa los datos en micro-batches con Spark y calcula promedios de temperatura y humedad por sensor.

## Tecnologias usadas
- Apache Spark 4.1.1
- Apache Kafka 3.9.2
- Python 3.10
- Ubuntu 22.04
