# Databricks notebook source
# MAGIC %md
# MAGIC #### Create DB

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS kafka
# MAGIC LOCATION 'dbfs:/FileStore/tables/Kafka'

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Table in the DB

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS kafka.bus_location
# MAGIC (
# MAGIC   vehicle_number INTEGER,
# MAGIC   latitude DOUBLE,
# MAGIC   longitude DOUBLE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

import os 
from pyspark.sql.functions import col , from_json

CLOUDKARAFKA_BROKERS = os.getenv('CLOUDKARAFKA_HOSTNAME')
CLOUDKARAFKA_USERNAME = os.getenv('CLOUDKARAFKA_USERNAME')
CLOUDKARAFKA_PASSWORD = os.getenv('CLOUDKARAFKA_PASSWORD')
CLOUDKARAFKA_TOPIC = os.getenv('CLOUDKARAFKA_TOPIC_NAME')
API_COLUMNNAME = os.getenv('API_COLUMN_NAME')
print(CLOUDKARAFKA_BROKERS,CLOUDKARAFKA_USERNAME,CLOUDKARAFKA_PASSWORD,CLOUDKARAFKA_TOPIC)

# COMMAND ----------

# MAGIC %md
# MAGIC #### schema specification for messages

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, LongType, DoubleType, StringType

schema = StructType(
    [
                        StructField("delay", LongType(), True),
                        StructField("direction", LongType(), True),
                        StructField("generated", StringType(), True),
                        StructField("gpsQuality", LongType(), True),
                        StructField("headsign", StringType(), True),
                        StructField("lat", DoubleType(), True),
                        StructField("lon", DoubleType(), True),
                        StructField("routeShortName", StringType(), True),
                        StructField("scheduledTripStartTime", StringType(), True),
                        StructField("speed", LongType(), True),
                        StructField("tripId", LongType(), True),
                        StructField("vehicleCode", StringType(), True),
                        StructField("vehicleId", LongType(), True),
                        StructField("vehicleService", StringType(), True),
    ]
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Stream from Kafka

# COMMAND ----------

df = (spark
  .readStream
  .format("kafka")
  .option("failOnDataLoss","false")
  .option("kafka.bootstrap.servers", CLOUDKARAFKA_BROKERS)
  .option("kafka.ssl.endpoint.identification.algorithm", "https")
  .option("kafka.sasl.mechanism", "SCRAM-SHA-512")
  .option("kafka.security.protocol", "SASL_SSL")
  .option("kafka.sasl.jaas.config", "kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule required username='{}' password='{}';".format(CLOUDKARAFKA_USERNAME, CLOUDKARAFKA_PASSWORD))
  .option("subscribe", CLOUDKARAFKA_TOPIC)
  .option("kafka.client.id", "Databricks")
  .option("kafka.group.id", f"{CLOUDKARAFKA_USERNAME}-consumer")
  .option("spark.streaming.kafka.maxRatePerPartition", "5")
  .option("startingOffsets", "earliest")
  .option("kafka.session.timeout.ms", "10000")
  .load()
  .withColumn("plain_text", col("value").cast("string"))
  .withColumn("json_converted_value",from_json(col=col("plain_text"),schema=schema) )
  .select(col("json_converted_value.vehicleCode"),col("json_converted_value.lat"), col("json_converted_value.lon"))
   )

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Upsert to created table

# COMMAND ----------

# Function to upsert microBatchOutputDF into Delta table using merge
def upsertToDelta(microBatchOutputDF, batchId):
  microBatchOutputDF.createOrReplaceTempView("updates")
  microBatchOutputDF.sparkSession.sql("""
    MERGE INTO kafka.bus_location bl
    USING updates s
    ON s.vehicleCode = bl.vehicle_number
    WHEN MATCHED THEN 
    UPDATE SET 
              bl.latitude = s.lat , 
              bl.longitude = s.lon
    WHEN NOT MATCHED 
    THEN INSERT (vehicle_number, latitude, longitude) 
    VALUES (s.vehicleCode, s.lat, s.lon)
  """)

# Write the output of a streaming aggregation query into Delta table
(df.writeStream
   .format("delta")
   .foreachBatch(upsertToDelta)
   .outputMode("update")
   .option("failOnDataLoss","false")
   .option("checkpointLocation","/FileStore/tables/kafka/incremental_data/checkpoint")
   .start()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM kafka.bus_location

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE kafka.bus_location

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE kafka.bus_location

# COMMAND ----------

