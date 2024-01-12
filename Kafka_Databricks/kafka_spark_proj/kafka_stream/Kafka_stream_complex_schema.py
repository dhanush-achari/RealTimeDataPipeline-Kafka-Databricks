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
from pyspark.sql.functions import col , from_json, explode

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

from pyspark.sql.types import StructType, StructField, LongType, DoubleType, StringType, ArrayType

inner_schema = StructType(
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

schema = StructType(fields=
                    [
                        StructField("message",ArrayType(inner_schema),True)
                    ]
                    )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Stream from Kafka

# COMMAND ----------

from pyspark.sql.functions import arrays_zip

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
  .withColumn("json_converted_message",from_json(col=col("plain_text"),schema=schema) )
  .select(
          col("json_converted_message.message.vehicleCode").alias("vehicleCodes")
          ,col("json_converted_message.message.lat").alias("lats")
          , col("json_converted_message.message.lon").alias("lons")
          )
  .withColumn("array_zipped", arrays_zip("vehicleCodes","lats","lons"))
  .withColumn("exploded", explode("array_zipped"))
  .select(
    col("exploded.vehicleCodes").alias("vehicleCode"),
    col("exploded.lats").alias("lat"),
    col("exploded.lons").alias("lon")
  )
   )

# COMMAND ----------

# display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Truncate and reload for each batch

# COMMAND ----------

# def truncate_reload(microBatchOutputDF, batchId):
#     microBatchOutputDF.sparkSession.sql("""
#                                         DROP TABLE IF EXISTS kafka.bus_location_1
#                                         """)
#     microBatchOutputDF.write.saveAsTable("kafka.bus_location_1")

# (df.writeStream
#    .format("delta")
#    .foreachBatch(truncate_reload)
#    .outputMode("update")
#    .option("failOnDataLoss","false")
#    .option("checkpointLocation","/FileStore/tables/kafka/incremental_data/checkpoint")
#    .start()
# )

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

# %sql
# SELECT * FROM kafka.bus_location_1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM kafka.bus_location

# COMMAND ----------

1003
54.3720703125
18.62483024597168

# COMMAND ----------

