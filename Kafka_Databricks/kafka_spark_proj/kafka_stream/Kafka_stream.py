# Databricks notebook source
# MAGIC %md
# MAGIC #### Create DB

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS kafka
# MAGIC LOCATION 'dbfs:/FileStore/tables/Kafka'

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
# MAGIC #### Stream from Kafka

# COMMAND ----------

df = spark \
      .readStream \
      .format("kafka") \
      .option("kafka.bootstrap.servers",  CLOUDKARAFKA_BROKERS) \
      .option("kafka.ssl.endpoint.identification.algorithm", "https") \
      .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
      .option("kafka.security.protocol","SASL_SSL") \
      .option(f"kafka.sasl.jaas.config", f"kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule required username='{CLOUDKARAFKA_USERNAME}'password='{CLOUDKARAFKA_PASSWORD}';") \
      .option("subscribe", CLOUDKARAFKA_TOPIC) \
      .option("kafka.client.id", "Databricks") \
      .option("group.id",f"{CLOUDKARAFKA_USERNAME}-consumer") \
      .option("spark.streaming.kafka.maxRatePerPartition", "5")\
      .option("startingOffsets", "earliest") \
      .option("kafka.session.timeout.ms", "10000")\
      .load() \
      .withColumn("str_value", col('value').cast('String'))
      # .withColumn("json", from_json(col("str_value"))) \
      # .select(["json.generated", "json.vehicleCode","json.routeShortName", "json.lat", "json.lon","json.headsign"])


# COMMAND ----------

display(df)

# COMMAND ----------

df = (spark
  .readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", CLOUDKARAFKA_BROKERS)
  .option("kafka.ssl.endpoint.identification.algorithm", "https")
  .option("kafka.sasl.mechanism", "SCRAM-SHA-512")
  .option("kafka.security.protocol", "SASL_SSL")
  .option(f"kafka.sasl.jaas.config", f"kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule required username='{CLOUDKARAFKA_USERNAME}'password='{CLOUDKARAFKA_PASSWORD}';") 
#   .option("kafka.sasl.jaas.config", "kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule required username='{}' password='{}';".format(CLOUDKARAFKA_USERNAME, CLOUDKARAFKA_PASSWORD))
  .option("subscribe", CLOUDKARAFKA_TOPIC)
  .option("kafka.client.id", "Databricks")
  .option("kafka.group.id", f"{CLOUDKARAFKA_USERNAME}-consumer")
  .option("spark.streaming.kafka.maxRatePerPartition", "5")
  .option("startingOffsets", "earliest")
  .option("kafka.session.timeout.ms", "10000")
  .load()
  .withC )

# COMMAND ----------

display(df)

# COMMAND ----------

inputDF = (spark
  .readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "glider.srvs.cloudkafka.com:9094")
  .option("kafka.ssl.endpoint.identification.algorithm", "https")
  .option("kafka.sasl.mechanism", "SCRAM-SHA-512")
  .option("kafka.security.protocol", "SASL_SSL")
  .option("kafka.sasl.jaas.config", "kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule required username='{}' password='{}';".format("dacntlpm", "IK0z1jP_iJcqUE3ctY8y1LAQk2ikGkWe"))
  .option("subscribe", "dacntlpm-test")
  .option("kafka.client.id", "Databricks")
  .option("kafka.group.id", "dacntlpm-consumer")
  .option("spark.streaming.kafka.maxRatePerPartition", "5")
  .option("startingOffsets", "earliest")
  .option("kafka.session.timeout.ms", "10000")
  .load() )

# COMMAND ----------

display(inputDF)

# COMMAND ----------

