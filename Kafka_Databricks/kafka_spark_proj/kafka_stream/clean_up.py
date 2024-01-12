# Databricks notebook source
# MAGIC %sql
# MAGIC TRUNCATE TABLE kafka.bus_location

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE kafka.bus_location

# COMMAND ----------

dbutils.fs.rm("/FileStore/tables/Kafka",True)

# COMMAND ----------

