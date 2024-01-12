# Databricks notebook source
dbutils.fs.rm("/FileStore/tables/Kafka/",True)


# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/")

# COMMAND ----------

