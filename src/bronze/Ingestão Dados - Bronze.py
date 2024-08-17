# Databricks notebook source




# COMMAND ----------

df = spark.read.format('csv').load("/mnt/project/raw")
df.display()

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS bronze")

# COMMAND ----------

(df.coalesce(1).
 write.
 format('delta').
 mode("overwrite").
 saveAsTable("bronze.house_price"))
