# Databricks notebook source
import sys
sys.path.insert(0, "../lib/")
import utils

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS hive_metastore.silver;

# COMMAND ----------

tableName = dbutils.widgets.get("tablename")

query = utils.import_query(f"{tableName}.sql")
print(query)

# COMMAND ----------

(spark.sql(query)
 .write
 .format('delta')
 .mode("overwrite")
 .saveAsTable(f"hive_metastore.silver.{tableName}"))

# COMMAND ----------


