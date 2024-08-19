# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS hive_metastore.silver;

# COMMAND ----------

def import_query(path):
    with open(path, 'r') as open_file:
        return open_file.read()


table_name = "informacoes_casas"

query = import_query(f"{table_name}.sql")
print(query)

# COMMAND ----------

(spark.sql(query)
 .write
 .format('delta')
 .mode("overwrite")
 .saveAsTable(f"hive_metastore.silver.{table_name}"))

# COMMAND ----------


