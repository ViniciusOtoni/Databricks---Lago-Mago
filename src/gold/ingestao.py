# Databricks notebook source
import sys
sys.path.insert(0, "../lib/")
import utils
import ingestion

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS hive_metastore.gold;

# COMMAND ----------

catalog = "hive_metastore"
schema = "gold" 
tableName = dbutils.widgets.get("tablename") 

# COMMAND ----------

ingest = ingestion.IngestorCubo(spark=spark,
                                    catalog=catalog,
                                    schemaname=schema,
                                    tablename=tableName)
    
ingest.execute()


