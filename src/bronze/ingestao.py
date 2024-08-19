# Databricks notebook source
import sys
sys.path.insert(0, "../lib/")

import utils
import ingestion
import os


# COMMAND ----------

# DBTITLE 1,Setup
catalog = "hive_metastore"
schema = "bronze"
tableName = "house_price" #dbutils.widgets.get("tablename") #"house_price"

# COMMAND ----------

# DBTITLE 1,Gerando Mount
#config

account_name = "catalogdatabricksvini"
account_key = os.getenv("BLOB_STORAGE_ACCOUNT_KEY")
#"BetVORhrr5D1RNc3AuagQCXpyh+ygUFeFnovjUNSp5iidUh7NUrR6DmUxce5okmDhhPuqFHx08Ws+ASt66si8A= =" 

container_name = "raw"
mount_name = f"/mnt/project/raw/{schema}/full_load/{tableName}" 

#url blob
source_url = f"wasbs://{container_name}@{account_name}.blob.core.windows.net"
conf_key = f"fs.azure.account.key.{account_name}.blob.core.windows.net"


# COMMAND ----------

utils.

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS bronze")

# COMMAND ----------

if not utils.table_exists(spark, catalog, schema, tableName):
    print("Tabela não existe, criando...")

    ingest_full_load = ingestion.Ingestor(spark = spark,
                                catalog=catalog,
                                 schemaname=schema,
                                 tablename=tableName, 
                                 data_format='csv')
    
    ingest_full_load.execute(f"/mnt/project/raw/{schema}/full_load/{tableName}")

    print("Tabela criada com sucesso!")

else:
    print("Tabela já existe, ignorando full-load")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --visualização
# MAGIC
# MAGIC SELECT * FROM hive_metastore.bronze.house_price
