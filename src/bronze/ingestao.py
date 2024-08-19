# Databricks notebook source
import sys
sys.path.insert(0, "../lib/")
import utils


# COMMAND ----------

# DBTITLE 1,Setup
catalog = "hive_metastore"
schema = "bronze"
tableName = dbutils.widgets.get("tablename")

# COMMAND ----------

# DBTITLE 1,Gerando Mount

import os
#config
account_name = "catalogdatabricksvini"
account_key = os.getenv("BLOB_STORAGE_ACCOUNT_KEY")
#"BetVORhrr5D1RNc3AuagQCXpyh+ygUFeFnovjUNSp5iidUh7NUrR6DmUxce5okmDhhPuqFHx08Ws+ASt66si8A= =" 
print("Account Key:", account_key)
container_name = "raw"
mount_name = f"/mnt/project/raw/{schema}/full_load/{tableName}" 

#url blob
source_url = f"wasbs://{container_name}@{account_name}.blob.core.windows.net"
conf_key = f"fs.azure.account.key.{account_name}.blob.core.windows.net"



if any(mount.mountPoint == mount_name for mount in dbutils.fs.mounts()):
    print(f"O ponto de montagem '{mount_name}' já existe. Desmontando...")
    dbutils.fs.unmount(mount_name)

#Montar o Blob Storage
dbutils.fs.mount(
  source = source_url,
  mount_point = mount_name,
  extra_configs = {conf_key: account_key}
)



# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS bronze")

# COMMAND ----------

class Ingestor:

    def __init__(self, spark, catalog, schemaname, tablename, data_format):
        self.spark = spark,
        self.catalog = catalog,
        self.schemaname = schemaname,
        self.tablename = tablename,
        self.format = data_format
        

    def load(self, path):
        df = (self.spark
              .read
              .format(self.format)
              .schema(self.data_schema)
              .load(path))
        
        return df

        
    def save(self, df):
        (df.coalesce(1)
            .write
            .format('delta')
            .mode("overwrite")
            .saveAsTable(f"{self.catalog}.{self.schemaname}.{self.tablename}"))
    
    def execute(self, path):
        df = self.load(path)
        self.save(df)


# COMMAND ----------

if not utils.table_exists(spark, catalog, schema, tableName):
    print("Tabela não existe, criando...")

    ingest_full_load = Ingestor(spark = spark,
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
# MAGIC SELECT * FROM hive_metastore.bronze.house_price
