# Databricks notebook source


# COMMAND ----------

# DBTITLE 1,Setup
catalog = "hive_metastore"
schema = "bronze"
tableName = dbutils.widgets.get("tablename")

# COMMAND ----------

# DBTITLE 1,Gerando Mount

#config
account_name = "catalogdatabricksvini"
account_key = "" #BetVORhrr5D1RNc3AuagQCXpyh+ygUFeFnovjUNSp5iidUh7NUrR6DmUxce5okmDhhPuqFHx08Ws+ASt66si8A= =
container_name = "raw"
mount_name = f"/mnt/project/raw/{tableName}" 

#url blob
source_url = f"wasbs://{container_name}@{account_name}.blob.core.windows.net"
conf_key = f"fs.azure.account.key.{account_name}.blob.core.windows.net"



if any(mount.mountPoint == mount_name for mount in dbutils.fs.mounts()):
    print(f"O ponto de montagem '{mount_name}' já existe. Desmontando...")
    dbutils.fs.unmount(mount_name)

# Montar o Blob Storage
dbutils.fs.mount(
  source = source_url,
  mount_point = mount_name,
  extra_configs = {conf_key: account_key}
)



# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS bronze")

# COMMAND ----------

# DBTITLE 1,Imports
def table_exists(catalog, database, table):
    count = (spark.sql(f"SHOW TABLES FROM {catalog}.{database}")
             .filter("database = '{database}' AND tableName= '{table}' ")
             .count())
    return count == 1

# COMMAND ----------

if not (table_exists(catalog, schema, tableName)):
    print("Tabela não existente, criando...")

    df = spark.read.format('csv').load(f"/mnt/project/raw/{tableName}")

    (df.coalesce(1).
        write.
        format('delta').
        mode("overwrite").
        saveAsTable(f"{catalog}.{schema}.{tableName}"))
else:
    print("Tabela já existente, ignorando full load...")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM hive_metastore.bronze.house_price
