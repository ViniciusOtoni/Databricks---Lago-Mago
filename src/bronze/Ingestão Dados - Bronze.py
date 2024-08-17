# Databricks notebook source



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
