import json

from pyspark.sql import types



def import_query(path):
    with open(path, 'r') as open_file:
        return open_file.read()

def table_exists(spark, catalog, database, table):
    count = (spark.sql(f"SHOW TABLES FROM {catalog}.{database}")
                .filter(f"database = '{database}' AND tableName = '{table}'")
                .count())
    return count == 1


def create_mount(spark, mount_name, source_url, conf_key, account_key):
    
    from pyspark.dbutils import DBUtils
    dbutils = DBUtils(spark)
    
    # Verifica se o ponto de montagem já existe
    if any(mount.mountPoint == mount_name for mount in dbutils.fs.mounts()):
        print(f"O ponto de montagem '{mount_name}' já existe.")
    else:
        # Montar o Blob Storage
        dbutils.fs.mount(
            source=source_url,
            mount_point=mount_name,
            extra_configs={conf_key: account_key}
        )




        
