import json
from pyspark.sql import types



def table_exists(spark, catalog, database, table):
    count = (spark.sql(f"SHOW TABLES FROM {catalog}.{database}")
                .filter(f"database = '{database}' AND tableName = '{table}'")
                .count())
    return count == 1