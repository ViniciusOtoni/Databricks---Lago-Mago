import sys
sys.path.insert(0, "../lib/")
import utils


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

class IngestorCubo:

    def __init__(self, spark, catalog, schemaname, tablename):
        self.spark = spark
        self.catalog = catalog
        self.schemaname = schemaname
        self.tablename = tablename
        self.table = f"{catalog}.{schemaname}.{tablename}"
        self.set_query()

    def set_query(self):
        self.query = utils.import_query(f"{self.tablename}.sql")

    def load(self): ##**kwargs == parâmetros para query.
        df = self.spark.sql(self.query)
        return df
    
    def save(self, df): 
        if not utils.table_exists(self.spark, self.catalog, self.schemaname, self.tablename):
            (df.coalesce(1)
            .write
            .format('delta')
            .mode("overwrite")
            .saveAsTable(self.table))
        else:
            print("Tabela já existe!")
        
    def execute(self):
        df = self.load()
        self.save(df)

