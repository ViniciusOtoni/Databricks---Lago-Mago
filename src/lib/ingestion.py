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