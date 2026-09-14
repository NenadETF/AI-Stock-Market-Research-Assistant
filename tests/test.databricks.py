from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.getOrCreate()

df = spark.range(10)

print("Broj redova:", df.count())

df.show()