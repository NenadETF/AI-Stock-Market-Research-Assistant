from databricks.connect import DatabricksSession


spark = DatabricksSession.builder.getOrCreate()

result = spark.sql("SELECT 1 AS test").collect()

print(result)