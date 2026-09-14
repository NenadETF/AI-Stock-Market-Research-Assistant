from databricks.connect import DatabricksSession

from src.ingestion.stock_prices import fetch_stock_prices
from src.bronze.stock_prices_bronze import create_bronze_stock_prices_df


spark = DatabricksSession.builder.getOrCreate()

records = fetch_stock_prices(
    ticker="AAPL",
    start_date="2026-08-01",
    end_date="2026-08-31"
)

print("Broj zapisa iz API-ja:", len(records))

bronze_df = create_bronze_stock_prices_df(
    spark=spark,
    records=records
)

print("\n=== SPARK SCHEMA ===")
bronze_df.printSchema()

print("\n=== PRVIH 10 REDOVA ===")
bronze_df.show(10, truncate=False)

print("\nBroj redova u Spark DataFrame-u:", bronze_df.count())

spark.stop()