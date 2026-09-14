from databricks.connect import DatabricksSession

from src.ingestion.stock_prices import fetch_stock_prices
from src.bronze.stock_prices_bronze import (
    create_bronze_stock_prices_df,
    write_bronze_stock_prices,
)


# ============================================================
# Databricks Connect sesija
# ============================================================

spark = DatabricksSession.builder.getOrCreate()


# ============================================================
# 1. Provjera Databricks okruženja
# ============================================================

print("\n=== DATABRICKS OKRUZENJE ===")

spark.sql("""
    SELECT
        current_catalog() AS catalog,
        current_schema() AS schema
""").show(truncate=False)


# ============================================================
# 2. Kreiranje Bronze schema-e
# ============================================================

print("\nKreiram workspace.bronze schema-u...")

spark.sql("""
    CREATE SCHEMA IF NOT EXISTS workspace.bronze
""")

print("Bronze schema je spremna.")


# ============================================================
# 3. Uklanjanje stare testne tabele
# ============================================================

print("\nUklanjam eventualnu staru testnu tabelu...")

spark.sql("""
    DROP TABLE IF EXISTS workspace.bronze.stock_prices
""")

print("Stara tabela je uklonjena ako je postojala.")


# ============================================================
# 4. Massive API ingestion
# ============================================================

print("\nPreuzimam AAPL podatke sa Massive API-ja...")

records = fetch_stock_prices(
    ticker="AAPL",
    start_date="2026-08-01",
    end_date="2026-08-31"
)

print("Broj zapisa iz Massive API-ja:", len(records))


# ============================================================
# 5. Kreiranje Bronze Spark DataFrame-a
# ============================================================

print("\nKreiram Bronze Spark DataFrame...")

bronze_df = create_bronze_stock_prices_df(
    spark=spark,
    records=records
)

print("Broj redova prije upisa:", bronze_df.count())


print("\n=== BRONZE DATAFRAME SCHEMA ===")

bronze_df.printSchema()


# ============================================================
# 6. Delta write
# ============================================================

print("\nUpisujem workspace.bronze.stock_prices...")

write_bronze_stock_prices(
    df=bronze_df,
    table_name="workspace.bronze.stock_prices"
)

print("Delta tabela je uspjesno upisana.")


# ============================================================
# 7. Čitanje podataka nazad iz Delta tabele
# ============================================================

print("\n=== PODACI IZ DELTA TABELE ===")

saved_df = spark.table(
    "workspace.bronze.stock_prices"
)

saved_df.show(
    10,
    truncate=False
)

row_count = saved_df.count()

print(
    "\nBroj redova u workspace.bronze.stock_prices:",
    row_count
)


# ============================================================
# 8. Provjera schema-e sačuvane Delta tabele
# ============================================================

print("\n=== SCHEMA DELTA TABELE ===")

saved_df.printSchema()


# ============================================================
# 9. Provjera Delta formata
# ============================================================

print("\n=== DETALJI TABELE ===")

spark.sql("""
    DESCRIBE DETAIL workspace.bronze.stock_prices
""").select(
    "format",
    "name",
    "numFiles",
    "sizeInBytes"
).show(
    truncate=False
)


# ============================================================
# 10. Završna provjera
# ============================================================

if row_count == len(records):
    print(
        "\nUSPJEH: Broj redova u Delta tabeli odgovara "
        "broju zapisa iz Massive API-ja."
    )
else:
    print(
        "\nUPOZORENJE: Broj redova u Delta tabeli "
        "ne odgovara broju zapisa iz API-ja."
    )


# ============================================================
# 11. Zatvaranje Spark sesije
# ============================================================

spark.stop()

print("\nTest Bronze Delta ingestion-a zavrsen.")