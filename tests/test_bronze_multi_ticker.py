from databricks.connect import DatabricksSession

from src.ingestion.stock_prices import (
    fetch_stock_prices_for_tickers,
)

from src.bronze.stock_prices_bronze import (
    create_bronze_stock_prices_df,
    ensure_bronze_stock_prices_table,
    merge_bronze_stock_prices,
)


# ============================================================
# 1. Databricks sesija
# ============================================================

spark = DatabricksSession.builder.getOrCreate()


# ============================================================
# 2. Konfiguracija ingestion-a
# ============================================================

TICKERS = [
    "AAPL",
    "MSFT",
    "GOOGL",
    "AMZN",
    "NVDA",
    "META",
    "TSLA",
]

START_DATE = "2026-08-01"
END_DATE = "2026-08-31"

TABLE_NAME = "workspace.bronze.stock_prices"


print("\n=== MULTI-TICKER BRONZE INGESTION ===")

print("Tickeri:")
for ticker in TICKERS:
    print(" -", ticker)

print(
    f"\nPeriod: {START_DATE} -> {END_DATE}"
)


# ============================================================
# 3. Kreiranje tabele ako ne postoji
# ============================================================

print("\nProvjeravam Bronze Delta tabelu...")

ensure_bronze_stock_prices_table(
    spark=spark,
    table_name=TABLE_NAME
)

print("Bronze Delta tabela je spremna.")


# ============================================================
# 4. Broj redova PRIJE ingestion-a
# ============================================================

before_count = spark.table(TABLE_NAME).count()

print(
    "\nBroj redova prije ingestion-a:",
    before_count
)


# ============================================================
# 5. Massive API ingestion
# ============================================================

print("\nPreuzimam podatke sa Massive API-ja...")

records = fetch_stock_prices_for_tickers(
    tickers=TICKERS,
    start_date=START_DATE,
    end_date=END_DATE
)

print(
    "Ukupan broj zapisa iz API-ja:",
    len(records)
)


# ============================================================
# 6. Statistika po tickerima
# ============================================================

print("\n=== BROJ ZAPISA PO TICKERU ===")

for ticker in TICKERS:

    ticker_count = sum(
        1
        for record in records
        if record["ticker"] == ticker
    )

    print(
        f"{ticker}: {ticker_count}"
    )


# ============================================================
# 7. Spark DataFrame
# ============================================================

print("\nKreiram Bronze Spark DataFrame...")

bronze_df = create_bronze_stock_prices_df(
    spark=spark,
    records=records
)

print(
    "Broj redova u DataFrame-u:",
    bronze_df.count()
)


print("\n=== DATAFRAME SCHEMA ===")

bronze_df.printSchema()


# ============================================================
# 8. MERGE
# ============================================================

print("\nRadim MERGE u Bronze Delta tabelu...")

merge_bronze_stock_prices(
    spark=spark,
    df=bronze_df,
    table_name=TABLE_NAME
)

print("MERGE završen.")


# ============================================================
# 9. Broj redova POSLIJE ingestion-a
# ============================================================

saved_df = spark.table(TABLE_NAME)

after_count = saved_df.count()

print(
    "\nBroj redova poslije ingestion-a:",
    after_count
)

print(
    "Promjena broja redova:",
    after_count - before_count
)


# ============================================================
# 10. Broj redova po tickeru iz Delta tabele
# ============================================================

print("\n=== DELTA PODACI PO TICKERU ===")

spark.sql(f"""
    SELECT
        ticker,
        COUNT(*) AS broj_redova,
        MIN(date) AS prvi_datum,
        MAX(date) AS zadnji_datum

    FROM {TABLE_NAME}

    GROUP BY ticker

    ORDER BY ticker
""").show(
    truncate=False
)


# ============================================================
# 11. Provjera duplikata
# ============================================================

print("\n=== PROVJERA DUPLIKATA ===")

duplicates_df = spark.sql(f"""
    SELECT
        ticker,
        date,
        COUNT(*) AS broj

    FROM {TABLE_NAME}

    GROUP BY
        ticker,
        date

    HAVING COUNT(*) > 1
""")

duplicate_count = duplicates_df.count()

if duplicate_count == 0:

    print(
        "USPJEH: Nema duplikata po (ticker, date)."
    )

else:

    print(
        "UPOZORENJE: Pronađeni su duplikati!"
    )

    duplicates_df.show(
        truncate=False
    )


# ============================================================
# 12. Prikaz nekoliko redova
# ============================================================

print("\n=== PRIMJER PODATAKA ===")

spark.sql(f"""
    SELECT *

    FROM {TABLE_NAME}

    ORDER BY
        ticker,
        date

    LIMIT 20
""").show(
    truncate=False
)


# ============================================================
# 13. Delta detalji
# ============================================================

print("\n=== DELTA DETALJI ===")

spark.sql(f"""
    DESCRIBE DETAIL {TABLE_NAME}
""").select(
    "format",
    "name",
    "numFiles",
    "sizeInBytes"
).show(
    truncate=False
)


# ============================================================
# 14. Završetak
# ============================================================

spark.stop()

print(
    "\nMulti-ticker Bronze ingestion završen."
)