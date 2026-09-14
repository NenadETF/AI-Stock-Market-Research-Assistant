from datetime import date, timedelta

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
# 2. Konfiguracija
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

TABLE_NAME = "workspace.bronze.stock_prices"


# Massive Basic ima oko 2 godine istorije.
# Ne uzimamo današnji datum jer Basic koristi end-of-day podatke.
END_DATE = date.today() - timedelta(days=1)

# Malo manje od pune 2 godine da ne udarimo u granicu plana.
START_DATE = END_DATE - timedelta(days=729)

START_DATE_STR = START_DATE.isoformat()
END_DATE_STR = END_DATE.isoformat()


print("\n==============================================")
print(" HISTORICAL STOCK PRICES BRONZE INGESTION")
print("==============================================")

print("\nTickeri:")

for ticker in TICKERS:
    print(" -", ticker)

print(
    f"\nPeriod: {START_DATE_STR} -> {END_DATE_STR}"
)


# ============================================================
# 3. Priprema Bronze tabele
# ============================================================

print("\nProvjeravam Bronze Delta tabelu...")

ensure_bronze_stock_prices_table(
    spark=spark,
    table_name=TABLE_NAME
)

print("Bronze Delta tabela je spremna.")


# ============================================================
# 4. Stanje prije ingestion-a
# ============================================================

before_count = spark.table(
    TABLE_NAME
).count()

print(
    "\nBroj redova prije historical ingestion-a:",
    before_count
)


# ============================================================
# 5. API ingestion
# ============================================================

print("\nPreuzimam istorijske podatke sa Massive API-ja...")

records = fetch_stock_prices_for_tickers(
    tickers=TICKERS,
    start_date=START_DATE_STR,
    end_date=END_DATE_STR
)

print(
    "\nUkupan broj istorijskih zapisa iz API-ja:",
    len(records)
)


# ============================================================
# 6. Broj zapisa po tickeru
# ============================================================

print("\n=== API ZAPISI PO TICKERU ===")

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
# 7. Provjera da API nije vratio prazan rezultat
# ============================================================

if not records:
    raise RuntimeError(
        "Massive API nije vratio nijedan istorijski zapis."
    )


# ============================================================
# 8. Spark DataFrame
# ============================================================

print("\nKreiram Historical Bronze Spark DataFrame...")

bronze_df = create_bronze_stock_prices_df(
    spark=spark,
    records=records
)

source_count = bronze_df.count()

print(
    "Broj redova u historical DataFrame-u:",
    source_count
)


# ============================================================
# 9. Schema
# ============================================================

print("\n=== DATAFRAME SCHEMA ===")

bronze_df.printSchema()


# ============================================================
# 10. MERGE
# ============================================================

print(
    "\nRadim historical MERGE u "
    "workspace.bronze.stock_prices..."
)

merge_bronze_stock_prices(
    spark=spark,
    df=bronze_df,
    table_name=TABLE_NAME
)

print("Historical MERGE završen.")


# ============================================================
# 11. Stanje poslije ingestion-a
# ============================================================

saved_df = spark.table(
    TABLE_NAME
)

after_count = saved_df.count()

print(
    "\nBroj redova poslije historical ingestion-a:",
    after_count
)

print(
    "Promjena broja redova:",
    after_count - before_count
)


# ============================================================
# 12. Statistika po tickeru
# ============================================================

print("\n=== HISTORIJSKI PODACI PO TICKERU ===")

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
# 13. Provjera duplikata
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
        "GRESKA: Pronadjeni su duplikati!"
    )

    duplicates_df.show(
        truncate=False
    )


# ============================================================
# 14. Null provjera
# ============================================================

print("\n=== NULL PROVJERA ===")

spark.sql(f"""
    SELECT
        SUM(CASE WHEN ticker IS NULL THEN 1 ELSE 0 END)
            AS null_ticker,

        SUM(CASE WHEN date IS NULL THEN 1 ELSE 0 END)
            AS null_date,

        SUM(CASE WHEN close IS NULL THEN 1 ELSE 0 END)
            AS null_close

    FROM {TABLE_NAME}
""").show(
    truncate=False
)


# ============================================================
# 15. Ukupan period u tabeli
# ============================================================

print("\n=== UKUPAN PERIOD BRONZE TABELE ===")

spark.sql(f"""
    SELECT
        MIN(date) AS prvi_datum,
        MAX(date) AS zadnji_datum,
        COUNT(*) AS ukupno_redova,
        COUNT(DISTINCT ticker) AS broj_tickera

    FROM {TABLE_NAME}
""").show(
    truncate=False
)


# ============================================================
# 16. Delta detalji
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
# 17. Završna validacija
# ============================================================

if duplicate_count == 0:

    print(
        "\nUSPJEH: Historical Bronze ingestion "
        "je zavrsen bez duplikata."
    )

else:

    print(
        "\nHistorical ingestion je zavrsen, "
        "ali postoje duplikati koje treba provjeriti."
    )


# ============================================================
# 18. Zatvaranje sesije
# ============================================================

spark.stop()

print(
    "\nHistorical Bronze ingestion zavrsen."
)