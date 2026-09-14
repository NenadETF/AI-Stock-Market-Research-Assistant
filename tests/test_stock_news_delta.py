from databricks.connect import DatabricksSession

from src.ingestion.stock_news import (
    fetch_stock_news_for_tickers,
)

from src.bronze.stock_news_bronze import (
    create_bronze_stock_news_df,
    ensure_bronze_stock_news_table,
    merge_bronze_stock_news,
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

LIMIT_PER_TICKER = 10

TABLE_NAME = "workspace.bronze.stock_news"


print("\n==============================================")
print(" STOCK NEWS BRONZE INGESTION")
print("==============================================")


# ============================================================
# 3. Priprema tabele
# ============================================================

print("\nProvjeravam Stock News Bronze tabelu...")

ensure_bronze_stock_news_table(
    spark=spark,
    table_name=TABLE_NAME
)

print("Stock News Bronze tabela je spremna.")


# ============================================================
# 4. Broj redova prije ingestion-a
# ============================================================

before_count = spark.table(
    TABLE_NAME
).count()

print(
    "\nBroj redova prije ingestion-a:",
    before_count
)


# ============================================================
# 5. Massive News API
# ============================================================

print(
    "\nPreuzimam vijesti sa Massive API-ja..."
)

records = fetch_stock_news_for_tickers(
    tickers=TICKERS,
    limit_per_ticker=LIMIT_PER_TICKER
)

print(
    "\nUkupan broj preuzetih zapisa:",
    len(records)
)


# ============================================================
# 6. Statistika po requested tickeru
# ============================================================

print("\n=== VIJESTI PO TICKERU ===")

for ticker in TICKERS:

    count = sum(
        1
        for record in records
        if record["requested_ticker"] == ticker
    )

    print(
        f"{ticker}: {count}"
    )


# ============================================================
# 7. Spark DataFrame
# ============================================================

print(
    "\nKreiram Stock News Bronze DataFrame..."
)

news_df = create_bronze_stock_news_df(
    spark=spark,
    records=records
)

print(
    "Broj redova u DataFrame-u:",
    news_df.count()
)


print("\n=== NEWS SCHEMA ===")

news_df.printSchema()


# ============================================================
# 8. MERGE
# ============================================================

print(
    "\nRadim MERGE u workspace.bronze.stock_news..."
)

merge_bronze_stock_news(
    spark=spark,
    df=news_df,
    table_name=TABLE_NAME
)

print("Stock News MERGE završen.")


# ============================================================
# 9. Stanje poslije MERGE-a
# ============================================================

saved_df = spark.table(
    TABLE_NAME
)

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
# 10. Pregled po tickeru
# ============================================================

print("\n=== DELTA VIJESTI PO TICKERU ===")

spark.sql(f"""
    SELECT
        requested_ticker,
        COUNT(*) AS broj_clanaka,
        MIN(published_utc) AS najstariji,
        MAX(published_utc) AS najnoviji

    FROM {TABLE_NAME}

    GROUP BY requested_ticker

    ORDER BY requested_ticker
""").show(
    truncate=False
)


# ============================================================
# 11. Provjera duplikata
# ============================================================

print("\n=== PROVJERA DUPLIKATA ===")

duplicates_df = spark.sql(f"""
    SELECT
        article_id,
        requested_ticker,
        COUNT(*) AS broj

    FROM {TABLE_NAME}

    GROUP BY
        article_id,
        requested_ticker

    HAVING COUNT(*) > 1
""")

duplicate_count = duplicates_df.count()

if duplicate_count == 0:

    print(
        "USPJEH: Nema duplikata po "
        "(article_id, requested_ticker)."
    )

else:

    print(
        "GRESKA: Pronadjeni su duplikati!"
    )

    duplicates_df.show(
        truncate=False
    )


# ============================================================
# 12. NULL provjera
# ============================================================

print("\n=== NULL PROVJERA ===")

spark.sql(f"""
    SELECT

        SUM(
            CASE WHEN article_id IS NULL
            THEN 1 ELSE 0 END
        ) AS null_article_id,

        SUM(
            CASE WHEN requested_ticker IS NULL
            THEN 1 ELSE 0 END
        ) AS null_requested_ticker,

        SUM(
            CASE WHEN title IS NULL
            THEN 1 ELSE 0 END
        ) AS null_title,

        SUM(
            CASE WHEN published_utc IS NULL
            THEN 1 ELSE 0 END
        ) AS null_published_utc

    FROM {TABLE_NAME}
""").show(
    truncate=False
)


# ============================================================
# 13. Primjer podataka
# ============================================================

print("\n=== PRIMJER VIJESTI ===")

spark.sql(f"""
    SELECT
        requested_ticker,
        published_utc,
        publisher_name,
        title

    FROM {TABLE_NAME}

    ORDER BY published_utc DESC

    LIMIT 20
""").show(
    truncate=False
)


# ============================================================
# 14. Koliko je jedinstvenih članaka?
# ============================================================

print("\n=== ARTICLE STATISTIKA ===")

spark.sql(f"""
    SELECT
        COUNT(*) AS broj_bronze_redova,
        COUNT(DISTINCT article_id) AS jedinstveni_clanci,
        COUNT(DISTINCT requested_ticker) AS broj_tickera

    FROM {TABLE_NAME}
""").show(
    truncate=False
)


# ============================================================
# 15. Delta detalji
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
# 16. Završetak
# ============================================================

if duplicate_count == 0:

    print(
        "\nUSPJEH: Stock News Bronze ingestion "
        "je zavrsen bez duplikata."
    )

spark.stop()

print(
    "\nStock News Bronze ingestion zavrsen."
)