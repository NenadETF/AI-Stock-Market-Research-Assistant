import time

from databricks.connect import DatabricksSession

from src.ingestion.stock_news import (
    fetch_historical_stock_news,
    REQUEST_DELAY_SECONDS,
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

TABLE_NAME = "workspace.bronze.stock_news"

START_DATE = "2026-08-01"
END_DATE = "2026-08-31"

LIMIT_PER_PAGE = 100


print("\n====================================================")
print(" HISTORICAL MULTI-TICKER STOCK NEWS INGESTION")
print("====================================================")

print("\nTickeri:")

for ticker in TICKERS:
    print(" -", ticker)

print(
    f"\nPeriod: {START_DATE} -> {END_DATE}"
)


# ============================================================
# 3. Priprema Delta tabele
# ============================================================

print("\nProvjeravam Bronze News tabelu...")

ensure_bronze_stock_news_table(
    spark=spark,
    table_name=TABLE_NAME
)

print("Bronze News tabela je spremna.")


# ============================================================
# 4. Stanje prije ingestion-a
# ============================================================

before_count = spark.table(
    TABLE_NAME
).count()

print(
    "\nBroj Bronze redova prije ingestion-a:",
    before_count
)


# ============================================================
# 5. Historical ingestion za svaki ticker
# ============================================================

all_records = []

total_tickers = len(TICKERS)


for index, ticker in enumerate(
    TICKERS,
    start=1
):

    print("\n================================================")
    print(
        f"[{index}/{total_tickers}] "
        f"HISTORICAL NEWS: {ticker}"
    )
    print("================================================")

    records = fetch_historical_stock_news(
        ticker=ticker,
        start_date=START_DATE,
        end_date=END_DATE,
        limit_per_page=LIMIT_PER_PAGE
    )

    print(
        f"\n{ticker}: ukupno preuzeto "
        f"{len(records)} historical clanaka."
    )

    all_records.extend(records)

    # Zaštita između tickera.
    if index < total_tickers:

        print(
            f"\nRate-limit zastita između tickera: "
            f"{REQUEST_DELAY_SECONDS}s."
        )

        time.sleep(
            REQUEST_DELAY_SECONDS
        )


# ============================================================
# 6. Ukupna API statistika
# ============================================================

print("\n================================================")
print(" API STATISTIKA")
print("================================================")

print(
    "\nUkupan broj preuzetih Bronze zapisa:",
    len(all_records)
)


print("\n=== BROJ ZAPISA PO TICKERU ===")

for ticker in TICKERS:

    count = sum(
        1
        for record in all_records
        if record["requested_ticker"] == ticker
    )

    print(
        f"{ticker}: {count}"
    )


# ============================================================
# 7. Provjera praznog rezultata
# ============================================================

if not all_records:

    raise RuntimeError(
        "Historical News ingestion nije vratio "
        "nijedan zapis."
    )


# ============================================================
# 8. Spark DataFrame
# ============================================================

print(
    "\nKreiram historical Stock News Spark DataFrame..."
)

news_df = create_bronze_stock_news_df(
    spark=spark,
    records=all_records
)

source_count = news_df.count()

print(
    "Broj redova u Spark DataFrame-u:",
    source_count
)


# ============================================================
# 9. Period source DataFrame-a
# ============================================================

print("\n=== PREUZETI PERIOD ===")

news_df.selectExpr(
    "MIN(published_utc) AS najstariji",
    "MAX(published_utc) AS najnoviji"
).show(
    truncate=False
)


# ============================================================
# 10. MERGE
# ============================================================

print(
    "\nRadim historical MERGE u "
    "workspace.bronze.stock_news..."
)

merge_bronze_stock_news(
    spark=spark,
    df=news_df,
    table_name=TABLE_NAME
)

print(
    "Historical multi-ticker News MERGE završen."
)


# ============================================================
# 11. Stanje poslije MERGE-a
# ============================================================

after_count = spark.table(
    TABLE_NAME
).count()

print(
    "\nBroj Bronze redova poslije ingestion-a:",
    after_count
)

print(
    "Promjena broja redova:",
    after_count - before_count
)


# ============================================================
# 12. Statistika po tickeru
# ============================================================

print("\n=== NEWS PODACI PO TICKERU ===")

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
# 13. Jedinstveni članci
# ============================================================

print("\n=== ARTICLE STATISTIKA ===")

spark.sql(f"""
    SELECT

        COUNT(*) AS broj_bronze_redova,

        COUNT(DISTINCT article_id)
            AS jedinstveni_clanci,

        COUNT(DISTINCT requested_ticker)
            AS broj_tickera

    FROM {TABLE_NAME}
""").show(
    truncate=False
)


# ============================================================
# 14. Provjera duplikata
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
# 15. NULL provjera
# ============================================================

print("\n=== NULL PROVJERA ===")

spark.sql(f"""
    SELECT

        SUM(
            CASE
                WHEN article_id IS NULL
                THEN 1
                ELSE 0
            END
        ) AS null_article_id,

        SUM(
            CASE
                WHEN requested_ticker IS NULL
                THEN 1
                ELSE 0
            END
        ) AS null_requested_ticker,

        SUM(
            CASE
                WHEN title IS NULL
                THEN 1
                ELSE 0
            END
        ) AS null_title,

        SUM(
            CASE
                WHEN published_utc IS NULL
                THEN 1
                ELSE 0
            END
        ) AS null_published_utc

    FROM {TABLE_NAME}
""").show(
    truncate=False
)


# ============================================================
# 16. Najnovije vijesti
# ============================================================

print("\n=== NAJNOVIJE VIJESTI ===")

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
# 17. Delta detalji
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
# 18. Završna validacija
# ============================================================

if duplicate_count == 0:

    print(
        "\nUSPJEH: Historical multi-ticker Stock News "
        "ingestion je zavrsen bez duplikata."
    )

else:

    print(
        "\nUPOZORENJE: Historical News ingestion "
        "ima podatke koje treba provjeriti."
    )


# ============================================================
# 19. Zatvaranje
# ============================================================

spark.stop()

print(
    "\nHistorical multi-ticker Stock News "
    "ingestion zavrsen."
)