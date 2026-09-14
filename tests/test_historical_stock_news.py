from databricks.connect import DatabricksSession

from src.ingestion.stock_news import (
    fetch_historical_stock_news,
)

from src.bronze.stock_news_bronze import (
    create_bronze_stock_news_df,
    ensure_bronze_stock_news_table,
    merge_bronze_stock_news,
)


spark = DatabricksSession.builder.getOrCreate()


TABLE_NAME = "workspace.bronze.stock_news"

TICKER = "AAPL"

START_DATE = "2026-08-01"
END_DATE = "2026-08-31"


print("\n==============================================")
print(" HISTORICAL STOCK NEWS TEST")
print("==============================================")

print(
    f"\nTicker: {TICKER}"
)

print(
    f"Period: {START_DATE} -> {END_DATE}"
)


# ============================================================
# 1. Tabela
# ============================================================

ensure_bronze_stock_news_table(
    spark=spark,
    table_name=TABLE_NAME
)


# ============================================================
# 2. Stanje prije
# ============================================================

before_count = spark.table(
    TABLE_NAME
).count()

print(
    "\nBroj Bronze redova prije ingestion-a:",
    before_count
)


# ============================================================
# 3. Historical API
# ============================================================

print(
    "\nPreuzimam historical AAPL news..."
)

records = fetch_historical_stock_news(
    ticker=TICKER,
    start_date=START_DATE,
    end_date=END_DATE,
    limit_per_page=100
)

print(
    "\nUkupan broj preuzetih AAPL clanaka:",
    len(records)
)


if not records:
    raise RuntimeError(
        "Historical News API nije vratio podatke."
    )


# ============================================================
# 4. Spark
# ============================================================

news_df = create_bronze_stock_news_df(
    spark=spark,
    records=records
)

print(
    "Broj redova u Spark DataFrame-u:",
    news_df.count()
)


# ============================================================
# 5. Period preuzetih vijesti
# ============================================================

print("\n=== PREUZETI PERIOD ===")

news_df.selectExpr(
    "MIN(published_utc) AS najstariji",
    "MAX(published_utc) AS najnoviji"
).show(
    truncate=False
)


# ============================================================
# 6. MERGE
# ============================================================

print(
    "\nRadim MERGE u Bronze News tabelu..."
)

merge_bronze_stock_news(
    spark=spark,
    df=news_df,
    table_name=TABLE_NAME
)

print(
    "Historical News MERGE zavrsen."
)


# ============================================================
# 7. Stanje poslije
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
# 8. AAPL istorija
# ============================================================

print("\n=== AAPL NEWS U BRONZE TABELI ===")

spark.sql(f"""
    SELECT
        requested_ticker,
        COUNT(*) AS broj_clanaka,
        MIN(published_utc) AS najstariji,
        MAX(published_utc) AS najnoviji

    FROM {TABLE_NAME}

    WHERE requested_ticker = 'AAPL'

    GROUP BY requested_ticker
""").show(
    truncate=False
)


# ============================================================
# 9. Duplikati
# ============================================================

print("\n=== PROVJERA DUPLIKATA ===")

duplicates = spark.sql(f"""
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

duplicate_count = duplicates.count()

if duplicate_count == 0:

    print(
        "USPJEH: Nema duplikata."
    )

else:

    print(
        "GRESKA: Pronadjeni duplikati."
    )

    duplicates.show(
        truncate=False
    )


spark.stop()

print(
    "\nHistorical Stock News test zavrsen."
)