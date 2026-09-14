from databricks.connect import DatabricksSession

from src.ingestion.company_details import (
    fetch_company_details_for_tickers,
)

from src.bronze.company_details_bronze import (
    create_bronze_company_details_df,
    ensure_bronze_company_details_table,
    merge_bronze_company_details,
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

TABLE_NAME = "workspace.bronze.company_details"


print("\n==============================================")
print(" COMPANY DETAILS BRONZE INGESTION")
print("==============================================")


# ============================================================
# 3. Priprema Delta tabele
# ============================================================

print("\nProvjeravam company_details Bronze tabelu...")

ensure_bronze_company_details_table(
    spark=spark,
    table_name=TABLE_NAME
)

print("Company details Bronze tabela je spremna.")


# ============================================================
# 4. Stanje prije ingestion-a
# ============================================================

before_count = spark.table(
    TABLE_NAME
).count()

print(
    "\nBroj kompanija prije ingestion-a:",
    before_count
)


# ============================================================
# 5. Massive API
# ============================================================

print(
    "\nPreuzimam company details sa Massive API-ja..."
)

records = fetch_company_details_for_tickers(
    tickers=TICKERS
)

print(
    "\nUkupan broj company details zapisa:",
    len(records)
)


# ============================================================
# 6. Spark DataFrame
# ============================================================

print(
    "\nKreiram Company Details Bronze DataFrame..."
)

company_df = create_bronze_company_details_df(
    spark=spark,
    records=records
)

print(
    "Broj redova u DataFrame-u:",
    company_df.count()
)


print("\n=== COMPANY DETAILS SCHEMA ===")

company_df.printSchema()


# ============================================================
# 7. MERGE
# ============================================================

print(
    "\nRadim MERGE u "
    "workspace.bronze.company_details..."
)

merge_bronze_company_details(
    spark=spark,
    df=company_df,
    table_name=TABLE_NAME
)

print("Company details MERGE završen.")


# ============================================================
# 8. Stanje poslije ingestion-a
# ============================================================

saved_df = spark.table(
    TABLE_NAME
)

after_count = saved_df.count()

print(
    "\nBroj kompanija poslije ingestion-a:",
    after_count
)

print(
    "Promjena broja redova:",
    after_count - before_count
)


# ============================================================
# 9. Pregled kompanija
# ============================================================

print("\n=== KOMPANIJE ===")

spark.sql(f"""
    SELECT
        ticker,
        name,
        primary_exchange,
        currency_name,
        market_cap,
        total_employees,
        sic_description,
        list_date

    FROM {TABLE_NAME}

    ORDER BY ticker
""").show(
    truncate=False
)


# ============================================================
# 10. Provjera duplikata
# ============================================================

print("\n=== PROVJERA DUPLIKATA ===")

duplicates_df = spark.sql(f"""
    SELECT
        ticker,
        COUNT(*) AS broj

    FROM {TABLE_NAME}

    GROUP BY ticker

    HAVING COUNT(*) > 1
""")

duplicate_count = duplicates_df.count()

if duplicate_count == 0:

    print(
        "USPJEH: Nema duplikata po tickeru."
    )

else:

    print(
        "GRESKA: Pronađeni su duplikati."
    )

    duplicates_df.show(
        truncate=False
    )


# ============================================================
# 11. Osnovna NULL provjera
# ============================================================

print("\n=== NULL PROVJERA ===")

spark.sql(f"""
    SELECT

        SUM(
            CASE
                WHEN ticker IS NULL
                THEN 1
                ELSE 0
            END
        ) AS null_ticker,

        SUM(
            CASE
                WHEN name IS NULL
                THEN 1
                ELSE 0
            END
        ) AS null_name,

        SUM(
            CASE
                WHEN market_cap IS NULL
                THEN 1
                ELSE 0
            END
        ) AS null_market_cap

    FROM {TABLE_NAME}
""").show(
    truncate=False
)


# ============================================================
# 12. Delta detalji
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
# 13. Završna validacija
# ============================================================

if (
    after_count == len(TICKERS)
    and duplicate_count == 0
):

    print(
        "\nUSPJEH: Company Details Bronze ingestion "
        "je potpuno uspjesan."
    )

else:

    print(
        "\nUPOZORENJE: Potrebno je provjeriti "
        "Company Details Bronze podatke."
    )


# ============================================================
# 14. Zatvaranje
# ============================================================

spark.stop()

print(
    "\nCompany Details Bronze ingestion završen."
)