from databricks.connect import DatabricksSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ============================================================
# SPARK SESSION
# ============================================================

spark = DatabricksSession.builder.getOrCreate()

BRONZE_TABLE = "workspace.bronze.company_details"
SILVER_TABLE = "workspace.silver.company_details"


print("=" * 80)
print("SILVER ETL - COMPANY DETAILS")
print("=" * 80)


# ============================================================
# 1. READ BRONZE
# ============================================================

bronze_df = spark.table(BRONZE_TABLE)

bronze_count = bronze_df.count()

print(f"\nBronze redova: {bronze_count}")


# ============================================================
# 2. BASIC CLEANING
# ============================================================

clean_df = (
    bronze_df

    # --------------------------------------------------------
    # TICKER
    # --------------------------------------------------------

    .withColumn(
        "ticker",
        F.upper(
            F.trim(F.col("ticker"))
        )
    )

    .withColumn(
        "ticker_root",
        F.upper(
            F.trim(F.col("ticker_root"))
        )
    )

    .withColumn(
        "ticker_suffix",
        F.upper(
            F.trim(F.col("ticker_suffix"))
        )
    )

    # --------------------------------------------------------
    # COMPANY INFO
    # --------------------------------------------------------

    .withColumn(
        "name",
        F.trim(F.col("name"))
    )

    .withColumn(
        "description",
        F.trim(F.col("description"))
    )

    # --------------------------------------------------------
    # MARKET INFO
    # --------------------------------------------------------

    .withColumn(
        "market",
        F.lower(
            F.trim(F.col("market"))
        )
    )

    .withColumn(
        "locale",
        F.lower(
            F.trim(F.col("locale"))
        )
    )

    .withColumn(
        "primary_exchange",
        F.upper(
            F.trim(F.col("primary_exchange"))
        )
    )

    .withColumn(
        "type",
        F.upper(
            F.trim(F.col("type"))
        )
    )

    .withColumn(
        "currency_name",
        F.upper(
            F.trim(F.col("currency_name"))
        )
    )

    # --------------------------------------------------------
    # IDENTIFIERS
    # --------------------------------------------------------

    .withColumn(
        "cik",
        F.trim(F.col("cik"))
    )

    .withColumn(
        "composite_figi",
        F.trim(F.col("composite_figi"))
    )

    .withColumn(
        "share_class_figi",
        F.trim(F.col("share_class_figi"))
    )

    .withColumn(
        "sic_code",
        F.trim(F.col("sic_code"))
    )

    .withColumn(
        "sic_description",
        F.trim(F.col("sic_description"))
    )

    # --------------------------------------------------------
    # CONTACT / LOCATION
    # --------------------------------------------------------

    .withColumn(
        "homepage_url",
        F.trim(F.col("homepage_url"))
    )

    .withColumn(
        "phone_number",
        F.trim(F.col("phone_number"))
    )

    .withColumn(
        "address1",
        F.trim(F.col("address1"))
    )

    .withColumn(
        "city",
        F.trim(F.col("city"))
    )

    .withColumn(
        "state",
        F.upper(
            F.trim(F.col("state"))
        )
    )

    .withColumn(
        "postal_code",
        F.trim(F.col("postal_code"))
    )

    .withColumn(
        "logo_url",
        F.trim(F.col("logo_url"))
    )

    .withColumn(
        "icon_url",
        F.trim(F.col("icon_url"))
    )
)


# ============================================================
# 3. DATA VALIDATION
# ============================================================

valid_condition = (

    # --------------------------------------------------------
    # Ključna polja
    # --------------------------------------------------------

    F.col("ticker").isNotNull()
    & (F.length(F.col("ticker")) > 0)

    & F.col("name").isNotNull()
    & (F.length(F.col("name")) > 0)

    # --------------------------------------------------------
    # Market cap
    # --------------------------------------------------------

    & (
        F.col("market_cap").isNull()
        | (F.col("market_cap") >= 0)
    )

    # --------------------------------------------------------
    # Employees
    # --------------------------------------------------------

    & (
        F.col("total_employees").isNull()
        | (F.col("total_employees") >= 0)
    )

    # --------------------------------------------------------
    # Shares outstanding
    # --------------------------------------------------------

    & (
        F.col("weighted_shares_outstanding").isNull()
        | (F.col("weighted_shares_outstanding") >= 0)
    )

    & (
        F.col("share_class_shares_outstanding").isNull()
        | (F.col("share_class_shares_outstanding") >= 0)
    )

    # --------------------------------------------------------
    # Round lot
    # --------------------------------------------------------

    & (
        F.col("round_lot").isNull()
        | (F.col("round_lot") >= 0)
    )
)


valid_df = clean_df.filter(valid_condition)

invalid_df = clean_df.filter(
    ~valid_condition
)


valid_count = valid_df.count()
invalid_count = invalid_df.count()


print(f"Validnih redova: {valid_count}")
print(f"Nevalidnih redova: {invalid_count}")


# ============================================================
# 4. BUSINESS KEY ANALYSIS
# ============================================================

unique_tickers = (
    valid_df
    .select("ticker")
    .distinct()
    .count()
)


print("\nBUSINESS KEY ANALIZA:")

print(
    f"Jedinstvenih tickera: {unique_tickers}"
)


duplicate_ticker_count = (
    valid_df
    .groupBy("ticker")
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)


print(
    f"Tickera sa više Bronze zapisa: "
    f"{duplicate_ticker_count}"
)


# ============================================================
# 5. DEDUPLICATION
# ============================================================
#
# Business key:
#
#   ticker
#
# Ako se ista kompanija ponovo ingestuje,
# zadržavamo najnoviju verziju prema _ingested_at.
#
# ============================================================

window_spec = (
    Window
    .partitionBy("ticker")
    .orderBy(
        F.col("_ingested_at").desc()
    )
)


deduplicated_df = (
    valid_df
    .withColumn(
        "_row_number",
        F.row_number().over(window_spec)
    )
    .filter(
        F.col("_row_number") == 1
    )
    .drop("_row_number")
)


deduplicated_count = deduplicated_df.count()


print(
    f"Redova nakon deduplikacije: "
    f"{deduplicated_count}"
)


# ============================================================
# 6. SILVER TRANSFORMATIONS
# ============================================================

silver_df = (
    deduplicated_df

    # --------------------------------------------------------
    # Market cap u milijardama
    #
    # Korisno za Gold sloj i dashboard
    # --------------------------------------------------------

    .withColumn(
        "market_cap_billions",
        F.when(
            F.col("market_cap").isNotNull(),
            F.round(
                F.col("market_cap") / 1_000_000_000,
                4
            )
        )
    )

    # --------------------------------------------------------
    # Starost kompanije na berzi
    # --------------------------------------------------------

    .withColumn(
        "years_listed",
        F.when(
            F.col("list_date").isNotNull(),
            F.floor(
                F.months_between(
                    F.current_date(),
                    F.col("list_date")
                ) / 12
            )
        )
    )

    # --------------------------------------------------------
    # Kompletna lokacija
    # --------------------------------------------------------

    .withColumn(
        "location",
        F.concat_ws(
            ", ",
            F.col("city"),
            F.col("state")
        )
    )

    # --------------------------------------------------------
    # Silver metadata
    # --------------------------------------------------------

    .withColumn(
        "_silver_processed_at",
        F.current_timestamp()
    )

    .select(
        "ticker",
        "name",

        "market",
        "locale",
        "primary_exchange",
        "type",
        "active",
        "currency_name",

        "cik",
        "composite_figi",
        "share_class_figi",

        "market_cap",
        "market_cap_billions",

        "description",

        "sic_code",
        "sic_description",

        "homepage_url",
        "phone_number",

        "total_employees",

        "list_date",
        "years_listed",

        "ticker_root",
        "ticker_suffix",

        "weighted_shares_outstanding",
        "share_class_shares_outstanding",

        "round_lot",

        "address1",
        "city",
        "state",
        "postal_code",
        "location",

        "logo_url",
        "icon_url",

        "_ingested_at",
        "_source",
        "_silver_processed_at"
    )
)


# ============================================================
# 7. CREATE SILVER SCHEMA
# ============================================================

spark.sql("""
    CREATE SCHEMA IF NOT EXISTS workspace.silver
""")


# ============================================================
# 8. CREATE SILVER TABLE
# ============================================================

spark.sql("""
    CREATE TABLE IF NOT EXISTS workspace.silver.company_details
    (
        ticker STRING,
        name STRING,

        market STRING,
        locale STRING,
        primary_exchange STRING,
        type STRING,
        active BOOLEAN,
        currency_name STRING,

        cik STRING,
        composite_figi STRING,
        share_class_figi STRING,

        market_cap DOUBLE,
        market_cap_billions DOUBLE,

        description STRING,

        sic_code STRING,
        sic_description STRING,

        homepage_url STRING,
        phone_number STRING,

        total_employees BIGINT,

        list_date DATE,
        years_listed BIGINT,

        ticker_root STRING,
        ticker_suffix STRING,

        weighted_shares_outstanding BIGINT,
        share_class_shares_outstanding BIGINT,

        round_lot BIGINT,

        address1 STRING,
        city STRING,
        state STRING,
        postal_code STRING,
        location STRING,

        logo_url STRING,
        icon_url STRING,

        _ingested_at TIMESTAMP,
        _source STRING,
        _silver_processed_at TIMESTAMP
    )
    USING DELTA
""")


# ============================================================
# 9. MERGE INTO SILVER
# ============================================================
#
# Business key = ticker
#
# Ako dođe noviji snapshot kompanije,
# postojeći Silver zapis se ažurira.
#
# ============================================================

silver_df.createOrReplaceTempView(
    "silver_company_details_stage"
)


spark.sql("""
    MERGE INTO workspace.silver.company_details AS target

    USING silver_company_details_stage AS source

    ON target.ticker = source.ticker

    WHEN MATCHED
         AND source._ingested_at > target._ingested_at
    THEN UPDATE SET *

    WHEN NOT MATCHED
    THEN INSERT *
""")


# ============================================================
# 10. FINAL VALIDATION
# ============================================================

final_df = spark.table(SILVER_TABLE)

final_count = final_df.count()


final_duplicate_count = (
    final_df
    .groupBy("ticker")
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)


print("\n" + "=" * 80)
print("SILVER COMPANY DETAILS ETL REZULTAT")
print("=" * 80)

print(
    f"Bronze redova:               "
    f"{bronze_count}"
)

print(
    f"Validnih redova:             "
    f"{valid_count}"
)

print(
    f"Nevalidnih redova:           "
    f"{invalid_count}"
)

print(
    f"Nakon deduplikacije:         "
    f"{deduplicated_count}"
)

print(
    f"Ukupno Silver redova:        "
    f"{final_count}"
)

print(
    f"Duplikata u Silver tabeli:   "
    f"{final_duplicate_count}"
)


# ============================================================
# 11. SAMPLE DATA
# ============================================================

print("\nSILVER COMPANY DETAILS:")

final_df.select(
    "ticker",
    "name",
    "primary_exchange",
    "currency_name",
    "market_cap_billions",
    "total_employees",
    "list_date",
    "years_listed",
    "location"
).orderBy(
    F.desc("market_cap")
).show(
    truncate=False
)


# ============================================================
# 12. SILVER SCHEMA
# ============================================================

print("\nSILVER COMPANY DETAILS SCHEMA:")

final_df.printSchema()


print(
    "\nSILVER COMPANY DETAILS ETL "
    "USPJESNO ZAVRSEN."
)