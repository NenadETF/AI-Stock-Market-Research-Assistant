from databricks.connect import DatabricksSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ============================================================
# SPARK SESSION
# ============================================================

spark = DatabricksSession.builder.getOrCreate()


BRONZE_TABLE = "workspace.bronze.stock_prices"
SILVER_TABLE = "workspace.silver.stock_prices"


print("=" * 80)
print("SILVER ETL - STOCK PRICES")
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

    # Standardizacija ticker-a
    .withColumn(
        "ticker",
        F.upper(F.trim(F.col("ticker")))
    )

    # Osnovni tipovi / zaokruživanje
    .withColumn("open", F.round(F.col("open"), 4))
    .withColumn("high", F.round(F.col("high"), 4))
    .withColumn("low", F.round(F.col("low"), 4))
    .withColumn("close", F.round(F.col("close"), 4))
    .withColumn("vwap", F.round(F.col("vwap"), 4))
)


# ============================================================
# 3. DATA VALIDATION
# ============================================================

valid_condition = (

    # Ključna polja
    F.col("ticker").isNotNull()
    & (F.length(F.col("ticker")) > 0)
    & F.col("date").isNotNull()

    # OHLC mora postojati
    & F.col("open").isNotNull()
    & F.col("high").isNotNull()
    & F.col("low").isNotNull()
    & F.col("close").isNotNull()

    # Cijene moraju biti pozitivne
    & (F.col("open") > 0)
    & (F.col("high") > 0)
    & (F.col("low") > 0)
    & (F.col("close") > 0)

    # High mora biti najveća cijena dana
    & (F.col("high") >= F.col("open"))
    & (F.col("high") >= F.col("close"))
    & (F.col("high") >= F.col("low"))

    # Low mora biti najmanja cijena dana
    & (F.col("low") <= F.col("open"))
    & (F.col("low") <= F.col("close"))
    & (F.col("low") <= F.col("high"))

    # Volume
    & (
        F.col("volume").isNull()
        | (F.col("volume") >= 0)
    )

    # Transactions
    & (
        F.col("transactions").isNull()
        | (F.col("transactions") >= 0)
    )

    # VWAP
    & (
        F.col("vwap").isNull()
        | (F.col("vwap") > 0)
    )
)


valid_df = clean_df.filter(valid_condition)

invalid_df = clean_df.filter(~valid_condition)


valid_count = valid_df.count()
invalid_count = invalid_df.count()

print(f"Validnih redova: {valid_count}")
print(f"Nevalidnih redova: {invalid_count}")


# ============================================================
# 4. DEDUPLICATION
# ============================================================

window_spec = (
    Window
    .partitionBy(
        "ticker",
        "date",
    )
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
    .filter(F.col("_row_number") == 1)
    .drop("_row_number")
)


deduplicated_count = deduplicated_df.count()

print(
    f"Redova nakon deduplikacije: "
    f"{deduplicated_count}"
)


# ============================================================
# 5. SILVER TRANSFORMATIONS
# ============================================================

silver_df = (
    deduplicated_df

    # Apsolutna promjena cijene tokom dana
    .withColumn(
        "price_change",
        F.round(
            F.col("close") - F.col("open"),
            4
        )
    )

    # Procentualna promjena open -> close
    .withColumn(
        "price_change_pct",
        F.round(
            (
                (F.col("close") - F.col("open"))
                / F.col("open")
            ) * 100,
            4
        )
    )

    # Dnevni raspon cijene
    .withColumn(
        "daily_range",
        F.round(
            F.col("high") - F.col("low"),
            4
        )
    )

    # Dnevni raspon kao procenat u odnosu na open
    .withColumn(
        "daily_range_pct",
        F.round(
            (
                (F.col("high") - F.col("low"))
                / F.col("open")
            ) * 100,
            4
        )
    )

    # Vrijeme Silver transformacije
    .withColumn(
        "_silver_processed_at",
        F.current_timestamp()
    )

    .select(
        "ticker",
        "date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "vwap",
        "transactions",
        "price_change",
        "price_change_pct",
        "daily_range",
        "daily_range_pct",
        "_ingested_at",
        "_source",
        "_silver_processed_at"
    )
)


# ============================================================
# 6. CREATE SILVER SCHEMA
# ============================================================

spark.sql("""
    CREATE SCHEMA IF NOT EXISTS workspace.silver
""")


# ============================================================
# 7. CREATE TARGET TABLE
# ============================================================

spark.sql("""
    CREATE TABLE IF NOT EXISTS workspace.silver.stock_prices
    (
        ticker STRING,
        date DATE,

        open DOUBLE,
        high DOUBLE,
        low DOUBLE,
        close DOUBLE,

        volume DOUBLE,
        vwap DOUBLE,
        transactions BIGINT,

        price_change DOUBLE,
        price_change_pct DOUBLE,

        daily_range DOUBLE,
        daily_range_pct DOUBLE,

        _ingested_at TIMESTAMP,
        _source STRING,
        _silver_processed_at TIMESTAMP
    )
    USING DELTA
""")


# ============================================================
# 8. MERGE INTO SILVER
# ============================================================

silver_df.createOrReplaceTempView(
    "silver_stock_prices_stage"
)


spark.sql("""
    MERGE INTO workspace.silver.stock_prices AS target

    USING silver_stock_prices_stage AS source

    ON target.ticker = source.ticker
       AND target.date = source.date

    WHEN MATCHED
         AND source._ingested_at > target._ingested_at
    THEN UPDATE SET *

    WHEN NOT MATCHED
    THEN INSERT *
""")


# ============================================================
# 9. FINAL VALIDATION
# ============================================================

final_df = spark.table(SILVER_TABLE)

final_count = final_df.count()


print("\n" + "=" * 80)
print("SILVER ETL REZULTAT")
print("=" * 80)

print(f"Bronze redova:               {bronze_count}")
print(f"Validnih redova:             {valid_count}")
print(f"Nevalidnih redova:           {invalid_count}")
print(f"Nakon deduplikacije:         {deduplicated_count}")
print(f"Ukupno Silver redova:        {final_count}")


print("\nPRVIH 10 SILVER REDOVA:")

final_df.orderBy(
    "ticker",
    "date"
).show(
    10,
    truncate=False
)


print("\nSILVER SCHEMA:")

final_df.printSchema()


print("\nSILVER STOCK PRICES ETL USPJESNO ZAVRSEN.")