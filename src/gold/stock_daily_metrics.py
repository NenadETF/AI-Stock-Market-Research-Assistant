from databricks.connect import DatabricksSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# =============================================================================
# CONFIGURATION
# =============================================================================

SILVER_TABLE = "workspace.silver.stock_prices"

GOLD_SCHEMA = "workspace.gold"
GOLD_TABLE = "workspace.gold.stock_daily_metrics"


# =============================================================================
# SPARK SESSION
# =============================================================================

spark = DatabricksSession.builder.getOrCreate()


print("=" * 90)
print("GOLD LAYER - STOCK DAILY METRICS")
print("=" * 90)

print(f"Source: {SILVER_TABLE}")
print(f"Target: {GOLD_TABLE}")


# =============================================================================
# 1. CREATE GOLD SCHEMA
# =============================================================================

spark.sql(
    f"""
    CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA}
    """
)

print(f"[OK] Gold schema spremna: {GOLD_SCHEMA}")


# =============================================================================
# 2. LOAD SILVER STOCK PRICES
# =============================================================================

silver_df = spark.table(SILVER_TABLE)

silver_count = silver_df.count()

print()
print("SILVER SOURCE")
print("-" * 90)

print(f"Broj Silver redova: {silver_count}")


# =============================================================================
# 3. SELECT BUSINESS COLUMNS
# =============================================================================

prices_df = silver_df.select(
    "ticker",
    "date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "vwap",
    "transactions"
)


# =============================================================================
# 4. DEFINE WINDOWS
# =============================================================================

# Osnovni window po tickeru.
# Koristi se za LAG i pristup prethodnom trading danu.

ticker_window = (
    Window
    .partitionBy("ticker")
    .orderBy("date")
)


# 7 posljednjih trading zapisa, ukljucujuci trenutni.

window_7 = (
    Window
    .partitionBy("ticker")
    .orderBy("date")
    .rowsBetween(-6, 0)
)


# 30 posljednjih trading zapisa, ukljucujuci trenutni.

window_30 = (
    Window
    .partitionBy("ticker")
    .orderBy("date")
    .rowsBetween(-29, 0)
)


# =============================================================================
# 5. PREVIOUS CLOSE
# =============================================================================

gold_df = prices_df.withColumn(
    "previous_close",
    F.lag("close").over(ticker_window)
)


# =============================================================================
# 6. PRICE CHANGE
# =============================================================================

gold_df = gold_df.withColumn(
    "price_change",
    F.when(
        F.col("previous_close").isNotNull(),
        F.col("close") - F.col("previous_close")
    )
)


# =============================================================================
# 7. DAILY RETURN
#
# Primjer:
#
# daily_return = 0.02
# daily_return_pct = 2.00
#
# znaci rast cijene od 2%.
# =============================================================================

gold_df = gold_df.withColumn(
    "daily_return",
    F.when(
        (F.col("previous_close").isNotNull())
        & (F.col("previous_close") != 0),

        (
            F.col("close") - F.col("previous_close")
        ) / F.col("previous_close")
    )
)


gold_df = gold_df.withColumn(
    "daily_return_pct",
    F.col("daily_return") * 100
)


# =============================================================================
# 8. LOG RETURN
#
# Koristan za statisticku i finansijsku analizu.
#
# ln(Close_t / Close_t-1)
# =============================================================================

gold_df = gold_df.withColumn(
    "log_return",
    F.when(
        (F.col("previous_close") > 0)
        & (F.col("close") > 0),

        F.log(
            F.col("close") / F.col("previous_close")
        )
    )
)


# =============================================================================
# 9. NUMBER OF OBSERVATIONS INSIDE WINDOWS
#
# Ne zelimo prikazati moving_avg_30d kao pravi 30-periodni indikator
# dok ne postoji svih 30 trading zapisa.
# =============================================================================

gold_df = gold_df.withColumn(
    "_close_count_7",
    F.count("close").over(window_7)
)

gold_df = gold_df.withColumn(
    "_close_count_30",
    F.count("close").over(window_30)
)

gold_df = gold_df.withColumn(
    "_volume_count_30",
    F.count("volume").over(window_30)
)

gold_df = gold_df.withColumn(
    "_return_count_30",
    F.count("daily_return").over(window_30)
)


# =============================================================================
# 10. 7-PERIOD MOVING AVERAGE
# =============================================================================

gold_df = gold_df.withColumn(
    "moving_avg_7d",
    F.when(
        F.col("_close_count_7") == 7,
        F.avg("close").over(window_7)
    )
)


# =============================================================================
# 11. 30-PERIOD MOVING AVERAGE
# =============================================================================

gold_df = gold_df.withColumn(
    "moving_avg_30d",
    F.when(
        F.col("_close_count_30") == 30,
        F.avg("close").over(window_30)
    )
)


# =============================================================================
# 12. 30-PERIOD AVERAGE VOLUME
# =============================================================================

gold_df = gold_df.withColumn(
    "volume_avg_30d",
    F.when(
        F.col("_volume_count_30") == 30,
        F.avg("volume").over(window_30)
    )
)


# =============================================================================
# 13. VOLUME RATIO
#
# volume_ratio = trenutni volume / 30-periodni prosjek
#
# 1.0  -> volume jednak prosjeku
# 1.5  -> 50% veci od prosjeka
# 2.0  -> dvostruko veci od prosjeka
# =============================================================================

gold_df = gold_df.withColumn(
    "volume_ratio",
    F.when(
        F.col("volume_avg_30d") > 0,
        F.col("volume") / F.col("volume_avg_30d")
    )
)


# =============================================================================
# 14. 30-PERIOD VOLATILITY
#
# Standardna devijacija dnevnih prinosa.
#
# Za 30 dnevnih return vrijednosti potrebna je istorija
# od najmanje 31 closing cijene.
# =============================================================================

gold_df = gold_df.withColumn(
    "volatility_30d",
    F.when(
        F.col("_return_count_30") == 30,
        F.stddev_samp("daily_return").over(window_30)
    )
)


# =============================================================================
# 15. ANNUALIZED VOLATILITY
#
# Standardna finansijska aproksimacija:
#
# daily volatility * sqrt(252)
#
# gdje je 252 priblizan broj trading dana u godini.
# =============================================================================

gold_df = gold_df.withColumn(
    "annualized_volatility_30d",
    F.when(
        F.col("volatility_30d").isNotNull(),
        F.col("volatility_30d") * F.sqrt(F.lit(252.0))
    )
)


gold_df = gold_df.withColumn(
    "annualized_volatility_30d_pct",
    F.col("annualized_volatility_30d") * 100
)


# =============================================================================
# 16. HIGH-LOW RANGE
#
# Mjeri intraday raspon cijene.
# =============================================================================

gold_df = gold_df.withColumn(
    "high_low_range_pct",
    F.when(
        F.col("open") > 0,

        (
            (F.col("high") - F.col("low"))
            / F.col("open")
        ) * 100
    )
)


# =============================================================================
# 17. CLOSE VS VWAP
#
# Pozitivna vrijednost:
# closing price iznad VWAP-a.
#
# Negativna vrijednost:
# closing price ispod VWAP-a.
# =============================================================================

gold_df = gold_df.withColumn(
    "close_vs_vwap_pct",
    F.when(
        F.col("vwap") > 0,

        (
            (F.col("close") - F.col("vwap"))
            / F.col("vwap")
        ) * 100
    )
)


# =============================================================================
# 18. SIMPLE DAILY DIRECTION
#
# Korisno kasnije za dashboard i AI agent.
# =============================================================================

gold_df = gold_df.withColumn(
    "daily_direction",
    F.when(
        F.col("daily_return") > 0,
        F.lit("UP")
    )
    .when(
        F.col("daily_return") < 0,
        F.lit("DOWN")
    )
    .when(
        F.col("daily_return") == 0,
        F.lit("FLAT")
    )
    .otherwise(
        F.lit(None)
    )
)


# =============================================================================
# 19. REMOVE TEMPORARY COLUMNS
# =============================================================================

gold_df = gold_df.drop(
    "_close_count_7",
    "_close_count_30",
    "_volume_count_30",
    "_return_count_30"
)


# =============================================================================
# 20. GOLD PROCESSING TIMESTAMP
# =============================================================================

gold_df = gold_df.withColumn(
    "_gold_processed_at",
    F.current_timestamp()
)


# =============================================================================
# 21. ROUND ANALYTICAL VALUES
# =============================================================================

round_columns = {
    "previous_close": 4,
    "price_change": 4,
    "daily_return": 8,
    "daily_return_pct": 4,
    "log_return": 8,
    "moving_avg_7d": 4,
    "moving_avg_30d": 4,
    "volume_avg_30d": 2,
    "volume_ratio": 4,
    "volatility_30d": 8,
    "annualized_volatility_30d": 8,
    "annualized_volatility_30d_pct": 4,
    "high_low_range_pct": 4,
    "close_vs_vwap_pct": 4
}


for column_name, decimals in round_columns.items():
    gold_df = gold_df.withColumn(
        column_name,
        F.round(
            F.col(column_name),
            decimals
        )
    )


# =============================================================================
# 22. FINAL COLUMN ORDER
# =============================================================================

gold_df = gold_df.select(
    "ticker",
    "date",

    "open",
    "high",
    "low",
    "close",
    "volume",
    "vwap",
    "transactions",

    "previous_close",
    "price_change",

    "daily_return",
    "daily_return_pct",
    "log_return",

    "moving_avg_7d",
    "moving_avg_30d",

    "volume_avg_30d",
    "volume_ratio",

    "volatility_30d",
    "annualized_volatility_30d",
    "annualized_volatility_30d_pct",

    "high_low_range_pct",
    "close_vs_vwap_pct",

    "daily_direction",

    "_gold_processed_at"
)


# =============================================================================
# 23. WRITE GOLD DELTA TABLE
#
# Za ovu tabelu radimo potpuni recalculation.
#
# Razlog:
# rolling indikatori zavise od prethodnih redova.
# Ako se istorijski Silver podatak promijeni, promjena moze uticati
# i na kasnije Gold redove.
#
# Za trenutnih 3500 redova full refresh je i jednostavan i pouzdan.
# =============================================================================

(
    gold_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_TABLE)
)


print()
print(f"[OK] Gold tabela kreirana: {GOLD_TABLE}")


# =============================================================================
# 24. BASIC VALIDATION
# =============================================================================

result_df = spark.table(GOLD_TABLE)

gold_count = result_df.count()

ticker_count = (
    result_df
    .select("ticker")
    .distinct()
    .count()
)


print()
print("=" * 90)
print("GOLD TABLE SUMMARY")
print("=" * 90)

print(f"Silver redova: {silver_count}")
print(f"Gold redova:   {gold_count}")
print(f"Broj tickera:  {ticker_count}")


# =============================================================================
# 25. PERIOD
# =============================================================================

print()
print("GOLD STOCK PRICE PERIOD:")

(
    result_df
    .select(
        F.min("date").alias("najstariji"),
        F.max("date").alias("najnoviji")
    )
    .show()
)


# =============================================================================
# 26. ROW COUNT BY TICKER
# =============================================================================

print()
print("BROJ GOLD REDOVA PO TICKERU:")

(
    result_df
    .groupBy("ticker")
    .count()
    .orderBy("ticker")
    .show(truncate=False)
)


# =============================================================================
# 27. LATEST METRICS FOR EACH TICKER
# =============================================================================

latest_window = (
    Window
    .partitionBy("ticker")
    .orderBy(F.col("date").desc())
)


latest_df = (
    result_df
    .withColumn(
        "_row_number",
        F.row_number().over(latest_window)
    )
    .filter(
        F.col("_row_number") == 1
    )
    .drop("_row_number")
)


print()
print("NAJNOVIJE GOLD METRIKE PO TICKERU:")

(
    latest_df
    .select(
        "ticker",
        "date",
        "close",
        "daily_return_pct",
        "moving_avg_7d",
        "moving_avg_30d",
        "volume_ratio",
        "annualized_volatility_30d_pct",
        "high_low_range_pct",
        "daily_direction"
    )
    .orderBy("ticker")
    .show(
        n=100,
        truncate=False
    )
)


# =============================================================================
# 28. NULL STATISTICS FOR ROLLING METRICS
# =============================================================================

print()
print("NULL STATISTIKA ROLLING INDIKATORA:")

result_df.select(
    F.sum(
        F.when(
            F.col("moving_avg_7d").isNull(),
            1
        ).otherwise(0)
    ).alias("null_moving_avg_7d"),

    F.sum(
        F.when(
            F.col("moving_avg_30d").isNull(),
            1
        ).otherwise(0)
    ).alias("null_moving_avg_30d"),

    F.sum(
        F.when(
            F.col("volatility_30d").isNull(),
            1
        ).otherwise(0)
    ).alias("null_volatility_30d"),

    F.sum(
        F.when(
            F.col("volume_ratio").isNull(),
            1
        ).otherwise(0)
    ).alias("null_volume_ratio")
).show()


print()
print("=" * 90)
print("GOLD STOCK DAILY METRICS COMPLETED")
print("=" * 90)