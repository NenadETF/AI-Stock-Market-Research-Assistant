from databricks.connect import DatabricksSession
from pyspark.sql import functions as F


NEWS_TABLE = "workspace.silver.stock_news"
ANOMALY_TABLE = "workspace.gold.stock_anomalies"


spark = DatabricksSession.builder.getOrCreate()


print("=" * 100)
print("NEWS COVERAGE DIAGNOSTIC")
print("=" * 100)


news_df = spark.table(NEWS_TABLE)

anomaly_df = (
    spark.table(ANOMALY_TABLE)
    .filter(F.col("is_anomaly"))
)


# =============================================================================
# 1. NEWS PERIOD BY TICKER
# =============================================================================

print()
print("NEWS PERIOD PO TICKERU:")

(
    news_df
    .groupBy("requested_ticker")
    .agg(
        F.count("*").alias("news_count"),
        F.min("published_date").alias("oldest_news"),
        F.max("published_date").alias("latest_news")
    )
    .orderBy("requested_ticker")
    .show(
        n=100,
        truncate=False
    )
)


# =============================================================================
# 2. ANOMALY PERIOD BY TICKER
# =============================================================================

print()
print("ANOMALY PERIOD PO TICKERU:")

(
    anomaly_df
    .groupBy("ticker")
    .agg(
        F.count("*").alias("anomaly_count"),
        F.min("date").alias("oldest_anomaly"),
        F.max("date").alias("latest_anomaly")
    )
    .orderBy("ticker")
    .show(
        n=100,
        truncate=False
    )
)


# =============================================================================
# 3. GLOBAL PERIOD COMPARISON
# =============================================================================

print()
print("GLOBAL NEWS PERIOD:")

news_df.select(
    F.min("published_date").alias("oldest_news"),
    F.max("published_date").alias("latest_news")
).show()


print()
print("GLOBAL ANOMALY PERIOD:")

anomaly_df.select(
    F.min("date").alias("oldest_anomaly"),
    F.max("date").alias("latest_anomaly")
).show()


# =============================================================================
# 4. NEWS COUNT BY MONTH
# =============================================================================

print()
print("NEWS REDOVI PO MJESECU:")

(
    news_df
    .withColumn(
        "month",
        F.date_format(
            "published_date",
            "yyyy-MM"
        )
    )
    .groupBy("month")
    .count()
    .orderBy("month")
    .show(
        n=100,
        truncate=False
    )
)


print()
print("=" * 100)
print("NEWS COVERAGE DIAGNOSTIC COMPLETED")
print("=" * 100)