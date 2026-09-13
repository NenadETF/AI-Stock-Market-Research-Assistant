from databricks.connect import DatabricksSession
from pyspark.sql import functions as F


STOCK_DAILY_METRICS_TABLE = "workspace.gold.stock_daily_metrics"


def get_spark():
    """
    Creates or returns the active Databricks Connect session.
    """
    return DatabricksSession.builder.getOrCreate()


def get_latest_stock_metrics(ticker: str):
    """
    Returns the latest available Gold stock metrics for a ticker.
    """

    ticker = ticker.strip().upper()

    spark = get_spark()

    rows = (
        spark.table(STOCK_DAILY_METRICS_TABLE)
        .filter(F.col("ticker") == ticker)
        .orderBy(F.col("date").desc())
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
            "previous_close",
            "price_change",
            "daily_return_pct",
            "moving_avg_7d",
            "moving_avg_30d",
            "volume_ratio",
            "annualized_volatility_30d_pct",
            "high_low_range_pct",
            "close_vs_vwap_pct",
            "daily_direction",
            F.col("_gold_processed_at").alias(
                "gold_processed_at"
            ),
        )
        .limit(1)
        .collect()
    )

    if not rows:
        return None

    return rows[0].asDict(recursive=True)

def get_stock_metrics_history(
    ticker: str,
    limit: int = 30,
):
    """
    Returns the latest N available Gold metric rows
    for a ticker, ordered chronologically.
    """

    ticker = ticker.strip().upper()

    spark = get_spark()

    rows = (
        spark.table(STOCK_DAILY_METRICS_TABLE)
        .filter(F.col("ticker") == ticker)
        .orderBy(F.col("date").desc())
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
            "previous_close",
            "price_change",
            "daily_return_pct",
            "moving_avg_7d",
            "moving_avg_30d",
            "volume_ratio",
            "annualized_volatility_30d_pct",
            "high_low_range_pct",
            "close_vs_vwap_pct",
            "daily_direction",
            F.col("_gold_processed_at").alias(
                "gold_processed_at"
            ),
        )
        .limit(limit)
        .collect()
    )

    result = [
        row.asDict(recursive=True)
        for row in rows
    ]

    # Spark je uzeo najnovije prvo.
    # API istoriju vraća hronološki.
    result.reverse()

    return result