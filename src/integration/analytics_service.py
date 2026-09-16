from databricks.connect import DatabricksSession
from pyspark.sql import functions as F


STOCK_DAILY_METRICS_TABLE = "workspace.gold.stock_daily_metrics"
NEWS_SUMMARY_TABLE = "workspace.gold.news_summary"
NEWS_DAILY_ANALYTICS_TABLE = "workspace.gold.news_daily_analytics"
STOCK_ANOMALIES_TABLE = "workspace.gold.stock_anomalies"

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

    # Query uzima najnovije redove prvo.
    # API istoriju vraća hronološki: starije -> novije.
    result.reverse()

    return result

def get_news_summary(ticker: str):
    """
    Returns Gold news summary analytics for a ticker.
    """

    ticker = ticker.strip().upper()

    spark = get_spark()

    rows = (
        spark.table(NEWS_SUMMARY_TABLE)
        .filter(F.col("ticker") == ticker)
        .select(
            "ticker",
            "company_name",
            "data_as_of",
            "total_news_count",
            "news_7d_count",
            "news_30d_count",
            "positive_30d_count",
            "negative_30d_count",
            "neutral_30d_count",
            "unknown_30d_count",
            "scored_sentiment_30d_count",
            "sentiment_score_30d",
            "sentiment_coverage_30d_pct",
            "positive_share_30d_pct",
            "negative_share_30d_pct",
            "neutral_share_30d_pct",
            "dominant_sentiment_30d",
            "publisher_count_30d",
            "avg_news_per_day_30d",
            "news_volume_rank_30d",
            "latest_news_at",
            "latest_news_age_days",
            "latest_article_id",
            "latest_news_title",
            "latest_news_publisher",
            "latest_news_url",
            "latest_news_sentiment",
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

def get_news_history(
    ticker: str,
    limit: int = 30,
):
    """
    Returns the latest N daily Gold news analytics rows
    for a ticker, ordered chronologically.
    """

    ticker = ticker.strip().upper()

    spark = get_spark()

    rows = (
        spark.table(NEWS_DAILY_ANALYTICS_TABLE)
        .filter(F.col("ticker") == ticker)
        .orderBy(F.col("published_date").desc())
        .select(
            "ticker",
            "company_name",
            "published_date",
            "news_count",
            "positive_count",
            "negative_count",
            "neutral_count",
            "unknown_sentiment_count",
            "scored_sentiment_count",
            "sentiment_score",
            "sentiment_coverage_pct",
            "positive_share_pct",
            "negative_share_pct",
            "neutral_share_pct",
            "dominant_sentiment",
            "publisher_count",
            "latest_news_at",
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

    result.reverse()

    return result

def get_stock_anomalies(
    ticker: str,
    limit: int = 20,
):
    """
    Returns detected Gold anomaly events for a ticker,
    newest first.
    """

    ticker = ticker.strip().upper()

    spark = get_spark()

    rows = (
        spark.table(STOCK_ANOMALIES_TABLE)
        .filter(
            (F.col("ticker") == ticker)
            & (F.col("is_anomaly") == True)
        )
        .orderBy(F.col("date").desc())
        .select(
            "ticker",
            "date",
            "close",
            "daily_return_pct",
            "daily_direction",
            "opening_gap_pct",
            "historical_volume_ratio",
            "return_zscore",
            "volume_zscore",
            "range_zscore",
            "gap_zscore",
            "transactions_zscore",
            "volatility_ratio",
            "rsi_14",
            "drawdown_60d_pct",
            "anomaly_signal_count",
            "anomaly_score",
            "anomaly_severity",
            "anomaly_type",
            "market_condition",
            "is_anomaly",
        )
        .limit(limit)
        .collect()
    )

    return [
        row.asDict(recursive=True)
        for row in rows
    ]

def get_anomaly_summary(ticker: str):
    """
    Returns summary statistics for detected Gold anomalies.
    """

    ticker = ticker.strip().upper()

    spark = get_spark()

    anomalies_df = (
        spark.table(STOCK_ANOMALIES_TABLE)
        .filter(
            (F.col("ticker") == ticker)
            & (F.col("is_anomaly") == True)
        )
    )

    summary_rows = (
        anomalies_df
        .agg(
            F.count("*").alias("total_anomalies"),

            F.sum(
                F.when(
                    F.col("anomaly_severity") == "HIGH",
                    1,
                ).otherwise(0)
            ).alias("high_count"),

            F.sum(
                F.when(
                    F.col("anomaly_severity") == "MEDIUM",
                    1,
                ).otherwise(0)
            ).alias("medium_count"),

            F.sum(
                F.when(
                    F.col("anomaly_severity") == "LOW",
                    1,
                ).otherwise(0)
            ).alias("low_count"),

            F.avg("anomaly_score").alias(
                "average_anomaly_score"
            ),

            F.max("anomaly_score").alias(
                "max_anomaly_score"
            ),
        )
        .collect()
    )

    if not summary_rows:
        return None

    summary = summary_rows[0].asDict(recursive=True)

    if summary["total_anomalies"] == 0:
        return None

    latest_rows = (
        anomalies_df
        .orderBy(F.col("date").desc())
        .select(
            "date",
            "anomaly_score",
            "anomaly_severity",
            "anomaly_type",
            "market_condition",
        )
        .limit(1)
        .collect()
    )

    latest = latest_rows[0].asDict(recursive=True)

    return {
        "ticker": ticker,
        "total_anomalies": summary["total_anomalies"],
        "high_count": summary["high_count"],
        "medium_count": summary["medium_count"],
        "low_count": summary["low_count"],
        "average_anomaly_score": summary[
            "average_anomaly_score"
        ],
        "max_anomaly_score": summary[
            "max_anomaly_score"
        ],
        "latest_anomaly_date": latest["date"],
        "latest_anomaly_score": latest[
            "anomaly_score"
        ],
        "latest_anomaly_severity": latest[
            "anomaly_severity"
        ],
        "latest_anomaly_type": latest[
            "anomaly_type"
        ],
        "latest_market_condition": latest[
            "market_condition"
        ],
    }