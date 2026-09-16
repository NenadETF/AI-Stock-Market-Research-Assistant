from databricks.connect import DatabricksSession
from pyspark.sql import functions as F

from src.lakebase.user_repository import get_user_by_id
from src.lakebase.watchlist_repository import get_watchlist_item


COMPANY_DETAILS_TABLE = "workspace.silver.company_details"

MARKET_TABLE = "workspace.gold.stock_daily_metrics"
PERFORMANCE_TABLE = "workspace.gold.company_performance"
TECHNICAL_TABLE = "workspace.gold.technical_indicators"

NEWS_SUMMARY_TABLE = "workspace.gold.news_summary"
NEWS_DAILY_TABLE = "workspace.gold.news_daily_analytics"

ANOMALIES_TABLE = "workspace.gold.stock_anomalies"
ANOMALY_NEWS_TABLE = "workspace.gold.stock_anomaly_news_summary"


def _first_row_as_dict(df):
    rows = df.limit(1).collect()

    if not rows:
        return None

    return rows[0].asDict(recursive=True)


def get_research_context(
    user_id: int,
    ticker: str,
    news_daily_rows: int = 7,
    anomaly_rows: int = 5,
) -> dict:
    """
    Kreira kompletan research context za jedan ticker.

    Context objedinjuje:
    - Lakebase user podatke
    - Lakebase watchlist thesis / notes
    - company details
    - latest market metrics
    - company performance
    - latest technical indicators
    - news summary
    - recent daily news analytics
    - recent stock anomalies
    - anomaly-news summaries
    """

    ticker = ticker.strip().upper()

    # ==========================================================
    # LAKEBASE USER
    # ==========================================================

    user = get_user_by_id(user_id)

    if user is None:
        raise ValueError(
            f"Korisnik sa ID={user_id} ne postoji."
        )

    # ==========================================================
    # LAKEBASE WATCHLIST ITEM
    # ==========================================================

    watchlist_item = get_watchlist_item(
        user_id=user_id,
        ticker=ticker,
    )

    if watchlist_item is None:
        raise ValueError(
            f"Ticker {ticker} se ne nalazi u watchlisti "
            f"korisnika ID={user_id}."
        )

    spark = DatabricksSession.builder.getOrCreate()

    # ==========================================================
    # COMPANY DETAILS
    # ==========================================================

    company = _first_row_as_dict(
        spark.table(COMPANY_DETAILS_TABLE)
        .filter(F.col("ticker") == ticker)
        .select(
            "ticker",
            "name",
            "market",
            "locale",
            "primary_exchange",
            "type",
            "active",
            "currency_name",
            "market_cap",
            "market_cap_billions",
            "description",
            "sic_code",
            "sic_description",
            "homepage_url",
            "total_employees",
            "list_date",
            "years_listed",
            "location",
            "logo_url",
            "icon_url",
        )
    )

    # ==========================================================
    # LATEST MARKET DATA
    # ==========================================================

    market = _first_row_as_dict(
        spark.table(MARKET_TABLE)
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
            "daily_return",
            "daily_return_pct",
            "moving_avg_7d",
            "moving_avg_30d",
            "volume_avg_30d",
            "volume_ratio",
            "annualized_volatility_30d_pct",
            "high_low_range_pct",
            "close_vs_vwap_pct",
            "daily_direction",
        )
    )

    # ==========================================================
    # COMPANY PERFORMANCE
    # ==========================================================

    performance = _first_row_as_dict(
        spark.table(PERFORMANCE_TABLE)
        .filter(F.col("ticker") == ticker)
        .select(
            "ticker",
            "company_name",
            "latest_date",
            "current_price",
            "return_7d_pct",
            "return_30d_pct",
            "return_90d_pct",
            "return_1y_pct",
            "total_period_return_pct",
            "high_52w",
            "low_52w",
            "distance_from_52w_high_pct",
            "distance_from_52w_low_pct",
            "volume_ratio",
            "annualized_volatility_30d_pct",
            "momentum_10d_pct",
            "trend_signal",
            "rsi_signal",
            "macd_state",
            "performance_score",
            "performance_rank",
            "performance_category",
        )
    )

    # ==========================================================
    # LATEST TECHNICAL INDICATORS
    # ==========================================================

    technical = _first_row_as_dict(
        spark.table(TECHNICAL_TABLE)
        .filter(F.col("ticker") == ticker)
        .orderBy(F.col("date").desc())
        .select(
            "ticker",
            "date",
            "close",
            "volume",
            "daily_return_pct",
            "volume_ratio",
            "annualized_volatility_30d_pct",
            "sma_20",
            "sma_50",
            "ema_12",
            "ema_26",
            "rsi_14",
            "macd",
            "macd_signal",
            "macd_histogram",
            "bollinger_middle",
            "bollinger_upper",
            "bollinger_lower",
            "bollinger_bandwidth_pct",
            "bollinger_position",
            "momentum_10d_pct",
            "price_vs_sma20_pct",
            "price_vs_sma50_pct",
            "trend_signal",
            "rsi_signal",
            "macd_state",
        )
    )

    # ==========================================================
    # NEWS SUMMARY
    # ==========================================================

    news_summary = _first_row_as_dict(
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
            "latest_news_title",
            "latest_news_publisher",
            "latest_news_url",
            "latest_news_sentiment",
        )
    )

    # ==========================================================
    # RECENT DAILY NEWS ANALYTICS
    # ==========================================================

    news_daily_rows_df = (
        spark.table(NEWS_DAILY_TABLE)
        .filter(F.col("ticker") == ticker)
        .orderBy(F.col("published_date").desc())
        .select(
            "ticker",
            "published_date",
            "news_count",
            "positive_count",
            "negative_count",
            "neutral_count",
            "scored_sentiment_count",
            "sentiment_score",
            "sentiment_coverage_pct",
            "positive_share_pct",
            "negative_share_pct",
            "neutral_share_pct",
            "dominant_sentiment",
            "publisher_count",
            "latest_news_at",
        )
        .limit(news_daily_rows)
        .collect()
    )

    news_daily = [
        row.asDict(recursive=True)
        for row in news_daily_rows_df
    ]

    # ==========================================================
    # RECENT ANOMALIES
    # ==========================================================

    anomaly_df = (
        spark.table(ANOMALIES_TABLE)
        .filter(
            (F.col("ticker") == ticker)
            & (F.col("is_anomaly") == True)
        )
        .orderBy(
            F.col("date").desc(),
            F.col("anomaly_score").desc(),
        )
        .select(
            "ticker",
            "date",
            "close",
            "daily_return_pct",
            "daily_direction",
            "opening_gap_pct",
            "return_7d_pct",
            "return_20d_pct",
            "return_30d_pct",
            "drawdown_60d_pct",
            "rsi_14",
            "volume_ratio",
            "annualized_volatility_30d_pct",
            "return_zscore",
            "volume_zscore",
            "range_zscore",
            "gap_zscore",
            "transactions_zscore",
            "volatility_ratio",
            "price_return_anomaly",
            "volume_anomaly",
            "range_anomaly",
            "gap_anomaly",
            "transactions_anomaly",
            "volatility_anomaly",
            "anomaly_signal_count",
            "anomaly_score",
            "anomaly_severity",
            "anomaly_type",
            "market_condition",
        )
        .limit(anomaly_rows)
        .collect()
    )

    anomalies = [
        row.asDict(recursive=True)
        for row in anomaly_df
    ]

    # ==========================================================
    # ANOMALY + NEWS CONTEXT
    # ==========================================================

    anomaly_news_df = (
        spark.table(ANOMALY_NEWS_TABLE)
        .filter(F.col("ticker") == ticker)
        .orderBy(F.col("anomaly_date").desc())
        .select(
            "anomaly_event_id",
            "ticker",
            "anomaly_date",
            "close",
            "daily_return_pct",
            "anomaly_score",
            "anomaly_severity",
            "anomaly_type",
            "market_condition",
            "has_related_news",
            "has_potential_context_news",
            "related_news_count",
            "pre_event_news_count",
            "same_day_news_count",
            "post_event_news_count",
            "potential_context_news_count",
            "reaction_news_count",
            "positive_news_count",
            "negative_news_count",
            "neutral_news_count",
            "avg_sentiment_score",
            "avg_weighted_sentiment_score",
            "dominant_sentiment",
            "max_temporal_relevance_score",
            "news_coverage",
            "potential_context_coverage",
        )
        .limit(anomaly_rows)
        .collect()
    )

    anomaly_news = [
        row.asDict(recursive=True)
        for row in anomaly_news_df
    ]

    # ==========================================================
    # FINAL RESEARCH CONTEXT
    # ==========================================================

    return {
        "user": {
            "user_id": user["user_id"],
            "username": user["username"],
            "full_name": user["full_name"],
        },

        "watchlist": {
            "ticker": watchlist_item["ticker"],
            "investment_thesis":
                watchlist_item["investment_thesis"],
            "notes":
                watchlist_item["notes"],
            "added_at":
                watchlist_item["added_at"],
            "updated_at":
                watchlist_item["updated_at"],
        },

        "company": company,

        "market": market,

        "performance": performance,

        "technical": technical,

        "news": {
            "summary": news_summary,
            "recent_daily": news_daily,
        },

        "anomalies": {
            "recent": anomalies,
            "news_context": anomaly_news,
        },
    }