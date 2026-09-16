from databricks.connect import DatabricksSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.lakebase.watchlist_repository import get_watchlist


GOLD_TABLE = "workspace.gold.stock_daily_metrics"


def get_watchlist_tickers(user_id: int) -> list[str]:
    """
    Vraća tickere iz Lakebase watchliste korisnika.
    """

    watchlist = get_watchlist(user_id)

    return [
        item["ticker"].strip().upper()
        for item in watchlist
    ]


def get_latest_gold_metrics_for_watchlist(
    user_id: int,
) -> list[dict]:
    """
    Čita watchlistu korisnika iz Lakebase-a i za svaki ticker
    vraća najnoviji dostupni zapis iz Gold tabele.
    """

    tickers = get_watchlist_tickers(user_id)

    if not tickers:
        return []

    spark = DatabricksSession.builder.getOrCreate()

    gold_df = (
        spark.table(GOLD_TABLE)
        .filter(F.col("ticker").isin(tickers))
    )

    window = (
        Window
        .partitionBy("ticker")
        .orderBy(F.col("date").desc())
    )

    latest_df = (
        gold_df
        .withColumn(
            "_row_number",
            F.row_number().over(window),
        )
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
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
            "volatility_30d",
            "annualized_volatility_30d",
            "annualized_volatility_30d_pct",
            "high_low_range_pct",
            "close_vs_vwap_pct",
            "daily_direction",
        )
        .orderBy("ticker")
    )

    rows = latest_df.collect()

    results = []

    found_tickers = set()

    for row in rows:
        data = row.asDict(recursive=True)

        found_tickers.add(data["ticker"])

        results.append(data)

    # Ako je nešto u Lakebase watchlisti, ali nije u Gold tabeli,
    # vratimo i informaciju o tome.
    missing_tickers = sorted(
        set(tickers) - found_tickers
    )

    for ticker in missing_tickers:
        results.append(
            {
                "ticker": ticker,
                "status": "NOT_FOUND_IN_GOLD",
            }
        )

    return results