from databricks.connect import DatabricksSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# =============================================================================
# CONFIGURATION
# =============================================================================

SOURCE_TABLE = "workspace.gold.stock_daily_metrics"

GOLD_SCHEMA = "workspace.gold"
TARGET_TABLE = "workspace.gold.stock_anomalies"


# =============================================================================
# ANOMALY CONFIGURATION
# =============================================================================

BASELINE_PERIOD = 30

RETURN_Z_THRESHOLD = 3.0
VOLUME_Z_THRESHOLD = 3.0
RANGE_Z_THRESHOLD = 3.0
GAP_Z_THRESHOLD = 3.0
TRANSACTIONS_Z_THRESHOLD = 3.0

VOLUME_RATIO_THRESHOLD = 2.0
VOLATILITY_RATIO_THRESHOLD = 1.5

ANOMALY_SCORE_THRESHOLD = 50.0


# =============================================================================
# SPARK SESSION
# =============================================================================

spark = DatabricksSession.builder.getOrCreate()


print("=" * 100)
print("GOLD LAYER - STOCK ANOMALY DETECTION")
print("=" * 100)

print(f"Source: {SOURCE_TABLE}")
print(f"Target: {TARGET_TABLE}")


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
# 2. LOAD GOLD DAILY METRICS
# =============================================================================

source_df = spark.table(SOURCE_TABLE)

source_count = source_df.count()

print()
print("SOURCE DATA")
print("-" * 100)
print(f"Broj redova: {source_count}")


# =============================================================================
# 3. SELECT REQUIRED COLUMNS
# =============================================================================

df = source_df.select(
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

    "daily_return",
    "daily_return_pct",

    "moving_avg_7d",
    "moving_avg_30d",

    "volume_avg_30d",
    "volume_ratio",

    "volatility_30d",
    "annualized_volatility_30d_pct",

    "high_low_range_pct",
    "close_vs_vwap_pct",

    "daily_direction"
)


# =============================================================================
# 4. BASE WINDOWS
# =============================================================================

ticker_window = (
    Window
    .partitionBy("ticker")
    .orderBy("date")
)


# IMPORTANT:
#
# Ovaj window NE UKLJUCUJE trenutni dan.
#
# Trenutni zapis se poredi sa prethodnih 30 trading dana.
#
# Time izbjegavamo data leakage:
#
# anomalni događaj ne utiče na baseline koji koristimo
# za utvrđivanje da li je taj događaj anomalija.
# =============================================================================

historical_30 = (
    Window
    .partitionBy("ticker")
    .orderBy("date")
    .rowsBetween(-BASELINE_PERIOD, -1)
)


historical_20 = (
    Window
    .partitionBy("ticker")
    .orderBy("date")
    .rowsBetween(-20, -1)
)


historical_14 = (
    Window
    .partitionBy("ticker")
    .orderBy("date")
    .rowsBetween(-14, -1)
)


rolling_60_including_current = (
    Window
    .partitionBy("ticker")
    .orderBy("date")
    .rowsBetween(-59, 0)
)


# =============================================================================
# 5. ADDITIONAL MARKET METRICS
# =============================================================================

# -----------------------------------------------------------------------------
# OPENING GAP
#
# Razlika izmedju danasnjeg open-a i prethodnog close-a.
#
# Npr.
#
# previous close = 100
# open = 105
#
# gap = +5%
# -----------------------------------------------------------------------------

df = df.withColumn(
    "opening_gap_pct",
    F.when(
        (F.col("previous_close").isNotNull())
        & (F.col("previous_close") != 0),

        (
            (
                F.col("open")
                - F.col("previous_close")
            )
            / F.col("previous_close")
        ) * 100
    )
)


# -----------------------------------------------------------------------------
# ABSOLUTE DAILY RETURN
# -----------------------------------------------------------------------------

df = df.withColumn(
    "abs_daily_return_pct",
    F.abs(F.col("daily_return_pct"))
)


# =============================================================================
# 6. MULTI-PERIOD RETURNS
# =============================================================================

df = df.withColumn(
    "_close_7_periods_ago",
    F.lag("close", 7).over(ticker_window)
)

df = df.withColumn(
    "_close_20_periods_ago",
    F.lag("close", 20).over(ticker_window)
)

df = df.withColumn(
    "_close_30_periods_ago",
    F.lag("close", 30).over(ticker_window)
)


df = df.withColumn(
    "return_7d_pct",
    F.when(
        F.col("_close_7_periods_ago") > 0,

        (
            (
                F.col("close")
                / F.col("_close_7_periods_ago")
            ) - 1
        ) * 100
    )
)


df = df.withColumn(
    "return_20d_pct",
    F.when(
        F.col("_close_20_periods_ago") > 0,

        (
            (
                F.col("close")
                / F.col("_close_20_periods_ago")
            ) - 1
        ) * 100
    )
)


df = df.withColumn(
    "return_30d_pct",
    F.when(
        F.col("_close_30_periods_ago") > 0,

        (
            (
                F.col("close")
                / F.col("_close_30_periods_ago")
            ) - 1
        ) * 100
    )
)


# =============================================================================
# 7. 20-PERIOD HISTORICAL MOVING AVERAGE
#
# Iskljucuje trenutni dan.
# =============================================================================

df = df.withColumn(
    "_close_count_20_prior",
    F.count("close").over(historical_20)
)


df = df.withColumn(
    "historical_ma_20",
    F.when(
        F.col("_close_count_20_prior") == 20,
        F.avg("close").over(historical_20)
    )
)


df = df.withColumn(
    "price_vs_historical_ma20_pct",
    F.when(
        F.col("historical_ma_20") > 0,

        (
            (
                F.col("close")
                - F.col("historical_ma_20")
            )
            / F.col("historical_ma_20")
        ) * 100
    )
)


# =============================================================================
# 8. 60-PERIOD ROLLING HIGH AND DRAWDOWN
# =============================================================================

df = df.withColumn(
    "rolling_high_60d",
    F.max("close").over(rolling_60_including_current)
)


df = df.withColumn(
    "drawdown_60d_pct",
    F.when(
        F.col("rolling_high_60d") > 0,

        (
            (
                F.col("close")
                - F.col("rolling_high_60d")
            )
            / F.col("rolling_high_60d")
        ) * 100
    )
)


# =============================================================================
# 9. RSI 14
#
# RSI = 100 - 100 / (1 + RS)
#
# RS = average gain / average loss
#
# Koristimo dnevne price change vrijednosti.
# =============================================================================

df = df.withColumn(
    "_price_change",
    F.col("close") - F.col("previous_close")
)


df = df.withColumn(
    "_gain",
    F.when(
        F.col("_price_change") > 0,
        F.col("_price_change")
    ).otherwise(F.lit(0.0))
)


df = df.withColumn(
    "_loss",
    F.when(
        F.col("_price_change") < 0,
        F.abs(F.col("_price_change"))
    ).otherwise(F.lit(0.0))
)


df = df.withColumn(
    "_rsi_count",
    F.count("previous_close").over(historical_14)
)


df = df.withColumn(
    "_avg_gain_14",
    F.when(
        F.col("_rsi_count") == 14,
        F.avg("_gain").over(historical_14)
    )
)


df = df.withColumn(
    "_avg_loss_14",
    F.when(
        F.col("_rsi_count") == 14,
        F.avg("_loss").over(historical_14)
    )
)


df = df.withColumn(
    "rsi_14",
    F.when(
        F.col("_avg_loss_14") == 0,
        F.lit(100.0)
    )
    .when(
        (
            F.col("_avg_gain_14").isNotNull()
            & F.col("_avg_loss_14").isNotNull()
        ),
        100 - (
            100
            / (
                1
                + (
                    F.col("_avg_gain_14")
                    / F.col("_avg_loss_14")
                )
            )
        )
    )
)


# =============================================================================
# 10. HISTORICAL BASELINE COUNTS
# =============================================================================

df = df.withColumn(
    "_return_baseline_count",
    F.count("daily_return").over(historical_30)
)

df = df.withColumn(
    "_volume_baseline_count",
    F.count("volume").over(historical_30)
)

df = df.withColumn(
    "_range_baseline_count",
    F.count("high_low_range_pct").over(historical_30)
)

df = df.withColumn(
    "_gap_baseline_count",
    F.count("opening_gap_pct").over(historical_30)
)

df = df.withColumn(
    "_transactions_baseline_count",
    F.count("transactions").over(historical_30)
)


# =============================================================================
# 11. RETURN BASELINE
# =============================================================================

df = df.withColumn(
    "return_mean_30d",
    F.when(
        F.col("_return_baseline_count") == BASELINE_PERIOD,
        F.avg("daily_return").over(historical_30)
    )
)


df = df.withColumn(
    "return_stddev_30d",
    F.when(
        F.col("_return_baseline_count") == BASELINE_PERIOD,
        F.stddev_samp("daily_return").over(historical_30)
    )
)


df = df.withColumn(
    "return_zscore",
    F.when(
        F.col("return_stddev_30d") > 0,

        (
            F.col("daily_return")
            - F.col("return_mean_30d")
        )
        / F.col("return_stddev_30d")
    )
)


# =============================================================================
# 12. VOLUME BASELINE
# =============================================================================

df = df.withColumn(
    "historical_volume_avg_30d",
    F.when(
        F.col("_volume_baseline_count") == BASELINE_PERIOD,
        F.avg("volume").over(historical_30)
    )
)


df = df.withColumn(
    "historical_volume_stddev_30d",
    F.when(
        F.col("_volume_baseline_count") == BASELINE_PERIOD,
        F.stddev_samp("volume").over(historical_30)
    )
)


df = df.withColumn(
    "historical_volume_ratio",
    F.when(
        F.col("historical_volume_avg_30d") > 0,

        F.col("volume")
        / F.col("historical_volume_avg_30d")
    )
)


df = df.withColumn(
    "volume_zscore",
    F.when(
        F.col("historical_volume_stddev_30d") > 0,

        (
            F.col("volume")
            - F.col("historical_volume_avg_30d")
        )
        / F.col("historical_volume_stddev_30d")
    )
)


# =============================================================================
# 13. INTRADAY RANGE BASELINE
# =============================================================================

df = df.withColumn(
    "range_mean_30d",
    F.when(
        F.col("_range_baseline_count") == BASELINE_PERIOD,
        F.avg("high_low_range_pct").over(historical_30)
    )
)


df = df.withColumn(
    "range_stddev_30d",
    F.when(
        F.col("_range_baseline_count") == BASELINE_PERIOD,
        F.stddev_samp("high_low_range_pct").over(historical_30)
    )
)


df = df.withColumn(
    "range_zscore",
    F.when(
        F.col("range_stddev_30d") > 0,

        (
            F.col("high_low_range_pct")
            - F.col("range_mean_30d")
        )
        / F.col("range_stddev_30d")
    )
)


# =============================================================================
# 14. OPENING GAP BASELINE
# =============================================================================

df = df.withColumn(
    "gap_mean_30d",
    F.when(
        F.col("_gap_baseline_count") == BASELINE_PERIOD,
        F.avg("opening_gap_pct").over(historical_30)
    )
)


df = df.withColumn(
    "gap_stddev_30d",
    F.when(
        F.col("_gap_baseline_count") == BASELINE_PERIOD,
        F.stddev_samp("opening_gap_pct").over(historical_30)
    )
)


df = df.withColumn(
    "gap_zscore",
    F.when(
        F.col("gap_stddev_30d") > 0,

        (
            F.col("opening_gap_pct")
            - F.col("gap_mean_30d")
        )
        / F.col("gap_stddev_30d")
    )
)


# =============================================================================
# 15. TRANSACTIONS BASELINE
# =============================================================================

df = df.withColumn(
    "transactions_mean_30d",
    F.when(
        F.col("_transactions_baseline_count") == BASELINE_PERIOD,
        F.avg("transactions").over(historical_30)
    )
)


df = df.withColumn(
    "transactions_stddev_30d",
    F.when(
        F.col("_transactions_baseline_count") == BASELINE_PERIOD,
        F.stddev_samp("transactions").over(historical_30)
    )
)


df = df.withColumn(
    "transactions_zscore",
    F.when(
        F.col("transactions_stddev_30d") > 0,

        (
            F.col("transactions")
            - F.col("transactions_mean_30d")
        )
        / F.col("transactions_stddev_30d")
    )
)


# =============================================================================
# 16. VOLATILITY REGIME
#
# Poredimo trenutnu 30d volatilnost sa prosjekom prethodnih
# vrijednosti 30d volatilnosti.
# =============================================================================

df = df.withColumn(
    "_volatility_history_count",
    F.count("volatility_30d").over(historical_30)
)


df = df.withColumn(
    "historical_volatility_avg_30d",
    F.when(
        F.col("_volatility_history_count") == BASELINE_PERIOD,
        F.avg("volatility_30d").over(historical_30)
    )
)


df = df.withColumn(
    "volatility_ratio",
    F.when(
        F.col("historical_volatility_avg_30d") > 0,

        F.col("volatility_30d")
        / F.col("historical_volatility_avg_30d")
    )
)


# =============================================================================
# 17. INDIVIDUAL ANOMALY FLAGS
# =============================================================================

df = df.withColumn(
    "price_return_anomaly",
    F.when(
        F.abs(F.col("return_zscore")) >= RETURN_Z_THRESHOLD,
        F.lit(True)
    ).otherwise(F.lit(False))
)


df = df.withColumn(
    "volume_anomaly",
    F.when(
        (
            F.col("volume_zscore") >= VOLUME_Z_THRESHOLD
        )
        |
        (
            F.col("historical_volume_ratio")
            >= VOLUME_RATIO_THRESHOLD
        ),
        F.lit(True)
    ).otherwise(F.lit(False))
)


df = df.withColumn(
    "range_anomaly",
    F.when(
        F.col("range_zscore") >= RANGE_Z_THRESHOLD,
        F.lit(True)
    ).otherwise(F.lit(False))
)


df = df.withColumn(
    "gap_anomaly",
    F.when(
        F.abs(F.col("gap_zscore")) >= GAP_Z_THRESHOLD,
        F.lit(True)
    ).otherwise(F.lit(False))
)


df = df.withColumn(
    "transactions_anomaly",
    F.when(
        F.col("transactions_zscore")
        >= TRANSACTIONS_Z_THRESHOLD,

        F.lit(True)
    ).otherwise(F.lit(False))
)


df = df.withColumn(
    "volatility_anomaly",
    F.when(
        F.col("volatility_ratio")
        >= VOLATILITY_RATIO_THRESHOLD,

        F.lit(True)
    ).otherwise(F.lit(False))
)


# =============================================================================
# 18. COMPONENT SCORES
#
# Cilj nije da jedna ekstremna vrijednost proizvede beskonacan score.
#
# Svaki component se ogranicava na svoj maksimalni broj bodova.
#
# RETURN       -> max 30
# VOLUME       -> max 20
# RANGE        -> max 15
# GAP          -> max 15
# TRANSACTIONS -> max 10
# VOLATILITY   -> max 10
#
# UKUPNO       -> max 100
# =============================================================================

df = df.withColumn(
    "_return_score",
    F.when(
        F.col("return_zscore").isNotNull(),

        F.least(
            F.abs(F.col("return_zscore")) / F.lit(4.0),
            F.lit(1.0)
        ) * 30
    ).otherwise(F.lit(0.0))
)


df = df.withColumn(
    "_volume_score",
    F.when(
        F.col("volume_zscore").isNotNull(),

        F.least(
            F.abs(F.col("volume_zscore")) / F.lit(4.0),
            F.lit(1.0)
        ) * 20
    ).otherwise(F.lit(0.0))
)


df = df.withColumn(
    "_range_score",
    F.when(
        F.col("range_zscore").isNotNull(),

        F.least(
            F.abs(F.col("range_zscore")) / F.lit(4.0),
            F.lit(1.0)
        ) * 15
    ).otherwise(F.lit(0.0))
)


df = df.withColumn(
    "_gap_score",
    F.when(
        F.col("gap_zscore").isNotNull(),

        F.least(
            F.abs(F.col("gap_zscore")) / F.lit(4.0),
            F.lit(1.0)
        ) * 15
    ).otherwise(F.lit(0.0))
)


df = df.withColumn(
    "_transactions_score",
    F.when(
        F.col("transactions_zscore").isNotNull(),

        F.least(
            F.abs(F.col("transactions_zscore")) / F.lit(4.0),
            F.lit(1.0)
        ) * 10
    ).otherwise(F.lit(0.0))
)


df = df.withColumn(
    "_volatility_score",
    F.when(
        F.col("volatility_ratio").isNotNull(),

        F.least(
            F.greatest(
                (
                    F.col("volatility_ratio")
                    - F.lit(1.0)
                )
                / F.lit(1.0),

                F.lit(0.0)
            ),
            F.lit(1.0)
        ) * 10
    ).otherwise(F.lit(0.0))
)


# =============================================================================
# 19. COMPOSITE ANOMALY SCORE
# =============================================================================

df = df.withColumn(
    "anomaly_score",

    F.col("_return_score")
    + F.col("_volume_score")
    + F.col("_range_score")
    + F.col("_gap_score")
    + F.col("_transactions_score")
    + F.col("_volatility_score")
)


# =============================================================================
# 20. NUMBER OF TRIGGERED ANOMALY SIGNALS
# =============================================================================

df = df.withColumn(
    "anomaly_signal_count",

    F.col("price_return_anomaly").cast("int")
    + F.col("volume_anomaly").cast("int")
    + F.col("range_anomaly").cast("int")
    + F.col("gap_anomaly").cast("int")
    + F.col("transactions_anomaly").cast("int")
    + F.col("volatility_anomaly").cast("int")
)


# =============================================================================
# 21. FINAL ANOMALY FLAG
#
# Dva nacina da događaj postane anomaly:
#
# 1. composite score >= 50
#
# ILI
#
# 2. najmanje dva nezavisna anomaly signala.
#
# Ovo omogucava da npr.:
#
# velik price move + ekstremni volume
#
# bude prepoznat cak i ako score ostane malo ispod thresholda.
# =============================================================================

df = df.withColumn(
    "is_anomaly",
    F.when(
        (
            F.col("anomaly_score")
            >= ANOMALY_SCORE_THRESHOLD
        )
        |
        (
            F.col("anomaly_signal_count") >= 2
        ),

        F.lit(True)
    ).otherwise(F.lit(False))
)


# =============================================================================
# 22. ANOMALY SEVERITY
# =============================================================================

df = df.withColumn(
    "anomaly_severity",

    F.when(
        F.col("anomaly_score") >= 80,
        F.lit("CRITICAL")
    )
    .when(
        F.col("anomaly_score") >= 65,
        F.lit("HIGH")
    )
    .when(
        F.col("anomaly_score") >= 50,
        F.lit("MEDIUM")
    )
    .when(
        F.col("is_anomaly"),
        F.lit("LOW")
    )
    .otherwise(
        F.lit("NORMAL")
    )
)


# =============================================================================
# 23. ANOMALY TYPE
#
# Kreiramo citljiv opis razloga.
# =============================================================================

df = df.withColumn(
    "anomaly_type",

    F.concat_ws(
        ", ",

        F.when(
            F.col("price_return_anomaly"),
            F.lit("PRICE_RETURN")
        ),

        F.when(
            F.col("volume_anomaly"),
            F.lit("VOLUME")
        ),

        F.when(
            F.col("range_anomaly"),
            F.lit("INTRADAY_RANGE")
        ),

        F.when(
            F.col("gap_anomaly"),
            F.lit("OPENING_GAP")
        ),

        F.when(
            F.col("transactions_anomaly"),
            F.lit("TRANSACTIONS")
        ),

        F.when(
            F.col("volatility_anomaly"),
            F.lit("VOLATILITY")
        )
    )
)


df = df.withColumn(
    "anomaly_type",

    F.when(
        (F.col("anomaly_type") == "")
        & F.col("is_anomaly"),

        F.lit("COMPOSITE_MULTI_FACTOR")
    )
    .when(
        F.col("anomaly_type") == "",
        F.lit("NONE")
    )
    .otherwise(
        F.col("anomaly_type")
    )
)


# =============================================================================
# 24. MARKET CONDITION
#
# Jednostavna interpretacija za dashboard / AI agent.
# =============================================================================

df = df.withColumn(
    "market_condition",

    F.when(
        F.col("is_anomaly") & (F.col("daily_return") > 0),
        F.lit("ABNORMAL_UP_MOVE")
    )
    .when(
        F.col("is_anomaly") & (F.col("daily_return") < 0),
        F.lit("ABNORMAL_DOWN_MOVE")
    )
    .when(
        F.col("rsi_14") >= 70,
        F.lit("OVERBOUGHT")
    )
    .when(
        F.col("rsi_14") <= 30,
        F.lit("OVERSOLD")
    )
    .when(
        F.col("daily_return") > 0,
        F.lit("NORMAL_UP")
    )
    .when(
        F.col("daily_return") < 0,
        F.lit("NORMAL_DOWN")
    )
    .otherwise(
        F.lit("NEUTRAL")
    )
)


# =============================================================================
# 25. ROUND ANALYTICAL VALUES
# =============================================================================

round_columns = {
    "opening_gap_pct": 4,
    "abs_daily_return_pct": 4,

    "return_7d_pct": 4,
    "return_20d_pct": 4,
    "return_30d_pct": 4,

    "historical_ma_20": 4,
    "price_vs_historical_ma20_pct": 4,

    "rolling_high_60d": 4,
    "drawdown_60d_pct": 4,

    "rsi_14": 4,

    "return_mean_30d": 8,
    "return_stddev_30d": 8,
    "return_zscore": 4,

    "historical_volume_avg_30d": 2,
    "historical_volume_stddev_30d": 2,
    "historical_volume_ratio": 4,
    "volume_zscore": 4,

    "range_mean_30d": 4,
    "range_stddev_30d": 4,
    "range_zscore": 4,

    "gap_mean_30d": 4,
    "gap_stddev_30d": 4,
    "gap_zscore": 4,

    "transactions_mean_30d": 2,
    "transactions_stddev_30d": 2,
    "transactions_zscore": 4,

    "historical_volatility_avg_30d": 8,
    "volatility_ratio": 4,

    "anomaly_score": 2
}


for column_name, decimals in round_columns.items():
    df = df.withColumn(
        column_name,
        F.round(
            F.col(column_name),
            decimals
        )
    )


# =============================================================================
# 26. PROCESSING TIMESTAMP
# =============================================================================

df = df.withColumn(
    "_anomaly_processed_at",
    F.current_timestamp()
)


# =============================================================================
# 27. REMOVE TEMPORARY COLUMNS
# =============================================================================

df = df.drop(
    "_close_7_periods_ago",
    "_close_20_periods_ago",
    "_close_30_periods_ago",

    "_close_count_20_prior",

    "_price_change",
    "_gain",
    "_loss",
    "_rsi_count",
    "_avg_gain_14",
    "_avg_loss_14",

    "_return_baseline_count",
    "_volume_baseline_count",
    "_range_baseline_count",
    "_gap_baseline_count",
    "_transactions_baseline_count",
    "_volatility_history_count",

    "_return_score",
    "_volume_score",
    "_range_score",
    "_gap_score",
    "_transactions_score",
    "_volatility_score"
)


# =============================================================================
# 28. FINAL COLUMN ORDER
# =============================================================================

df = df.select(
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
    "daily_return",
    "daily_return_pct",
    "daily_direction",

    "opening_gap_pct",
    "abs_daily_return_pct",

    "return_7d_pct",
    "return_20d_pct",
    "return_30d_pct",

    "moving_avg_7d",
    "moving_avg_30d",
    "historical_ma_20",
    "price_vs_historical_ma20_pct",

    "rolling_high_60d",
    "drawdown_60d_pct",

    "rsi_14",

    "volume_avg_30d",
    "volume_ratio",

    "volatility_30d",
    "annualized_volatility_30d_pct",

    "high_low_range_pct",
    "close_vs_vwap_pct",

    "return_mean_30d",
    "return_stddev_30d",
    "return_zscore",

    "historical_volume_avg_30d",
    "historical_volume_stddev_30d",
    "historical_volume_ratio",
    "volume_zscore",

    "range_mean_30d",
    "range_stddev_30d",
    "range_zscore",

    "gap_mean_30d",
    "gap_stddev_30d",
    "gap_zscore",

    "transactions_mean_30d",
    "transactions_stddev_30d",
    "transactions_zscore",

    "historical_volatility_avg_30d",
    "volatility_ratio",

    "price_return_anomaly",
    "volume_anomaly",
    "range_anomaly",
    "gap_anomaly",
    "transactions_anomaly",
    "volatility_anomaly",

    "anomaly_signal_count",
    "anomaly_score",
    "is_anomaly",
    "anomaly_severity",
    "anomaly_type",

    "market_condition",

    "_anomaly_processed_at"
)


# =============================================================================
# 29. WRITE DELTA TABLE
#
# Full refresh ima smisla jer svi rolling baseline-i zavise
# od istorijskih zapisa.
# =============================================================================

(
    df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)


print()
print(f"[OK] Anomaly tabela kreirana: {TARGET_TABLE}")


# =============================================================================
# 30. BASIC VALIDATION
# =============================================================================

result_df = spark.table(TARGET_TABLE)

result_count = result_df.count()

anomaly_count = (
    result_df
    .filter(F.col("is_anomaly"))
    .count()
)


print()
print("=" * 100)
print("ANOMALY TABLE SUMMARY")
print("=" * 100)

print(f"Source redova:  {source_count}")
print(f"Target redova:  {result_count}")
print(f"Broj anomalija: {anomaly_count}")


if result_count > 0:

    anomaly_pct = (
        anomaly_count
        / result_count
    ) * 100

    print(
        f"Udio anomalija: {anomaly_pct:.2f}%"
    )


# =============================================================================
# 31. ANOMALIES BY TICKER
# =============================================================================

print()
print("ANOMALIJE PO TICKERU:")

(
    result_df
    .groupBy("ticker")
    .agg(
        F.count("*").alias("total_rows"),

        F.sum(
            F.col("is_anomaly").cast("int")
        ).alias("anomaly_count"),

        F.round(
            F.avg("anomaly_score"),
            2
        ).alias("avg_anomaly_score"),

        F.round(
            F.max("anomaly_score"),
            2
        ).alias("max_anomaly_score")
    )
    .orderBy("ticker")
    .show(
        n=100,
        truncate=False
    )
)


# =============================================================================
# 32. ANOMALIES BY SEVERITY
# =============================================================================

print()
print("ANOMALIJE PO SEVERITY NIVOU:")

(
    result_df
    .groupBy("anomaly_severity")
    .count()
    .orderBy(
        F.col("count").desc()
    )
    .show(
        truncate=False
    )
)


# =============================================================================
# 33. TOP MARKET ANOMALIES
# =============================================================================

print()
print("TOP 30 DETEKTOVANIH ANOMALIJA:")

(
    result_df
    .filter(
        F.col("is_anomaly")
    )
    .select(
        "ticker",
        "date",
        "close",
        "daily_return_pct",

        "historical_volume_ratio",

        "return_zscore",
        "volume_zscore",
        "range_zscore",
        "gap_zscore",
        "transactions_zscore",

        "anomaly_signal_count",
        "anomaly_score",
        "anomaly_severity",
        "anomaly_type",
        "market_condition"
    )
    .orderBy(
        F.col("anomaly_score").desc(),
        F.abs(F.col("daily_return_pct")).desc()
    )
    .show(
        n=30,
        truncate=False
    )
)


# =============================================================================
# 34. LATEST ANOMALIES
# =============================================================================

print()
print("NAJNOVIJE DETEKTOVANE ANOMALIJE:")

(
    result_df
    .filter(
        F.col("is_anomaly")
    )
    .select(
        "ticker",
        "date",
        "daily_return_pct",
        "historical_volume_ratio",
        "rsi_14",
        "drawdown_60d_pct",
        "anomaly_score",
        "anomaly_severity",
        "anomaly_type"
    )
    .orderBy(
        F.col("date").desc(),
        F.col("anomaly_score").desc()
    )
    .show(
        n=30,
        truncate=False
    )
)


print()
print("=" * 100)
print("GOLD STOCK ANOMALY DETECTION COMPLETED")
print("=" * 100)