from databricks.connect import DatabricksSession

from pyspark.sql import functions as F
from pyspark.sql.window import Window


# =============================================================================
# CONFIGURATION
# =============================================================================

DAILY_METRICS_TABLE = "workspace.gold.stock_daily_metrics"
TECHNICAL_TABLE = "workspace.gold.technical_indicators"
COMPANY_DETAILS_TABLE = "workspace.silver.company_details"

GOLD_SCHEMA = "workspace.gold"
TARGET_TABLE = "workspace.gold.company_performance"


# =============================================================================
# SPARK SESSION
# =============================================================================

spark = DatabricksSession.builder.getOrCreate()


print("=" * 90)
print("GOLD LAYER - COMPANY PERFORMANCE")
print("=" * 90)

print(f"Daily metrics:   {DAILY_METRICS_TABLE}")
print(f"Technical data:  {TECHNICAL_TABLE}")
print(f"Company details: {COMPANY_DETAILS_TABLE}")
print(f"Target:          {TARGET_TABLE}")


# =============================================================================
# 1. CREATE GOLD SCHEMA
# =============================================================================

spark.sql(
    f"""
    CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA}
    """
)

print()
print(f"[OK] Gold schema spremna: {GOLD_SCHEMA}")


# =============================================================================
# 2. LOAD SOURCE TABLES
# =============================================================================

daily_df = spark.table(
    DAILY_METRICS_TABLE
)

technical_df = spark.table(
    TECHNICAL_TABLE
)

company_df = spark.table(
    COMPANY_DETAILS_TABLE
)


daily_count = daily_df.count()
technical_count = technical_df.count()
company_count = company_df.count()


print()
print("SOURCE TABLE COUNTS")
print("-" * 90)

print(f"Daily metrics redova:   {daily_count}")
print(f"Technical redova:       {technical_count}")
print(f"Company details redova: {company_count}")


if daily_count == 0:
    raise ValueError(
        f"Tabela {DAILY_METRICS_TABLE} je prazna."
    )


if technical_count == 0:
    raise ValueError(
        f"Tabela {TECHNICAL_TABLE} je prazna."
    )


if company_count == 0:
    raise ValueError(
        f"Tabela {COMPANY_DETAILS_TABLE} je prazna."
    )


# =============================================================================
# 3. NORMALIZE COMPANY DETAILS
#
# Silver company_details moze imati naziv kompanije kao:
#
# company_name
# ili
# name
#
# Kod automatski pronalazi odgovarajucu kolonu.
# =============================================================================

company_columns = set(company_df.columns)


if "ticker" not in company_columns:

    raise ValueError(
        f"Tabela {COMPANY_DETAILS_TABLE} nema ticker kolonu."
    )


if "company_name" in company_columns:

    company_name_column = "company_name"

elif "name" in company_columns:

    company_name_column = "name"

else:

    raise ValueError(
        "Nije pronadjena kolona company_name ili name "
        "u Silver company_details tabeli."
    )


print()
print(
    f"[OK] Kolona za naziv kompanije: "
    f"{company_name_column}"
)


company_base_df = (
    company_df
    .select(
        F.upper(
            F.trim(
                F.col("ticker")
            )
        ).alias("ticker"),

        F.col(
            company_name_column
        ).cast("string").alias(
            "company_name"
        )
    )
    .dropDuplicates(
        ["ticker"]
    )
)


# =============================================================================
# 4. PRICE WINDOWS
#
# lag(7)   -> cijena prije 7 trading observations
# lag(30)  -> prije 30 trading observations
# lag(90)  -> prije 90 trading observations
# lag(252) -> priblizno prije jedne trading godine
# =============================================================================

ticker_order_window = (
    Window
    .partitionBy("ticker")
    .orderBy("date")
)


# Posljednjih 252 trading zapisa ukljucujuci trenutni.
#
# Koristi se za 52-week high / low.
# =============================================================================

window_52w = (
    Window
    .partitionBy("ticker")
    .orderBy("date")
    .rowsBetween(-251, 0)
)


# =============================================================================
# 5. HISTORICAL PRICE METRICS
# =============================================================================

price_metrics_df = (
    daily_df
    .select(
        "ticker",
        "date",
        "close",
        "volume_ratio",
        "annualized_volatility_30d_pct"
    )
    .withColumn(
        "close_7d_ago",
        F.lag(
            "close",
            7
        ).over(
            ticker_order_window
        )
    )
    .withColumn(
        "close_30d_ago",
        F.lag(
            "close",
            30
        ).over(
            ticker_order_window
        )
    )
    .withColumn(
        "close_90d_ago",
        F.lag(
            "close",
            90
        ).over(
            ticker_order_window
        )
    )
    .withColumn(
        "close_1y_ago",
        F.lag(
            "close",
            252
        ).over(
            ticker_order_window
        )
    )
    .withColumn(
        "high_52w",
        F.max(
            "close"
        ).over(
            window_52w
        )
    )
    .withColumn(
        "low_52w",
        F.min(
            "close"
        ).over(
            window_52w
        )
    )
)


# =============================================================================
# 6. CALCULATE RETURNS
# =============================================================================

price_metrics_df = (
    price_metrics_df

    .withColumn(
        "return_7d_pct",

        F.when(
            F.col(
                "close_7d_ago"
            ) > 0,

            (
                (
                    F.col("close")
                    /
                    F.col("close_7d_ago")
                )
                - 1
            ) * 100
        )
    )

    .withColumn(
        "return_30d_pct",

        F.when(
            F.col(
                "close_30d_ago"
            ) > 0,

            (
                (
                    F.col("close")
                    /
                    F.col("close_30d_ago")
                )
                - 1
            ) * 100
        )
    )

    .withColumn(
        "return_90d_pct",

        F.when(
            F.col(
                "close_90d_ago"
            ) > 0,

            (
                (
                    F.col("close")
                    /
                    F.col("close_90d_ago")
                )
                - 1
            ) * 100
        )
    )

    .withColumn(
        "return_1y_pct",

        F.when(
            F.col(
                "close_1y_ago"
            ) > 0,

            (
                (
                    F.col("close")
                    /
                    F.col("close_1y_ago")
                )
                - 1
            ) * 100
        )
    )
)


# =============================================================================
# 7. FIRST AVAILABLE PRICE FOR EACH TICKER
#
# Potreban za total-period return.
# =============================================================================

first_price_window = (
    Window
    .partitionBy("ticker")
    .orderBy(
        F.col("date").asc()
    )
)


first_price_df = (
    daily_df
    .select(
        "ticker",
        "date",
        "close"
    )
    .withColumn(
        "_rn",
        F.row_number().over(
            first_price_window
        )
    )
    .filter(
        F.col("_rn") == 1
    )
    .select(
        "ticker",

        F.col(
            "date"
        ).alias(
            "first_date"
        ),

        F.col(
            "close"
        ).alias(
            "first_close"
        )
    )
)


# =============================================================================
# 8. GET LATEST MARKET ROW FOR EACH TICKER
# =============================================================================

latest_price_window = (
    Window
    .partitionBy("ticker")
    .orderBy(
        F.col("date").desc()
    )
)


latest_market_df = (
    price_metrics_df
    .withColumn(
        "_rn",

        F.row_number().over(
            latest_price_window
        )
    )
    .filter(
        F.col("_rn") == 1
    )
    .drop("_rn")
)


# =============================================================================
# 9. TOTAL PERIOD RETURN
# =============================================================================

latest_market_df = (
    latest_market_df
    .join(
        first_price_df,
        on="ticker",
        how="left"
    )
    .withColumn(
        "total_period_return_pct",

        F.when(
            F.col("first_close") > 0,

            (
                (
                    F.col("close")
                    /
                    F.col("first_close")
                )
                - 1
            ) * 100
        )
    )
)


# =============================================================================
# 10. DISTANCE FROM 52-WEEK HIGH / LOW
#
# distance_from_52w_high_pct:
#
# 0%      -> trenutno na 52w high
# -10%    -> 10% ispod 52w high
#
#
# distance_from_52w_low_pct:
#
# 0%      -> trenutno na 52w low
# +50%    -> 50% iznad 52w low
# =============================================================================

latest_market_df = (
    latest_market_df

    .withColumn(
        "distance_from_52w_high_pct",

        F.when(
            F.col("high_52w") > 0,

            (
                (
                    F.col("close")
                    /
                    F.col("high_52w")
                )
                - 1
            ) * 100
        )
    )

    .withColumn(
        "distance_from_52w_low_pct",

        F.when(
            F.col("low_52w") > 0,

            (
                (
                    F.col("close")
                    /
                    F.col("low_52w")
                )
                - 1
            ) * 100
        )
    )
)


# =============================================================================
# 11. GET LATEST TECHNICAL INDICATORS
# =============================================================================

latest_technical_window = (
    Window
    .partitionBy("ticker")
    .orderBy(
        F.col("date").desc()
    )
)


latest_technical_df = (
    technical_df
    .withColumn(
        "_rn",

        F.row_number().over(
            latest_technical_window
        )
    )
    .filter(
        F.col("_rn") == 1
    )
    .select(
        "ticker",

        F.col(
            "date"
        ).alias(
            "technical_date"
        ),

        "sma_20",
        "sma_50",

        "ema_12",
        "ema_26",

        "rsi_14",

        "macd",
        "macd_signal",
        "macd_histogram",

        "bollinger_upper",
        "bollinger_middle",
        "bollinger_lower",
        "bollinger_bandwidth_pct",
        "bollinger_position",

        "momentum_10d_pct",

        "price_vs_sma20_pct",
        "price_vs_sma50_pct",

        "trend_signal",
        "rsi_signal",
        "macd_state"
    )
)


# =============================================================================
# 12. JOIN MARKET + TECHNICAL + COMPANY DETAILS
# =============================================================================

performance_df = (
    latest_market_df

    .join(
        latest_technical_df,
        on="ticker",
        how="left"
    )

    .join(
        company_base_df,
        on="ticker",
        how="left"
    )
)


# =============================================================================
# 13. VALIDATE DATE CONSISTENCY
#
# Najnoviji market i technical datum treba da budu isti.
# =============================================================================

performance_df = performance_df.withColumn(
    "date_consistent",

    F.when(
        F.col("date")
        ==
        F.col("technical_date"),

        F.lit(True)
    )
    .otherwise(
        F.lit(False)
    )
)


# =============================================================================
# 14. CROSS-SECTIONAL PERFORMANCE COMPONENTS
#
# Zelimo interpretabilan score 0-100.
#
# Kompanije rangiramo jedna u odnosu na drugu.
#
# Za pozitivne metrike veca vrijednost je bolja.
#
# Za volatility je manja vrijednost bolja.
# =============================================================================

global_window = Window.orderBy(
    F.lit(1)
)


return_30_rank_window = Window.orderBy(
    F.col(
        "return_30d_pct"
    ).asc()
)


return_90_rank_window = Window.orderBy(
    F.col(
        "return_90d_pct"
    ).asc()
)


return_1y_rank_window = Window.orderBy(
    F.col(
        "return_1y_pct"
    ).asc()
)


momentum_rank_window = Window.orderBy(
    F.col(
        "momentum_10d_pct"
    ).asc()
)


volatility_rank_window = Window.orderBy(
    F.col(
        "annualized_volatility_30d_pct"
    ).desc()
)


performance_df = (
    performance_df

    .withColumn(
        "_return_30_score",

        F.percent_rank().over(
            return_30_rank_window
        ) * 100
    )

    .withColumn(
        "_return_90_score",

        F.percent_rank().over(
            return_90_rank_window
        ) * 100
    )

    .withColumn(
        "_return_1y_score",

        F.percent_rank().over(
            return_1y_rank_window
        ) * 100
    )

    .withColumn(
        "_momentum_score",

        F.percent_rank().over(
            momentum_rank_window
        ) * 100
    )

    .withColumn(
        "_volatility_score",

        F.percent_rank().over(
            volatility_rank_window
        ) * 100
    )
)


# =============================================================================
# 15. TECHNICAL SCORE
#
# Trend:
# BULLISH = 100
# NEUTRAL = 50
# BEARISH = 0
#
# MACD:
# BULLISH = 100
# NEUTRAL = 50
# BEARISH = 0
#
# RSI nije direktno tretiran kao:
#
# "veca vrijednost = bolja"
#
# jer je RSI >= 70 potencijalno overbought.
#
# Neutral RSI dobija najveci score.
# =============================================================================

performance_df = (
    performance_df

    .withColumn(
        "_trend_score",

        F.when(
            F.col("trend_signal") == "BULLISH",
            100.0
        )
        .when(
            F.col("trend_signal") == "BEARISH",
            0.0
        )
        .otherwise(
            50.0
        )
    )

    .withColumn(
        "_macd_score",

        F.when(
            F.col("macd_state") == "BULLISH",
            100.0
        )
        .when(
            F.col("macd_state") == "BEARISH",
            0.0
        )
        .otherwise(
            50.0
        )
    )

    .withColumn(
        "_rsi_score",

        F.when(
            F.col("rsi_signal") == "NEUTRAL",
            100.0
        )
        .when(
            F.col("rsi_signal") == "OVERSOLD",
            60.0
        )
        .when(
            F.col("rsi_signal") == "OVERBOUGHT",
            40.0
        )
        .otherwise(
            50.0
        )
    )
)


# =============================================================================
# 16. COMPOSITE PERFORMANCE SCORE
#
# Score nije investment recommendation.
#
# On predstavlja relativni analiticki ranking kompanija
# na osnovu price performance + momentum + volatility + technical state.
#
# Tezine:
#
# 30-day return       20%
# 90-day return       20%
# 1-year return       20%
# 10-day momentum     10%
# volatility          10%
# trend                8%
# MACD                 7%
# RSI                  5%
#
# TOTAL              100%
# =============================================================================

performance_df = performance_df.withColumn(
    "performance_score",

    (
        F.col("_return_30_score") * 0.20
        +
        F.col("_return_90_score") * 0.20
        +
        F.col("_return_1y_score") * 0.20
        +
        F.col("_momentum_score") * 0.10
        +
        F.col("_volatility_score") * 0.10
        +
        F.col("_trend_score") * 0.08
        +
        F.col("_macd_score") * 0.07
        +
        F.col("_rsi_score") * 0.05
    )
)


# =============================================================================
# 17. PERFORMANCE RANK
#
# Rank 1 = najveci performance score.
# =============================================================================

performance_rank_window = (
    Window
    .orderBy(
        F.col(
            "performance_score"
        ).desc()
    )
)


performance_df = performance_df.withColumn(
    "performance_rank",

    F.row_number().over(
        performance_rank_window
    )
)


# =============================================================================
# 18. PERFORMANCE CATEGORY
# =============================================================================

performance_df = performance_df.withColumn(
    "performance_category",

    F.when(
        F.col("performance_score") >= 75,
        "STRONG"
    )
    .when(
        F.col("performance_score") >= 55,
        "POSITIVE"
    )
    .when(
        F.col("performance_score") >= 40,
        "NEUTRAL"
    )
    .when(
        F.col("performance_score") >= 25,
        "WEAK"
    )
    .otherwise(
        "VERY_WEAK"
    )
)


# =============================================================================
# 19. ROUND NUMERIC VALUES
# =============================================================================

round_columns = {

    "current_price": 4,

    "return_7d_pct": 4,
    "return_30d_pct": 4,
    "return_90d_pct": 4,
    "return_1y_pct": 4,
    "total_period_return_pct": 4,

    "high_52w": 4,
    "low_52w": 4,

    "distance_from_52w_high_pct": 4,
    "distance_from_52w_low_pct": 4,

    "volume_ratio": 4,
    "annualized_volatility_30d_pct": 4,

    "sma_20": 4,
    "sma_50": 4,

    "ema_12": 4,
    "ema_26": 4,

    "rsi_14": 4,

    "macd": 6,
    "macd_signal": 6,
    "macd_histogram": 6,

    "bollinger_upper": 4,
    "bollinger_middle": 4,
    "bollinger_lower": 4,

    "bollinger_bandwidth_pct": 4,
    "bollinger_position": 4,

    "momentum_10d_pct": 4,

    "price_vs_sma20_pct": 4,
    "price_vs_sma50_pct": 4,

    "performance_score": 2
}


# Rename close before rounding.

performance_df = performance_df.withColumnRenamed(
    "close",
    "current_price"
)


for column_name, decimals in round_columns.items():

    performance_df = performance_df.withColumn(
        column_name,

        F.round(
            F.col(column_name),
            decimals
        )
    )


# =============================================================================
# 20. REMOVE TEMPORARY SCORE COLUMNS
# =============================================================================

performance_df = performance_df.drop(
    "_return_30_score",
    "_return_90_score",
    "_return_1y_score",
    "_momentum_score",
    "_volatility_score",
    "_trend_score",
    "_macd_score",
    "_rsi_score"
)


# =============================================================================
# 21. PROCESSING TIMESTAMP
# =============================================================================

performance_df = performance_df.withColumn(
    "_gold_processed_at",
    F.current_timestamp()
)


# =============================================================================
# 22. FINAL COLUMN ORDER
# =============================================================================

performance_df = performance_df.select(

    "ticker",
    "company_name",

    F.col(
        "date"
    ).alias(
        "latest_date"
    ),

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

    "sma_20",
    "sma_50",

    "ema_12",
    "ema_26",

    "rsi_14",

    "macd",
    "macd_signal",
    "macd_histogram",

    "bollinger_upper",
    "bollinger_middle",
    "bollinger_lower",

    "bollinger_bandwidth_pct",
    "bollinger_position",

    "momentum_10d_pct",

    "price_vs_sma20_pct",
    "price_vs_sma50_pct",

    "trend_signal",
    "rsi_signal",
    "macd_state",

    "performance_score",
    "performance_rank",
    "performance_category",

    "date_consistent",

    "first_date",
    "first_close",

    "_gold_processed_at"
)


# =============================================================================
# 23. WRITE GOLD DELTA TABLE
#
# Ova tabela ima samo jedan aktuelni snapshot po tickeru.
# Full refresh je jednostavniji i sigurniji.
# =============================================================================

(
    performance_df.write
    .format("delta")
    .mode("overwrite")
    .option(
        "overwriteSchema",
        "true"
    )
    .saveAsTable(
        TARGET_TABLE
    )
)


print()
print(
    f"[OK] Gold tabela kreirana: "
    f"{TARGET_TABLE}"
)


# =============================================================================
# 24. LOAD RESULT
# =============================================================================

result_df = spark.table(
    TARGET_TABLE
)


result_count = result_df.count()


# =============================================================================
# 25. SUMMARY
# =============================================================================

print()
print("=" * 90)
print("COMPANY PERFORMANCE SUMMARY")
print("=" * 90)

print(
    f"Broj kompanija u Gold tabeli: "
    f"{result_count}"
)


# =============================================================================
# 26. DISPLAY COMPANY PERFORMANCE
# =============================================================================

print()
print("COMPANY PERFORMANCE RANKING:")


(
    result_df
    .select(
        "performance_rank",
        "ticker",
        "company_name",
        "latest_date",
        "current_price",

        "return_30d_pct",
        "return_90d_pct",
        "return_1y_pct",

        "annualized_volatility_30d_pct",
        "rsi_14",

        "trend_signal",
        "macd_state",

        "performance_score",
        "performance_category"
    )
    .orderBy(
        "performance_rank"
    )
    .show(
        n=100,
        truncate=False
    )
)


# =============================================================================
# 27. VALIDATION - EXPECTED COMPANY COUNT
# =============================================================================

expected_company_count = (
    daily_df
    .select("ticker")
    .distinct()
    .count()
)


if result_count == expected_company_count:

    print(
        f"[PASS] Company count je ispravan: "
        f"{result_count}"
    )

else:

    raise ValueError(
        f"Gold company_performance ima "
        f"{result_count} redova, a ocekuje se "
        f"{expected_company_count}."
    )


# =============================================================================
# 28. VALIDATION - DUPLICATE TICKERS
# =============================================================================

duplicate_tickers = (
    result_df
    .groupBy("ticker")
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)


if duplicate_tickers == 0:

    print(
        "[PASS] Nema duplikata po tickeru"
    )

else:

    raise ValueError(
        f"Pronadjeno {duplicate_tickers} "
        f"duplih tickera."
    )


# =============================================================================
# 29. VALIDATION - COMPANY NAMES
# =============================================================================

missing_company_names = (
    result_df
    .filter(
        F.col(
            "company_name"
        ).isNull()
    )
    .count()
)


if missing_company_names == 0:

    print(
        "[PASS] Sve kompanije imaju naziv"
    )

else:

    raise ValueError(
        f"{missing_company_names} kompanija "
        f"nema company_name."
    )


# =============================================================================
# 30. VALIDATION - DATES
# =============================================================================

invalid_dates = (
    result_df
    .filter(
        F.col(
            "date_consistent"
        ) != True
    )
    .count()
)


if invalid_dates == 0:

    print(
        "[PASS] Market i technical datumi "
        "su konzistentni"
    )

else:

    raise ValueError(
        f"Pronadjeno {invalid_dates} redova "
        f"sa nekonzistentnim datumima."
    )


# =============================================================================
# 31. VALIDATION - 52W HIGH / LOW
#
# Mora da vazi:
#
# low_52w <= current_price <= high_52w
# =============================================================================

invalid_52w = (
    result_df
    .filter(
        (
            F.col("current_price")
            <
            F.col("low_52w")
        )
        |
        (
            F.col("current_price")
            >
            F.col("high_52w")
        )
    )
    .count()
)


if invalid_52w == 0:

    print(
        "[PASS] 52-week high/low vrijednosti "
        "su konzistentne"
    )

else:

    raise ValueError(
        f"Pronadjeno {invalid_52w} "
        f"nevalidnih 52w vrijednosti."
    )


# =============================================================================
# 32. VALIDATION - PERFORMANCE SCORE
# =============================================================================

invalid_scores = (
    result_df
    .filter(
        F.col(
            "performance_score"
        ).isNull()
        |
        (
            F.col(
                "performance_score"
            ) < 0
        )
        |
        (
            F.col(
                "performance_score"
            ) > 100
        )
    )
    .count()
)


if invalid_scores == 0:

    print(
        "[PASS] Performance score je u "
        "opsegu 0-100"
    )

else:

    raise ValueError(
        f"Pronadjeno {invalid_scores} "
        f"nevalidnih performance score vrijednosti."
    )


# =============================================================================
# 33. VALIDATION - RANK
# =============================================================================

rank_count = (
    result_df
    .select(
        "performance_rank"
    )
    .distinct()
    .count()
)


if rank_count == result_count:

    print(
        "[PASS] Performance rank je jedinstven"
    )

else:

    raise ValueError(
        "Performance rank nije jedinstven."
    )


# =============================================================================
# 34. FINAL
# =============================================================================

print()
print("=" * 90)
print("GOLD COMPANY PERFORMANCE COMPLETED")
print("=" * 90)