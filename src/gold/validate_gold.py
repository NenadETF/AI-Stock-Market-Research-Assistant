from databricks.connect import DatabricksSession

from pyspark.sql import functions as F
from pyspark.sql.window import Window


# =============================================================================
# CONFIGURATION
# =============================================================================

SILVER_PRICES = "workspace.silver.stock_prices"
SILVER_NEWS = "workspace.silver.stock_news"
SILVER_COMPANIES = "workspace.silver.company_details"

GOLD_DAILY = "workspace.gold.stock_daily_metrics"
GOLD_TECHNICAL = "workspace.gold.technical_indicators"
GOLD_PERFORMANCE = "workspace.gold.company_performance"
GOLD_NEWS_DAILY = "workspace.gold.news_daily_analytics"
GOLD_NEWS_SUMMARY = "workspace.gold.news_summary"


# =============================================================================
# SPARK SESSION
# =============================================================================

spark = DatabricksSession.builder.getOrCreate()


# =============================================================================
# VALIDATION STATE
# =============================================================================

failures = []


def pass_check(message):
    print(f"[PASS] {message}")


def fail_check(message):
    print(f"[FAIL] {message}")
    failures.append(message)


def section(title):
    print()
    print("=" * 90)
    print(title)
    print("=" * 90)


# =============================================================================
# HEADER
# =============================================================================

print("=" * 90)
print("FINAL GOLD LAYER VALIDATION")
print("=" * 90)


# =============================================================================
# 1. LOAD TABLES
# =============================================================================

section("1. TABLE AVAILABILITY AND COUNTS")


tables = {
    "silver_prices": SILVER_PRICES,
    "silver_news": SILVER_NEWS,
    "silver_companies": SILVER_COMPANIES,

    "gold_daily": GOLD_DAILY,
    "gold_technical": GOLD_TECHNICAL,
    "gold_performance": GOLD_PERFORMANCE,
    "gold_news_daily": GOLD_NEWS_DAILY,
    "gold_news_summary": GOLD_NEWS_SUMMARY,
}


dataframes = {}


for logical_name, table_name in tables.items():

    try:

        df = spark.table(table_name)

        count = df.count()

        dataframes[logical_name] = df

        print(
            f"{table_name}: "
            f"{count} redova"
        )

        pass_check(
            f"Tabela postoji: {table_name}"
        )

    except Exception as exc:

        fail_check(
            f"Tabela nije dostupna: {table_name} | {exc}"
        )


# Ako neka tabela nedostaje, nema smisla nastaviti.

if failures:

    print()
    print("=" * 90)
    print("GOLD LAYER VALIDATION: FAILED")
    print("=" * 90)

    for failure in failures:
        print(f"- {failure}")

    raise RuntimeError(
        "Gold validation prekinut jer nedostaju potrebne tabele."
    )


silver_prices = dataframes["silver_prices"]
silver_news = dataframes["silver_news"]
silver_companies = dataframes["silver_companies"]

gold_daily = dataframes["gold_daily"]
gold_technical = dataframes["gold_technical"]
gold_performance = dataframes["gold_performance"]
gold_news_daily = dataframes["gold_news_daily"]
gold_news_summary = dataframes["gold_news_summary"]


# =============================================================================
# SOURCE COUNTS
# =============================================================================

silver_price_count = silver_prices.count()
silver_news_count = silver_news.count()

silver_ticker_count = (
    silver_companies
    .select("ticker")
    .distinct()
    .count()
)


print()
print("SOURCE EXPECTATIONS")
print("-" * 90)

print(
    f"Silver stock prices:    "
    f"{silver_price_count}"
)

print(
    f"Silver news:            "
    f"{silver_news_count}"
)

print(
    f"Tracked companies:      "
    f"{silver_ticker_count}"
)


# =============================================================================
# 2. GOLD DAILY METRICS
# =============================================================================

section("2. STOCK DAILY METRICS VALIDATION")


gold_daily_count = gold_daily.count()


if gold_daily_count == silver_price_count:

    pass_check(
        f"stock_daily_metrics broj redova odgovara Silveru: "
        f"{gold_daily_count}"
    )

else:

    fail_check(
        f"stock_daily_metrics ima {gold_daily_count}, "
        f"a Silver ima {silver_price_count} redova"
    )


# -----------------------------------------------------------------------------
# BUSINESS KEYS
# -----------------------------------------------------------------------------

null_daily_keys = (
    gold_daily
    .filter(
        F.col("ticker").isNull()
        |
        F.col("date").isNull()
    )
    .count()
)


if null_daily_keys == 0:

    pass_check(
        "stock_daily_metrics nema NULL poslovne kljuceve"
    )

else:

    fail_check(
        f"stock_daily_metrics ima {null_daily_keys} "
        f"NULL poslovnih kljuceva"
    )


# -----------------------------------------------------------------------------
# DUPLICATES
# -----------------------------------------------------------------------------

daily_duplicates = (
    gold_daily
    .groupBy(
        "ticker",
        "date"
    )
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)


if daily_duplicates == 0:

    pass_check(
        "stock_daily_metrics nema duplikate po (ticker, date)"
    )

else:

    fail_check(
        f"stock_daily_metrics ima {daily_duplicates} "
        f"duplikata"
    )


# -----------------------------------------------------------------------------
# TICKER COUNT
# -----------------------------------------------------------------------------

daily_ticker_count = (
    gold_daily
    .select("ticker")
    .distinct()
    .count()
)


if daily_ticker_count == silver_ticker_count:

    pass_check(
        f"stock_daily_metrics sadrzi svih "
        f"{silver_ticker_count} tickera"
    )

else:

    fail_check(
        f"stock_daily_metrics ima {daily_ticker_count} tickera, "
        f"a ocekuje se {silver_ticker_count}"
    )


# -----------------------------------------------------------------------------
# PRICE CHANGE / DAILY RETURN MATHEMATICAL VALIDATION
# -----------------------------------------------------------------------------

price_window = (
    Window
    .partitionBy("ticker")
    .orderBy("date")
)


daily_math_df = (
    gold_daily

    .withColumn(
        "_expected_previous_close",
        F.lag("close").over(price_window)
    )

    .withColumn(
        "_expected_price_change",
        F.col("close")
        -
        F.col("_expected_previous_close")
    )

    .withColumn(
        "_expected_daily_return",

        F.when(
            F.col("_expected_previous_close") != 0,

            (
                F.col("close")
                -
                F.col("_expected_previous_close")
            )
            /
            F.col("_expected_previous_close")
        )
    )
)


invalid_previous_close = (
    daily_math_df
    .filter(
        F.col("_expected_previous_close").isNotNull()
        &
        (
            F.abs(
                F.col("previous_close")
                -
                F.col("_expected_previous_close")
            )
            > 0.0001
        )
    )
    .count()
)


if invalid_previous_close == 0:

    pass_check(
        "previous_close vrijednosti su matematicki konzistentne"
    )

else:

    fail_check(
        f"Pronadjeno {invalid_previous_close} "
        f"nekonzistentnih previous_close vrijednosti"
    )


invalid_price_change = (
    daily_math_df
    .filter(
        F.col("_expected_price_change").isNotNull()
        &
        (
            F.abs(
                F.col("price_change")
                -
                F.col("_expected_price_change")
            )
            > 0.001
        )
    )
    .count()
)


if invalid_price_change == 0:

    pass_check(
        "price_change vrijednosti su matematicki konzistentne"
    )

else:

    fail_check(
        f"Pronadjeno {invalid_price_change} "
        f"nekonzistentnih price_change vrijednosti"
    )


invalid_daily_returns = (
    daily_math_df
    .filter(
        F.col("_expected_daily_return").isNotNull()
        &
        (
            F.abs(
                F.col("daily_return")
                -
                F.col("_expected_daily_return")
            )
            > 0.000001
        )
    )
    .count()
)


if invalid_daily_returns == 0:

    pass_check(
        "daily_return vrijednosti su matematicki konzistentne"
    )

else:

    fail_check(
        f"Pronadjeno {invalid_daily_returns} "
        f"nekonzistentnih daily_return vrijednosti"
    )


# -----------------------------------------------------------------------------
# DAILY DIRECTION
# -----------------------------------------------------------------------------

invalid_direction = (
    gold_daily
    .filter(
        F.col("daily_return").isNotNull()
        &
        (
            (
                (F.col("daily_return") > 0)
                &
                (F.col("daily_direction") != "UP")
            )
            |
            (
                (F.col("daily_return") < 0)
                &
                (F.col("daily_direction") != "DOWN")
            )
            |
            (
                (F.col("daily_return") == 0)
                &
                (F.col("daily_direction") != "FLAT")
            )
        )
    )
    .count()
)


if invalid_direction == 0:

    pass_check(
        "daily_direction odgovara daily_return vrijednostima"
    )

else:

    fail_check(
        f"Pronadjeno {invalid_direction} "
        f"nekonzistentnih daily_direction vrijednosti"
    )


# -----------------------------------------------------------------------------
# PERIOD
# -----------------------------------------------------------------------------

print()
print("STOCK DAILY METRICS PERIOD:")


(
    gold_daily
    .select(
        F.min("date").alias("najstariji"),
        F.max("date").alias("najnoviji")
    )
    .show()
)


# =============================================================================
# 3. TECHNICAL INDICATORS
# =============================================================================

section("3. TECHNICAL INDICATORS VALIDATION")


technical_count = gold_technical.count()


if technical_count == gold_daily_count:

    pass_check(
        f"technical_indicators ima isti broj redova kao "
        f"stock_daily_metrics: {technical_count}"
    )

else:

    fail_check(
        f"technical_indicators ima {technical_count}, "
        f"a stock_daily_metrics {gold_daily_count} redova"
    )


# -----------------------------------------------------------------------------
# DUPLICATES
# -----------------------------------------------------------------------------

technical_duplicates = (
    gold_technical
    .groupBy(
        "ticker",
        "date"
    )
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)


if technical_duplicates == 0:

    pass_check(
        "technical_indicators nema duplikate po (ticker, date)"
    )

else:

    fail_check(
        f"technical_indicators ima {technical_duplicates} "
        f"duplikata"
    )


# -----------------------------------------------------------------------------
# RSI
# -----------------------------------------------------------------------------

invalid_rsi = (
    gold_technical
    .filter(
        F.col("rsi_14").isNotNull()
        &
        (
            (F.col("rsi_14") < 0)
            |
            (F.col("rsi_14") > 100)
        )
    )
    .count()
)


if invalid_rsi == 0:

    pass_check(
        "RSI14 vrijednosti su u opsegu 0-100"
    )

else:

    fail_check(
        f"Pronadjeno {invalid_rsi} "
        f"RSI vrijednosti van opsega 0-100"
    )


# -----------------------------------------------------------------------------
# BOLLINGER
# -----------------------------------------------------------------------------

invalid_bollinger = (
    gold_technical
    .filter(
        F.col("bollinger_middle").isNotNull()
        &
        (
            (
                F.col("bollinger_lower")
                >
                F.col("bollinger_middle")
            )
            |
            (
                F.col("bollinger_middle")
                >
                F.col("bollinger_upper")
            )
        )
    )
    .count()
)


if invalid_bollinger == 0:

    pass_check(
        "Bollinger Bands su konzistentni"
    )

else:

    fail_check(
        f"Pronadjeno {invalid_bollinger} "
        f"nevalidnih Bollinger vrijednosti"
    )


# -----------------------------------------------------------------------------
# MACD HISTOGRAM
#
# histogram = MACD - signal
# -----------------------------------------------------------------------------

invalid_macd_histogram = (
    gold_technical
    .filter(
        F.col("macd_histogram").isNotNull()
        &
        (
            F.abs(
                F.col("macd_histogram")
                -
                (
                    F.col("macd")
                    -
                    F.col("macd_signal")
                )
            )
            > 0.00001
        )
    )
    .count()
)


if invalid_macd_histogram == 0:

    pass_check(
        "MACD histogram odgovara MACD - Signal"
    )

else:

    fail_check(
        f"Pronadjeno {invalid_macd_histogram} "
        f"nekonzistentnih MACD histogram vrijednosti"
    )


# -----------------------------------------------------------------------------
# SIGNAL ENUM VALUES
# -----------------------------------------------------------------------------

invalid_trend_signals = (
    gold_technical
    .filter(
        F.col("trend_signal").isNotNull()
        &
        (
            ~F.col("trend_signal").isin(
                "BULLISH",
                "BEARISH",
                "NEUTRAL"
            )
        )
    )
    .count()
)


invalid_rsi_signals = (
    gold_technical
    .filter(
        F.col("rsi_signal").isNotNull()
        &
        (
            ~F.col("rsi_signal").isin(
                "OVERBOUGHT",
                "OVERSOLD",
                "NEUTRAL"
            )
        )
    )
    .count()
)


invalid_macd_states = (
    gold_technical
    .filter(
        F.col("macd_state").isNotNull()
        &
        (
            ~F.col("macd_state").isin(
                "BULLISH",
                "BEARISH",
                "NEUTRAL"
            )
        )
    )
    .count()
)


if (
    invalid_trend_signals == 0
    and
    invalid_rsi_signals == 0
    and
    invalid_macd_states == 0
):

    pass_check(
        "Svi technical signal statusi imaju dozvoljene vrijednosti"
    )

else:

    fail_check(
        "Pronadjene nevalidne technical signal vrijednosti"
    )


# =============================================================================
# 4. COMPANY PERFORMANCE
# =============================================================================

section("4. COMPANY PERFORMANCE VALIDATION")


performance_count = gold_performance.count()


if performance_count == silver_ticker_count:

    pass_check(
        f"company_performance ima jedan red za svih "
        f"{silver_ticker_count} kompanija"
    )

else:

    fail_check(
        f"company_performance ima {performance_count} redova, "
        f"a ocekuje se {silver_ticker_count}"
    )


# -----------------------------------------------------------------------------
# DUPLICATES
# -----------------------------------------------------------------------------

performance_duplicates = (
    gold_performance
    .groupBy("ticker")
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)


if performance_duplicates == 0:

    pass_check(
        "company_performance nema duplikate po tickeru"
    )

else:

    fail_check(
        f"company_performance ima {performance_duplicates} "
        f"duplih tickera"
    )


# -----------------------------------------------------------------------------
# COMPANY NAMES
# -----------------------------------------------------------------------------

missing_company_names = (
    gold_performance
    .filter(
        F.col("company_name").isNull()
        |
        (
            F.length(
                F.trim(
                    F.col("company_name")
                )
            )
            == 0
        )
    )
    .count()
)


if missing_company_names == 0:

    pass_check(
        "Sve company_performance kompanije imaju naziv"
    )

else:

    fail_check(
        f"{missing_company_names} kompanija nema validan naziv"
    )


# -----------------------------------------------------------------------------
# 52 WEEK RANGE
# -----------------------------------------------------------------------------

invalid_52w = (
    gold_performance
    .filter(
        F.col("current_price").isNotNull()
        &
        (
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
            |
            (
                F.col("low_52w")
                >
                F.col("high_52w")
            )
        )
    )
    .count()
)


if invalid_52w == 0:

    pass_check(
        "52-week low/current/high vrijednosti su konzistentne"
    )

else:

    fail_check(
        f"Pronadjeno {invalid_52w} "
        f"nevalidnih 52-week vrijednosti"
    )


# -----------------------------------------------------------------------------
# PERFORMANCE SCORE
# -----------------------------------------------------------------------------

invalid_performance_scores = (
    gold_performance
    .filter(
        F.col("performance_score").isNull()
        |
        (F.col("performance_score") < 0)
        |
        (F.col("performance_score") > 100)
    )
    .count()
)


if invalid_performance_scores == 0:

    pass_check(
        "Performance score vrijednosti su u opsegu 0-100"
    )

else:

    fail_check(
        f"Pronadjeno {invalid_performance_scores} "
        f"nevalidnih performance score vrijednosti"
    )


# -----------------------------------------------------------------------------
# RANK
# -----------------------------------------------------------------------------

rank_stats = (
    gold_performance
    .agg(
        F.min(
            "performance_rank"
        ).alias(
            "min_rank"
        ),

        F.max(
            "performance_rank"
        ).alias(
            "max_rank"
        ),

        F.countDistinct(
            "performance_rank"
        ).alias(
            "distinct_ranks"
        )
    )
    .collect()[0]
)


rank_valid = (
    rank_stats["min_rank"] == 1
    and
    rank_stats["max_rank"] == performance_count
    and
    rank_stats["distinct_ranks"] == performance_count
)


if rank_valid:

    pass_check(
        "Performance rank je jedinstven i kontinuiran od 1 do N"
    )

else:

    fail_check(
        "Performance rank nije validan ili nije kontinuiran"
    )


# -----------------------------------------------------------------------------
# DATE CONSISTENCY
# -----------------------------------------------------------------------------

invalid_performance_dates = (
    gold_performance
    .filter(
        F.col("date_consistent") != True
    )
    .count()
)


if invalid_performance_dates == 0:

    pass_check(
        "Market i technical datumi su konzistentni"
    )

else:

    fail_check(
        f"Pronadjeno {invalid_performance_dates} "
        f"nekonzistentnih market/technical datuma"
    )


# =============================================================================
# 5. NEWS DAILY ANALYTICS
# =============================================================================

section("5. NEWS DAILY ANALYTICS VALIDATION")


news_daily_count = gold_news_daily.count()


if news_daily_count > 0:

    pass_check(
        f"news_daily_analytics nije prazna: "
        f"{news_daily_count} redova"
    )

else:

    fail_check(
        "news_daily_analytics je prazna"
    )


# -----------------------------------------------------------------------------
# DUPLICATES
# -----------------------------------------------------------------------------

news_daily_duplicates = (
    gold_news_daily
    .groupBy(
        "ticker",
        "published_date"
    )
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)


if news_daily_duplicates == 0:

    pass_check(
        "news_daily_analytics nema duplikate po "
        "(ticker, published_date)"
    )

else:

    fail_check(
        f"news_daily_analytics ima "
        f"{news_daily_duplicates} duplikata"
    )


# -----------------------------------------------------------------------------
# SENTIMENT COUNTS
# -----------------------------------------------------------------------------

invalid_news_daily_counts = (
    gold_news_daily
    .filter(
        (
            F.col("positive_count")
            +
            F.col("negative_count")
            +
            F.col("neutral_count")
            +
            F.col("unknown_sentiment_count")
        )
        !=
        F.col("news_count")
    )
    .count()
)


if invalid_news_daily_counts == 0:

    pass_check(
        "Daily news sentiment counts odgovaraju news_count"
    )

else:

    fail_check(
        f"Pronadjeno {invalid_news_daily_counts} "
        f"nekonzistentnih daily news count redova"
    )


# -----------------------------------------------------------------------------
# SENTIMENT SCORE
# -----------------------------------------------------------------------------

invalid_daily_sentiment_scores = (
    gold_news_daily
    .filter(
        F.col("sentiment_score").isNotNull()
        &
        (
            (F.col("sentiment_score") < -100)
            |
            (F.col("sentiment_score") > 100)
        )
    )
    .count()
)


if invalid_daily_sentiment_scores == 0:

    pass_check(
        "Daily sentiment score je u opsegu -100 do 100"
    )

else:

    fail_check(
        f"Pronadjeno {invalid_daily_sentiment_scores} "
        f"daily sentiment score vrijednosti van opsega"
    )


# -----------------------------------------------------------------------------
# COVERAGE
# -----------------------------------------------------------------------------

invalid_daily_coverage = (
    gold_news_daily
    .filter(
        F.col("sentiment_coverage_pct").isNotNull()
        &
        (
            (F.col("sentiment_coverage_pct") < 0)
            |
            (F.col("sentiment_coverage_pct") > 100)
        )
    )
    .count()
)


if invalid_daily_coverage == 0:

    pass_check(
        "Daily sentiment coverage je u opsegu 0-100%"
    )

else:

    fail_check(
        f"Pronadjeno {invalid_daily_coverage} "
        f"nevalidnih sentiment coverage vrijednosti"
    )


# =============================================================================
# 6. NEWS SUMMARY
# =============================================================================

section("6. NEWS SUMMARY VALIDATION")


news_summary_count = gold_news_summary.count()


if news_summary_count == silver_ticker_count:

    pass_check(
        f"news_summary ima svih "
        f"{silver_ticker_count} kompanija"
    )

else:

    fail_check(
        f"news_summary ima {news_summary_count} redova, "
        f"a ocekuje se {silver_ticker_count}"
    )


# -----------------------------------------------------------------------------
# DUPLICATES
# -----------------------------------------------------------------------------

news_summary_duplicates = (
    gold_news_summary
    .groupBy("ticker")
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)


if news_summary_duplicates == 0:

    pass_check(
        "news_summary nema duplikate po tickeru"
    )

else:

    fail_check(
        f"news_summary ima {news_summary_duplicates} "
        f"duplih tickera"
    )


# -----------------------------------------------------------------------------
# SENTIMENT COUNTS
# -----------------------------------------------------------------------------

invalid_news_summary_counts = (
    gold_news_summary
    .filter(
        (
            F.col("positive_30d_count")
            +
            F.col("negative_30d_count")
            +
            F.col("neutral_30d_count")
            +
            F.col("unknown_30d_count")
        )
        !=
        F.col("news_30d_count")
    )
    .count()
)


if invalid_news_summary_counts == 0:

    pass_check(
        "News summary 30D sentiment counts su konzistentni"
    )

else:

    fail_check(
        f"Pronadjeno {invalid_news_summary_counts} "
        f"nekonzistentnih news summary redova"
    )


# -----------------------------------------------------------------------------
# SENTIMENT SCORE
# -----------------------------------------------------------------------------

invalid_summary_scores = (
    gold_news_summary
    .filter(
        F.col("sentiment_score_30d").isNotNull()
        &
        (
            (F.col("sentiment_score_30d") < -100)
            |
            (F.col("sentiment_score_30d") > 100)
        )
    )
    .count()
)


if invalid_summary_scores == 0:

    pass_check(
        "News summary sentiment score je u opsegu -100 do 100"
    )

else:

    fail_check(
        f"Pronadjeno {invalid_summary_scores} "
        f"summary sentiment score vrijednosti van opsega"
    )


# -----------------------------------------------------------------------------
# COVERAGE
# -----------------------------------------------------------------------------

invalid_summary_coverage = (
    gold_news_summary
    .filter(
        F.col(
            "sentiment_coverage_30d_pct"
        ).isNotNull()
        &
        (
            (
                F.col(
                    "sentiment_coverage_30d_pct"
                ) < 0
            )
            |
            (
                F.col(
                    "sentiment_coverage_30d_pct"
                ) > 100
            )
        )
    )
    .count()
)


if invalid_summary_coverage == 0:

    pass_check(
        "News summary sentiment coverage je u opsegu 0-100%"
    )

else:

    fail_check(
        f"Pronadjeno {invalid_summary_coverage} "
        f"nevalidnih summary coverage vrijednosti"
    )


# -----------------------------------------------------------------------------
# NEWS VOLUME RANK
# -----------------------------------------------------------------------------

news_rank_stats = (
    gold_news_summary
    .agg(
        F.min(
            "news_volume_rank_30d"
        ).alias(
            "min_rank"
        ),

        F.max(
            "news_volume_rank_30d"
        ).alias(
            "max_rank"
        ),

        F.countDistinct(
            "news_volume_rank_30d"
        ).alias(
            "distinct_ranks"
        )
    )
    .collect()[0]
)


news_rank_valid = (
    news_rank_stats["min_rank"] == 1
    and
    news_rank_stats["max_rank"] == news_summary_count
    and
    news_rank_stats["distinct_ranks"] == news_summary_count
)


if news_rank_valid:

    pass_check(
        "News volume rank je jedinstven i kontinuiran od 1 do N"
    )

else:

    fail_check(
        "News volume rank nije validan"
    )


# =============================================================================
# 7. CROSS-TABLE TICKER CONSISTENCY
# =============================================================================

section("7. CROSS-TABLE TICKER CONSISTENCY")


company_tickers = {
    row["ticker"]
    for row in (
        silver_companies
        .select(
            F.upper(
                F.trim(
                    F.col("ticker")
                )
            ).alias("ticker")
        )
        .distinct()
        .collect()
    )
}


daily_tickers = {
    row["ticker"]
    for row in (
        gold_daily
        .select("ticker")
        .distinct()
        .collect()
    )
}


technical_tickers = {
    row["ticker"]
    for row in (
        gold_technical
        .select("ticker")
        .distinct()
        .collect()
    )
}


performance_tickers = {
    row["ticker"]
    for row in (
        gold_performance
        .select("ticker")
        .distinct()
        .collect()
    )
}


news_summary_tickers = {
    row["ticker"]
    for row in (
        gold_news_summary
        .select("ticker")
        .distinct()
        .collect()
    )
}


if daily_tickers == company_tickers:

    pass_check(
        "stock_daily_metrics ticker set odgovara company_details"
    )

else:

    fail_check(
        "stock_daily_metrics ticker set se razlikuje od company_details"
    )


if technical_tickers == company_tickers:

    pass_check(
        "technical_indicators ticker set odgovara company_details"
    )

else:

    fail_check(
        "technical_indicators ticker set se razlikuje od company_details"
    )


if performance_tickers == company_tickers:

    pass_check(
        "company_performance ticker set odgovara company_details"
    )

else:

    fail_check(
        "company_performance ticker set se razlikuje od company_details"
    )


if news_summary_tickers == company_tickers:

    pass_check(
        "news_summary ticker set odgovara company_details"
    )

else:

    fail_check(
        "news_summary ticker set se razlikuje od company_details"
    )


# =============================================================================
# 8. FINAL SUMMARY
# =============================================================================

section("8. FINAL GOLD VALIDATION RESULT")


print("GOLD TABLE COUNTS")
print("-" * 90)

print(
    f"stock_daily_metrics:     "
    f"{gold_daily_count}"
)

print(
    f"technical_indicators:    "
    f"{technical_count}"
)

print(
    f"company_performance:     "
    f"{performance_count}"
)

print(
    f"news_daily_analytics:    "
    f"{news_daily_count}"
)

print(
    f"news_summary:            "
    f"{news_summary_count}"
)


print()


if len(failures) == 0:

    print("=" * 90)
    print("ALL GOLD VALIDATION CHECKS PASSED")
    print("GOLD LAYER VALIDATION: SUCCESS")
    print("=" * 90)

else:

    print("=" * 90)
    print(
        f"GOLD LAYER VALIDATION: FAILED "
        f"({len(failures)} problema)"
    )
    print("=" * 90)

    for index, failure in enumerate(
        failures,
        start=1
    ):

        print(
            f"{index}. {failure}"
        )


    raise RuntimeError(
        f"Gold validation nije prosao. "
        f"Broj problema: {len(failures)}"
    )