from databricks.connect import DatabricksSession
from pyspark.sql import functions as F


# =============================================================================
# CONFIGURATION
# =============================================================================

ANOMALY_TABLE = "workspace.gold.stock_anomalies"
NEWS_TABLE = "workspace.silver.stock_news"

GOLD_SCHEMA = "workspace.gold"

IMPACT_TABLE = "workspace.gold.stock_news_impact"
SUMMARY_TABLE = "workspace.gold.stock_anomaly_news_summary"

NEWS_WINDOW_DAYS = 3


# =============================================================================
# SPARK SESSION
# =============================================================================

spark = DatabricksSession.builder.getOrCreate()


print("=" * 100)
print("GOLD LAYER - STOCK ANOMALY NEWS IMPACT")
print("=" * 100)

print(f"Anomaly source: {ANOMALY_TABLE}")
print(f"News source:    {NEWS_TABLE}")
print(f"Impact target:  {IMPACT_TABLE}")
print(f"Summary target: {SUMMARY_TABLE}")
print(f"News window:    +/- {NEWS_WINDOW_DAYS} calendar days")


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
# 2. LOAD DATA
# =============================================================================

anomalies_source_df = spark.table(ANOMALY_TABLE)
news_source_df = spark.table(NEWS_TABLE)


anomaly_source_count = anomalies_source_df.count()
news_source_count = news_source_df.count()


print()
print("=" * 100)
print("SOURCE DATA")
print("=" * 100)

print(f"Ukupno anomaly redova: {anomaly_source_count}")
print(f"Ukupno news redova:    {news_source_count}")


# =============================================================================
# 3. FILTER FINAL ANOMALIES
# =============================================================================

anomalies_df = (
    anomalies_source_df
    .filter(
        F.col("is_anomaly")
    )
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

        "market_condition"
    )
)


final_anomaly_count = anomalies_df.count()


print()
print(f"Finalnih anomalija: {final_anomaly_count}")


# =============================================================================
# 4. CREATE ANOMALY EVENT ID
#
# Deterministicki ID:
#
# ticker + date
#
# Primjer:
#
# NVDA | 2025-01-27
# =============================================================================

anomalies_df = anomalies_df.withColumn(
    "anomaly_event_id",

    F.sha2(
        F.concat_ws(
            "|",
            F.col("ticker"),
            F.col("date").cast("string")
        ),
        256
    )
)


# =============================================================================
# 5. PREPARE NEWS
#
# Vazna promjena:
#
# Ne koristimo vise samo requested_ticker.
#
# Massive clanak moze pripadati vecem broju kompanija:
#
# tickers = ["NVDA", "MSFT", "GOOGL"]
#
# Zato svaki clanak razbijamo na:
#
# article_id + news_ticker
#
# i povezujemo anomaly ticker sa stvarnim tickerom iz clanka.
# =============================================================================

news_base_df = (
    news_source_df
    .select(
        "article_id",
        "requested_ticker",

        "title",
        "author",
        "description",

        "article_url",
        "image_url",

        "published_utc",
        "published_date",

        "publisher_name",

        "tickers",
        "keywords",
        "insights",

        "requested_ticker_sentiment",
        "requested_ticker_sentiment_reasoning",

        "_source"
    )
    .filter(
        F.col("article_id").isNotNull()
        & F.col("published_date").isNotNull()
    )
)


# =============================================================================
# 6. EXPAND ARTICLE TICKERS
# =============================================================================

news_df = news_base_df.withColumn(
    "news_ticker",
    F.explode_outer(
        F.col("tickers")
    )
)


# Ako clanak nema tickers array,
# requested_ticker koristimo kao fallback.

news_df = news_df.withColumn(
    "news_ticker",

    F.coalesce(
        F.col("news_ticker"),
        F.col("requested_ticker")
    )
)


news_df = news_df.filter(
    F.col("news_ticker").isNotNull()
)


# =============================================================================
# 7. IDENTIFY TICKER MATCH SOURCE
#
# ARTICLE_TICKERS:
# ticker je eksplicitno naveden u tickers array-u.
#
# REQUESTED_TICKER_FALLBACK:
# tickers array nije bio dostupan i koristimo ticker preko kojeg
# je clanak preuzet.
# =============================================================================

news_df = news_df.withColumn(
    "_ticker_array_match",

    F.when(
        F.array_contains(
            F.col("tickers"),
            F.col("news_ticker")
        ),
        F.lit(1)
    ).otherwise(
        F.lit(0)
    )
)


# =============================================================================
# 8. FIND TICKER-SPECIFIC INSIGHT
#
# insights je array structura:
#
# [
#   {
#       ticker: "NVDA",
#       sentiment: "positive",
#       sentiment_reasoning: "..."
#   }
# ]
#
# Uzimamo insight bas za news_ticker.
# =============================================================================

news_df = news_df.withColumn(
    "_ticker_insight",

    F.expr(
        """
        element_at(
            filter(
                insights,
                x -> x.ticker = news_ticker
            ),
            1
        )
        """
    )
)


# =============================================================================
# 9. TICKER-SPECIFIC SENTIMENT
#
# Prioritet:
#
# 1. insight za konkretan ticker
# 2. requested_ticker sentiment kao fallback
# =============================================================================

news_df = news_df.withColumn(
    "_ticker_sentiment_candidate",

    F.coalesce(
        F.col("_ticker_insight.sentiment"),

        F.when(
            F.col("news_ticker")
            == F.col("requested_ticker"),

            F.col(
                "requested_ticker_sentiment"
            )
        )
    )
)


news_df = news_df.withColumn(
    "_ticker_sentiment_reasoning_candidate",

    F.coalesce(
        F.col(
            "_ticker_insight.sentiment_reasoning"
        ),

        F.when(
            F.col("news_ticker")
            == F.col("requested_ticker"),

            F.col(
                "requested_ticker_sentiment_reasoning"
            )
        )
    )
)


# =============================================================================
# 10. SENTIMENT SOURCE FLAGS
# =============================================================================

news_df = news_df.withColumn(
    "_has_insight_sentiment",

    F.when(
        F.col("_ticker_insight.sentiment")
        .isNotNull(),

        F.lit(1)
    ).otherwise(
        F.lit(0)
    )
)


news_df = news_df.withColumn(
    "_has_requested_sentiment",

    F.when(
        (
            F.col("news_ticker")
            == F.col("requested_ticker")
        )
        &
        F.col(
            "requested_ticker_sentiment"
        ).isNotNull(),

        F.lit(1)
    ).otherwise(
        F.lit(0)
    )
)


# =============================================================================
# 11. DEDUPLICATE ARTICLE + NEWS TICKER
#
# Isti article_id moze postojati vise puta u Silver tabeli jer je
# preuzet preko vise requested tickera.
#
# Za Gold zelimo samo:
#
# article_id + news_ticker
#
# jednom.
#
# FIRST(..., ignorenulls=True) nam omogucava da ne izgubimo sentiment
# samo zato sto jedan od duplikata nije imao requested sentiment.
# =============================================================================

news_df = (
    news_df
    .groupBy(
        "article_id",
        "news_ticker"
    )
    .agg(

        F.sort_array(
            F.collect_set(
                "requested_ticker"
            )
        ).alias(
            "requested_tickers"
        ),

        F.first(
            "title",
            ignorenulls=True
        ).alias(
            "title"
        ),

        F.first(
            "author",
            ignorenulls=True
        ).alias(
            "author"
        ),

        F.first(
            "description",
            ignorenulls=True
        ).alias(
            "description"
        ),

        F.first(
            "article_url",
            ignorenulls=True
        ).alias(
            "article_url"
        ),

        F.first(
            "image_url",
            ignorenulls=True
        ).alias(
            "image_url"
        ),

        F.first(
            "published_utc",
            ignorenulls=True
        ).alias(
            "published_utc"
        ),

        F.first(
            "published_date",
            ignorenulls=True
        ).alias(
            "published_date"
        ),

        F.first(
            "publisher_name",
            ignorenulls=True
        ).alias(
            "publisher_name"
        ),

        F.first(
            "tickers",
            ignorenulls=True
        ).alias(
            "tickers"
        ),

        F.first(
            "keywords",
            ignorenulls=True
        ).alias(
            "keywords"
        ),

        F.first(
            "_ticker_sentiment_candidate",
            ignorenulls=True
        ).alias(
            "ticker_sentiment"
        ),

        F.first(
            "_ticker_sentiment_reasoning_candidate",
            ignorenulls=True
        ).alias(
            "ticker_sentiment_reasoning"
        ),

        F.max(
            "_ticker_array_match"
        ).alias(
            "_ticker_array_match"
        ),

        F.max(
            "_has_insight_sentiment"
        ).alias(
            "_has_insight_sentiment"
        ),

        F.max(
            "_has_requested_sentiment"
        ).alias(
            "_has_requested_sentiment"
        ),

        F.first(
            "_source",
            ignorenulls=True
        ).alias(
            "_source"
        )
    )
)


# =============================================================================
# 12. FINAL TICKER MATCH SOURCE
# =============================================================================

news_df = news_df.withColumn(
    "ticker_match_source",

    F.when(
        F.col("_ticker_array_match") == 1,
        F.lit("ARTICLE_TICKERS")
    )
    .otherwise(
        F.lit("REQUESTED_TICKER_FALLBACK")
    )
)


# =============================================================================
# 13. FINAL SENTIMENT SOURCE
# =============================================================================

news_df = news_df.withColumn(
    "sentiment_source",

    F.when(
        F.col("_has_insight_sentiment") == 1,
        F.lit("INSIGHTS")
    )
    .when(
        F.col("_has_requested_sentiment") == 1,
        F.lit("REQUESTED_TICKER")
    )
    .otherwise(
        F.lit("NONE")
    )
)


news_df = news_df.drop(
    "_ticker_array_match",
    "_has_insight_sentiment",
    "_has_requested_sentiment"
)


# =============================================================================
# 14. ALIAS DATAFRAMES
# =============================================================================

a = anomalies_df.alias("a")
n = news_df.alias("n")


# =============================================================================
# 15. MATCH ANOMALIES WITH NEWS
#
# Uslovi:
#
# 1. anomaly ticker == stvarni ticker iz clanka
# 2. vijest je unutar +/- 3 kalendarska dana
# =============================================================================

join_condition = (

    (
        F.col("a.ticker")
        == F.col("n.news_ticker")
    )

    &

    (
        F.col("n.published_date")
        >= F.date_sub(
            F.col("a.date"),
            NEWS_WINDOW_DAYS
        )
    )

    &

    (
        F.col("n.published_date")
        <= F.date_add(
            F.col("a.date"),
            NEWS_WINDOW_DAYS
        )
    )
)


matched_df = (
    a
    .join(
        n,
        join_condition,
        "inner"
    )
)


# =============================================================================
# 16. SELECT MATCHED DATA
# =============================================================================

impact_df = matched_df.select(

    F.col("a.anomaly_event_id")
    .alias("anomaly_event_id"),

    F.col("a.ticker")
    .alias("ticker"),

    F.col("a.date")
    .alias("anomaly_date"),

    F.col("a.close")
    .alias("close"),

    F.col("a.daily_return_pct")
    .alias("daily_return_pct"),

    F.col("a.daily_direction")
    .alias("daily_direction"),

    F.col("a.opening_gap_pct")
    .alias("opening_gap_pct"),

    F.col("a.historical_volume_ratio")
    .alias("historical_volume_ratio"),

    F.col("a.return_zscore")
    .alias("return_zscore"),

    F.col("a.volume_zscore")
    .alias("volume_zscore"),

    F.col("a.range_zscore")
    .alias("range_zscore"),

    F.col("a.gap_zscore")
    .alias("gap_zscore"),

    F.col("a.transactions_zscore")
    .alias("transactions_zscore"),

    F.col("a.volatility_ratio")
    .alias("volatility_ratio"),

    F.col("a.rsi_14")
    .alias("rsi_14"),

    F.col("a.drawdown_60d_pct")
    .alias("drawdown_60d_pct"),

    F.col("a.anomaly_signal_count")
    .alias("anomaly_signal_count"),

    F.col("a.anomaly_score")
    .alias("anomaly_score"),

    F.col("a.anomaly_severity")
    .alias("anomaly_severity"),

    F.col("a.anomaly_type")
    .alias("anomaly_type"),

    F.col("a.market_condition")
    .alias("market_condition"),

    F.col("n.article_id")
    .alias("article_id"),

    F.col("n.news_ticker")
    .alias("news_ticker"),

    F.col("n.requested_tickers")
    .alias("requested_tickers"),

    F.col("n.ticker_match_source")
    .alias("ticker_match_source"),

    F.col("n.title")
    .alias("news_title"),

    F.col("n.author")
    .alias("news_author"),

    F.col("n.description")
    .alias("news_description"),

    F.col("n.article_url")
    .alias("article_url"),

    F.col("n.image_url")
    .alias("image_url"),

    F.col("n.published_utc")
    .alias("published_utc"),

    F.col("n.published_date")
    .alias("published_date"),

    F.col("n.publisher_name")
    .alias("publisher_name"),

    F.col("n.tickers")
    .alias("news_tickers"),

    F.col("n.keywords")
    .alias("news_keywords"),

    F.col("n.ticker_sentiment")
    .alias("sentiment"),

    F.col("n.ticker_sentiment_reasoning")
    .alias("sentiment_reasoning"),

    F.col("n.sentiment_source")
    .alias("sentiment_source"),

    F.col("n._source")
    .alias("news_source")
)


# =============================================================================
# 17. NEWS DAY OFFSET
#
# -3 -> tri dana prije
# -1 -> dan prije
#  0 -> isti dan
# +1 -> dan poslije
# +3 -> tri dana poslije
# =============================================================================

impact_df = impact_df.withColumn(
    "news_day_offset",

    F.datediff(
        F.col("published_date"),
        F.col("anomaly_date")
    )
)


# =============================================================================
# 18. TEMPORAL RELATION
# =============================================================================

impact_df = impact_df.withColumn(
    "temporal_relation",

    F.when(
        F.col("news_day_offset") <= -2,
        F.lit("BEFORE_2_3_DAYS")
    )
    .when(
        F.col("news_day_offset") == -1,
        F.lit("DAY_BEFORE")
    )
    .when(
        F.col("news_day_offset") == 0,
        F.lit("SAME_DAY")
    )
    .when(
        F.col("news_day_offset") == 1,
        F.lit("DAY_AFTER")
    )
    .otherwise(
        F.lit("AFTER_2_3_DAYS")
    )
)


# =============================================================================
# 19. EVENT RELATION
#
# PRE_EVENT:
# moguci explanatory kontekst.
#
# SAME_DAY:
# vrlo relevantan kontekst, ali bez intraday timestamp analize
# ne tvrdimo uzrocnost.
#
# POST_EVENT:
# reakcija ili naknadni kontekst.
# =============================================================================

impact_df = impact_df.withColumn(
    "event_relation",

    F.when(
        F.col("news_day_offset") < 0,
        F.lit("PRE_EVENT")
    )
    .when(
        F.col("news_day_offset") == 0,
        F.lit("SAME_DAY")
    )
    .otherwise(
        F.lit("POST_EVENT")
    )
)


# =============================================================================
# 20. POTENTIAL EXPLANATORY CONTEXT
#
# PRE_EVENT + SAME_DAY mogu biti dati AI agentu kao potencijalni
# explanatory context.
#
# POST_EVENT koristimo kao reakciju/naknadni kontekst.
# =============================================================================

impact_df = impact_df.withColumn(
    "is_potential_context",

    F.col("event_relation").isin(
        "PRE_EVENT",
        "SAME_DAY"
    )
)


# =============================================================================
# 21. TEMPORAL RELEVANCE SCORE
#
# 0 dana = 1.00
# 1 dan  = 0.85
# 2 dana = 0.65
# 3 dana = 0.50
#
# Ovo NIJE probability niti causal score.
# =============================================================================

impact_df = impact_df.withColumn(
    "temporal_relevance_score",

    F.when(
        F.abs(
            F.col("news_day_offset")
        ) == 0,

        F.lit(1.00)
    )
    .when(
        F.abs(
            F.col("news_day_offset")
        ) == 1,

        F.lit(0.85)
    )
    .when(
        F.abs(
            F.col("news_day_offset")
        ) == 2,

        F.lit(0.65)
    )
    .when(
        F.abs(
            F.col("news_day_offset")
        ) == 3,

        F.lit(0.50)
    )
    .otherwise(
        F.lit(0.0)
    )
)


# =============================================================================
# 22. NORMALIZE SENTIMENT
#
# Silver trenutno sadrzi:
#
# positive
# neutral
# negative
# neutral/positive
# mixed
# =============================================================================

impact_df = impact_df.withColumn(
    "sentiment_normalized",

    F.lower(
        F.trim(
            F.col("sentiment")
        )
    )
)


# =============================================================================
# 23. NUMERIC SENTIMENT SCORE
#
# positive          -> +1.0
# neutral/positive  -> +0.5
# neutral           ->  0.0
# mixed             ->  0.0
# negative          -> -1.0
# =============================================================================

impact_df = impact_df.withColumn(
    "sentiment_score",

    F.when(
        F.col("sentiment_normalized")
        == "positive",

        F.lit(1.0)
    )
    .when(
        F.col("sentiment_normalized")
        == "neutral/positive",

        F.lit(0.5)
    )
    .when(
        F.col("sentiment_normalized")
        == "neutral",

        F.lit(0.0)
    )
    .when(
        F.col("sentiment_normalized")
        == "mixed",

        F.lit(0.0)
    )
    .when(
        F.col("sentiment_normalized")
        == "negative",

        F.lit(-1.0)
    )
)


# =============================================================================
# 24. SENTIMENT BUCKET
#
# neutral/positive racunamo kao positive-leaning.
# =============================================================================

impact_df = impact_df.withColumn(
    "sentiment_bucket",

    F.when(
        F.col("sentiment_normalized")
        .isin(
            "positive",
            "neutral/positive"
        ),

        F.lit("POSITIVE")
    )
    .when(
        F.col("sentiment_normalized")
        == "negative",

        F.lit("NEGATIVE")
    )
    .when(
        F.col("sentiment_normalized")
        == "neutral",

        F.lit("NEUTRAL")
    )
    .when(
        F.col("sentiment_normalized")
        == "mixed",

        F.lit("MIXED")
    )
    .otherwise(
        F.lit("UNKNOWN")
    )
)


# =============================================================================
# 25. WEIGHTED SENTIMENT
#
# Sentiment * temporal proximity.
# =============================================================================

impact_df = impact_df.withColumn(
    "weighted_sentiment_score",

    F.when(
        F.col("sentiment_score")
        .isNotNull(),

        F.col("sentiment_score")
        * F.col(
            "temporal_relevance_score"
        )
    )
)


# =============================================================================
# 26. POTENTIAL CONTEXT SENTIMENT
#
# Za explanatory analizu koristimo samo:
#
# PRE_EVENT
# SAME_DAY
#
# POST_EVENT ne tretiramo kao potencijalni uzrok.
# =============================================================================

impact_df = impact_df.withColumn(
    "potential_context_sentiment_score",

    F.when(
        F.col("is_potential_context")
        &
        F.col(
            "weighted_sentiment_score"
        ).isNotNull(),

        F.col(
            "weighted_sentiment_score"
        )
    )
)


# =============================================================================
# 27. NEWS RELEVANCE CLASS
# =============================================================================

impact_df = impact_df.withColumn(
    "news_relevance",

    F.when(
        F.col(
            "temporal_relevance_score"
        ) >= 1.0,

        F.lit("VERY_HIGH")
    )
    .when(
        F.col(
            "temporal_relevance_score"
        ) >= 0.85,

        F.lit("HIGH")
    )
    .when(
        F.col(
            "temporal_relevance_score"
        ) >= 0.65,

        F.lit("MEDIUM")
    )
    .otherwise(
        F.lit("LOW")
    )
)


# =============================================================================
# 28. PROCESSING TIMESTAMP
# =============================================================================

impact_df = impact_df.withColumn(
    "_news_impact_processed_at",
    F.current_timestamp()
)


# =============================================================================
# 29. FINAL DEDUPLICATION
#
# Jedan article_id moze jednom pripadati jednom anomaly eventu.
# =============================================================================

impact_df = impact_df.dropDuplicates(
    [
        "anomaly_event_id",
        "article_id"
    ]
)


# =============================================================================
# 30. FINAL IMPACT COLUMN ORDER
# =============================================================================

impact_df = impact_df.select(

    "anomaly_event_id",

    "ticker",
    "anomaly_date",

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

    "article_id",

    "news_ticker",
    "requested_tickers",
    "ticker_match_source",

    "news_title",
    "news_author",
    "news_description",

    "article_url",
    "image_url",

    "published_utc",
    "published_date",

    "publisher_name",

    "news_tickers",
    "news_keywords",

    "sentiment",
    "sentiment_normalized",
    "sentiment_bucket",
    "sentiment_reasoning",
    "sentiment_source",

    "news_day_offset",
    "temporal_relation",
    "event_relation",

    "is_potential_context",

    "temporal_relevance_score",

    "sentiment_score",
    "weighted_sentiment_score",
    "potential_context_sentiment_score",

    "news_relevance",

    "news_source",

    "_news_impact_processed_at"
)


# =============================================================================
# 31. WRITE DETAILED IMPACT TABLE
# =============================================================================

(
    impact_df.write
    .format("delta")
    .mode("overwrite")
    .option(
        "overwriteSchema",
        "true"
    )
    .saveAsTable(
        IMPACT_TABLE
    )
)


print()
print(f"[OK] Kreirana tabela: {IMPACT_TABLE}")


# =============================================================================
# 32. CREATE NEWS SUMMARY PER ANOMALY
# =============================================================================

news_summary_df = (
    impact_df
    .groupBy(
        "anomaly_event_id"
    )
    .agg(

        # ---------------------------------------------------------------------
        # TOTAL NEWS
        # ---------------------------------------------------------------------

        F.countDistinct(
            "article_id"
        ).alias(
            "related_news_count"
        ),


        # ---------------------------------------------------------------------
        # TEMPORAL DISTRIBUTION
        # ---------------------------------------------------------------------

        F.sum(
            F.when(
                F.col("event_relation")
                == "PRE_EVENT",

                1
            ).otherwise(0)
        ).alias(
            "pre_event_news_count"
        ),

        F.sum(
            F.when(
                F.col("event_relation")
                == "SAME_DAY",

                1
            ).otherwise(0)
        ).alias(
            "same_day_news_count"
        ),

        F.sum(
            F.when(
                F.col("event_relation")
                == "POST_EVENT",

                1
            ).otherwise(0)
        ).alias(
            "post_event_news_count"
        ),

        F.sum(
            F.when(
                F.col("is_potential_context"),
                1
            ).otherwise(0)
        ).alias(
            "potential_context_news_count"
        ),


        # ---------------------------------------------------------------------
        # SENTIMENT DISTRIBUTION
        # ---------------------------------------------------------------------

        F.sum(
            F.when(
                F.col("sentiment_bucket")
                == "POSITIVE",

                1
            ).otherwise(0)
        ).alias(
            "positive_news_count"
        ),

        F.sum(
            F.when(
                F.col("sentiment_bucket")
                == "NEGATIVE",

                1
            ).otherwise(0)
        ).alias(
            "negative_news_count"
        ),

        F.sum(
            F.when(
                F.col("sentiment_bucket")
                == "NEUTRAL",

                1
            ).otherwise(0)
        ).alias(
            "neutral_news_count"
        ),

        F.sum(
            F.when(
                F.col("sentiment_bucket")
                == "MIXED",

                1
            ).otherwise(0)
        ).alias(
            "mixed_news_count"
        ),

        F.sum(
            F.when(
                F.col("sentiment_bucket")
                == "UNKNOWN",

                1
            ).otherwise(0)
        ).alias(
            "unknown_sentiment_news_count"
        ),

        F.sum(
            F.when(
                F.col("sentiment_normalized")
                == "neutral/positive",

                1
            ).otherwise(0)
        ).alias(
            "neutral_positive_news_count"
        ),


        # ---------------------------------------------------------------------
        # SENTIMENT SCORES
        # ---------------------------------------------------------------------

        F.round(
            F.avg(
                "sentiment_score"
            ),
            4
        ).alias(
            "avg_sentiment_score"
        ),

        F.round(
            F.avg(
                "weighted_sentiment_score"
            ),
            4
        ).alias(
            "avg_weighted_sentiment_score"
        ),

        F.round(
            F.avg(
                "potential_context_sentiment_score"
            ),
            4
        ).alias(
            "avg_potential_context_sentiment_score"
        ),


        # ---------------------------------------------------------------------
        # RELEVANCE
        # ---------------------------------------------------------------------

        F.round(
            F.max(
                "temporal_relevance_score"
            ),
            2
        ).alias(
            "max_temporal_relevance_score"
        ),

        F.round(
            F.max(
                F.when(
                    F.col(
                        "is_potential_context"
                    ),

                    F.col(
                        "temporal_relevance_score"
                    )
                )
            ),
            2
        ).alias(
            "max_potential_context_relevance_score"
        )
    )
)


# =============================================================================
# 33. BASE ANOMALY SUMMARY
#
# LEFT JOIN je obavezan.
#
# Svaka anomaly mora ostati u summary tabeli,
# cak i kada nema nijednu vijest.
# =============================================================================

summary_base_df = anomalies_df.select(

    "anomaly_event_id",

    "ticker",

    F.col(
        "date"
    ).alias(
        "anomaly_date"
    ),

    "close",
    "daily_return_pct",

    "anomaly_score",
    "anomaly_severity",
    "anomaly_type",

    "market_condition"
)


summary_df = (
    summary_base_df
    .join(
        news_summary_df,
        "anomaly_event_id",
        "left"
    )
)


# =============================================================================
# 34. FILL MISSING COUNTS
# =============================================================================

count_columns = [

    "related_news_count",

    "pre_event_news_count",
    "same_day_news_count",
    "post_event_news_count",

    "potential_context_news_count",

    "positive_news_count",
    "negative_news_count",
    "neutral_news_count",
    "mixed_news_count",
    "unknown_sentiment_news_count",

    "neutral_positive_news_count"
]


for column_name in count_columns:

    summary_df = summary_df.withColumn(
        column_name,

        F.coalesce(
            F.col(column_name),
            F.lit(0)
        )
    )


# =============================================================================
# 35. HAS RELATED NEWS
# =============================================================================

summary_df = summary_df.withColumn(
    "has_related_news",

    F.col(
        "related_news_count"
    ) > 0
)


# =============================================================================
# 36. HAS POTENTIAL EXPLANATORY CONTEXT
#
# Znaci da postoji barem jedna PRE_EVENT ili SAME_DAY vijest.
# =============================================================================

summary_df = summary_df.withColumn(
    "has_potential_context_news",

    F.col(
        "potential_context_news_count"
    ) > 0
)


# =============================================================================
# 37. REACTION NEWS COUNT
# =============================================================================

summary_df = summary_df.withColumn(
    "reaction_news_count",

    F.col(
        "post_event_news_count"
    )
)


# =============================================================================
# 38. DOMINANT SENTIMENT
# =============================================================================

summary_df = summary_df.withColumn(
    "dominant_sentiment",

    # -------------------------------------------------------------------------
    # No news
    # -------------------------------------------------------------------------

    F.when(
        F.col(
            "related_news_count"
        ) == 0,

        F.lit("NO_NEWS")
    )

    # -------------------------------------------------------------------------
    # Sve sentiment vrijednosti nepoznate
    # -------------------------------------------------------------------------

    .when(
        F.col(
            "unknown_sentiment_news_count"
        )
        ==
        F.col(
            "related_news_count"
        ),

        F.lit("UNKNOWN")
    )

    # -------------------------------------------------------------------------
    # Positive dominant
    # -------------------------------------------------------------------------

    .when(
        (
            F.col(
                "positive_news_count"
            )
            >
            F.col(
                "negative_news_count"
            )
        )
        &
        (
            F.col(
                "positive_news_count"
            )
            >
            F.col(
                "neutral_news_count"
            )
        )
        &
        (
            F.col(
                "positive_news_count"
            )
            >
            F.col(
                "mixed_news_count"
            )
        ),

        F.lit("POSITIVE")
    )

    # -------------------------------------------------------------------------
    # Negative dominant
    # -------------------------------------------------------------------------

    .when(
        (
            F.col(
                "negative_news_count"
            )
            >
            F.col(
                "positive_news_count"
            )
        )
        &
        (
            F.col(
                "negative_news_count"
            )
            >
            F.col(
                "neutral_news_count"
            )
        )
        &
        (
            F.col(
                "negative_news_count"
            )
            >
            F.col(
                "mixed_news_count"
            )
        ),

        F.lit("NEGATIVE")
    )

    # -------------------------------------------------------------------------
    # Neutral dominant
    # -------------------------------------------------------------------------

    .when(
        (
            F.col(
                "neutral_news_count"
            )
            >
            F.col(
                "positive_news_count"
            )
        )
        &
        (
            F.col(
                "neutral_news_count"
            )
            >
            F.col(
                "negative_news_count"
            )
        )
        &
        (
            F.col(
                "neutral_news_count"
            )
            >
            F.col(
                "mixed_news_count"
            )
        ),

        F.lit("NEUTRAL")
    )

    # -------------------------------------------------------------------------
    # Otherwise mixed/tied
    # -------------------------------------------------------------------------

    .otherwise(
        F.lit("MIXED")
    )
)


# =============================================================================
# 39. NEWS COVERAGE CLASS
# =============================================================================

summary_df = summary_df.withColumn(
    "news_coverage",

    F.when(
        F.col(
            "related_news_count"
        ) == 0,

        F.lit("NO_COVERAGE")
    )
    .when(
        F.col(
            "related_news_count"
        ) <= 2,

        F.lit("LOW")
    )
    .when(
        F.col(
            "related_news_count"
        ) <= 5,

        F.lit("MEDIUM")
    )
    .otherwise(
        F.lit("HIGH")
    )
)


# =============================================================================
# 40. POTENTIAL CONTEXT COVERAGE
# =============================================================================

summary_df = summary_df.withColumn(
    "potential_context_coverage",

    F.when(
        F.col(
            "potential_context_news_count"
        ) == 0,

        F.lit("NO_CONTEXT")
    )
    .when(
        F.col(
            "potential_context_news_count"
        ) <= 2,

        F.lit("LOW")
    )
    .when(
        F.col(
            "potential_context_news_count"
        ) <= 5,

        F.lit("MEDIUM")
    )
    .otherwise(
        F.lit("HIGH")
    )
)


# =============================================================================
# 41. PROCESSING TIMESTAMP
# =============================================================================

summary_df = summary_df.withColumn(
    "_news_summary_processed_at",
    F.current_timestamp()
)


# =============================================================================
# 42. FINAL SUMMARY COLUMN ORDER
# =============================================================================

summary_df = summary_df.select(

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
    "mixed_news_count",
    "unknown_sentiment_news_count",

    "neutral_positive_news_count",

    "avg_sentiment_score",
    "avg_weighted_sentiment_score",
    "avg_potential_context_sentiment_score",

    "dominant_sentiment",

    "max_temporal_relevance_score",
    "max_potential_context_relevance_score",

    "news_coverage",
    "potential_context_coverage",

    "_news_summary_processed_at"
)


# =============================================================================
# 43. WRITE SUMMARY TABLE
# =============================================================================

(
    summary_df.write
    .format("delta")
    .mode("overwrite")
    .option(
        "overwriteSchema",
        "true"
    )
    .saveAsTable(
        SUMMARY_TABLE
    )
)


print()
print(f"[OK] Kreirana tabela: {SUMMARY_TABLE}")


# =============================================================================
# 44. LOAD WRITTEN RESULTS
# =============================================================================

impact_result_df = spark.table(
    IMPACT_TABLE
)

summary_result_df = spark.table(
    SUMMARY_TABLE
)


impact_count = impact_result_df.count()
summary_count = summary_result_df.count()


anomalies_with_news = (
    summary_result_df
    .filter(
        F.col("has_related_news")
    )
    .count()
)


anomalies_without_news = (
    summary_result_df
    .filter(
        ~F.col("has_related_news")
    )
    .count()
)


anomalies_with_context = (
    summary_result_df
    .filter(
        F.col(
            "has_potential_context_news"
        )
    )
    .count()
)


# =============================================================================
# 45. BASIC DATA QUALITY CHECKS
# =============================================================================

impact_duplicates = (
    impact_result_df
    .groupBy(
        "anomaly_event_id",
        "article_id"
    )
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)


invalid_offsets = (
    impact_result_df
    .filter(
        F.abs(
            F.col("news_day_offset")
        ) > NEWS_WINDOW_DAYS
    )
    .count()
)


null_impact_keys = (
    impact_result_df
    .filter(
        F.col(
            "anomaly_event_id"
        ).isNull()
        |
        F.col(
            "article_id"
        ).isNull()
        |
        F.col(
            "ticker"
        ).isNull()
    )
    .count()
)


summary_duplicates = (
    summary_result_df
    .groupBy(
        "anomaly_event_id"
    )
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)


# =============================================================================
# 46. NEWS IMPACT SUMMARY
# =============================================================================

print()
print("=" * 100)
print("NEWS IMPACT SUMMARY")
print("=" * 100)

print(
    f"Finalnih anomalija:          "
    f"{final_anomaly_count}"
)

print(
    f"Anomaly-news veza:           "
    f"{impact_count}"
)

print(
    f"Summary redova:              "
    f"{summary_count}"
)

print(
    f"Anomalija sa vijestima:      "
    f"{anomalies_with_news}"
)

print(
    f"Anomalija bez vijesti:       "
    f"{anomalies_without_news}"
)

print(
    f"Sa potential context news:   "
    f"{anomalies_with_context}"
)


# =============================================================================
# 47. NEWS COVERAGE
# =============================================================================

if final_anomaly_count > 0:

    coverage_pct = (
        anomalies_with_news
        / final_anomaly_count
    ) * 100


    context_coverage_pct = (
        anomalies_with_context
        / final_anomaly_count
    ) * 100


    print(
        f"News coverage anomalija:     "
        f"{coverage_pct:.2f}%"
    )

    print(
        f"Potential context coverage:  "
        f"{context_coverage_pct:.2f}%"
    )


# =============================================================================
# 48. BASIC VALIDATION OUTPUT
# =============================================================================

print()
print("=" * 100)
print("BASIC VALIDATION")
print("=" * 100)


if summary_count == final_anomaly_count:

    print(
        "[PASS] Summary ima tacno jedan red po finalnoj anomaliji"
    )

else:

    print(
        "[FAIL] Summary broj redova nije jednak broju anomalija"
    )


if impact_duplicates == 0:

    print(
        "[PASS] Nema duplikata po "
        "(anomaly_event_id, article_id)"
    )

else:

    print(
        f"[FAIL] Impact duplikata: "
        f"{impact_duplicates}"
    )


if summary_duplicates == 0:

    print(
        "[PASS] Nema duplicate anomaly_event_id u summary tabeli"
    )

else:

    print(
        f"[FAIL] Summary duplikata: "
        f"{summary_duplicates}"
    )


if invalid_offsets == 0:

    print(
        "[PASS] Sve news veze su unutar definisanog "
        "+/-3 day window-a"
    )

else:

    print(
        f"[FAIL] Veza izvan window-a: "
        f"{invalid_offsets}"
    )


if null_impact_keys == 0:

    print(
        "[PASS] Impact poslovni kljucevi nisu NULL"
    )

else:

    print(
        f"[FAIL] NULL impact kljuceva: "
        f"{null_impact_keys}"
    )


# =============================================================================
# 49. NEWS RELATION DISTRIBUTION
# =============================================================================

print()
print("=" * 100)
print("VREMENSKI ODNOS VIJESTI I ANOMALIJA")
print("=" * 100)

(
    impact_result_df
    .groupBy(
        "event_relation"
    )
    .count()
    .orderBy(
        F.col("count").desc()
    )
    .show(
        truncate=False
    )
)


# =============================================================================
# 50. SENTIMENT DISTRIBUTION
# =============================================================================

print()
print("=" * 100)
print("NEWS SENTIMENT DISTRIBUCIJA")
print("=" * 100)

(
    impact_result_df
    .groupBy(
        "sentiment_bucket"
    )
    .count()
    .orderBy(
        F.col("count").desc()
    )
    .show(
        truncate=False
    )
)


# =============================================================================
# 51. SENTIMENT SOURCE DISTRIBUTION
# =============================================================================

print()
print("=" * 100)
print("SENTIMENT SOURCE DISTRIBUCIJA")
print("=" * 100)

(
    impact_result_df
    .groupBy(
        "sentiment_source"
    )
    .count()
    .orderBy(
        F.col("count").desc()
    )
    .show(
        truncate=False
    )
)


# =============================================================================
# 52. TICKER MATCH SOURCE DISTRIBUTION
# =============================================================================

print()
print("=" * 100)
print("TICKER MATCH SOURCE DISTRIBUCIJA")
print("=" * 100)

(
    impact_result_df
    .groupBy(
        "ticker_match_source"
    )
    .count()
    .orderBy(
        F.col("count").desc()
    )
    .show(
        truncate=False
    )
)


# =============================================================================
# 53. NEWS COVERAGE BY TICKER
# =============================================================================

print()
print("=" * 100)
print("NEWS COVERAGE PO TICKERU")
print("=" * 100)

(
    summary_result_df
    .groupBy(
        "ticker"
    )
    .agg(

        F.count("*")
        .alias(
            "anomaly_count"
        ),

        F.sum(
            F.col(
                "has_related_news"
            ).cast("int")
        )
        .alias(
            "anomalies_with_news"
        ),

        F.sum(
            F.col(
                "has_potential_context_news"
            ).cast("int")
        )
        .alias(
            "anomalies_with_context"
        ),

        F.sum(
            "related_news_count"
        )
        .alias(
            "related_news"
        )
    )
    .withColumn(
        "coverage_pct",

        F.round(
            (
                F.col(
                    "anomalies_with_news"
                )
                /
                F.col(
                    "anomaly_count"
                )
            ) * 100,
            2
        )
    )
    .withColumn(
        "context_coverage_pct",

        F.round(
            (
                F.col(
                    "anomalies_with_context"
                )
                /
                F.col(
                    "anomaly_count"
                )
            ) * 100,
            2
        )
    )
    .orderBy(
        "ticker"
    )
    .show(
        n=100,
        truncate=False
    )
)


# =============================================================================
# 54. TOP ANOMALIES WITH NEWS CONTEXT
# =============================================================================

print()
print("=" * 100)
print("TOP ANOMALIJE SA NEWS KONTEKSTOM")
print("=" * 100)

(
    summary_result_df
    .filter(
        F.col(
            "has_related_news"
        )
    )
    .select(

        "ticker",
        "anomaly_date",

        "daily_return_pct",

        "anomaly_score",
        "anomaly_severity",
        "anomaly_type",

        "related_news_count",

        "pre_event_news_count",
        "same_day_news_count",
        "post_event_news_count",

        "potential_context_news_count",

        "positive_news_count",
        "negative_news_count",
        "neutral_news_count",
        "mixed_news_count",

        "dominant_sentiment",

        "avg_potential_context_sentiment_score",

        "news_coverage",
        "potential_context_coverage"
    )
    .orderBy(
        F.col(
            "anomaly_score"
        ).desc()
    )
    .show(
        n=30,
        truncate=False
    )
)


# =============================================================================
# 55. MOST RELEVANT POTENTIAL CONTEXT NEWS
#
# PRE_EVENT i SAME_DAY imaju prednost.
# =============================================================================

print()
print("=" * 100)
print("NAJRELEVANTNIJE POTENTIAL CONTEXT VIJESTI")
print("=" * 100)

(
    impact_result_df
    .filter(
        F.col(
            "is_potential_context"
        )
    )
    .select(

        "ticker",
        "anomaly_date",

        "daily_return_pct",
        "anomaly_score",
        "anomaly_severity",

        "published_date",
        "news_day_offset",

        "event_relation",
        "temporal_relevance_score",

        "sentiment",
        "sentiment_bucket",
        "sentiment_source",

        "publisher_name",
        "news_title"
    )
    .orderBy(

        F.col(
            "anomaly_score"
        ).desc(),

        F.col(
            "temporal_relevance_score"
        ).desc(),

        F.col(
            "published_utc"
        ).desc()
    )
    .show(
        n=50,
        truncate=False
    )
)


# =============================================================================
# 56. POST-EVENT REACTION EXAMPLES
# =============================================================================

print()
print("=" * 100)
print("POST-EVENT NEWS REACTION EXAMPLES")
print("=" * 100)

(
    impact_result_df
    .filter(
        F.col(
            "event_relation"
        ) == "POST_EVENT"
    )
    .select(

        "ticker",
        "anomaly_date",

        "daily_return_pct",
        "anomaly_score",

        "published_date",
        "news_day_offset",

        "sentiment_bucket",

        "publisher_name",
        "news_title"
    )
    .orderBy(

        F.col(
            "anomaly_score"
        ).desc(),

        F.col(
            "temporal_relevance_score"
        ).desc()
    )
    .show(
        n=30,
        truncate=False
    )
)


# =============================================================================
# 57. FINAL
# =============================================================================

print()
print("=" * 100)
print("GOLD STOCK ANOMALY NEWS IMPACT COMPLETED")
print("=" * 100)