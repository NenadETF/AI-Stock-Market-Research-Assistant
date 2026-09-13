from databricks.connect import DatabricksSession
from pyspark.sql import functions as F


# =============================================================================
# CONFIGURATION
# =============================================================================

ANOMALY_TABLE = "workspace.gold.stock_anomalies"
IMPACT_TABLE = "workspace.gold.stock_news_impact"
SUMMARY_TABLE = "workspace.gold.stock_anomaly_news_summary"

NEWS_WINDOW_DAYS = 3


# =============================================================================
# SPARK
# =============================================================================

spark = DatabricksSession.builder.getOrCreate()


print("=" * 100)
print("FINAL STOCK ANOMALY NEWS IMPACT VALIDATION")
print("=" * 100)


anomaly_df = (
    spark.table(ANOMALY_TABLE)
    .filter(F.col("is_anomaly"))
)

impact_df = spark.table(IMPACT_TABLE)
summary_df = spark.table(SUMMARY_TABLE)


# =============================================================================
# HELPER
# =============================================================================

def validate(condition, message, details=None):

    if condition:
        print(f"[PASS] {message}")
    else:
        print(f"[FAIL] {message}")

    if details is not None:
        print(f"       {details}")


# =============================================================================
# 1. TABLE COUNTS
# =============================================================================

print()
print("=" * 100)
print("1. TABLE COUNTS")
print("=" * 100)


anomaly_count = anomaly_df.count()
impact_count = impact_df.count()
summary_count = summary_df.count()


print(f"Final anomalies: {anomaly_count}")
print(f"News impact:     {impact_count}")
print(f"News summary:    {summary_count}")


validate(
    anomaly_count > 0,
    "Postoje finalne anomalije"
)

validate(
    impact_count > 0,
    "Impact tabela nije prazna"
)

validate(
    summary_count == anomaly_count,
    "Summary ima tacno jedan red po finalnoj anomaliji",
    f"Anomalije: {anomaly_count}, summary: {summary_count}"
)


# =============================================================================
# 2. IMPACT BUSINESS KEYS
# =============================================================================

print()
print("=" * 100)
print("2. IMPACT BUSINESS KEYS")
print("=" * 100)


null_impact_keys = (
    impact_df
    .filter(
        F.col("anomaly_event_id").isNull()
        | F.col("article_id").isNull()
        | F.col("ticker").isNull()
        | F.col("news_ticker").isNull()
        | F.col("anomaly_date").isNull()
        | F.col("published_date").isNull()
    )
    .count()
)


validate(
    null_impact_keys == 0,
    "Impact tabela nema NULL poslovne kljuceve",
    f"NULL redova: {null_impact_keys}"
)


impact_duplicates = (
    impact_df
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


validate(
    impact_duplicates == 0,
    "Nema duplikata po (anomaly_event_id, article_id)",
    f"Duplikata: {impact_duplicates}"
)


# =============================================================================
# 3. SUMMARY BUSINESS KEY
# =============================================================================

print()
print("=" * 100)
print("3. SUMMARY BUSINESS KEY")
print("=" * 100)


summary_null_keys = (
    summary_df
    .filter(
        F.col("anomaly_event_id").isNull()
        | F.col("ticker").isNull()
        | F.col("anomaly_date").isNull()
    )
    .count()
)


validate(
    summary_null_keys == 0,
    "Summary tabela nema NULL poslovne kljuceve",
    f"NULL redova: {summary_null_keys}"
)


summary_duplicates = (
    summary_df
    .groupBy("anomaly_event_id")
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)


validate(
    summary_duplicates == 0,
    "Summary ima jedinstven anomaly_event_id",
    f"Duplikata: {summary_duplicates}"
)


# =============================================================================
# 4. TICKER RELATION
# =============================================================================

print()
print("=" * 100)
print("4. TICKER RELATION VALIDATION")
print("=" * 100)


ticker_mismatch = (
    impact_df
    .filter(
        F.col("ticker")
        != F.col("news_ticker")
    )
    .count()
)


validate(
    ticker_mismatch == 0,
    "Svaka vijest je povezana sa odgovarajucim anomaly tickerom",
    f"Ticker mismatch redova: {ticker_mismatch}"
)


invalid_article_ticker_matches = (
    impact_df
    .filter(
        ~F.array_contains(
            F.col("news_tickers"),
            F.col("ticker")
        )
    )
    .count()
)


validate(
    invalid_article_ticker_matches == 0,
    "Anomaly ticker postoji u tickers listi svakog povezanog clanka",
    f"Nevalidnih veza: {invalid_article_ticker_matches}"
)


# =============================================================================
# 5. TEMPORAL WINDOW
# =============================================================================

print()
print("=" * 100)
print("5. TEMPORAL WINDOW VALIDATION")
print("=" * 100)


invalid_offsets = (
    impact_df
    .filter(
        F.abs(
            F.col("news_day_offset")
        ) > NEWS_WINDOW_DAYS
    )
    .count()
)


validate(
    invalid_offsets == 0,
    "Sve vijesti su unutar +/-3 dana od anomalije",
    f"Veza izvan window-a: {invalid_offsets}"
)


invalid_relation = (
    impact_df
    .filter(

        (
            (F.col("news_day_offset") < 0)
            & (F.col("event_relation") != "PRE_EVENT")
        )

        |

        (
            (F.col("news_day_offset") == 0)
            & (F.col("event_relation") != "SAME_DAY")
        )

        |

        (
            (F.col("news_day_offset") > 0)
            & (F.col("event_relation") != "POST_EVENT")
        )
    )
    .count()
)


validate(
    invalid_relation == 0,
    "event_relation odgovara news_day_offset vrijednosti",
    f"Nekonzistentnih redova: {invalid_relation}"
)


# =============================================================================
# 6. POTENTIAL CONTEXT LOGIC
# =============================================================================

print()
print("=" * 100)
print("6. POTENTIAL CONTEXT VALIDATION")
print("=" * 100)


invalid_context_flags = (
    impact_df
    .filter(

        F.col("is_potential_context")
        !=
        F.col("event_relation").isin(
            "PRE_EVENT",
            "SAME_DAY"
        )
    )
    .count()
)


validate(
    invalid_context_flags == 0,
    "is_potential_context odgovara PRE_EVENT/SAME_DAY pravilima",
    f"Nekonzistentnih redova: {invalid_context_flags}"
)


post_event_context_errors = (
    impact_df
    .filter(
        (F.col("event_relation") == "POST_EVENT")
        & F.col("is_potential_context")
    )
    .count()
)


validate(
    post_event_context_errors == 0,
    "POST_EVENT vijesti se ne tretiraju kao potential explanatory context",
    f"Gresaka: {post_event_context_errors}"
)


# =============================================================================
# 7. TEMPORAL RELEVANCE SCORE
# =============================================================================

print()
print("=" * 100)
print("7. TEMPORAL RELEVANCE SCORE")
print("=" * 100)


invalid_relevance_scores = (
    impact_df
    .filter(
        F.col("temporal_relevance_score").isNull()
        | (F.col("temporal_relevance_score") < 0)
        | (F.col("temporal_relevance_score") > 1)
    )
    .count()
)


validate(
    invalid_relevance_scores == 0,
    "temporal_relevance_score je u rasponu 0-1",
    f"Nevalidnih vrijednosti: {invalid_relevance_scores}"
)


invalid_exact_relevance = (
    impact_df
    .filter(

        (
            (F.abs(F.col("news_day_offset")) == 0)
            & (F.col("temporal_relevance_score") != 1.00)
        )

        |

        (
            (F.abs(F.col("news_day_offset")) == 1)
            & (F.col("temporal_relevance_score") != 0.85)
        )

        |

        (
            (F.abs(F.col("news_day_offset")) == 2)
            & (F.col("temporal_relevance_score") != 0.65)
        )

        |

        (
            (F.abs(F.col("news_day_offset")) == 3)
            & (F.col("temporal_relevance_score") != 0.50)
        )
    )
    .count()
)


validate(
    invalid_exact_relevance == 0,
    "Temporal relevance koristi definisane tezine",
    f"Nekonzistentnih vrijednosti: {invalid_exact_relevance}"
)


# =============================================================================
# 8. SENTIMENT SCORE
# =============================================================================

print()
print("=" * 100)
print("8. SENTIMENT VALIDATION")
print("=" * 100)


invalid_sentiment_scores = (
    impact_df
    .filter(
        F.col("sentiment_score").isNotNull()
        &
        (
            (F.col("sentiment_score") < -1)
            | (F.col("sentiment_score") > 1)
        )
    )
    .count()
)


validate(
    invalid_sentiment_scores == 0,
    "sentiment_score je u rasponu -1 do +1",
    f"Nevalidnih vrijednosti: {invalid_sentiment_scores}"
)


unknown_sentiments = (
    impact_df
    .filter(
        F.col("sentiment_bucket") == "UNKNOWN"
    )
    .count()
)


validate(
    unknown_sentiments == 0,
    "Sve povezane vijesti imaju prepoznat sentiment",
    f"UNKNOWN sentiment redova: {unknown_sentiments}"
)


# =============================================================================
# 9. SENTIMENT SOURCE
# =============================================================================

print()
print("=" * 100)
print("9. SENTIMENT SOURCE VALIDATION")
print("=" * 100)


no_sentiment_source = (
    impact_df
    .filter(
        F.col("sentiment_source") == "NONE"
    )
    .count()
)


validate(
    no_sentiment_source == 0,
    "Sve vijesti imaju poznat sentiment source",
    f"NONE source redova: {no_sentiment_source}"
)


print()
print("SENTIMENT SOURCE:")

(
    impact_df
    .groupBy("sentiment_source")
    .count()
    .orderBy(
        F.col("count").desc()
    )
    .show(
        truncate=False
    )
)


# =============================================================================
# 10. TICKER MATCH SOURCE
# =============================================================================

print()
print("=" * 100)
print("10. TICKER MATCH SOURCE")
print("=" * 100)


fallback_matches = (
    impact_df
    .filter(
        F.col("ticker_match_source")
        == "REQUESTED_TICKER_FALLBACK"
    )
    .count()
)


print(
    f"ARTICLE_TICKERS/FALLBACK analiza:"
)

(
    impact_df
    .groupBy("ticker_match_source")
    .count()
    .show(
        truncate=False
    )
)


validate(
    fallback_matches == 0,
    "Sve Gold veze koriste stvarne ARTICLE_TICKERS vrijednosti",
    f"Fallback veza: {fallback_matches}"
)


# =============================================================================
# 11. SUMMARY COUNT CONSISTENCY
# =============================================================================

print()
print("=" * 100)
print("11. SUMMARY COUNT CONSISTENCY")
print("=" * 100)


invalid_total_counts = (
    summary_df
    .filter(

        F.col("related_news_count")
        !=
        (
            F.col("pre_event_news_count")
            + F.col("same_day_news_count")
            + F.col("post_event_news_count")
        )
    )
    .count()
)


validate(
    invalid_total_counts == 0,
    "related_news_count odgovara PRE + SAME + POST zbiru",
    f"Nekonzistentnih anomalija: {invalid_total_counts}"
)


invalid_context_counts = (
    summary_df
    .filter(

        F.col("potential_context_news_count")
        !=
        (
            F.col("pre_event_news_count")
            + F.col("same_day_news_count")
        )
    )
    .count()
)


validate(
    invalid_context_counts == 0,
    "potential_context_news_count = PRE_EVENT + SAME_DAY",
    f"Nekonzistentnih anomalija: {invalid_context_counts}"
)


invalid_reaction_counts = (
    summary_df
    .filter(
        F.col("reaction_news_count")
        != F.col("post_event_news_count")
    )
    .count()
)


validate(
    invalid_reaction_counts == 0,
    "reaction_news_count odgovara POST_EVENT broju",
    f"Nekonzistentnih anomalija: {invalid_reaction_counts}"
)


# =============================================================================
# 12. HAS NEWS FLAGS
# =============================================================================

print()
print("=" * 100)
print("12. NEWS FLAG CONSISTENCY")
print("=" * 100)


invalid_news_flags = (
    summary_df
    .filter(
        F.col("has_related_news")
        != (F.col("related_news_count") > 0)
    )
    .count()
)


validate(
    invalid_news_flags == 0,
    "has_related_news odgovara related_news_count",
    f"Nekonzistentnih redova: {invalid_news_flags}"
)


invalid_context_news_flags = (
    summary_df
    .filter(
        F.col("has_potential_context_news")
        != (
            F.col("potential_context_news_count") > 0
        )
    )
    .count()
)


validate(
    invalid_context_news_flags == 0,
    "has_potential_context_news odgovara context count-u",
    f"Nekonzistentnih redova: {invalid_context_news_flags}"
)


# =============================================================================
# 13. COVERAGE
# =============================================================================

print()
print("=" * 100)
print("13. NEWS COVERAGE")
print("=" * 100)


with_news = (
    summary_df
    .filter(
        F.col("has_related_news")
    )
    .count()
)


with_context = (
    summary_df
    .filter(
        F.col("has_potential_context_news")
    )
    .count()
)


coverage_pct = (
    with_news
    / anomaly_count
    * 100
)


context_coverage_pct = (
    with_context
    / anomaly_count
    * 100
)


print(
    f"News coverage:              "
    f"{coverage_pct:.2f}%"
)

print(
    f"Potential context coverage: "
    f"{context_coverage_pct:.2f}%"
)


validate(
    coverage_pct == 100.0,
    "Sve finalne anomalije imaju news coverage"
)


validate(
    context_coverage_pct == 100.0,
    "Sve finalne anomalije imaju potential explanatory context"
)


# =============================================================================
# 14. COVERAGE BY TICKER
# =============================================================================

print()
print("=" * 100)
print("14. NEWS COVERAGE BY TICKER")
print("=" * 100)


ticker_coverage_df = (
    summary_df
    .groupBy("ticker")
    .agg(

        F.count("*")
        .alias("anomaly_count"),

        F.sum(
            F.col(
                "has_related_news"
            ).cast("int")
        )
        .alias(
            "with_news"
        ),

        F.sum(
            F.col(
                "has_potential_context_news"
            ).cast("int")
        )
        .alias(
            "with_context"
        ),

        F.sum(
            "related_news_count"
        )
        .alias(
            "news_links"
        )
    )
    .withColumn(
        "coverage_pct",

        F.round(
            F.col("with_news")
            /
            F.col("anomaly_count")
            * 100,
            2
        )
    )
    .withColumn(
        "context_coverage_pct",

        F.round(
            F.col("with_context")
            /
            F.col("anomaly_count")
            * 100,
            2
        )
    )
    .orderBy("ticker")
)


ticker_coverage_df.show(
    n=100,
    truncate=False
)


# =============================================================================
# 15. NEWS COUNT DISTRIBUTION
# =============================================================================

print()
print("=" * 100)
print("15. NEWS COUNT DISTRIBUTION")
print("=" * 100)


(
    summary_df
    .agg(

        F.min(
            "related_news_count"
        ).alias(
            "min_news_per_anomaly"
        ),

        F.round(
            F.avg(
                "related_news_count"
            ),
            2
        ).alias(
            "avg_news_per_anomaly"
        ),

        F.max(
            "related_news_count"
        ).alias(
            "max_news_per_anomaly"
        ),

        F.min(
            "potential_context_news_count"
        ).alias(
            "min_context_news"
        ),

        F.round(
            F.avg(
                "potential_context_news_count"
            ),
            2
        ).alias(
            "avg_context_news"
        ),

        F.max(
            "potential_context_news_count"
        ).alias(
            "max_context_news"
        )
    )
    .show(
        truncate=False
    )
)


# =============================================================================
# 16. TOP CONTEXT-RICH ANOMALIES
# =============================================================================

print()
print("=" * 100)
print("16. TOP CONTEXT-RICH ANOMALIES")
print("=" * 100)


(
    summary_df
    .select(

        "ticker",
        "anomaly_date",

        "daily_return_pct",

        "anomaly_score",
        "anomaly_severity",

        "related_news_count",
        "potential_context_news_count",

        "positive_news_count",
        "negative_news_count",
        "neutral_news_count",

        "dominant_sentiment",

        "avg_potential_context_sentiment_score"
    )
    .orderBy(
        F.col(
            "potential_context_news_count"
        ).desc()
    )
    .show(
        n=20,
        truncate=False
    )
)


# =============================================================================
# 17. FINAL SUMMARY
# =============================================================================

print()
print("=" * 100)
print("FINAL VALIDATION SUMMARY")
print("=" * 100)


hard_failures = [

    summary_count != anomaly_count,

    null_impact_keys != 0,
    impact_duplicates != 0,

    summary_null_keys != 0,
    summary_duplicates != 0,

    ticker_mismatch != 0,
    invalid_article_ticker_matches != 0,

    invalid_offsets != 0,
    invalid_relation != 0,

    invalid_context_flags != 0,
    post_event_context_errors != 0,

    invalid_relevance_scores != 0,
    invalid_exact_relevance != 0,

    invalid_sentiment_scores != 0,
    unknown_sentiments != 0,

    no_sentiment_source != 0,

    invalid_total_counts != 0,
    invalid_context_counts != 0,
    invalid_reaction_counts != 0,

    invalid_news_flags != 0,
    invalid_context_news_flags != 0,

    coverage_pct != 100.0,
    context_coverage_pct != 100.0
]


failure_count = sum(
    1
    for failure in hard_failures
    if failure
)


if failure_count == 0:

    print(
        "[PASS] FINAL STOCK ANOMALY NEWS "
        "IMPACT VALIDATION USPJESNA"
    )

else:

    print(
        f"[FAIL] FINAL VALIDATION IMA "
        f"{failure_count} PROBLEMA"
    )


print("=" * 100)