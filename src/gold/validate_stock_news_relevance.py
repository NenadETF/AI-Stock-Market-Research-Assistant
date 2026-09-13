from databricks.connect import DatabricksSession
from pyspark.sql import functions as F


# =============================================================================
# CONFIGURATION
# =============================================================================

IMPACT_TABLE = "workspace.gold.stock_news_impact"
RELEVANCE_TABLE = "workspace.gold.stock_news_relevance"
TOP_NEWS_TABLE = "workspace.gold.stock_anomaly_top_news"

EXPECTED_ANOMALY_COUNT = 155

MIN_TOP_NEWS_PER_ANOMALY = 10
MAX_TOP_NEWS_PER_ANOMALY = 15

MIN_TOP_DIRECT_MATCH_PCT = 90.0
MIN_TOP_TITLE_MATCH_PCT = 75.0


# =============================================================================
# SPARK
# =============================================================================

spark = DatabricksSession.builder.getOrCreate()


print("=" * 100)
print("FINAL STOCK NEWS RELEVANCE VALIDATION")
print("=" * 100)


impact_df = spark.table(IMPACT_TABLE)
relevance_df = spark.table(RELEVANCE_TABLE)
top_df = spark.table(TOP_NEWS_TABLE)


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


impact_count = impact_df.count()
relevance_count = relevance_df.count()
top_count = top_df.count()


anomaly_count = (
    relevance_df
    .select("anomaly_event_id")
    .distinct()
    .count()
)


print(f"Impact rows:       {impact_count}")
print(f"Relevance rows:    {relevance_count}")
print(f"Top AI rows:       {top_count}")
print(f"Anomaly events:    {anomaly_count}")


validate(
    relevance_count == impact_count,
    "Svaka anomaly-news veza je rangirana",
    f"Impact={impact_count}, relevance={relevance_count}"
)


validate(
    anomaly_count == EXPECTED_ANOMALY_COUNT,
    "Relevance tabela sadrzi svih 155 anomaly eventa",
    f"Pronadjeno: {anomaly_count}"
)


validate(
    top_count > 0,
    "Top-news tabela nije prazna"
)


# =============================================================================
# 2. BUSINESS KEYS
# =============================================================================

print()
print("=" * 100)
print("2. BUSINESS KEY VALIDATION")
print("=" * 100)


null_keys = (
    relevance_df
    .filter(
        F.col("anomaly_event_id").isNull()
        |
        F.col("article_id").isNull()
        |
        F.col("ticker").isNull()
        |
        F.col("news_rank").isNull()
    )
    .count()
)


validate(
    null_keys == 0,
    "Relevance tabela nema NULL poslovne kljuceve",
    f"NULL redova: {null_keys}"
)


duplicate_links = (
    relevance_df
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
    duplicate_links == 0,
    "Nema duplicate anomaly-article veza",
    f"Duplikata: {duplicate_links}"
)


# =============================================================================
# 3. RANK VALIDATION
# =============================================================================

print()
print("=" * 100)
print("3. RANK VALIDATION")
print("=" * 100)


duplicate_news_ranks = (
    relevance_df
    .groupBy(
        "anomaly_event_id",
        "news_rank"
    )
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)


validate(
    duplicate_news_ranks == 0,
    "news_rank je jedinstven unutar svakog anomaly eventa",
    f"Duplicate rankova: {duplicate_news_ranks}"
)


invalid_rank_values = (
    relevance_df
    .filter(
        F.col("news_rank").isNull()
        |
        (F.col("news_rank") <= 0)
    )
    .count()
)


validate(
    invalid_rank_values == 0,
    "Svi news_rank brojevi su pozitivni",
    f"Nevalidnih rankova: {invalid_rank_values}"
)


# =============================================================================
# 4. CONTEXT RANK VALIDATION
# =============================================================================

print()
print("=" * 100)
print("4. CONTEXT RANK VALIDATION")
print("=" * 100)


invalid_context_rank = (
    relevance_df
    .filter(
        F.col("context_rank").isNotNull()
        &
        ~F.col("is_potential_context")
    )
    .count()
)


validate(
    invalid_context_rank == 0,
    "context_rank postoji samo za PRE_EVENT/SAME_DAY vijesti",
    f"Nevalidnih redova: {invalid_context_rank}"
)


missing_context_rank = (
    relevance_df
    .filter(
        F.col("is_potential_context")
        &
        F.col("context_rank").isNull()
    )
    .count()
)


validate(
    missing_context_rank == 0,
    "Sve potential-context vijesti imaju context_rank",
    f"Bez context_rank-a: {missing_context_rank}"
)


duplicate_context_ranks = (
    relevance_df
    .filter(
        F.col("context_rank").isNotNull()
    )
    .groupBy(
        "anomaly_event_id",
        "context_rank"
    )
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)


validate(
    duplicate_context_ranks == 0,
    "context_rank je jedinstven unutar anomaly eventa",
    f"Duplikata: {duplicate_context_ranks}"
)


# =============================================================================
# 5. SCORE VALIDATION
# =============================================================================

print()
print("=" * 100)
print("5. SCORE VALIDATION")
print("=" * 100)


invalid_scores = (
    relevance_df
    .filter(
        F.col("news_relevance_score").isNull()
        |
        (F.col("news_relevance_score") < 0)
        |
        (F.col("news_relevance_score") > 100)
    )
    .count()
)


validate(
    invalid_scores == 0,
    "news_relevance_score je u rasponu 0-100",
    f"Nevalidnih score-ova: {invalid_scores}"
)


# =============================================================================
# 6. SCORE COMPONENT CONSISTENCY
# =============================================================================

print()
print("=" * 100)
print("6. SCORE COMPONENT CONSISTENCY")
print("=" * 100)


score_difference = (
    relevance_df
    .withColumn(
        "calculated_score",

        F.round(
            F.col("temporal_component")
            + F.col("event_relation_component")
            + F.col("title_company_component")
            + F.col("description_company_component")
            + F.col("ticker_specificity_component")
            + F.col("sentiment_evidence_component")
            + F.col("sentiment_reasoning_component")
            + F.col("content_quality_component"),
            2
        )
    )
    .filter(
        F.abs(
            F.col("calculated_score")
            -
            F.col("news_relevance_score")
        ) > 0.001
    )
    .count()
)


validate(
    score_difference == 0,
    "Finalni score odgovara zbiru svih V2 komponenti",
    f"Nekonzistentnih redova: {score_difference}"
)


# =============================================================================
# 7. DIRECT MATCH LOGIC
# =============================================================================

print()
print("=" * 100)
print("7. DIRECT COMPANY MATCH LOGIC")
print("=" * 100)


invalid_direct_match = (
    relevance_df
    .filter(
        F.col("direct_company_match")
        !=
        (
            F.col("title_company_match")
            |
            F.col("description_company_match")
        )
    )
    .count()
)


validate(
    invalid_direct_match == 0,
    "direct_company_match odgovara title/description signalima",
    f"Nekonzistentnih redova: {invalid_direct_match}"
)


invalid_direct_levels = (
    relevance_df
    .filter(

        (
            F.col("title_company_match")
            &
            F.col("description_company_match")
            &
            (
                F.col("direct_match_level")
                != "TITLE_AND_DESCRIPTION"
            )
        )

        |

        (
            F.col("title_company_match")
            &
            ~F.col("description_company_match")
            &
            (
                F.col("direct_match_level")
                != "TITLE"
            )
        )

        |

        (
            ~F.col("title_company_match")
            &
            F.col("description_company_match")
            &
            (
                F.col("direct_match_level")
                != "DESCRIPTION"
            )
        )

        |

        (
            ~F.col("title_company_match")
            &
            ~F.col("description_company_match")
            &
            (
                F.col("direct_match_level")
                != "NONE"
            )
        )
    )
    .count()
)


validate(
    invalid_direct_levels == 0,
    "direct_match_level je konzistentan",
    f"Nekonzistentnih redova: {invalid_direct_levels}"
)


# =============================================================================
# 8. RELEVANCE TIER CONSISTENCY
# =============================================================================

print()
print("=" * 100)
print("8. RELEVANCE TIER VALIDATION")
print("=" * 100)


invalid_tiers = (
    relevance_df
    .filter(

        (
            (F.col("news_relevance_score") >= 85)
            &
            (F.col("relevance_tier") != "VERY_HIGH")
        )

        |

        (
            (F.col("news_relevance_score") >= 70)
            &
            (F.col("news_relevance_score") < 85)
            &
            (F.col("relevance_tier") != "HIGH")
        )

        |

        (
            (F.col("news_relevance_score") >= 55)
            &
            (F.col("news_relevance_score") < 70)
            &
            (F.col("relevance_tier") != "MEDIUM")
        )

        |

        (
            (F.col("news_relevance_score") < 55)
            &
            (F.col("relevance_tier") != "LOW")
        )
    )
    .count()
)


validate(
    invalid_tiers == 0,
    "relevance_tier odgovara score pragovima",
    f"Nekonzistentnih redova: {invalid_tiers}"
)


# =============================================================================
# 9. AI USAGE CLASS
# =============================================================================

print()
print("=" * 100)
print("9. AI USAGE CLASS VALIDATION")
print("=" * 100)


invalid_primary = (
    relevance_df
    .filter(
        (F.col("ai_usage_class") == "PRIMARY_CONTEXT")
        &
        ~F.col("is_top_5_context")
    )
    .count()
)


validate(
    invalid_primary == 0,
    "PRIMARY_CONTEXT oznaka odgovara top context pravilima",
    f"Gresaka: {invalid_primary}"
)


invalid_reaction = (
    relevance_df
    .filter(
        (F.col("ai_usage_class") == "REACTION")
        &
        (
            (F.col("event_relation") != "POST_EVENT")
            |
            ~F.col("is_top_10_news")
        )
    )
    .count()
)


validate(
    invalid_reaction == 0,
    "REACTION se koristi samo za top-10 POST_EVENT vijesti",
    f"Gresaka: {invalid_reaction}"
)


# =============================================================================
# 10. TOP TABLE BUSINESS KEYS
# =============================================================================

print()
print("=" * 100)
print("10. TOP TABLE VALIDATION")
print("=" * 100)


top_duplicates = (
    top_df
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
    top_duplicates == 0,
    "Top-news tabela nema duplicate anomaly-article veze",
    f"Duplikata: {top_duplicates}"
)


top_anomaly_count = (
    top_df
    .select("anomaly_event_id")
    .distinct()
    .count()
)


validate(
    top_anomaly_count == EXPECTED_ANOMALY_COUNT,
    "Svaka anomalija postoji u top-news tabeli",
    f"Anomalija: {top_anomaly_count}"
)


# =============================================================================
# 11. TOP NEWS COUNT PER ANOMALY
# =============================================================================

print()
print("=" * 100)
print("11. TOP NEWS COUNT PER ANOMALY")
print("=" * 100)


top_counts_df = (
    top_df
    .groupBy("anomaly_event_id")
    .count()
)


top_stats = (
    top_counts_df
    .agg(
        F.min("count").alias("min_count"),
        F.round(
            F.avg("count"),
            2
        ).alias("avg_count"),
        F.max("count").alias("max_count")
    )
    .first()
)


min_top = top_stats["min_count"]
avg_top = top_stats["avg_count"]
max_top = top_stats["max_count"]


print(f"Min top news: {min_top}")
print(f"Avg top news: {avg_top}")
print(f"Max top news: {max_top}")


validate(
    min_top >= MIN_TOP_NEWS_PER_ANOMALY,
    "Svaka anomalija ima najmanje 10 top vijesti"
)


validate(
    max_top <= MAX_TOP_NEWS_PER_ANOMALY,
    "Top tabela nema nekontrolisan broj clanaka po anomaliji",
    f"Maximum: {max_top}"
)


# =============================================================================
# 12. TOP SEMANTIC QUALITY
# =============================================================================

print()
print("=" * 100)
print("12. TOP NEWS SEMANTIC QUALITY")
print("=" * 100)


top_direct_count = (
    top_df
    .filter(
        F.col("direct_company_match")
    )
    .count()
)


top_title_count = (
    top_df
    .filter(
        F.col("title_company_match")
    )
    .count()
)


direct_pct = (
    top_direct_count
    / top_count
    * 100
)


title_pct = (
    top_title_count
    / top_count
    * 100
)


print(
    f"Direct company match: "
    f"{top_direct_count}/{top_count} "
    f"({direct_pct:.2f}%)"
)


print(
    f"Title company match:  "
    f"{top_title_count}/{top_count} "
    f"({title_pct:.2f}%)"
)


validate(
    direct_pct >= MIN_TOP_DIRECT_MATCH_PCT,
    "Top-news direct company coverage je najmanje 90%",
    f"Coverage: {direct_pct:.2f}%"
)


validate(
    title_pct >= MIN_TOP_TITLE_MATCH_PCT,
    "Najmanje 75% top vijesti direktno pominje kompaniju u naslovu",
    f"Coverage: {title_pct:.2f}%"
)


# =============================================================================
# 13. RANK #1 SEMANTIC QUALITY
# =============================================================================

print()
print("=" * 100)
print("13. RANK #1 SEMANTIC QUALITY")
print("=" * 100)


rank_one_df = (
    relevance_df
    .filter(
        F.col("news_rank") == 1
    )
)


rank_one_count = rank_one_df.count()


rank_one_direct = (
    rank_one_df
    .filter(
        F.col("direct_company_match")
    )
    .count()
)


rank_one_title = (
    rank_one_df
    .filter(
        F.col("title_company_match")
    )
    .count()
)


rank_one_direct_pct = (
    rank_one_direct
    / rank_one_count
    * 100
)


rank_one_title_pct = (
    rank_one_title
    / rank_one_count
    * 100
)


print(
    f"Rank #1 direct match: "
    f"{rank_one_direct}/{rank_one_count} "
    f"({rank_one_direct_pct:.2f}%)"
)


print(
    f"Rank #1 title match:  "
    f"{rank_one_title}/{rank_one_count} "
    f"({rank_one_title_pct:.2f}%)"
)


# Rank #1 je dijagnosticki KPI.
# Ne pravimo hard failure ako nekoliko eventa zahtijeva siri market context.

validate(
    rank_one_direct_pct >= 95.0,
    "Najmanje 95% rank #1 vijesti direktno je povezano sa kompanijom",
    f"Coverage: {rank_one_direct_pct:.2f}%"
)


# =============================================================================
# 14. TOP QUALITY BY TICKER
# =============================================================================

print()
print("=" * 100)
print("14. TOP NEWS QUALITY BY TICKER")
print("=" * 100)


(
    top_df
    .groupBy("ticker")
    .agg(

        F.count("*")
        .alias("top_news"),

        F.sum(
            F.col(
                "direct_company_match"
            ).cast("int")
        )
        .alias(
            "direct_matches"
        ),

        F.sum(
            F.col(
                "title_company_match"
            ).cast("int")
        )
        .alias(
            "title_matches"
        )
    )
    .withColumn(
        "direct_pct",

        F.round(
            F.col("direct_matches")
            /
            F.col("top_news")
            * 100,
            2
        )
    )
    .withColumn(
        "title_pct",

        F.round(
            F.col("title_matches")
            /
            F.col("top_news")
            * 100,
            2
        )
    )
    .orderBy("ticker")
    .show(
        n=100,
        truncate=False
    )
)


# =============================================================================
# 15. RELEVANCE DISTRIBUTION
# =============================================================================

print()
print("=" * 100)
print("15. RELEVANCE DISTRIBUTION")
print("=" * 100)


(
    relevance_df
    .groupBy("relevance_tier")
    .count()
    .orderBy(
        F.col("count").desc()
    )
    .show(
        truncate=False
    )
)


# =============================================================================
# 16. AI USAGE DISTRIBUTION
# =============================================================================

print()
print("=" * 100)
print("16. AI USAGE DISTRIBUTION")
print("=" * 100)


(
    relevance_df
    .groupBy("ai_usage_class")
    .count()
    .orderBy(
        F.col("count").desc()
    )
    .show(
        truncate=False
    )
)


# =============================================================================
# 17. FINAL VALIDATION
# =============================================================================

print()
print("=" * 100)
print("FINAL VALIDATION SUMMARY")
print("=" * 100)


hard_failures = [

    relevance_count != impact_count,

    anomaly_count != EXPECTED_ANOMALY_COUNT,

    null_keys != 0,

    duplicate_links != 0,

    duplicate_news_ranks != 0,

    invalid_rank_values != 0,

    invalid_context_rank != 0,

    missing_context_rank != 0,

    duplicate_context_ranks != 0,

    invalid_scores != 0,

    score_difference != 0,

    invalid_direct_match != 0,

    invalid_direct_levels != 0,

    invalid_tiers != 0,

    invalid_primary != 0,

    invalid_reaction != 0,

    top_duplicates != 0,

    top_anomaly_count != EXPECTED_ANOMALY_COUNT,

    min_top < MIN_TOP_NEWS_PER_ANOMALY,

    max_top > MAX_TOP_NEWS_PER_ANOMALY,

    direct_pct < MIN_TOP_DIRECT_MATCH_PCT,

    title_pct < MIN_TOP_TITLE_MATCH_PCT,

    rank_one_direct_pct < 95.0
]


failure_count = sum(
    1
    for failure in hard_failures
    if failure
)


if failure_count == 0:

    print(
        "[PASS] FINAL STOCK NEWS RELEVANCE "
        "VALIDATION USPJESNA"
    )

else:

    print(
        f"[FAIL] FINAL STOCK NEWS RELEVANCE "
        f"VALIDATION IMA {failure_count} PROBLEMA"
    )


print("=" * 100)