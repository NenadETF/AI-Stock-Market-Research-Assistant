from databricks.connect import DatabricksSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# =============================================================================
# CONFIGURATION
# =============================================================================

SOURCE_TABLE = "workspace.gold.stock_news_impact"

GOLD_SCHEMA = "workspace.gold"

RELEVANCE_TABLE = "workspace.gold.stock_news_relevance"
TOP_NEWS_TABLE = "workspace.gold.stock_anomaly_top_news"

TOP_NEWS_PER_ANOMALY = 10
TOP_CONTEXT_NEWS_PER_ANOMALY = 5


# =============================================================================
# SPARK
# =============================================================================

spark = DatabricksSession.builder.getOrCreate()


print("=" * 100)
print("GOLD LAYER - STOCK NEWS RELEVANCE RANKING V2")
print("=" * 100)

print(f"Source:           {SOURCE_TABLE}")
print(f"Relevance target: {RELEVANCE_TABLE}")
print(f"Top news target:  {TOP_NEWS_TABLE}")
print(f"Top news/anomaly: {TOP_NEWS_PER_ANOMALY}")
print(f"Top context:      {TOP_CONTEXT_NEWS_PER_ANOMALY}")


# =============================================================================
# 1. CREATE GOLD SCHEMA
# =============================================================================

spark.sql(
    f"""
    CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA}
    """
)


# =============================================================================
# 2. LOAD SOURCE
# =============================================================================

source_df = spark.table(SOURCE_TABLE)

source_count = source_df.count()


print()
print(f"Source anomaly-news veza: {source_count}")


# =============================================================================
# 3. COMPANY DIRECT-MENTION LOGIC
#
# Massive moze clanak tagovati tickerom i kada kompanija nije glavni fokus.
#
# Zato provjeravamo da li title ili description direktno pominju:
#
# - kompaniju
# - ticker
# - poznati proizvod / servis / brend
#
# Ovo je kljucna V2 promjena relevance rankinga.
# =============================================================================

def company_text_match(text_column):

    text = F.lower(
        F.coalesce(
            text_column,
            F.lit("")
        )
    )

    return (
        F.when(
            F.col("ticker") == "AAPL",

            text.rlike(
                r"\b("
                r"apple|"
                r"aapl|"
                r"iphone|"
                r"ipad|"
                r"macbook|"
                r"mac|"
                r"ios|"
                r"app store|"
                r"apple watch"
                r")\b"
            )
        )

        .when(
            F.col("ticker") == "AMZN",

            text.rlike(
                r"\b("
                r"amazon|"
                r"amzn|"
                r"aws|"
                r"amazon web services|"
                r"prime video|"
                r"amazon prime"
                r")\b"
            )
        )

        .when(
            F.col("ticker") == "GOOGL",

            text.rlike(
                r"\b("
                r"alphabet|"
                r"google|"
                r"googl|"
                r"youtube|"
                r"gemini|"
                r"waymo|"
                r"google cloud"
                r")\b"
            )
        )

        .when(
            F.col("ticker") == "META",

            text.rlike(
                r"\b("
                r"meta platforms|"
                r"meta|"
                r"facebook|"
                r"instagram|"
                r"whatsapp|"
                r"threads|"
                r"reality labs"
                r")\b"
            )
        )

        .when(
            F.col("ticker") == "MSFT",

            text.rlike(
                r"\b("
                r"microsoft|"
                r"msft|"
                r"azure|"
                r"windows|"
                r"xbox|"
                r"copilot|"
                r"office 365|"
                r"microsoft 365"
                r")\b"
            )
        )

        .when(
            F.col("ticker") == "NVDA",

            text.rlike(
                r"\b("
                r"nvidia|"
                r"nvda|"
                r"geforce|"
                r"cuda|"
                r"blackwell"
                r")\b"
            )
        )

        .when(
            F.col("ticker") == "TSLA",

            text.rlike(
                r"\b("
                r"tesla|"
                r"tsla|"
                r"elon musk|"
                r"cybertruck|"
                r"model 3|"
                r"model y|"
                r"model s|"
                r"model x"
                r")\b"
            )
        )

        .otherwise(
            F.lit(False)
        )
    )


# =============================================================================
# 4. ARTICLE TICKER BREADTH
#
# Clanak koji ima samo 1 ticker je obicno specificniji od clanka
# koji je tagovan za 10 ili 20 kompanija.
# =============================================================================

df = source_df.withColumn(
    "article_ticker_count",

    F.when(
        F.col("news_tickers").isNotNull(),

        F.size(
            F.col("news_tickers")
        )
    )
    .otherwise(
        F.lit(0)
    )
)


# =============================================================================
# 5. DIRECT COMPANY MATCH
# =============================================================================

df = df.withColumn(
    "title_company_match",

    company_text_match(
        F.col("news_title")
    )
)


df = df.withColumn(
    "description_company_match",

    company_text_match(
        F.col("news_description")
    )
)


df = df.withColumn(
    "direct_company_match",

    F.col("title_company_match")
    |
    F.col("description_company_match")
)


# =============================================================================
# 6. DIRECT MATCH LEVEL
#
# TITLE_AND_DESCRIPTION
# TITLE
# DESCRIPTION
# NONE
#
# Ova kolona je korisna kasnije za AI i monitoring kvaliteta.
# =============================================================================

df = df.withColumn(
    "direct_match_level",

    F.when(
        F.col("title_company_match")
        &
        F.col("description_company_match"),

        F.lit("TITLE_AND_DESCRIPTION")
    )

    .when(
        F.col("title_company_match"),

        F.lit("TITLE")
    )

    .when(
        F.col("description_company_match"),

        F.lit("DESCRIPTION")
    )

    .otherwise(
        F.lit("NONE")
    )
)


# =============================================================================
# 7. TEMPORAL COMPONENT
#
# MAX = 25
#
# same day -> 25.00
# +/-1    -> 21.25
# +/-2    -> 16.25
# +/-3    -> 12.50
#
# V1 je koristio max 35.
#
# V2 smanjuje uticaj same-day signala jer sama vremenska blizina
# nije dovoljna za semantic relevance.
# =============================================================================

df = df.withColumn(
    "temporal_component",

    F.round(
        F.col("temporal_relevance_score")
        * F.lit(25.0),
        2
    )
)


# =============================================================================
# 8. EVENT RELATION COMPONENT
#
# MAX = 15
#
# SAME_DAY  = 15
# PRE_EVENT = 12
# POST_EVENT = 4
#
# POST_EVENT ostaje koristan kao reakcija, ali ne zelimo da clanak
# objavljen poslije anomalije bude dominantan explanatory context.
# =============================================================================

df = df.withColumn(
    "event_relation_component",

    F.when(
        F.col("event_relation") == "SAME_DAY",
        F.lit(15.0)
    )

    .when(
        F.col("event_relation") == "PRE_EVENT",
        F.lit(12.0)
    )

    .when(
        F.col("event_relation") == "POST_EVENT",
        F.lit(4.0)
    )

    .otherwise(
        F.lit(0.0)
    )
)


# =============================================================================
# 9. DIRECT TITLE COMPANY COMPONENT
#
# MAX = 25
#
# Ovo je jedan od najjacih relevance signala.
#
# Primjeri:
#
# "Why Microsoft Stock Is Sinking Today"
# "Amazon Stock Drops After Earnings"
# "Why Tesla Stock Is Soaring Today"
#
# dobijaju puni bonus.
# =============================================================================

df = df.withColumn(
    "title_company_component",

    F.when(
        F.col("title_company_match"),
        F.lit(25.0)
    )
    .otherwise(
        F.lit(0.0)
    )
)


# =============================================================================
# 10. DIRECT DESCRIPTION COMPANY COMPONENT
#
# MAX = 10
#
# Description moze direktno govoriti o kompaniji iako naslov
# opisuje siri market event.
# =============================================================================

df = df.withColumn(
    "description_company_component",

    F.when(
        F.col("description_company_match"),
        F.lit(10.0)
    )
    .otherwise(
        F.lit(0.0)
    )
)


# =============================================================================
# 11. TICKER SPECIFICITY COMPONENT
#
# MAX = 10
#
# V1 je koristio max 20 i time previse nagradjivao genericke clanke
# koji su slucajno imali mali broj ticker tagova.
# =============================================================================

df = df.withColumn(
    "ticker_specificity_component",

    F.when(
        F.col("article_ticker_count") == 1,
        F.lit(10.0)
    )

    .when(
        F.col("article_ticker_count") == 2,
        F.lit(8.0)
    )

    .when(
        F.col("article_ticker_count") == 3,
        F.lit(6.0)
    )

    .when(
        F.col("article_ticker_count") <= 5,
        F.lit(4.0)
    )

    .when(
        F.col("article_ticker_count") <= 10,
        F.lit(2.0)
    )

    .when(
        F.col("article_ticker_count") > 10,
        F.lit(1.0)
    )

    .otherwise(
        F.lit(0.0)
    )
)


# =============================================================================
# 12. SENTIMENT EVIDENCE COMPONENT
#
# MAX = 5
#
# INSIGHTS znaci da Massive ima sentiment bas za konkretan ticker.
# =============================================================================

df = df.withColumn(
    "sentiment_evidence_component",

    F.when(
        F.col("sentiment_source") == "INSIGHTS",
        F.lit(5.0)
    )

    .when(
        F.col("sentiment_source") == "REQUESTED_TICKER",
        F.lit(3.0)
    )

    .otherwise(
        F.lit(0.0)
    )
)


# =============================================================================
# 13. SENTIMENT REASONING COMPONENT
#
# MAX = 5
#
# Reasoning je koristan AI agentu jer daje ticker-specific
# objasnjenje odnosa izmedju clanka i kompanije.
# =============================================================================

df = df.withColumn(
    "sentiment_reasoning_component",

    F.when(
        F.col("sentiment_reasoning").isNotNull()
        &
        (
            F.length(
                F.trim(
                    F.col("sentiment_reasoning")
                )
            ) >= 20
        ),

        F.lit(5.0)
    )

    .when(
        F.col("sentiment_reasoning").isNotNull()
        &
        (
            F.length(
                F.trim(
                    F.col("sentiment_reasoning")
                )
            ) > 0
        ),

        F.lit(3.0)
    )

    .otherwise(
        F.lit(0.0)
    )
)


# =============================================================================
# 14. CONTENT QUALITY COMPONENT
#
# MAX = 5
#
# Preferiramo clanak koji ima i title i dovoljno detaljan description.
# =============================================================================

df = df.withColumn(
    "content_quality_component",

    F.when(
        F.col("news_title").isNotNull()
        &
        F.col("news_description").isNotNull()
        &
        (
            F.length(
                F.trim(
                    F.col("news_description")
                )
            ) >= 50
        ),

        F.lit(5.0)
    )

    .when(
        F.col("news_title").isNotNull()
        &
        F.col("news_description").isNotNull(),

        F.lit(4.0)
    )

    .when(
        F.col("news_title").isNotNull(),

        F.lit(3.0)
    )

    .otherwise(
        F.lit(0.0)
    )
)


# =============================================================================
# 15. FINAL RELEVANCE SCORE V2
#
# Maximum:
#
# temporal                     25
# event relation               15
# direct company in title      25
# direct company description   10
# ticker specificity           10
# sentiment evidence            5
# sentiment reasoning           5
# content quality               5
#
# TOTAL                       100
#
# Vazno:
#
# Ovo NIJE probability.
# Ovo NIJE causal probability.
# Ovo NIJE prediction score.
#
# Ovo je samo ranking score za izbor najboljeg research konteksta.
# =============================================================================

df = df.withColumn(
    "news_relevance_score",

    F.round(

        F.col("temporal_component")

        + F.col(
            "event_relation_component"
        )

        + F.col(
            "title_company_component"
        )

        + F.col(
            "description_company_component"
        )

        + F.col(
            "ticker_specificity_component"
        )

        + F.col(
            "sentiment_evidence_component"
        )

        + F.col(
            "sentiment_reasoning_component"
        )

        + F.col(
            "content_quality_component"
        ),

        2
    )
)


# =============================================================================
# 16. RELEVANCE TIER
# =============================================================================

df = df.withColumn(
    "relevance_tier",

    F.when(
        F.col("news_relevance_score") >= 85,
        F.lit("VERY_HIGH")
    )

    .when(
        F.col("news_relevance_score") >= 70,
        F.lit("HIGH")
    )

    .when(
        F.col("news_relevance_score") >= 55,
        F.lit("MEDIUM")
    )

    .otherwise(
        F.lit("LOW")
    )
)


# =============================================================================
# 17. OVERALL RANK PER ANOMALY
#
# Score je glavni ranking signal.
#
# Tie-break:
#
# 1. direct title match
# 2. direct description match
# 3. temporal relevance
# 4. absolute day distance
# 5. latest timestamp
# 6. article ID
# =============================================================================

overall_rank_window = (
    Window
    .partitionBy(
        "anomaly_event_id"
    )
    .orderBy(

        F.col(
            "news_relevance_score"
        ).desc(),

        F.col(
            "title_company_match"
        ).desc(),

        F.col(
            "description_company_match"
        ).desc(),

        F.col(
            "temporal_relevance_score"
        ).desc(),

        F.abs(
            F.col("news_day_offset")
        ).asc(),

        F.col(
            "published_utc"
        ).desc(),

        F.col(
            "article_id"
        ).asc()
    )
)


df = df.withColumn(
    "news_rank",

    F.row_number().over(
        overall_rank_window
    )
)


# =============================================================================
# 18. POTENTIAL CONTEXT RANK
#
# Posebno rangiramo samo:
#
# PRE_EVENT
# SAME_DAY
#
# Window mora biti primijenjen prije select-a.
# =============================================================================

context_df = (
    df
    .filter(
        F.col("is_potential_context")
    )
)


context_rank_window = (
    Window
    .partitionBy(
        "anomaly_event_id"
    )
    .orderBy(

        F.col(
            "news_relevance_score"
        ).desc(),

        F.col(
            "title_company_match"
        ).desc(),

        F.col(
            "description_company_match"
        ).desc(),

        F.col(
            "temporal_relevance_score"
        ).desc(),

        F.abs(
            F.col("news_day_offset")
        ).asc(),

        F.col(
            "published_utc"
        ).desc(),

        F.col(
            "article_id"
        ).asc()
    )
)


context_rank_df = (
    context_df
    .withColumn(
        "context_rank",

        F.row_number().over(
            context_rank_window
        )
    )
    .select(
        "anomaly_event_id",
        "article_id",
        "context_rank"
    )
)


# =============================================================================
# 19. ADD CONTEXT RANK
# =============================================================================

df = (
    df
    .join(
        context_rank_df,

        [
            "anomaly_event_id",
            "article_id"
        ],

        "left"
    )
)


# =============================================================================
# 20. TOP FLAGS
# =============================================================================

df = df.withColumn(
    "is_top_5_news",

    F.col("news_rank") <= 5
)


df = df.withColumn(
    "is_top_10_news",

    F.col("news_rank") <= TOP_NEWS_PER_ANOMALY
)


df = df.withColumn(
    "is_top_5_context",

    F.col("context_rank").isNotNull()

    &

    (
        F.col("context_rank")
        <= TOP_CONTEXT_NEWS_PER_ANOMALY
    )
)


# =============================================================================
# 21. AI USAGE CLASS
#
# PRIMARY_CONTEXT
# ----------------
# Top-5 PRE_EVENT / SAME_DAY clanaka.
#
# SECONDARY_CONTEXT
# -----------------
# Ostali top-10 explanatory clanaci.
#
# REACTION
# --------
# Top-10 POST_EVENT clanak.
#
# BACKGROUND
# ----------
# Sve ostalo.
# =============================================================================

df = df.withColumn(
    "ai_usage_class",

    F.when(
        F.col("is_top_5_context"),
        F.lit("PRIMARY_CONTEXT")
    )

    .when(
        F.col("is_top_10_news")
        &
        (
            F.col("event_relation")
            == "POST_EVENT"
        ),

        F.lit("REACTION")
    )

    .when(
        F.col("is_top_10_news"),
        F.lit("SECONDARY_CONTEXT")
    )

    .otherwise(
        F.lit("BACKGROUND")
    )
)


# =============================================================================
# 22. AI DIRECTNESS CLASS
#
# Dodatna kolona korisna kasnije pri izgradnji prompta.
# =============================================================================

df = df.withColumn(
    "ai_directness_class",

    F.when(
        F.col("title_company_match")
        &
        F.col("description_company_match"),

        F.lit("DIRECT")
    )

    .when(
        F.col("title_company_match"),

        F.lit("DIRECT_TITLE")
    )

    .when(
        F.col("description_company_match"),

        F.lit("DIRECT_DESCRIPTION")
    )

    .otherwise(
        F.lit("INDIRECT")
    )
)


# =============================================================================
# 23. PROCESSING TIMESTAMP
# =============================================================================

df = df.withColumn(
    "_news_relevance_processed_at",
    F.current_timestamp()
)


# =============================================================================
# 24. FINAL COLUMN ORDER
# =============================================================================

relevance_df = df.select(

    # -------------------------------------------------------------------------
    # Anomaly
    # -------------------------------------------------------------------------

    "anomaly_event_id",

    "ticker",
    "anomaly_date",

    "daily_return_pct",

    "anomaly_score",
    "anomaly_severity",
    "anomaly_type",

    "market_condition",

    # -------------------------------------------------------------------------
    # Article
    # -------------------------------------------------------------------------

    "article_id",

    "news_title",
    "news_description",

    "publisher_name",

    "article_url",
    "image_url",

    "published_utc",
    "published_date",

    # -------------------------------------------------------------------------
    # Temporal
    # -------------------------------------------------------------------------

    "news_day_offset",
    "temporal_relation",
    "event_relation",

    "is_potential_context",

    # -------------------------------------------------------------------------
    # Sentiment
    # -------------------------------------------------------------------------

    "sentiment",
    "sentiment_bucket",
    "sentiment_reasoning",
    "sentiment_source",

    # -------------------------------------------------------------------------
    # Original news metadata
    # -------------------------------------------------------------------------

    "news_tickers",
    "news_keywords",

    "article_ticker_count",

    # -------------------------------------------------------------------------
    # Semantic direct-match signals
    # -------------------------------------------------------------------------

    "title_company_match",
    "description_company_match",

    "direct_company_match",
    "direct_match_level",

    "ai_directness_class",

    # -------------------------------------------------------------------------
    # Existing temporal score
    # -------------------------------------------------------------------------

    "temporal_relevance_score",

    # -------------------------------------------------------------------------
    # Relevance components
    # -------------------------------------------------------------------------

    "temporal_component",
    "event_relation_component",

    "title_company_component",
    "description_company_component",

    "ticker_specificity_component",

    "sentiment_evidence_component",
    "sentiment_reasoning_component",

    "content_quality_component",

    # -------------------------------------------------------------------------
    # Final ranking
    # -------------------------------------------------------------------------

    "news_relevance_score",
    "relevance_tier",

    "news_rank",
    "context_rank",

    # -------------------------------------------------------------------------
    # AI flags
    # -------------------------------------------------------------------------

    "is_top_5_news",
    "is_top_10_news",
    "is_top_5_context",

    "ai_usage_class",

    # -------------------------------------------------------------------------
    # Metadata
    # -------------------------------------------------------------------------

    "_news_relevance_processed_at"
)


# =============================================================================
# 25. WRITE FULL RELEVANCE TABLE
# =============================================================================

(
    relevance_df.write
    .format("delta")
    .mode("overwrite")
    .option(
        "overwriteSchema",
        "true"
    )
    .saveAsTable(
        RELEVANCE_TABLE
    )
)


print()
print(
    f"[OK] Kreirana tabela: "
    f"{RELEVANCE_TABLE}"
)


# =============================================================================
# 26. CREATE TOP NEWS TABLE
#
# Za AI sloj cuvamo:
#
# - top 10 overall
# - plus top 5 explanatory context ako neki od njih nije vec u top 10
#
# U vecini slucajeva finalna tabela ce imati 10 redova po anomaliji,
# ali logika je namjerno napravljena tako da ne izgubimo kvalitetan
# PRE_EVENT/SAME_DAY context.
# =============================================================================

top_news_df = (
    relevance_df
    .filter(

        (
            F.col("news_rank")
            <= TOP_NEWS_PER_ANOMALY
        )

        |

        (
            F.col("context_rank")
            <= TOP_CONTEXT_NEWS_PER_ANOMALY
        )
    )
)


top_news_df = top_news_df.dropDuplicates(
    [
        "anomaly_event_id",
        "article_id"
    ]
)


(
    top_news_df.write
    .format("delta")
    .mode("overwrite")
    .option(
        "overwriteSchema",
        "true"
    )
    .saveAsTable(
        TOP_NEWS_TABLE
    )
)


print(
    f"[OK] Kreirana tabela: "
    f"{TOP_NEWS_TABLE}"
)


# =============================================================================
# 27. LOAD WRITTEN RESULTS
# =============================================================================

result_df = spark.table(
    RELEVANCE_TABLE
)

top_result_df = spark.table(
    TOP_NEWS_TABLE
)


relevance_count = result_df.count()
top_count = top_result_df.count()


anomaly_count = (
    result_df
    .select(
        "anomaly_event_id"
    )
    .distinct()
    .count()
)


# =============================================================================
# 28. NEWS RELEVANCE SUMMARY
# =============================================================================

print()
print("=" * 100)
print("NEWS RELEVANCE SUMMARY")
print("=" * 100)

print(
    f"Anomalija:                  "
    f"{anomaly_count}"
)

print(
    f"Ukupno ranked veza:         "
    f"{relevance_count}"
)

print(
    f"Top-news redova za AI:      "
    f"{top_count}"
)


if anomaly_count > 0:

    print(
        f"Prosjecno top news/anomaly: "
        f"{top_count / anomaly_count:.2f}"
    )


# =============================================================================
# 29. SCORE VALIDATION
# =============================================================================

invalid_scores = (
    result_df
    .filter(

        F.col(
            "news_relevance_score"
        ).isNull()

        |

        (
            F.col(
                "news_relevance_score"
            ) < 0
        )

        |

        (
            F.col(
                "news_relevance_score"
            ) > 100
        )
    )
    .count()
)


if invalid_scores == 0:

    print(
        "[PASS] Svi relevance score-ovi "
        "su u rasponu 0-100"
    )

else:

    print(
        f"[FAIL] Nevalidnih score-ova: "
        f"{invalid_scores}"
    )


# =============================================================================
# 30. BUSINESS KEY VALIDATION
# =============================================================================

null_business_keys = (
    result_df
    .filter(
        F.col("anomaly_event_id").isNull()
        |
        F.col("article_id").isNull()
        |
        F.col("ticker").isNull()
    )
    .count()
)


if null_business_keys == 0:

    print(
        "[PASS] Relevance poslovni kljucevi nisu NULL"
    )

else:

    print(
        f"[FAIL] NULL poslovnih kljuceva: "
        f"{null_business_keys}"
    )


duplicate_articles = (
    result_df
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


if duplicate_articles == 0:

    print(
        "[PASS] Nema duplicate anomaly-article veza"
    )

else:

    print(
        f"[FAIL] Duplicate anomaly-article veza: "
        f"{duplicate_articles}"
    )


# =============================================================================
# 31. RANK VALIDATION
# =============================================================================

duplicate_ranks = (
    result_df
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


if duplicate_ranks == 0:

    print(
        "[PASS] news_rank je jedinstven "
        "unutar svake anomalije"
    )

else:

    print(
        f"[FAIL] Duplicate rankova: "
        f"{duplicate_ranks}"
    )


# =============================================================================
# 32. DIRECT MATCH FLAG CONSISTENCY
# =============================================================================

invalid_direct_flags = (
    result_df
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


if invalid_direct_flags == 0:

    print(
        "[PASS] direct_company_match flag je konzistentan"
    )

else:

    print(
        f"[FAIL] Nekonzistentnih direct match flagova: "
        f"{invalid_direct_flags}"
    )


# =============================================================================
# 33. RELEVANCE TIER DISTRIBUTION
# =============================================================================

print()
print("=" * 100)
print("RELEVANCE TIER DISTRIBUCIJA")
print("=" * 100)

(
    result_df
    .groupBy(
        "relevance_tier"
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
# 34. DIRECT MATCH DISTRIBUTION
# =============================================================================

print()
print("=" * 100)
print("DIRECT COMPANY MATCH DISTRIBUCIJA")
print("=" * 100)

(
    result_df
    .groupBy(
        "direct_match_level"
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
# 35. AI DIRECTNESS DISTRIBUTION
# =============================================================================

print()
print("=" * 100)
print("AI DIRECTNESS DISTRIBUCIJA")
print("=" * 100)

(
    result_df
    .groupBy(
        "ai_directness_class"
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
# 36. AI USAGE DISTRIBUTION
# =============================================================================

print()
print("=" * 100)
print("AI USAGE DISTRIBUCIJA")
print("=" * 100)

(
    result_df
    .groupBy(
        "ai_usage_class"
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
# 37. TOP NEWS COUNTS PER ANOMALY
# =============================================================================

print()
print("=" * 100)
print("TOP NEWS COUNT DISTRIBUCIJA PO ANOMALIJI")
print("=" * 100)

(
    top_result_df
    .groupBy(
        "anomaly_event_id"
    )
    .agg(

        F.count("*")
        .alias(
            "top_news_count"
        ),

        F.sum(
            F.col(
                "is_top_5_context"
            ).cast("int")
        )
        .alias(
            "primary_context_count"
        ),

        F.sum(
            F.col(
                "direct_company_match"
            ).cast("int")
        )
        .alias(
            "direct_match_count"
        ),

        F.sum(
            F.col(
                "title_company_match"
            ).cast("int")
        )
        .alias(
            "title_match_count"
        )
    )
    .agg(

        F.min(
            "top_news_count"
        ).alias(
            "min_top_news"
        ),

        F.round(
            F.avg(
                "top_news_count"
            ),
            2
        ).alias(
            "avg_top_news"
        ),

        F.max(
            "top_news_count"
        ).alias(
            "max_top_news"
        ),

        F.min(
            "primary_context_count"
        ).alias(
            "min_primary_context"
        ),

        F.round(
            F.avg(
                "primary_context_count"
            ),
            2
        ).alias(
            "avg_primary_context"
        ),

        F.max(
            "primary_context_count"
        ).alias(
            "max_primary_context"
        ),

        F.round(
            F.avg(
                "direct_match_count"
            ),
            2
        ).alias(
            "avg_direct_matches"
        ),

        F.round(
            F.avg(
                "title_match_count"
            ),
            2
        ).alias(
            "avg_title_matches"
        )
    )
    .show(
        truncate=False
    )
)


# =============================================================================
# 38. TOP NEWS DIRECT-MENTION QUALITY
# =============================================================================

top_direct_count = (
    top_result_df
    .filter(
        F.col(
            "direct_company_match"
        )
    )
    .count()
)


top_title_count = (
    top_result_df
    .filter(
        F.col(
            "title_company_match"
        )
    )
    .count()
)


top_description_count = (
    top_result_df
    .filter(
        F.col(
            "description_company_match"
        )
    )
    .count()
)


print()
print("=" * 100)
print("TOP NEWS SEMANTIC QUALITY")
print("=" * 100)


if top_count > 0:

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

    description_pct = (
        top_description_count
        / top_count
        * 100
    )


    print(
        f"Top news sa direct company match: "
        f"{top_direct_count}/{top_count} "
        f"({direct_pct:.2f}%)"
    )

    print(
        f"Top news sa title company match:  "
        f"{top_title_count}/{top_count} "
        f"({title_pct:.2f}%)"
    )

    print(
        f"Top news sa description match:    "
        f"{top_description_count}/{top_count} "
        f"({description_pct:.2f}%)"
    )


# =============================================================================
# 39. TOP NEWS SEMANTIC QUALITY BY TICKER
# =============================================================================

print()
print("=" * 100)
print("TOP NEWS SEMANTIC QUALITY PO TICKERU")
print("=" * 100)

(
    top_result_df
    .groupBy(
        "ticker"
    )
    .agg(

        F.count("*")
        .alias(
            "top_news"
        ),

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
        ),

        F.sum(
            F.col(
                "description_company_match"
            ).cast("int")
        )
        .alias(
            "description_matches"
        )
    )
    .withColumn(
        "direct_match_pct",

        F.round(
            F.col("direct_matches")
            /
            F.col("top_news")
            * 100,
            2
        )
    )
    .withColumn(
        "title_match_pct",

        F.round(
            F.col("title_matches")
            /
            F.col("top_news")
            * 100,
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
# 40. SCORE DISTRIBUTION
# =============================================================================

print()
print("=" * 100)
print("RELEVANCE SCORE DISTRIBUCIJA")
print("=" * 100)

(
    result_df
    .agg(

        F.round(
            F.min(
                "news_relevance_score"
            ),
            2
        ).alias(
            "min_score"
        ),

        F.round(
            F.avg(
                "news_relevance_score"
            ),
            2
        ).alias(
            "avg_score"
        ),

        F.round(
            F.max(
                "news_relevance_score"
            ),
            2
        ).alias(
            "max_score"
        )
    )
    .show(
        truncate=False
    )
)


# =============================================================================
# 41. TOP RANKED NEWS EXAMPLES
#
# Ovo je najvazniji manual quality-check output.
# =============================================================================

print()
print("=" * 100)
print("TOP RANKED NEWS EXAMPLES")
print("=" * 100)

(
    result_df
    .filter(
        F.col("news_rank") <= 3
    )
    .select(

        "ticker",
        "anomaly_date",

        "daily_return_pct",

        "anomaly_score",
        "anomaly_severity",

        "news_rank",
        "context_rank",

        "news_relevance_score",
        "relevance_tier",

        "event_relation",
        "news_day_offset",

        "article_ticker_count",

        "title_company_match",
        "description_company_match",
        "direct_match_level",

        "sentiment_bucket",

        "ai_usage_class",

        "publisher_name",
        "news_title"
    )
    .orderBy(

        F.col(
            "anomaly_score"
        ).desc(),

        F.col(
            "news_rank"
        ).asc()
    )
    .show(
        n=60,
        truncate=False
    )
)


# =============================================================================
# 42. INDIRECT TOP NEWS EXAMPLES
#
# Ako neki indirektni clanak ipak zavrsi u top 10,
# zelimo ga posebno vidjeti.
# =============================================================================

print()
print("=" * 100)
print("INDIRECT TOP NEWS EXAMPLES")
print("=" * 100)

(
    top_result_df
    .filter(
        ~F.col(
            "direct_company_match"
        )
    )
    .select(

        "ticker",
        "anomaly_date",

        "news_rank",
        "context_rank",

        "news_relevance_score",

        "event_relation",
        "news_day_offset",

        "article_ticker_count",

        "publisher_name",
        "news_title"
    )
    .orderBy(

        F.col(
            "news_relevance_score"
        ).desc(),

        F.col(
            "news_rank"
        ).asc()
    )
    .show(
        n=30,
        truncate=False
    )
)


# =============================================================================
# 43. FINAL VALIDATION
# =============================================================================

print()
print("=" * 100)
print("FINAL NEWS RELEVANCE CHECK")
print("=" * 100)


hard_failures = [

    invalid_scores != 0,

    null_business_keys != 0,

    duplicate_articles != 0,

    duplicate_ranks != 0,

    invalid_direct_flags != 0,

    anomaly_count == 0,

    top_count == 0
]


failure_count = sum(
    1
    for failure in hard_failures
    if failure
)


if failure_count == 0:

    print(
        "[PASS] STOCK NEWS RELEVANCE "
        "RANKING V2 USPJESNO ZAVRSEN"
    )

else:

    print(
        f"[FAIL] STOCK NEWS RELEVANCE "
        f"RANKING V2 IMA "
        f"{failure_count} PROBLEMA"
    )


print("=" * 100)