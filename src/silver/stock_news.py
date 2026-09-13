from databricks.connect import DatabricksSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    StringType,
    StructType,
    StructField,
)
from pyspark.sql.window import Window


# ============================================================
# SPARK SESSION
# ============================================================

spark = DatabricksSession.builder.getOrCreate()

BRONZE_TABLE = "workspace.bronze.stock_news"
SILVER_TABLE = "workspace.silver.stock_news"


print("=" * 80)
print("SILVER ETL - STOCK NEWS")
print("=" * 80)


# ============================================================
# 1. READ BRONZE
# ============================================================

bronze_df = spark.table(
    BRONZE_TABLE
)

bronze_count = bronze_df.count()

print(
    f"\nBronze redova: "
    f"{bronze_count}"
)


# ============================================================
# 2. JSON SCHEMAS
# ============================================================

string_array_schema = (
    ArrayType(
        StringType()
    )
)

insights_schema = (
    ArrayType(
        StructType(
            [
                StructField(
                    "ticker",
                    StringType(),
                    True,
                ),
                StructField(
                    "sentiment",
                    StringType(),
                    True,
                ),
                StructField(
                    "sentiment_reasoning",
                    StringType(),
                    True,
                ),
            ]
        )
    )
)


# ============================================================
# 3. BASIC CLEANING
# ============================================================

clean_df = (
    bronze_df

    # Standardizacija requested ticker-a
    .withColumn(
        "requested_ticker",
        F.upper(
            F.trim(
                F.col(
                    "requested_ticker"
                )
            )
        ),
    )

    # Čišćenje tekstualnih polja
    .withColumn(
        "title",
        F.trim(
            F.col("title")
        ),
    )
    .withColumn(
        "author",
        F.trim(
            F.col("author")
        ),
    )
    .withColumn(
        "description",
        F.trim(
            F.col("description")
        ),
    )
    .withColumn(
        "article_url",
        F.trim(
            F.col("article_url")
        ),
    )
    .withColumn(
        "amp_url",
        F.trim(
            F.col("amp_url")
        ),
    )
    .withColumn(
        "image_url",
        F.trim(
            F.col("image_url")
        ),
    )
    .withColumn(
        "publisher_name",
        F.trim(
            F.col(
                "publisher_name"
            )
        ),
    )
    .withColumn(
        "publisher_homepage_url",
        F.trim(
            F.col(
                "publisher_homepage_url"
            )
        ),
    )

    # Parse JSON stringova
    .withColumn(
        "tickers",
        F.from_json(
            F.col(
                "tickers_json"
            ),
            string_array_schema,
        ),
    )
    .withColumn(
        "keywords",
        F.from_json(
            F.col(
                "keywords_json"
            ),
            string_array_schema,
        ),
    )
    .withColumn(
        "insights",
        F.from_json(
            F.col(
                "insights_json"
            ),
            insights_schema,
        ),
    )
)


# ============================================================
# 4. NORMALIZE ARRAYS
# ============================================================

clean_df = (
    clean_df

    # Ticker-i unutar članka:
    # uppercase + trim
    .withColumn(
        "tickers",
        F.expr(
            """
            transform(
                tickers,
                x -> upper(trim(x))
            )
            """
        ),
    )

    # Keywords: trim
    .withColumn(
        "keywords",
        F.expr(
            """
            transform(
                keywords,
                x -> trim(x)
            )
            """
        ),
    )

    # Standardizacija insight sentiment vrijednosti
    .withColumn(
        "insights",
        F.expr(
            """
            transform(
                insights,
                x -> named_struct(
                    'ticker',
                    upper(trim(x.ticker)),

                    'sentiment',
                    lower(trim(x.sentiment)),

                    'sentiment_reasoning',
                    trim(
                        x.sentiment_reasoning
                    )
                )
            )
            """
        ),
    )
)


# ============================================================
# 5. DATA VALIDATION
# ============================================================

valid_condition = (
    F.col(
        "article_id"
    ).isNotNull()

    & (
        F.length(
            F.trim(
                F.col(
                    "article_id"
                )
            )
        )
        > 0
    )

    & F.col(
        "requested_ticker"
    ).isNotNull()

    & (
        F.length(
            F.col(
                "requested_ticker"
            )
        )
        > 0
    )

    & F.col(
        "title"
    ).isNotNull()

    & (
        F.length(
            F.col(
                "title"
            )
        )
        > 0
    )

    & F.col(
        "published_utc"
    ).isNotNull()
)


valid_df = (
    clean_df.filter(
        valid_condition
    )
)

invalid_df = (
    clean_df.filter(
        ~valid_condition
    )
)


valid_count = (
    valid_df.count()
)

invalid_count = (
    invalid_df.count()
)

print(
    f"Validnih redova: "
    f"{valid_count}"
)

print(
    f"Nevalidnih redova: "
    f"{invalid_count}"
)


# ============================================================
# 6. JSON PARSING QUALITY CHECK
# ============================================================

invalid_tickers_json_count = (
    valid_df
    .filter(
        F.col(
            "tickers_json"
        ).isNotNull()
        & F.col(
            "tickers"
        ).isNull()
    )
    .count()
)

invalid_keywords_json_count = (
    valid_df
    .filter(
        F.col(
            "keywords_json"
        ).isNotNull()
        & F.col(
            "keywords"
        ).isNull()
    )
    .count()
)

invalid_insights_json_count = (
    valid_df
    .filter(
        F.col(
            "insights_json"
        ).isNotNull()
        & F.col(
            "insights"
        ).isNull()
    )
    .count()
)


print(
    "\nJSON parsing:"
)

print(
    f"Neispravan tickers_json:  "
    f"{invalid_tickers_json_count}"
)

print(
    f"Neispravan keywords_json: "
    f"{invalid_keywords_json_count}"
)

print(
    f"Neispravan insights_json: "
    f"{invalid_insights_json_count}"
)


# ============================================================
# 7. DUPLICATE ANALYSIS
# ============================================================

print(
    "\n"
    + "=" * 80
)

print(
    "DUPLICATE ANALYSIS"
)

print(
    "=" * 80
)


unique_article_count = (
    valid_df
    .select(
        "article_id"
    )
    .distinct()
    .count()
)

unique_article_ticker_count = (
    valid_df
    .select(
        "article_id",
        "requested_ticker",
    )
    .distinct()
    .count()
)


print(
    "\nBroj jedinstvenih "
    "article_id:"
)

print(
    unique_article_count
)


print(
    "\nBroj jedinstvenih "
    "(article_id, requested_ticker) "
    "parova:"
)

print(
    unique_article_ticker_count
)


print(
    "\nNajčešći duplikati "
    "po poslovnom ključu:"
)

(
    valid_df
    .groupBy(
        "article_id",
        "requested_ticker",
    )
    .count()
    .filter(
        F.col("count") > 1
    )
    .orderBy(
        F.desc("count")
    )
    .show(
        20,
        truncate=50,
    )
)


# ============================================================
# 8. DEDUPLICATION
# ============================================================
#
# VAŽNO:
# Poslovni ključ Stock News zapisa nije samo article_id.
#
# Isti članak može biti relevantan za više tickera.
#
# Zato:
#
#   (article_id, requested_ticker)
#
# predstavlja jedinstveni ključ.
#
# Ovo mora biti identično MERGE ključu koji koristimo
# prilikom upisa u Silver tabelu.
# ============================================================

window_spec = (
    Window
    .partitionBy(
        "article_id",
        "requested_ticker",
    )
    .orderBy(
        F.col(
            "_ingested_at"
        ).desc()
    )
)


deduplicated_df = (
    valid_df
    .withColumn(
        "_row_number",
        F.row_number().over(
            window_spec
        ),
    )
    .filter(
        F.col(
            "_row_number"
        )
        == 1
    )
    .drop(
        "_row_number"
    )
)


deduplicated_count = (
    deduplicated_df.count()
)


print(
    "\nRedova nakon "
    "deduplikacije po "
    "(article_id, requested_ticker): "
    f"{deduplicated_count}"
)


# ============================================================
# 9. SILVER TRANSFORMATIONS
# ============================================================

silver_df = (
    deduplicated_df

    # Posebna kolona za lakše
    # spajanje sa stock prices
    .withColumn(
        "published_date",
        F.to_date(
            F.col(
                "published_utc"
            )
        ),
    )

    # Sentiment koji se odnosi
    # baš na requested ticker.
    .withColumn(
        "_requested_insight",
        F.expr(
            """
            element_at(
                filter(
                    insights,
                    x ->
                        x.ticker =
                        requested_ticker
                ),
                1
            )
            """
        ),
    )

    .withColumn(
        "requested_ticker_sentiment",
        F.col(
            "_requested_insight.sentiment"
        ),
    )

    .withColumn(
        "requested_ticker_sentiment_reasoning",
        F.col(
            "_requested_insight."
            "sentiment_reasoning"
        ),
    )

    .drop(
        "_requested_insight"
    )

    # Vrijeme Silver obrade
    .withColumn(
        "_silver_processed_at",
        F.current_timestamp(),
    )

    .select(
        "article_id",
        "requested_ticker",

        "title",
        "author",
        "description",

        "article_url",
        "amp_url",
        "image_url",

        "published_utc",
        "published_date",

        "publisher_name",
        "publisher_homepage_url",
        "publisher_logo_url",
        "publisher_favicon_url",

        "tickers",
        "keywords",
        "insights",

        "requested_ticker_sentiment",
        "requested_ticker_sentiment_reasoning",

        "_ingested_at",
        "_source",
        "_silver_processed_at",
    )
)


# ============================================================
# 10. CREATE SILVER SCHEMA
# ============================================================

spark.sql(
    """
    CREATE SCHEMA IF NOT EXISTS
        workspace.silver
    """
)


# ============================================================
# 11. CREATE SILVER TABLE
# ============================================================

spark.sql(
    """
    CREATE TABLE IF NOT EXISTS
        workspace.silver.stock_news
    (
        article_id STRING,
        requested_ticker STRING,

        title STRING,
        author STRING,
        description STRING,

        article_url STRING,
        amp_url STRING,
        image_url STRING,

        published_utc TIMESTAMP,
        published_date DATE,

        publisher_name STRING,
        publisher_homepage_url STRING,
        publisher_logo_url STRING,
        publisher_favicon_url STRING,

        tickers ARRAY<STRING>,
        keywords ARRAY<STRING>,

        insights ARRAY<
            STRUCT<
                ticker: STRING,
                sentiment: STRING,
                sentiment_reasoning: STRING
            >
        >,

        requested_ticker_sentiment STRING,
        requested_ticker_sentiment_reasoning STRING,

        _ingested_at TIMESTAMP,
        _source STRING,
        _silver_processed_at TIMESTAMP
    )
    USING DELTA
    """
)


# ============================================================
# 12. MERGE INTO SILVER
# ============================================================

silver_df.createOrReplaceTempView(
    "silver_stock_news_stage"
)


spark.sql(
    """
    MERGE INTO
        workspace.silver.stock_news
        AS target

    USING
        silver_stock_news_stage
        AS source

    ON
        target.article_id =
            source.article_id

    AND
        target.requested_ticker =
            source.requested_ticker

    WHEN MATCHED
         AND source._ingested_at
             > target._ingested_at

    THEN UPDATE SET *

    WHEN NOT MATCHED

    THEN INSERT *
    """
)


# ============================================================
# 13. FINAL VALIDATION
# ============================================================

final_df = spark.table(
    SILVER_TABLE
)

final_count = (
    final_df.count()
)


# Provjera da Silver nema duplikate
# po pravom poslovnom ključu.

duplicate_business_keys = (
    final_df
    .groupBy(
        "article_id",
        "requested_ticker",
    )
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)


print(
    "\n"
    + "=" * 80
)

print(
    "SILVER NEWS ETL REZULTAT"
)

print(
    "=" * 80
)

print(
    f"Bronze redova:               "
    f"{bronze_count}"
)

print(
    f"Validnih redova:             "
    f"{valid_count}"
)

print(
    f"Nevalidnih redova:           "
    f"{invalid_count}"
)

print(
    f"Jedinstvenih article_id:     "
    f"{unique_article_count}"
)

print(
    f"Jedinstvenih article+ticker: "
    f"{unique_article_ticker_count}"
)

print(
    f"Nakon deduplikacije:         "
    f"{deduplicated_count}"
)

print(
    f"Ukupno Silver redova:        "
    f"{final_count}"
)

print(
    f"Duplih poslovnih ključeva:   "
    f"{duplicate_business_keys}"
)


# ============================================================
# 14. FAIL IF DUPLICATES EXIST
# ============================================================

if duplicate_business_keys > 0:

    raise RuntimeError(
        "Silver stock_news sadrži "
        "duplikate po poslovnom ključu "
        "(article_id, requested_ticker)."
    )


# ============================================================
# 15. SENTIMENT DISTRIBUTION
# ============================================================

print(
    "\nSENTIMENT DISTRIBUCIJA:"
)

(
    final_df
    .groupBy(
        "requested_ticker_sentiment"
    )
    .count()
    .orderBy(
        F.desc("count")
    )
    .show(
        truncate=False
    )
)


# ============================================================
# 16. LATEST NEWS
# ============================================================

print(
    "\nPOSLJEDNJIH "
    "10 SILVER NEWS REDOVA:"
)

(
    final_df
    .select(
        "requested_ticker",
        "published_utc",
        "title",
        "requested_ticker_sentiment",
        "publisher_name",
    )
    .orderBy(
        F.col(
            "published_utc"
        ).desc()
    )
    .show(
        10,
        truncate=80,
    )
)


# ============================================================
# 17. SCHEMA
# ============================================================

print(
    "\nSILVER NEWS SCHEMA:"
)

final_df.printSchema()


# ============================================================
# 18. FINAL
# ============================================================

print()

if final_count < deduplicated_count:

    raise RuntimeError(
        "Silver tabela ima manje redova "
        "od trenutno deduplikovanog "
        "Bronze skupa."
    )


print(
    "=" * 80
)

print(
    "[PASS] SILVER STOCK NEWS ETL "
    "USPJESNO ZAVRSEN"
)

print(
    "=" * 80
)