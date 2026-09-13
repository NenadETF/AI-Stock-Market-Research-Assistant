from databricks.connect import DatabricksSession

from pyspark.sql import functions as F
from pyspark.sql.window import Window


# =============================================================================
# CONFIGURATION
# =============================================================================

NEWS_TABLE = "workspace.silver.stock_news"
COMPANY_TABLE = "workspace.silver.company_details"

GOLD_SCHEMA = "workspace.gold"

DAILY_TARGET_TABLE = "workspace.gold.news_daily_analytics"
SUMMARY_TARGET_TABLE = "workspace.gold.news_summary"


# =============================================================================
# SPARK SESSION
# =============================================================================

spark = DatabricksSession.builder.getOrCreate()


print("=" * 90)
print("GOLD LAYER - NEWS ANALYTICS")
print("=" * 90)

print(f"News source:    {NEWS_TABLE}")
print(f"Company source: {COMPANY_TABLE}")
print(f"Daily target:   {DAILY_TARGET_TABLE}")
print(f"Summary target: {SUMMARY_TARGET_TABLE}")


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

news_df = spark.table(NEWS_TABLE)
company_df = spark.table(COMPANY_TABLE)

news_count = news_df.count()
company_count = company_df.count()


print()
print("SOURCE TABLE COUNTS")
print("-" * 90)

print(f"Silver news redova:      {news_count}")
print(f"Company details redova:  {company_count}")


if news_count == 0:
    raise ValueError(
        f"Tabela {NEWS_TABLE} je prazna."
    )


if company_count == 0:
    raise ValueError(
        f"Tabela {COMPANY_TABLE} je prazna."
    )


# =============================================================================
# 3. COMPANY DETAILS
# =============================================================================

company_columns = set(company_df.columns)


if "company_name" in company_columns:
    company_name_column = "company_name"

elif "name" in company_columns:
    company_name_column = "name"

else:
    raise ValueError(
        "Company details nema company_name ili name kolonu."
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
    .dropDuplicates(["ticker"])
)


tracked_ticker_count = (
    company_base_df
    .select("ticker")
    .distinct()
    .count()
)


print()
print(
    f"[OK] Broj pracenih tickera: "
    f"{tracked_ticker_count}"
)


# =============================================================================
# 4. NORMALIZE NEWS SOURCE
# =============================================================================

news_base_df = (
    news_df
    .select(
        "article_id",

        F.upper(
            F.trim(
                F.col("requested_ticker")
            )
        ).alias("requested_ticker"),

        F.trim(
            F.col("title")
        ).alias("title"),

        F.trim(
            F.col("article_url")
        ).alias("article_url"),

        F.col(
            "published_utc"
        ).alias("published_utc"),

        F.col(
            "published_date"
        ).alias("published_date"),

        F.trim(
            F.col("publisher_name")
        ).alias("publisher_name"),

        "insights",

        F.lower(
            F.trim(
                F.col(
                    "requested_ticker_sentiment"
                )
            )
        ).alias(
            "requested_ticker_sentiment"
        ),

        F.col(
            "requested_ticker_sentiment_reasoning"
        ).alias(
            "requested_ticker_sentiment_reasoning"
        )
    )
)


# =============================================================================
# 5. RELATIONS FROM INSIGHTS
#
# Jedan clanak moze imati insight za vise tickera.
#
# Primjer:
#
# article_123
#     AAPL -> positive
#     MSFT -> neutral
#
# Zato clanak treba biti dostupan u analitici obje kompanije.
# =============================================================================

insight_relations_df = (
    news_base_df
    .select(
        "*",
        F.explode_outer(
            "insights"
        ).alias("_insight")
    )
    .filter(
        F.col("_insight.ticker").isNotNull()
    )
    .select(
        "article_id",

        F.upper(
            F.trim(
                F.col("_insight.ticker")
            )
        ).alias("ticker"),

        "title",
        "article_url",
        "published_utc",
        "published_date",
        "publisher_name",

        F.lower(
            F.trim(
                F.col("_insight.sentiment")
            )
        ).alias("sentiment"),

        F.col(
            "_insight.sentiment_reasoning"
        ).alias(
            "sentiment_reasoning"
        ),

        F.lit(1).alias("_priority")
    )
)


# =============================================================================
# 6. FALLBACK RELATIONS FROM REQUESTED TICKER
#
# Ako API nije vratio usable insight, i dalje mozemo povezati clanak
# sa tickerom za koji je preuzet.
# =============================================================================

requested_relations_df = (
    news_base_df
    .filter(
        F.col("requested_ticker").isNotNull()
    )
    .select(
        "article_id",

        F.col(
            "requested_ticker"
        ).alias("ticker"),

        "title",
        "article_url",
        "published_utc",
        "published_date",
        "publisher_name",

        F.col(
            "requested_ticker_sentiment"
        ).alias("sentiment"),

        F.col(
            "requested_ticker_sentiment_reasoning"
        ).alias(
            "sentiment_reasoning"
        ),

        F.lit(0).alias("_priority")
    )
)


# =============================================================================
# 7. UNION ARTICLE-TICKER RELATIONS
# =============================================================================

relations_df = (
    insight_relations_df
    .unionByName(
        requested_relations_df
    )
)


# =============================================================================
# 8. KEEP ONLY TRACKED COMPANIES
# =============================================================================

relations_df = (
    relations_df
    .join(
        company_base_df.select("ticker"),
        on="ticker",
        how="inner"
    )
)


# =============================================================================
# 9. DEDUPLICATE ARTICLE-TICKER RELATIONS
#
# Ako isti article-ticker postoji i kroz insights i kao requested ticker,
# insight zapis ima prioritet.
# =============================================================================

relation_window = (
    Window
    .partitionBy(
        "article_id",
        "ticker"
    )
    .orderBy(
        F.col("_priority").desc(),
        F.col("published_utc").desc()
    )
)


relations_df = (
    relations_df
    .withColumn(
        "_rn",
        F.row_number().over(
            relation_window
        )
    )
    .filter(
        F.col("_rn") == 1
    )
    .drop(
        "_rn",
        "_priority"
    )
)


# =============================================================================
# 10. NORMALIZE SENTIMENT
#
# Dozvoljene Gold kategorije:
#
# positive
# negative
# neutral
#
# Sve ostalo -> NULL / unknown
# =============================================================================

relations_df = relations_df.withColumn(
    "sentiment",

    F.when(
        F.col("sentiment").isin(
            "positive",
            "negative",
            "neutral"
        ),
        F.col("sentiment")
    )
    .otherwise(
        F.lit(None).cast("string")
    )
)


relation_count = relations_df.count()


print()
print("ARTICLE-TICKER RELATIONS")
print("-" * 90)

print(f"Silver clanaka:            {news_count}")
print(f"Article-ticker relacija:   {relation_count}")

print(
    "Napomena: broj relacija moze biti veci od broja clanaka "
    "jer jedan clanak moze biti povezan sa vise tickera."
)


# =============================================================================
# 11. DATA AS OF
# =============================================================================

data_as_of_row = (
    relations_df
    .select(
        F.max(
            "published_date"
        ).alias(
            "data_as_of"
        )
    )
    .collect()[0]
)


data_as_of = data_as_of_row["data_as_of"]


if data_as_of is None:
    raise ValueError(
        "Nije moguce odrediti data_as_of datum."
    )


print()
print(f"News data_as_of: {data_as_of}")


# =============================================================================
# 12. DAILY NEWS AGGREGATION
# =============================================================================

daily_df = (
    relations_df
    .groupBy(
        "ticker",
        "published_date"
    )
    .agg(
        F.countDistinct(
            "article_id"
        ).alias(
            "news_count"
        ),

        F.sum(
            F.when(
                F.col("sentiment") == "positive",
                1
            ).otherwise(0)
        ).alias(
            "positive_count"
        ),

        F.sum(
            F.when(
                F.col("sentiment") == "negative",
                1
            ).otherwise(0)
        ).alias(
            "negative_count"
        ),

        F.sum(
            F.when(
                F.col("sentiment") == "neutral",
                1
            ).otherwise(0)
        ).alias(
            "neutral_count"
        ),

        F.sum(
            F.when(
                F.col("sentiment").isNull(),
                1
            ).otherwise(0)
        ).alias(
            "unknown_sentiment_count"
        ),

        F.countDistinct(
            "publisher_name"
        ).alias(
            "publisher_count"
        ),

        F.max(
            "published_utc"
        ).alias(
            "latest_news_at"
        )
    )
)


# =============================================================================
# 13. DAILY SENTIMENT METRICS
#
# sentiment_score:
#
# +100 -> svi sentimenti positive
#    0 -> balans positive/negative ili neutral
# -100 -> svi negative
# =============================================================================

daily_df = (
    daily_df

    .withColumn(
        "scored_sentiment_count",

        F.col("positive_count")
        +
        F.col("negative_count")
        +
        F.col("neutral_count")
    )

    .withColumn(
        "sentiment_score",

        F.when(
            F.col("scored_sentiment_count") > 0,

            (
                (
                    F.col("positive_count")
                    -
                    F.col("negative_count")
                )
                /
                F.col("scored_sentiment_count")
            ) * 100
        )
    )

    .withColumn(
        "sentiment_coverage_pct",

        F.when(
            F.col("news_count") > 0,

            (
                F.col("scored_sentiment_count")
                /
                F.col("news_count")
            ) * 100
        )
    )

    .withColumn(
        "positive_share_pct",

        F.when(
            F.col("scored_sentiment_count") > 0,

            (
                F.col("positive_count")
                /
                F.col("scored_sentiment_count")
            ) * 100
        )
    )

    .withColumn(
        "negative_share_pct",

        F.when(
            F.col("scored_sentiment_count") > 0,

            (
                F.col("negative_count")
                /
                F.col("scored_sentiment_count")
            ) * 100
        )
    )

    .withColumn(
        "neutral_share_pct",

        F.when(
            F.col("scored_sentiment_count") > 0,

            (
                F.col("neutral_count")
                /
                F.col("scored_sentiment_count")
            ) * 100
        )
    )
)


# =============================================================================
# 14. DAILY DOMINANT SENTIMENT
# =============================================================================

daily_df = daily_df.withColumn(
    "dominant_sentiment",

    F.when(
        F.col("scored_sentiment_count") == 0,
        "NO_DATA"
    )

    .when(
        (F.col("positive_count") > F.col("negative_count"))
        &
        (F.col("positive_count") > F.col("neutral_count")),
        "POSITIVE"
    )

    .when(
        (F.col("negative_count") > F.col("positive_count"))
        &
        (F.col("negative_count") > F.col("neutral_count")),
        "NEGATIVE"
    )

    .when(
        (F.col("neutral_count") > F.col("positive_count"))
        &
        (F.col("neutral_count") > F.col("negative_count")),
        "NEUTRAL"
    )

    .otherwise(
        "MIXED"
    )
)


# =============================================================================
# 15. ADD COMPANY NAME TO DAILY TABLE
# =============================================================================

daily_df = (
    daily_df
    .join(
        company_base_df,
        on="ticker",
        how="left"
    )
)


# =============================================================================
# 16. ROUND DAILY VALUES
# =============================================================================

for column_name in [
    "sentiment_score",
    "sentiment_coverage_pct",
    "positive_share_pct",
    "negative_share_pct",
    "neutral_share_pct"
]:

    daily_df = daily_df.withColumn(
        column_name,
        F.round(
            F.col(column_name),
            2
        )
    )


daily_df = daily_df.withColumn(
    "_gold_processed_at",
    F.current_timestamp()
)


daily_df = daily_df.select(
    "ticker",
    "company_name",
    "published_date",

    "news_count",

    "positive_count",
    "negative_count",
    "neutral_count",
    "unknown_sentiment_count",
    "scored_sentiment_count",

    "sentiment_score",
    "sentiment_coverage_pct",

    "positive_share_pct",
    "negative_share_pct",
    "neutral_share_pct",

    "dominant_sentiment",

    "publisher_count",
    "latest_news_at",

    "_gold_processed_at"
)


# =============================================================================
# 17. WRITE DAILY NEWS TABLE
# =============================================================================

(
    daily_df.write
    .format("delta")
    .mode("overwrite")
    .option(
        "overwriteSchema",
        "true"
    )
    .saveAsTable(
        DAILY_TARGET_TABLE
    )
)


print()
print(
    f"[OK] Gold tabela kreirana: "
    f"{DAILY_TARGET_TABLE}"
)


# =============================================================================
# 18. 7D / 30D WINDOWS RELATIVE TO DATA AS OF
#
# 7D  -> data_as_of i prethodnih 6 kalendarskih dana
# 30D -> data_as_of i prethodnih 29 kalendarskih dana
# =============================================================================

date_7d_start = F.date_sub(
    F.lit(data_as_of),
    6
)

date_30d_start = F.date_sub(
    F.lit(data_as_of),
    29
)


is_7d = (
    F.col("published_date")
    >=
    date_7d_start
)


is_30d = (
    F.col("published_date")
    >=
    date_30d_start
)


# =============================================================================
# 19. SUMMARY AGGREGATION
# =============================================================================

summary_metrics_df = (
    relations_df
    .groupBy("ticker")
    .agg(
        F.countDistinct(
            "article_id"
        ).alias(
            "total_news_count"
        ),

        F.sum(
            F.when(
                is_7d,
                1
            ).otherwise(0)
        ).alias(
            "news_7d_count"
        ),

        F.sum(
            F.when(
                is_30d,
                1
            ).otherwise(0)
        ).alias(
            "news_30d_count"
        ),

        F.sum(
            F.when(
                is_30d
                &
                (
                    F.col("sentiment")
                    ==
                    "positive"
                ),
                1
            ).otherwise(0)
        ).alias(
            "positive_30d_count"
        ),

        F.sum(
            F.when(
                is_30d
                &
                (
                    F.col("sentiment")
                    ==
                    "negative"
                ),
                1
            ).otherwise(0)
        ).alias(
            "negative_30d_count"
        ),

        F.sum(
            F.when(
                is_30d
                &
                (
                    F.col("sentiment")
                    ==
                    "neutral"
                ),
                1
            ).otherwise(0)
        ).alias(
            "neutral_30d_count"
        ),

        F.sum(
            F.when(
                is_30d
                &
                F.col("sentiment").isNull(),
                1
            ).otherwise(0)
        ).alias(
            "unknown_30d_count"
        ),

        F.countDistinct(
            F.when(
                is_30d,
                F.col("publisher_name")
            )
        ).alias(
            "publisher_count_30d"
        ),

        F.max(
            "published_utc"
        ).alias(
            "latest_news_at"
        )
    )
)


# =============================================================================
# 20. LATEST ARTICLE PER TICKER
# =============================================================================

latest_news_window = (
    Window
    .partitionBy("ticker")
    .orderBy(
        F.col("published_utc").desc(),
        F.col("article_id").asc()
    )
)


latest_news_df = (
    relations_df
    .withColumn(
        "_rn",
        F.row_number().over(
            latest_news_window
        )
    )
    .filter(
        F.col("_rn") == 1
    )
    .select(
        "ticker",

        F.col(
            "article_id"
        ).alias(
            "latest_article_id"
        ),

        F.col(
            "title"
        ).alias(
            "latest_news_title"
        ),

        F.col(
            "publisher_name"
        ).alias(
            "latest_news_publisher"
        ),

        F.col(
            "article_url"
        ).alias(
            "latest_news_url"
        ),

        F.col(
            "sentiment"
        ).alias(
            "latest_news_sentiment"
        )
    )
)


# =============================================================================
# 21. BUILD SUMMARY FOR ALL TRACKED COMPANIES
#
# Left join osigurava da i kompanija bez vijesti ostane u Gold snapshotu.
# =============================================================================

summary_df = (
    company_base_df

    .join(
        summary_metrics_df,
        on="ticker",
        how="left"
    )

    .join(
        latest_news_df,
        on="ticker",
        how="left"
    )
)


# =============================================================================
# 22. FILL COUNT COLUMNS
# =============================================================================

count_columns = [
    "total_news_count",
    "news_7d_count",
    "news_30d_count",

    "positive_30d_count",
    "negative_30d_count",
    "neutral_30d_count",
    "unknown_30d_count",

    "publisher_count_30d"
]


summary_df = summary_df.fillna(
    0,
    subset=count_columns
)


# =============================================================================
# 23. SUMMARY SENTIMENT METRICS
# =============================================================================

summary_df = (
    summary_df

    .withColumn(
        "scored_sentiment_30d_count",

        F.col("positive_30d_count")
        +
        F.col("negative_30d_count")
        +
        F.col("neutral_30d_count")
    )

    .withColumn(
        "sentiment_score_30d",

        F.when(
            F.col(
                "scored_sentiment_30d_count"
            ) > 0,

            (
                (
                    F.col("positive_30d_count")
                    -
                    F.col("negative_30d_count")
                )
                /
                F.col(
                    "scored_sentiment_30d_count"
                )
            ) * 100
        )
    )

    .withColumn(
        "sentiment_coverage_30d_pct",

        F.when(
            F.col("news_30d_count") > 0,

            (
                F.col(
                    "scored_sentiment_30d_count"
                )
                /
                F.col("news_30d_count")
            ) * 100
        )
    )

    .withColumn(
        "positive_share_30d_pct",

        F.when(
            F.col(
                "scored_sentiment_30d_count"
            ) > 0,

            (
                F.col("positive_30d_count")
                /
                F.col(
                    "scored_sentiment_30d_count"
                )
            ) * 100
        )
    )

    .withColumn(
        "negative_share_30d_pct",

        F.when(
            F.col(
                "scored_sentiment_30d_count"
            ) > 0,

            (
                F.col("negative_30d_count")
                /
                F.col(
                    "scored_sentiment_30d_count"
                )
            ) * 100
        )
    )

    .withColumn(
        "neutral_share_30d_pct",

        F.when(
            F.col(
                "scored_sentiment_30d_count"
            ) > 0,

            (
                F.col("neutral_30d_count")
                /
                F.col(
                    "scored_sentiment_30d_count"
                )
            ) * 100
        )
    )
)


# =============================================================================
# 24. DOMINANT 30D SENTIMENT
# =============================================================================

summary_df = summary_df.withColumn(
    "dominant_sentiment_30d",

    F.when(
        F.col(
            "scored_sentiment_30d_count"
        ) == 0,
        "NO_DATA"
    )

    .when(
        (
            F.col("positive_30d_count")
            >
            F.col("negative_30d_count")
        )
        &
        (
            F.col("positive_30d_count")
            >
            F.col("neutral_30d_count")
        ),
        "POSITIVE"
    )

    .when(
        (
            F.col("negative_30d_count")
            >
            F.col("positive_30d_count")
        )
        &
        (
            F.col("negative_30d_count")
            >
            F.col("neutral_30d_count")
        ),
        "NEGATIVE"
    )

    .when(
        (
            F.col("neutral_30d_count")
            >
            F.col("positive_30d_count")
        )
        &
        (
            F.col("neutral_30d_count")
            >
            F.col("negative_30d_count")
        ),
        "NEUTRAL"
    )

    .otherwise(
        "MIXED"
    )
)


# =============================================================================
# 25. NEWS ACTIVITY METRICS
# =============================================================================

summary_df = (
    summary_df

    .withColumn(
        "avg_news_per_day_30d",

        F.col(
            "news_30d_count"
        ) / F.lit(30.0)
    )

    .withColumn(
        "latest_news_age_days",

        F.when(
            F.col("latest_news_at").isNotNull(),

            F.datediff(
                F.lit(data_as_of),
                F.to_date(
                    F.col("latest_news_at")
                )
            )
        )
    )
)


# =============================================================================
# 26. NEWS VOLUME RANK
#
# Rank 1 = najveci broj relevantnih clanaka u posljednjih 30 dana.
# =============================================================================

news_rank_window = (
    Window
    .orderBy(
        F.col(
            "news_30d_count"
        ).desc(),
        F.col("ticker").asc()
    )
)


summary_df = summary_df.withColumn(
    "news_volume_rank_30d",

    F.row_number().over(
        news_rank_window
    )
)


# =============================================================================
# 27. ADD DATA AS OF
# =============================================================================

summary_df = (
    summary_df

    .withColumn(
        "data_as_of",
        F.lit(data_as_of)
    )

    .withColumn(
        "_gold_processed_at",
        F.current_timestamp()
    )
)


# =============================================================================
# 28. ROUND SUMMARY VALUES
# =============================================================================

for column_name in [
    "sentiment_score_30d",
    "sentiment_coverage_30d_pct",

    "positive_share_30d_pct",
    "negative_share_30d_pct",
    "neutral_share_30d_pct",

    "avg_news_per_day_30d"
]:

    summary_df = summary_df.withColumn(
        column_name,
        F.round(
            F.col(column_name),
            2
        )
    )


# =============================================================================
# 29. FINAL SUMMARY COLUMN ORDER
# =============================================================================

summary_df = summary_df.select(
    "ticker",
    "company_name",

    "data_as_of",

    "total_news_count",
    "news_7d_count",
    "news_30d_count",

    "positive_30d_count",
    "negative_30d_count",
    "neutral_30d_count",
    "unknown_30d_count",
    "scored_sentiment_30d_count",

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

    "latest_article_id",
    "latest_news_title",
    "latest_news_publisher",
    "latest_news_url",
    "latest_news_sentiment",

    "_gold_processed_at"
)


# =============================================================================
# 30. WRITE SUMMARY TABLE
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
        SUMMARY_TARGET_TABLE
    )
)


print()
print(
    f"[OK] Gold tabela kreirana: "
    f"{SUMMARY_TARGET_TABLE}"
)


# =============================================================================
# 31. LOAD RESULTS
# =============================================================================

daily_result_df = spark.table(
    DAILY_TARGET_TABLE
)

summary_result_df = spark.table(
    SUMMARY_TARGET_TABLE
)


daily_result_count = daily_result_df.count()
summary_result_count = summary_result_df.count()


# =============================================================================
# 32. SUMMARY OUTPUT
# =============================================================================

print()
print("=" * 90)
print("NEWS ANALYTICS SUMMARY")
print("=" * 90)

print(
    f"Daily analytics redova: "
    f"{daily_result_count}"
)

print(
    f"News summary redova:    "
    f"{summary_result_count}"
)


print()
print("NEWS SUMMARY PO TICKERU:")


(
    summary_result_df
    .select(
        "ticker",
        "company_name",
        "data_as_of",

        "news_7d_count",
        "news_30d_count",

        "positive_30d_count",
        "negative_30d_count",
        "neutral_30d_count",

        "sentiment_score_30d",
        "dominant_sentiment_30d",

        "publisher_count_30d",
        "news_volume_rank_30d"
    )
    .orderBy(
        "news_volume_rank_30d"
    )
    .show(
        n=100,
        truncate=False
    )
)


# =============================================================================
# 33. VALIDATE SUMMARY COMPANY COUNT
# =============================================================================

if summary_result_count == tracked_ticker_count:

    print(
        f"[PASS] News summary ima svih "
        f"{tracked_ticker_count} tickera"
    )

else:

    raise ValueError(
        f"News summary ima {summary_result_count} redova, "
        f"a ocekuje se {tracked_ticker_count}."
    )


# =============================================================================
# 34. VALIDATE DAILY BUSINESS KEY
# =============================================================================

daily_duplicates = (
    daily_result_df
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


if daily_duplicates == 0:

    print(
        "[PASS] News daily nema duplikata po "
        "(ticker, published_date)"
    )

else:

    raise ValueError(
        f"Pronadjeno {daily_duplicates} "
        f"daily news duplikata."
    )


# =============================================================================
# 35. VALIDATE SUMMARY TICKER UNIQUENESS
# =============================================================================

summary_duplicates = (
    summary_result_df
    .groupBy("ticker")
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)


if summary_duplicates == 0:

    print(
        "[PASS] News summary nema duplikata po tickeru"
    )

else:

    raise ValueError(
        f"Pronadjeno {summary_duplicates} "
        f"duplih tickera u news summary."
    )


# =============================================================================
# 36. VALIDATE DAILY SENTIMENT COUNTS
# =============================================================================

invalid_daily_counts = (
    daily_result_df
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


if invalid_daily_counts == 0:

    print(
        "[PASS] Daily sentiment counts su konzistentni"
    )

else:

    raise ValueError(
        f"Pronadjeno {invalid_daily_counts} "
        f"nekonzistentnih daily sentiment redova."
    )


# =============================================================================
# 37. VALIDATE SUMMARY 30D COUNTS
# =============================================================================

invalid_summary_counts = (
    summary_result_df
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


if invalid_summary_counts == 0:

    print(
        "[PASS] 30D sentiment counts su konzistentni"
    )

else:

    raise ValueError(
        f"Pronadjeno {invalid_summary_counts} "
        f"nekonzistentnih 30D summary redova."
    )


# =============================================================================
# 38. VALIDATE SENTIMENT SCORE RANGE
# =============================================================================

invalid_daily_scores = (
    daily_result_df
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


invalid_summary_scores = (
    summary_result_df
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


if (
    invalid_daily_scores == 0
    and
    invalid_summary_scores == 0
):

    print(
        "[PASS] Sentiment score vrijednosti su "
        "u opsegu -100 do 100"
    )

else:

    raise ValueError(
        "Pronadjene nevalidne sentiment score vrijednosti."
    )


# =============================================================================
# 39. FINAL
# =============================================================================

print()
print("=" * 90)
print("GOLD NEWS ANALYTICS COMPLETED")
print("=" * 90)