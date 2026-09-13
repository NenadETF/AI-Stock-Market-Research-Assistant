from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
)


# ============================================================
# Bronze schema
# ============================================================

BRONZE_STOCK_NEWS_SCHEMA = StructType([

    StructField(
        "article_id",
        StringType(),
        False
    ),

    StructField(
        "requested_ticker",
        StringType(),
        False
    ),

    StructField(
        "title",
        StringType(),
        True
    ),

    StructField(
        "author",
        StringType(),
        True
    ),

    StructField(
        "description",
        StringType(),
        True
    ),

    StructField(
        "article_url",
        StringType(),
        True
    ),

    StructField(
        "amp_url",
        StringType(),
        True
    ),

    StructField(
        "image_url",
        StringType(),
        True
    ),

    StructField(
        "published_utc",
        StringType(),
        True
    ),

    StructField(
        "publisher_name",
        StringType(),
        True
    ),

    StructField(
        "publisher_homepage_url",
        StringType(),
        True
    ),

    StructField(
        "publisher_logo_url",
        StringType(),
        True
    ),

    StructField(
        "publisher_favicon_url",
        StringType(),
        True
    ),

    StructField(
        "tickers_json",
        StringType(),
        True
    ),

    StructField(
        "keywords_json",
        StringType(),
        True
    ),

    StructField(
        "insights_json",
        StringType(),
        True
    ),

    StructField(
        "_ingested_at",
        StringType(),
        False
    ),

    StructField(
        "_source",
        StringType(),
        False
    ),
])


# ============================================================
# Python records -> Spark DataFrame
# ============================================================

def create_bronze_stock_news_df(
    spark: SparkSession,
    records: list[dict]
) -> DataFrame:

    if not records:
        raise ValueError(
            "Nema stock news podataka za kreiranje "
            "Bronze DataFrame-a."
        )

    df = spark.createDataFrame(
        records,
        schema=BRONZE_STOCK_NEWS_SCHEMA
    )

    df = (
        df
        .withColumn(
            "published_utc",
            to_timestamp(
                col("published_utc")
            )
        )
        .withColumn(
            "_ingested_at",
            to_timestamp(
                col("_ingested_at")
            )
        )
    )

    return df


# ============================================================
# Kreiranje Delta tabele
# ============================================================

def ensure_bronze_stock_news_table(
    spark: SparkSession,
    table_name: str = "workspace.bronze.stock_news"
) -> None:

    spark.sql("""
        CREATE SCHEMA IF NOT EXISTS workspace.bronze
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (

            article_id STRING,
            requested_ticker STRING,

            title STRING,
            author STRING,
            description STRING,

            article_url STRING,
            amp_url STRING,
            image_url STRING,

            published_utc TIMESTAMP,

            publisher_name STRING,
            publisher_homepage_url STRING,
            publisher_logo_url STRING,
            publisher_favicon_url STRING,

            tickers_json STRING,
            keywords_json STRING,
            insights_json STRING,

            _ingested_at TIMESTAMP,
            _source STRING

        )
        USING DELTA
    """)


# ============================================================
# MERGE
# ============================================================

def merge_bronze_stock_news(
    spark: SparkSession,
    df: DataFrame,
    table_name: str = "workspace.bronze.stock_news"
) -> None:

    if df.count() == 0:
        print(
            "Nema Stock News podataka za MERGE."
        )
        return

    temp_view = "bronze_stock_news_updates"

    df.createOrReplaceTempView(
        temp_view
    )

    spark.sql(f"""
        MERGE INTO {table_name} AS target

        USING {temp_view} AS source

        ON target.article_id = source.article_id
        AND target.requested_ticker = source.requested_ticker

        WHEN MATCHED THEN
            UPDATE SET

                target.title =
                    source.title,

                target.author =
                    source.author,

                target.description =
                    source.description,

                target.article_url =
                    source.article_url,

                target.amp_url =
                    source.amp_url,

                target.image_url =
                    source.image_url,

                target.published_utc =
                    source.published_utc,

                target.publisher_name =
                    source.publisher_name,

                target.publisher_homepage_url =
                    source.publisher_homepage_url,

                target.publisher_logo_url =
                    source.publisher_logo_url,

                target.publisher_favicon_url =
                    source.publisher_favicon_url,

                target.tickers_json =
                    source.tickers_json,

                target.keywords_json =
                    source.keywords_json,

                target.insights_json =
                    source.insights_json,

                target._ingested_at =
                    source._ingested_at,

                target._source =
                    source._source


        WHEN NOT MATCHED THEN

            INSERT (
                article_id,
                requested_ticker,
                title,
                author,
                description,
                article_url,
                amp_url,
                image_url,
                published_utc,
                publisher_name,
                publisher_homepage_url,
                publisher_logo_url,
                publisher_favicon_url,
                tickers_json,
                keywords_json,
                insights_json,
                _ingested_at,
                _source
            )

            VALUES (
                source.article_id,
                source.requested_ticker,
                source.title,
                source.author,
                source.description,
                source.article_url,
                source.amp_url,
                source.image_url,
                source.published_utc,
                source.publisher_name,
                source.publisher_homepage_url,
                source.publisher_logo_url,
                source.publisher_favicon_url,
                source.tickers_json,
                source.keywords_json,
                source.insights_json,
                source._ingested_at,
                source._source
            )
    """)