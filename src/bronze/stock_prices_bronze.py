from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_date, to_timestamp
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType,
)


BRONZE_STOCK_PRICES_SCHEMA = StructType([
    StructField("ticker", StringType(), False),
    StructField("date", StringType(), True),

    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),

    StructField("volume", DoubleType(), True),
    StructField("vwap", DoubleType(), True),

    StructField("transactions", LongType(), True),

    StructField("_ingested_at", StringType(), False),
    StructField("_source", StringType(), False),
])


def create_bronze_stock_prices_df(
    spark: SparkSession,
    records: list[dict]
) -> DataFrame:
    """
    Pretvara raw podatke sa Massive API-ja u Spark DataFrame
    spreman za Bronze sloj.
    """

    if not records:
        raise ValueError(
            "Nema podataka za kreiranje Bronze DataFrame-a."
        )

    # Eksplicitna schema sprečava probleme sa Spark type inference-om.
    df = spark.createDataFrame(
        records,
        schema=BRONZE_STOCK_PRICES_SCHEMA
    )

    # Pretvaramo tekstualne datume u Spark tipove.
    df = (
        df
        .withColumn(
            "date",
            to_date(col("date"))
        )
        .withColumn(
            "_ingested_at",
            to_timestamp(col("_ingested_at"))
        )
    )

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
        "_ingested_at",
        "_source"
    )

    return df

def write_bronze_stock_prices(
    df: DataFrame,
    table_name: str = "workspace.bronze.stock_prices"
) -> None:
    """
    Upisuje stock prices DataFrame u Bronze Delta tabelu.
    Tokom razvoja koristimo overwrite i overwriteSchema.
    """

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )

def ensure_bronze_stock_prices_table(
    spark: SparkSession,
    table_name: str = "workspace.bronze.stock_prices"
) -> None:
    """
    Kreira Bronze Delta tabelu ako još ne postoji.
    """

    spark.sql("""
        CREATE SCHEMA IF NOT EXISTS workspace.bronze
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            ticker STRING,
            date DATE,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume DOUBLE,
            vwap DOUBLE,
            transactions BIGINT,
            _ingested_at TIMESTAMP,
            _source STRING
        )
        USING DELTA
    """)


def merge_bronze_stock_prices(
    spark: SparkSession,
    df: DataFrame,
    table_name: str = "workspace.bronze.stock_prices"
) -> None:
    """
    Radi MERGE novih podataka u Bronze Delta tabelu.

    Jedinstveni poslovni ključ:
        ticker + date

    Ako zapis već postoji -> UPDATE
    Ako ne postoji       -> INSERT
    """

    if df.count() == 0:
        print("Nema novih podataka za MERGE.")
        return

    temp_view = "bronze_stock_prices_updates"

    df.createOrReplaceTempView(temp_view)

    spark.sql(f"""
        MERGE INTO {table_name} AS target

        USING {temp_view} AS source

        ON target.ticker = source.ticker
        AND target.date = source.date

        WHEN MATCHED THEN
            UPDATE SET
                target.open = source.open,
                target.high = source.high,
                target.low = source.low,
                target.close = source.close,
                target.volume = source.volume,
                target.vwap = source.vwap,
                target.transactions = source.transactions,
                target._ingested_at = source._ingested_at,
                target._source = source._source

        WHEN NOT MATCHED THEN
            INSERT (
                ticker,
                date,
                open,
                high,
                low,
                close,
                volume,
                vwap,
                transactions,
                _ingested_at,
                _source
            )
            VALUES (
                source.ticker,
                source.date,
                source.open,
                source.high,
                source.low,
                source.close,
                source.volume,
                source.vwap,
                source.transactions,
                source._ingested_at,
                source._source
            )
    """)