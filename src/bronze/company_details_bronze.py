from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_date, to_timestamp
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType,
    BooleanType,
)


# ============================================================
# Spark schema za company details Bronze podatke
# ============================================================

BRONZE_COMPANY_DETAILS_SCHEMA = StructType([

    StructField("ticker", StringType(), False),
    StructField("name", StringType(), True),

    StructField("market", StringType(), True),
    StructField("locale", StringType(), True),
    StructField("primary_exchange", StringType(), True),
    StructField("type", StringType(), True),

    StructField("active", BooleanType(), True),
    StructField("currency_name", StringType(), True),

    StructField("cik", StringType(), True),
    StructField("composite_figi", StringType(), True),
    StructField("share_class_figi", StringType(), True),

    StructField("market_cap", DoubleType(), True),

    StructField("description", StringType(), True),

    StructField("sic_code", StringType(), True),
    StructField("sic_description", StringType(), True),

    StructField("homepage_url", StringType(), True),
    StructField("phone_number", StringType(), True),

    StructField("total_employees", LongType(), True),

    # Za početni DataFrame dolaze kao string,
    # a kasnije ih konvertujemo.
    StructField("list_date", StringType(), True),

    StructField("ticker_root", StringType(), True),
    StructField("ticker_suffix", StringType(), True),

    StructField(
        "weighted_shares_outstanding",
        LongType(),
        True
    ),

    StructField(
        "share_class_shares_outstanding",
        LongType(),
        True
    ),

    StructField("round_lot", LongType(), True),

    StructField("address1", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("postal_code", StringType(), True),

    StructField("logo_url", StringType(), True),
    StructField("icon_url", StringType(), True),

    StructField("_ingested_at", StringType(), False),
    StructField("_source", StringType(), False),
])


# ============================================================
# Kreiranje Spark DataFrame-a
# ============================================================

def create_bronze_company_details_df(
    spark: SparkSession,
    records: list[dict]
) -> DataFrame:

    if not records:
        raise ValueError(
            "Nema company details podataka za kreiranje "
            "Bronze DataFrame-a."
        )

    df = spark.createDataFrame(
        records,
        schema=BRONZE_COMPANY_DETAILS_SCHEMA
    )

    df = (
        df
        .withColumn(
            "list_date",
            to_date(col("list_date"))
        )
        .withColumn(
            "_ingested_at",
            to_timestamp(col("_ingested_at"))
        )
    )

    return df


# ============================================================
# Kreiranje Delta tabele
# ============================================================

def ensure_bronze_company_details_table(
    spark: SparkSession,
    table_name: str = "workspace.bronze.company_details"
) -> None:

    spark.sql("""
        CREATE SCHEMA IF NOT EXISTS workspace.bronze
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (

            ticker STRING,
            name STRING,

            market STRING,
            locale STRING,
            primary_exchange STRING,
            type STRING,

            active BOOLEAN,
            currency_name STRING,

            cik STRING,
            composite_figi STRING,
            share_class_figi STRING,

            market_cap DOUBLE,

            description STRING,

            sic_code STRING,
            sic_description STRING,

            homepage_url STRING,
            phone_number STRING,

            total_employees BIGINT,

            list_date DATE,

            ticker_root STRING,
            ticker_suffix STRING,

            weighted_shares_outstanding BIGINT,
            share_class_shares_outstanding BIGINT,

            round_lot BIGINT,

            address1 STRING,
            city STRING,
            state STRING,
            postal_code STRING,

            logo_url STRING,
            icon_url STRING,

            _ingested_at TIMESTAMP,
            _source STRING

        )
        USING DELTA
    """)


# ============================================================
# MERGE u Delta tabelu
# ============================================================

def merge_bronze_company_details(
    spark: SparkSession,
    df: DataFrame,
    table_name: str = "workspace.bronze.company_details"
) -> None:

    if df.count() == 0:
        print(
            "Nema company details podataka za MERGE."
        )
        return

    temp_view = "bronze_company_details_updates"

    df.createOrReplaceTempView(
        temp_view
    )

    spark.sql(f"""
        MERGE INTO {table_name} AS target

        USING {temp_view} AS source

        ON target.ticker = source.ticker

        WHEN MATCHED THEN
            UPDATE SET

                target.name =
                    source.name,

                target.market =
                    source.market,

                target.locale =
                    source.locale,

                target.primary_exchange =
                    source.primary_exchange,

                target.type =
                    source.type,

                target.active =
                    source.active,

                target.currency_name =
                    source.currency_name,

                target.cik =
                    source.cik,

                target.composite_figi =
                    source.composite_figi,

                target.share_class_figi =
                    source.share_class_figi,

                target.market_cap =
                    source.market_cap,

                target.description =
                    source.description,

                target.sic_code =
                    source.sic_code,

                target.sic_description =
                    source.sic_description,

                target.homepage_url =
                    source.homepage_url,

                target.phone_number =
                    source.phone_number,

                target.total_employees =
                    source.total_employees,

                target.list_date =
                    source.list_date,

                target.ticker_root =
                    source.ticker_root,

                target.ticker_suffix =
                    source.ticker_suffix,

                target.weighted_shares_outstanding =
                    source.weighted_shares_outstanding,

                target.share_class_shares_outstanding =
                    source.share_class_shares_outstanding,

                target.round_lot =
                    source.round_lot,

                target.address1 =
                    source.address1,

                target.city =
                    source.city,

                target.state =
                    source.state,

                target.postal_code =
                    source.postal_code,

                target.logo_url =
                    source.logo_url,

                target.icon_url =
                    source.icon_url,

                target._ingested_at =
                    source._ingested_at,

                target._source =
                    source._source


        WHEN NOT MATCHED THEN

            INSERT (
                ticker,
                name,
                market,
                locale,
                primary_exchange,
                type,
                active,
                currency_name,
                cik,
                composite_figi,
                share_class_figi,
                market_cap,
                description,
                sic_code,
                sic_description,
                homepage_url,
                phone_number,
                total_employees,
                list_date,
                ticker_root,
                ticker_suffix,
                weighted_shares_outstanding,
                share_class_shares_outstanding,
                round_lot,
                address1,
                city,
                state,
                postal_code,
                logo_url,
                icon_url,
                _ingested_at,
                _source
            )

            VALUES (
                source.ticker,
                source.name,
                source.market,
                source.locale,
                source.primary_exchange,
                source.type,
                source.active,
                source.currency_name,
                source.cik,
                source.composite_figi,
                source.share_class_figi,
                source.market_cap,
                source.description,
                source.sic_code,
                source.sic_description,
                source.homepage_url,
                source.phone_number,
                source.total_employees,
                source.list_date,
                source.ticker_root,
                source.ticker_suffix,
                source.weighted_shares_outstanding,
                source.share_class_shares_outstanding,
                source.round_lot,
                source.address1,
                source.city,
                source.state,
                source.postal_code,
                source.logo_url,
                source.icon_url,
                source._ingested_at,
                source._source
            )
    """)