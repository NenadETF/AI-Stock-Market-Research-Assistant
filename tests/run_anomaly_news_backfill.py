import json
import os
import time
from datetime import datetime, timezone

import requests

from databricks.connect import DatabricksSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
)


# =============================================================================
# CONFIGURATION
# =============================================================================

PLAN_TABLE = "workspace.gold.stock_anomaly_news_backfill_plan"
BRONZE_TABLE = "workspace.bronze.stock_news"

MASSIVE_NEWS_URL = "https://api.massive.com/v2/reference/news"

PAGE_LIMIT = 100
RATE_LIMIT_SECONDS = 13

MAX_RETRIES = 5


# =============================================================================
# API KEY
#
# Pokusavamo najcesce nazive environment varijabli.
# API kljuc se NE hardkodira.
# =============================================================================

API_KEY = (
    os.getenv("MASSIVE_API_KEY")
    or os.getenv("MASSIVE_API_SECRET")
    or os.getenv("POLYGON_API_KEY")
)


if not API_KEY:
    raise RuntimeError(
        "Massive API key nije pronadjen. "
        "Koristi isti environment variable koji je radio "
        "u run_historical_stock_news_ingestion.py."
    )


# =============================================================================
# SPARK
# =============================================================================

spark = DatabricksSession.builder.getOrCreate()


print("=" * 100)
print("ANOMALY NEWS TARGETED HISTORICAL BACKFILL")
print("=" * 100)

print(f"Plan:   {PLAN_TABLE}")
print(f"Bronze: {BRONZE_TABLE}")


# =============================================================================
# BRONZE SCHEMA
# =============================================================================

BRONZE_SCHEMA = StructType(
    [
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
            TimestampType(),
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
            TimestampType(),
            True
        ),
        StructField(
            "_source",
            StringType(),
            False
        ),
    ]
)


# =============================================================================
# HELPERS
# =============================================================================

session = requests.Session()

last_request_time = None


def parse_timestamp(value):

    if not value:
        return None

    return datetime.fromisoformat(
        value.replace(
            "Z",
            "+00:00"
        )
    ).replace(
        tzinfo=None
    )


def wait_for_rate_limit():

    global last_request_time

    if last_request_time is None:
        return

    elapsed = time.time() - last_request_time

    remaining = RATE_LIMIT_SECONDS - elapsed

    if remaining > 0:

        print(
            f"Rate-limit zastita: "
            f"{remaining:.1f}s do sljedeceg API poziva."
        )

        time.sleep(remaining)


def massive_get(url, params=None):

    global last_request_time

    for attempt in range(
        1,
        MAX_RETRIES + 1
    ):

        wait_for_rate_limit()

        try:

            response = session.get(
                url,
                params=params,
                timeout=60
            )

            last_request_time = time.time()


            # ---------------------------------------------------------
            # SUCCESS
            # ---------------------------------------------------------

            if response.status_code == 200:

                return response


            # ---------------------------------------------------------
            # RATE LIMIT
            # ---------------------------------------------------------

            if response.status_code == 429:

                print(
                    f"[WARN] HTTP 429. "
                    f"Pokusaj {attempt}/{MAX_RETRIES}."
                )

                time.sleep(
                    RATE_LIMIT_SECONDS
                )

                continue


            # ---------------------------------------------------------
            # SERVER ERROR
            # ---------------------------------------------------------

            if 500 <= response.status_code < 600:

                print(
                    f"[WARN] HTTP {response.status_code}. "
                    f"Pokusaj {attempt}/{MAX_RETRIES}."
                )

                time.sleep(
                    RATE_LIMIT_SECONDS
                )

                continue


            # ---------------------------------------------------------
            # OTHER ERROR
            # ---------------------------------------------------------

            raise RuntimeError(
                f"Massive API greska "
                f"{response.status_code}: "
                f"{response.text[:500]}"
            )


        except requests.RequestException as exc:

            print(
                f"[WARN] Request greska: {exc}"
            )

            if attempt == MAX_RETRIES:
                raise

            time.sleep(
                RATE_LIMIT_SECONDS
            )


    raise RuntimeError(
        "Massive API request nije uspio "
        "nakon maksimalnog broja pokusaja."
    )


# =============================================================================
# FETCH ONE INTERVAL
# =============================================================================

def fetch_news_interval(
    ticker,
    start_date,
    end_date
):

    all_articles = []

    page_number = 1


    params = {
        "ticker": ticker,

        "published_utc.gte":
            f"{start_date}T00:00:00Z",

        "published_utc.lte":
            f"{end_date}T23:59:59Z",

        "sort": "published_utc",
        "order": "asc",

        "limit": PAGE_LIMIT,

        "apiKey": API_KEY,
    }


    next_url = MASSIVE_NEWS_URL


    while next_url:

        print(
            f"{ticker}: preuzimam stranicu "
            f"{page_number}..."
        )


        # -------------------------------------------------------------
        # Prvi request koristi kompletne parametre.
        #
        # next_url vec sadrzi cursor i ostale pagination parametre.
        # API key dodajemo posebno.
        # -------------------------------------------------------------

        if page_number == 1:

            response = massive_get(
                next_url,
                params=params
            )

        else:

            response = massive_get(
                next_url,
                params={
                    "apiKey": API_KEY
                }
            )


        payload = response.json()


        results = payload.get(
            "results",
            []
        )


        print(
            f"{ticker}: stranica "
            f"{page_number} -> "
            f"{len(results)} clanaka."
        )


        all_articles.extend(
            results
        )


        next_url = payload.get(
            "next_url"
        )


        page_number += 1


    return all_articles


# =============================================================================
# NORMALIZE API ARTICLES
# =============================================================================

def normalize_articles(
    articles,
    requested_ticker
):

    rows = []

    ingested_at = datetime.utcnow()


    for article in articles:

        article_id = article.get("id")


        if not article_id:
            continue


        publisher = (
            article.get("publisher")
            or {}
        )


        rows.append(
            {
                "article_id":
                    article_id,

                "requested_ticker":
                    requested_ticker,

                "title":
                    article.get("title"),

                "author":
                    article.get("author"),

                "description":
                    article.get("description"),

                "article_url":
                    article.get("article_url"),

                "amp_url":
                    article.get("amp_url"),

                "image_url":
                    article.get("image_url"),

                "published_utc":
                    parse_timestamp(
                        article.get(
                            "published_utc"
                        )
                    ),

                "publisher_name":
                    publisher.get("name"),

                "publisher_homepage_url":
                    publisher.get(
                        "homepage_url"
                    ),

                "publisher_logo_url":
                    publisher.get(
                        "logo_url"
                    ),

                "publisher_favicon_url":
                    publisher.get(
                        "favicon_url"
                    ),

                "tickers_json":
                    json.dumps(
                        article.get(
                            "tickers"
                        )
                    )
                    if article.get("tickers")
                    is not None
                    else None,

                "keywords_json":
                    json.dumps(
                        article.get(
                            "keywords"
                        )
                    )
                    if article.get("keywords")
                    is not None
                    else None,

                "insights_json":
                    json.dumps(
                        article.get(
                            "insights"
                        )
                    )
                    if article.get("insights")
                    is not None
                    else None,

                "_ingested_at":
                    ingested_at,

                "_source":
                    "massive_api_anomaly_backfill",
            }
        )


    return rows


# =============================================================================
# MERGE INTO BRONZE
#
# Koristimo article_id kao globalni identitet clanka.
#
# Ako je clanak vec u Bronze tabeli, ne pravimo duplikat.
# Tickers JSON svakako sadrzi sve tickere povezane sa clankom.
# =============================================================================

def merge_into_bronze(rows):

    if not rows:
        return 0


    incoming_df = spark.createDataFrame(
        rows,
        schema=BRONZE_SCHEMA
    )


    incoming_df = (
        incoming_df
        .dropDuplicates(
            ["article_id"]
        )
    )


    incoming_df.createOrReplaceTempView(
        "anomaly_news_backfill_stage"
    )


    spark.sql(
        f"""
        MERGE INTO {BRONZE_TABLE} AS target

        USING anomaly_news_backfill_stage AS source

        ON target.article_id = source.article_id

        WHEN NOT MATCHED
        THEN INSERT *
        """
    )


    return incoming_df.count()


# =============================================================================
# LOAD BACKFILL PLAN
# =============================================================================

plan_df = (
    spark.table(PLAN_TABLE)
    .select(
        "ticker",
        "start_date",
        "end_date",
        "days",
        "anomaly_count"
    )
    .orderBy(
        "ticker",
        "start_date"
    )
)


plan_rows = plan_df.collect()

plan_count = len(plan_rows)


print()
print(f"Broj planiranih intervala: {plan_count}")


bronze_before = (
    spark.table(BRONZE_TABLE)
    .count()
)


print(
    f"Bronze redova prije backfilla: "
    f"{bronze_before}"
)


# =============================================================================
# EXECUTION
#
# Svaki interval se odmah MERGE-a u Bronze.
#
# To znaci:
# ako se proces prekine, vec zavrseni intervali ostaju sacuvani.
# Ponovno pokretanje je bezbjedno jer MERGE ne pravi duplikate.
# =============================================================================

total_api_articles = 0
total_staged_articles = 0

successful_intervals = 0
failed_intervals = []


for index, row in enumerate(
    plan_rows,
    start=1
):

    ticker = row["ticker"]

    start_date = row[
        "start_date"
    ].isoformat()

    end_date = row[
        "end_date"
    ].isoformat()


    print()
    print("=" * 100)

    print(
        f"[{index}/{plan_count}] "
        f"{ticker}: "
        f"{start_date} -> {end_date}"
    )

    print("=" * 100)


    try:

        articles = fetch_news_interval(
            ticker,
            start_date,
            end_date
        )


        article_count = len(
            articles
        )


        total_api_articles += (
            article_count
        )


        print(
            f"{ticker}: ukupno API clanaka "
            f"za interval: {article_count}"
        )


        normalized_rows = normalize_articles(
            articles,
            ticker
        )


        staged_count = merge_into_bronze(
            normalized_rows
        )


        total_staged_articles += (
            staged_count
        )


        successful_intervals += 1


        print(
            f"[OK] Interval zavrsen. "
            f"Stage clanaka: {staged_count}"
        )


    except Exception as exc:

        failed_intervals.append(
            {
                "ticker": ticker,
                "start_date": start_date,
                "end_date": end_date,
                "error": str(exc),
            }
        )


        print(
            f"[FAIL] {ticker} "
            f"{start_date} -> {end_date}"
        )

        print(
            f"       {exc}"
        )


# =============================================================================
# FINAL BRONZE STATISTICS
# =============================================================================

bronze_after = (
    spark.table(BRONZE_TABLE)
    .count()
)


print()
print("=" * 100)
print("ANOMALY NEWS BACKFILL SUMMARY")
print("=" * 100)

print(
    f"Planiranih intervala:      "
    f"{plan_count}"
)

print(
    f"Uspjesnih intervala:       "
    f"{successful_intervals}"
)

print(
    f"Neuspjesnih intervala:     "
    f"{len(failed_intervals)}"
)

print(
    f"API vratio clanaka:        "
    f"{total_api_articles}"
)

print(
    f"Stage clanaka:             "
    f"{total_staged_articles}"
)

print(
    f"Bronze prije:              "
    f"{bronze_before}"
)

print(
    f"Bronze poslije:            "
    f"{bronze_after}"
)

print(
    f"Novi Bronze redovi:        "
    f"{bronze_after - bronze_before}"
)


# =============================================================================
# FAILURE DETAILS
# =============================================================================

if failed_intervals:

    print()
    print("NEUSPJESNI INTERVALI:")
    print("-" * 100)


    for item in failed_intervals:

        print(
            f"{item['ticker']} | "
            f"{item['start_date']} -> "
            f"{item['end_date']} | "
            f"{item['error']}"
        )


    print()
    print(
        "[WARN] Backfill nije potpuno zavrsen."
    )


else:

    print()
    print(
        "[PASS] SVI BACKFILL INTERVALI "
        "USPJESNO OBRADJENI"
    )


print()
print("=" * 100)
print("ANOMALY NEWS TARGETED BACKFILL COMPLETED")
print("=" * 100)