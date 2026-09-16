from __future__ import annotations

import json
import os
import time

from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

from databricks.connect import DatabricksSession
from dotenv import load_dotenv
from pyspark.sql import types as T


# =============================================================================
# PROJECT / CONFIGURATION
# =============================================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

load_dotenv(
    PROJECT_ROOT / ".env"
)

MASSIVE_API_KEY = os.getenv(
    "MASSIVE_API_KEY"
)

BASE_URL = "https://api.massive.com"

NEWS_ENDPOINT = (
    f"{BASE_URL}/v2/reference/news"
)

BRONZE_NEWS_TABLE = (
    "workspace.bronze.stock_news"
)

TICKERS = [
    "AAPL",
    "MSFT",
    "GOOGL",
    "AMZN",
    "NVDA",
    "META",
    "TSLA",
]

REQUEST_TIMEOUT = 30

PAGE_LIMIT = 100

MAX_PAGES_PER_TICKER = 50

# Free API zaštita.
RATE_LIMIT_WAIT_SECONDS = 13

MAX_API_ATTEMPTS = 3

DEFAULT_RETRY_AFTER_SECONDS = 15

# Ponovo provjeravamo posljednja 2 dana.
# MERGE sprečava duplikate, a overlap omogućava
# hvatanje zakašnjelo indeksiranih članaka.
NEWS_OVERLAP_DAYS = 2


# =============================================================================
# RATE LIMIT STATE
# =============================================================================

_last_api_request_started_at: float | None = None


# =============================================================================
# SPARK
# =============================================================================

spark = (
    DatabricksSession
    .builder
    .getOrCreate()
)


# =============================================================================
# OUTPUT HELPERS
# =============================================================================

def separator(
    character: str = "=",
    width: int = 100,
):
    print(
        character * width
    )


# =============================================================================
# CONFIGURATION VALIDATION
# =============================================================================

def validate_configuration():

    if not MASSIVE_API_KEY:

        raise RuntimeError(
            "MASSIVE_API_KEY nije pronađen. "
            "Provjeri .env fajl."
        )


# =============================================================================
# RATE LIMIT
# =============================================================================

def wait_for_api_slot():

    global _last_api_request_started_at

    if (
        _last_api_request_started_at
        is not None
    ):

        elapsed = (
            time.monotonic()
            - _last_api_request_started_at
        )

        remaining = (
            RATE_LIMIT_WAIT_SECONDS
            - elapsed
        )

        if remaining > 0:

            print(
                f"Rate-limit protection: "
                f"{remaining:.1f}s"
            )

            time.sleep(
                remaining
            )

    _last_api_request_started_at = (
        time.monotonic()
    )


# =============================================================================
# ERROR HELPERS
# =============================================================================

def get_api_error_message(
    response: requests.Response,
) -> str:

    try:

        payload = response.json()

        if isinstance(
            payload,
            dict,
        ):

            return str(
                payload.get("error")
                or payload.get("message")
                or payload.get("status")
                or "Unknown Massive API error"
            )

    except Exception:
        pass

    text = (
        response.text
        or ""
    ).strip()

    if text:

        return text[:300]

    return (
        "Unknown Massive API error"
    )


def get_retry_after_seconds(
    response: requests.Response,
) -> float:

    value = response.headers.get(
        "Retry-After"
    )

    if value is None:

        return float(
            DEFAULT_RETRY_AFTER_SECONDS
        )

    try:

        value = float(
            value
        )

        return max(
            value,
            float(
                RATE_LIMIT_WAIT_SECONDS
            ),
        )

    except (
        TypeError,
        ValueError,
    ):

        return float(
            DEFAULT_RETRY_AFTER_SECONDS
        )


# =============================================================================
# SAFE MASSIVE REQUEST
# =============================================================================

def massive_get(
    url: str,
    params: dict | None = None,
) -> dict:

    headers = {
        "Authorization": (
            f"Bearer {MASSIVE_API_KEY}"
        ),
        "Accept": "application/json",
    }

    response = None

    for attempt in range(
        1,
        MAX_API_ATTEMPTS + 1,
    ):

        wait_for_api_slot()

        try:

            response = requests.get(
                url,
                headers=headers,
                params=params,
                timeout=REQUEST_TIMEOUT,
            )

        except requests.RequestException as exc:

            raise RuntimeError(
                "Massive News API request "
                "nije uspio."
            ) from exc

        # ---------------------------------------------------------------------
        # RATE LIMIT
        # ---------------------------------------------------------------------

        if (
            response.status_code == 429
            and attempt
            < MAX_API_ATTEMPTS
        ):

            retry_after = (
                get_retry_after_seconds(
                    response
                )
            )

            print(
                f"[WARN] HTTP 429. "
                f"Pokušaj {attempt}/"
                f"{MAX_API_ATTEMPTS}. "
                f"Čekam "
                f"{retry_after:.0f}s."
            )

            time.sleep(
                retry_after
            )

            continue

        break

    if response is None:

        raise RuntimeError(
            "Massive News API nije "
            "vratio response."
        )

    if response.status_code >= 400:

        message = (
            get_api_error_message(
                response
            )
        )

        # Namjerno ne ispisujemo URL
        # kako API ključ nikada ne bi
        # završio u logovima.
        raise RuntimeError(
            f"Massive News API HTTP "
            f"{response.status_code}. "
            f"{message}"
        )

    try:

        return response.json()

    except ValueError as exc:

        raise RuntimeError(
            "Massive News API je vratio "
            "nevalidan JSON."
        ) from exc


# =============================================================================
# TIMESTAMP HELPERS
# =============================================================================

def parse_utc_timestamp(
    value: str | None,
) -> datetime | None:

    if not value:
        return None

    try:

        parsed = datetime.fromisoformat(
            value.replace(
                "Z",
                "+00:00",
            )
        )

        if parsed.tzinfo is None:

            parsed = parsed.replace(
                tzinfo=timezone.utc
            )

        return (
            parsed
            .astimezone(
                timezone.utc
            )
            .replace(
                tzinfo=None
            )
        )

    except ValueError:
        return None


def to_api_timestamp(
    value: datetime,
) -> str:

    aware = value.replace(
        tzinfo=timezone.utc
    )

    return (
        aware.isoformat(
            timespec="seconds"
        )
        .replace(
            "+00:00",
            "Z",
        )
    )


# =============================================================================
# GET LATEST NEWS DATE
# =============================================================================

def get_latest_news_timestamp(
    ticker: str,
) -> datetime | None:

    rows = spark.sql(
        f"""
        SELECT
            MAX(published_utc)
                AS latest_published_utc
        FROM {BRONZE_NEWS_TABLE}
        WHERE requested_ticker = '{ticker}'
        """
    ).collect()

    if not rows:
        return None

    return rows[0][
        "latest_published_utc"
    ]


# =============================================================================
# BRONZE NEWS SCHEMA
# =============================================================================

NEWS_SCHEMA = T.StructType(
    [
        T.StructField(
            "article_id",
            T.StringType(),
            False,
        ),
        T.StructField(
            "requested_ticker",
            T.StringType(),
            False,
        ),
        T.StructField(
            "title",
            T.StringType(),
            True,
        ),
        T.StructField(
            "author",
            T.StringType(),
            True,
        ),
        T.StructField(
            "description",
            T.StringType(),
            True,
        ),
        T.StructField(
            "article_url",
            T.StringType(),
            True,
        ),
        T.StructField(
            "amp_url",
            T.StringType(),
            True,
        ),
        T.StructField(
            "image_url",
            T.StringType(),
            True,
        ),
        T.StructField(
            "published_utc",
            T.TimestampType(),
            True,
        ),
        T.StructField(
            "publisher_name",
            T.StringType(),
            True,
        ),
        T.StructField(
            "publisher_homepage_url",
            T.StringType(),
            True,
        ),
        T.StructField(
            "publisher_logo_url",
            T.StringType(),
            True,
        ),
        T.StructField(
            "publisher_favicon_url",
            T.StringType(),
            True,
        ),
        T.StructField(
            "tickers_json",
            T.StringType(),
            True,
        ),
        T.StructField(
            "keywords_json",
            T.StringType(),
            True,
        ),
        T.StructField(
            "insights_json",
            T.StringType(),
            True,
        ),
        T.StructField(
            "_ingested_at",
            T.TimestampType(),
            False,
        ),
        T.StructField(
            "_source",
            T.StringType(),
            False,
        ),
    ]
)


# =============================================================================
# NORMALIZE MASSIVE NEWS RESULT
# =============================================================================

def normalize_news_item(
    item: dict,
    requested_ticker: str,
    ingestion_time: datetime,
) -> dict | None:

    article_id = item.get(
        "id"
    )

    published_utc = (
        parse_utc_timestamp(
            item.get(
                "published_utc"
            )
        )
    )

    if (
        not article_id
        or published_utc is None
    ):

        return None

    publisher = (
        item.get("publisher")
        or {}
    )

    return {
        "article_id": (
            str(article_id)
        ),
        "requested_ticker": (
            requested_ticker
        ),
        "title": (
            item.get("title")
        ),
        "author": (
            item.get("author")
        ),
        "description": (
            item.get("description")
        ),
        "article_url": (
            item.get("article_url")
        ),
        "amp_url": (
            item.get("amp_url")
        ),
        "image_url": (
            item.get("image_url")
        ),
        "published_utc": (
            published_utc
        ),
        "publisher_name": (
            publisher.get("name")
        ),
        "publisher_homepage_url": (
            publisher.get(
                "homepage_url"
            )
        ),
        "publisher_logo_url": (
            publisher.get(
                "logo_url"
            )
        ),
        "publisher_favicon_url": (
            publisher.get(
                "favicon_url"
            )
        ),
        "tickers_json": json.dumps(
            item.get("tickers")
            or [],
            ensure_ascii=False,
        ),
        "keywords_json": json.dumps(
            item.get("keywords")
            or [],
            ensure_ascii=False,
        ),
        "insights_json": json.dumps(
            item.get("insights")
            or [],
            ensure_ascii=False,
        ),
        "_ingested_at": (
            ingestion_time
        ),
        "_source": (
            "massive_api"
        ),
    }


# =============================================================================
# FETCH NEWS FOR ONE TICKER
# =============================================================================

def fetch_stock_news(
    ticker: str,
    start_time: datetime,
    end_time: datetime,
) -> list[dict]:

    print(
        f"News period: "
        f"{start_time} -> {end_time}"
    )

    params = {
        "ticker": ticker,
        "published_utc.gte": (
            to_api_timestamp(
                start_time
            )
        ),
        "published_utc.lte": (
            to_api_timestamp(
                end_time
            )
        ),
        "order": "asc",
        "sort": "published_utc",
        "limit": PAGE_LIMIT,
    }

    current_url = (
        NEWS_ENDPOINT
    )

    current_params = params

    all_rows = []

    ingestion_time = (
        datetime
        .now(
            timezone.utc
        )
        .replace(
            tzinfo=None
        )
    )

    page_number = 0

    while current_url:

        page_number += 1

        if (
            page_number
            > MAX_PAGES_PER_TICKER
        ):

            raise RuntimeError(
                f"{ticker}: previše News "
                f"API stranica "
                f"(>{MAX_PAGES_PER_TICKER})."
            )

        print(
            f"{ticker}: "
            f"preuzimam news "
            f"stranicu {page_number}..."
        )

        payload = massive_get(
            url=current_url,
            params=current_params,
        )

        api_results = (
            payload.get("results")
            or []
        )

        print(
            f"{ticker}: stranica "
            f"{page_number} -> "
            f"{len(api_results)} članaka."
        )

        for item in api_results:

            row = normalize_news_item(
                item=item,
                requested_ticker=ticker,
                ingestion_time=(
                    ingestion_time
                ),
            )

            if row is not None:

                all_rows.append(
                    row
                )

        next_url = (
            payload.get(
                "next_url"
            )
        )

        if not next_url:
            break

        # next_url već sadrži query filtere.
        current_url = next_url

        current_params = None

    # -------------------------------------------------------------------------
    # LOCAL BUSINESS KEY DEDUP
    # -------------------------------------------------------------------------

    unique_rows = {}

    for row in all_rows:

        business_key = (
            row["article_id"],
            row[
                "requested_ticker"
            ],
        )

        unique_rows[
            business_key
        ] = row

    return list(
        unique_rows.values()
    )


# =============================================================================
# MERGE INTO BRONZE
# =============================================================================

def merge_news_into_bronze(
    rows: list[dict],
) -> int:

    if not rows:
        return 0

    dataframe = (
        spark.createDataFrame(
            rows,
            schema=NEWS_SCHEMA,
        )
    )

    dataframe.createOrReplaceTempView(
        "incremental_stock_news_stage"
    )

    spark.sql(
        f"""
        MERGE INTO
            {BRONZE_NEWS_TABLE}
            AS target

        USING
            incremental_stock_news_stage
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

    return len(
        rows
    )


# =============================================================================
# REFRESH ONE TICKER
# =============================================================================

def refresh_news_ticker(
    ticker: str,
    target_time: datetime,
) -> dict:

    latest_before = (
        get_latest_news_timestamp(
            ticker
        )
    )

    if latest_before is None:

        raise RuntimeError(
            f"{ticker}: nema postojeće "
            f"News istorije u Bronze tabeli."
        )

    overlap_start = (
        latest_before
        - timedelta(
            days=NEWS_OVERLAP_DAYS
        )
    )

    print()

    separator("-")

    print(
        f"Ticker: "
        f"{ticker}"
    )

    print(
        f"Latest Bronze news: "
        f"{latest_before}"
    )

    print(
        f"Overlap start: "
        f"{overlap_start}"
    )

    rows = fetch_stock_news(
        ticker=ticker,
        start_time=overlap_start,
        end_time=target_time,
    )

    print(
        f"{ticker}: ukupno "
        f"{len(rows)} jedinstvenih "
        f"news zapisa iz API-ja."
    )

    if not rows:

        return {
            "ticker": ticker,
            "status": (
                "NO_DATA"
            ),
            "rows_received": 0,
            "latest_before": (
                latest_before
            ),
            "latest_after": (
                latest_before
            ),
        }

    merged = (
        merge_news_into_bronze(
            rows
        )
    )

    latest_after = (
        get_latest_news_timestamp(
            ticker
        )
    )

    if (
        latest_after is not None
        and latest_after
        > latest_before
    ):

        status = "UPDATED"

    else:

        status = "REFRESHED"

    print(
        f"[OK] {ticker}: "
        f"{merged} news zapisa "
        f"obrađeno."
    )

    print(
        f"Latest poslije: "
        f"{latest_after}"
    )

    return {
        "ticker": ticker,
        "status": status,
        "rows_received": (
            len(rows)
        ),
        "latest_before": (
            latest_before
        ),
        "latest_after": (
            latest_after
        ),
    }


# =============================================================================
# MAIN
# =============================================================================

def main():

    separator()

    print(
        "AI STOCK MARKET "
        "RESEARCH ASSISTANT"
    )

    print(
        "INCREMENTAL STOCK "
        "NEWS INGESTION"
    )

    separator()

    validate_configuration()

    if not (
        spark.catalog.tableExists(
            BRONZE_NEWS_TABLE
        )
    ):

        raise RuntimeError(
            f"Bronze News tabela "
            f"ne postoji: "
            f"{BRONZE_NEWS_TABLE}"
        )

    target_time = (
        datetime
        .now(
            timezone.utc
        )
        .replace(
            tzinfo=None
        )
    )

    print(
        f"Target UTC: "
        f"{target_time}"
    )

    print(
        f"Bronze table: "
        f"{BRONZE_NEWS_TABLE}"
    )

    print(
        f"Overlap: "
        f"{NEWS_OVERLAP_DAYS} dana"
    )

    print(
        f"Tickers: "
        f"{', '.join(TICKERS)}"
    )

    results = []

    failed = []

    for index, ticker in enumerate(
        TICKERS,
        start=1,
    ):

        print()

        print(
            f"[{index}/"
            f"{len(TICKERS)}] "
            f"{ticker}"
        )

        try:

            result = (
                refresh_news_ticker(
                    ticker=ticker,
                    target_time=(
                        target_time
                    ),
                )
            )

            results.append(
                result
            )

        except Exception as exc:

            print(
                f"[FAIL] {ticker}: "
                f"{type(exc).__name__}: "
                f"{exc}"
            )

            failed.append(
                {
                    "ticker": ticker,
                    "error": str(exc),
                }
            )

    # =========================================================================
    # SUMMARY
    # =========================================================================

    print()

    separator()

    print(
        "INCREMENTAL STOCK NEWS "
        "INGESTION SUMMARY"
    )

    separator()

    for result in results:

        print(
            f"{result['ticker']:<6} "
            f"{result['status']:<12} "
            f"rows="
            f"{result['rows_received']:<5} "
            f"latest="
            f"{result['latest_after']}"
        )

    if failed:

        print()

        print(
            "FAILED TICKERS:"
        )

        for item in failed:

            print(
                f" - "
                f"{item['ticker']}: "
                f"{item['error']}"
            )

        raise RuntimeError(
            f"Incremental Stock News "
            f"ingestion failed for "
            f"{len(failed)} ticker(s)."
        )

    print()

    print(
        "[PASS] INCREMENTAL "
        "STOCK NEWS INGESTION "
        "COMPLETED"
    )

    separator()


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    main()