from __future__ import annotations

import os
import time

from datetime import date, datetime, timedelta, timezone
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

BRONZE_TABLE = (
    "workspace.bronze.stock_prices"
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

# Massive free tier ima ograničenje broja API zahtjeva.
# Razmak od 13 sekundi drži nas ispod 5 zahtjeva/min.
RATE_LIMIT_WAIT_SECONDS = 13

# Ako ipak dobijemo HTTP 429, pokušaćemo ponovo.
MAX_API_ATTEMPTS = 3

DEFAULT_RETRY_AFTER_SECONDS = 15

# Za daily OHLC podatke koristimo samo potpuno završene dane.
# Tako ne pokušavamo dohvatiti današnji nepotpuni trading bar.
MARKET_DATA_LAG_DAYS = 1


# =============================================================================
# INTERNAL RATE LIMIT STATE
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
# VALIDATE CONFIGURATION
# =============================================================================

def validate_configuration():

    if not MASSIVE_API_KEY:

        raise RuntimeError(
            "MASSIVE_API_KEY nije pronađen. "
            "Provjeri .env fajl u root folderu projekta."
        )


# =============================================================================
# RATE LIMIT PROTECTION
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
# GET LAST BRONZE DATE
# =============================================================================

def get_latest_date(
    ticker: str,
) -> date | None:

    rows = spark.sql(
        f"""
        SELECT
            MAX(date) AS latest_date
        FROM {BRONZE_TABLE}
        WHERE ticker = '{ticker}'
        """
    ).collect()

    if not rows:
        return None

    return rows[0][
        "latest_date"
    ]


# =============================================================================
# MASSIVE API ERROR HELPERS
# =============================================================================

def get_api_error_message(
    response: requests.Response,
) -> str:

    try:

        payload = (
            response.json()
        )

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

        seconds = float(
            value
        )

        return max(
            seconds,
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
# MASSIVE API
# =============================================================================

def fetch_stock_prices(
    ticker: str,
    start_date: date,
    end_date: date,
) -> list[dict]:

    url = (
        f"{BASE_URL}"
        f"/v2/aggs/ticker/{ticker}"
        f"/range/1/day/"
        f"{start_date.isoformat()}/"
        f"{end_date.isoformat()}"
    )

    # API ključ NE stavljamo u URL.
    # Massive podržava Bearer authentication.
    headers = {
        "Authorization": (
            f"Bearer {MASSIVE_API_KEY}"
        ),
        "Accept": "application/json",
    }

    params = {
        "adjusted": "true",
        "sort": "asc",
        "limit": 50000,
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
                f"Massive API request nije uspio "
                f"za {ticker}, period "
                f"{start_date} -> {end_date}."
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
                f"[WARN] Massive API HTTP 429 "
                f"za {ticker}. "
                f"Pokušaj {attempt}/"
                f"{MAX_API_ATTEMPTS}. "
                f"Čekam {retry_after:.0f}s."
            )

            time.sleep(
                retry_after
            )

            continue

        break

    if response is None:

        raise RuntimeError(
            f"Massive API nije vratio "
            f"response za {ticker}."
        )

    # -------------------------------------------------------------------------
    # SAFE HTTP ERROR
    # -------------------------------------------------------------------------

    if response.status_code >= 400:

        error_message = (
            get_api_error_message(
                response
            )
        )

        raise RuntimeError(
            f"Massive API HTTP "
            f"{response.status_code} "
            f"za {ticker}, period "
            f"{start_date} -> {end_date}. "
            f"{error_message}"
        )

    # -------------------------------------------------------------------------
    # JSON
    # -------------------------------------------------------------------------

    try:

        payload = (
            response.json()
        )

    except ValueError as exc:

        raise RuntimeError(
            f"Massive API je vratio "
            f"nevalidan JSON za {ticker}."
        ) from exc

    api_results = payload.get(
        "results",
        [],
    )

    results = []

    # Spark TimestampType kod postojećeg projekta koristi
    # timezone-naive UTC timestamp.
    ingestion_time = (
        datetime
        .now(timezone.utc)
        .replace(
            tzinfo=None
        )
    )

    for item in api_results:

        timestamp_ms = (
            item.get("t")
        )

        if timestamp_ms is None:
            continue

        trading_date = (
            datetime
            .fromtimestamp(
                timestamp_ms / 1000,
                timezone.utc,
            )
            .date()
        )

        results.append(
            {
                "ticker": ticker,
                "date": trading_date,
                "open": item.get("o"),
                "high": item.get("h"),
                "low": item.get("l"),
                "close": item.get("c"),
                "volume": item.get("v"),
                "vwap": item.get("vw"),
                "transactions": item.get("n"),
                "_ingested_at": (
                    ingestion_time
                ),
                "_source": (
                    "massive_api"
                ),
            }
        )

    return results


# =============================================================================
# SPARK SCHEMA
# =============================================================================

STOCK_PRICE_SCHEMA = (
    T.StructType(
        [
            T.StructField(
                "ticker",
                T.StringType(),
                False,
            ),
            T.StructField(
                "date",
                T.DateType(),
                False,
            ),
            T.StructField(
                "open",
                T.DoubleType(),
                True,
            ),
            T.StructField(
                "high",
                T.DoubleType(),
                True,
            ),
            T.StructField(
                "low",
                T.DoubleType(),
                True,
            ),
            T.StructField(
                "close",
                T.DoubleType(),
                True,
            ),
            T.StructField(
                "volume",
                T.DoubleType(),
                True,
            ),
            T.StructField(
                "vwap",
                T.DoubleType(),
                True,
            ),
            T.StructField(
                "transactions",
                T.LongType(),
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
)


# =============================================================================
# MERGE INTO BRONZE
# =============================================================================

def merge_into_bronze(
    rows: list[dict],
) -> int:

    if not rows:
        return 0

    dataframe = (
        spark.createDataFrame(
            rows,
            schema=(
                STOCK_PRICE_SCHEMA
            ),
        )
    )

    dataframe.createOrReplaceTempView(
        "incremental_stock_prices_stage"
    )

    spark.sql(
        f"""
        MERGE INTO {BRONZE_TABLE} AS target

        USING
            incremental_stock_prices_stage
            AS source

        ON target.ticker = source.ticker
        AND target.date = source.date

        WHEN MATCHED
             AND source._ingested_at
                 > target._ingested_at
        THEN UPDATE SET *

        WHEN NOT MATCHED
        THEN INSERT *
        """
    )

    # Nema potrebe za dodatnim Spark count jobom.
    return len(
        rows
    )


# =============================================================================
# REFRESH ONE TICKER
# =============================================================================

def refresh_ticker(
    ticker: str,
    end_date: date,
) -> dict:

    latest_date = (
        get_latest_date(
            ticker
        )
    )

    if latest_date is None:

        raise RuntimeError(
            f"{ticker}: ticker nema "
            f"postojeću istoriju u "
            f"Bronze tabeli."
        )

    start_date = (
        latest_date
        + timedelta(days=1)
    )

    print()

    separator("-")

    print(
        f"Ticker:      {ticker}"
    )

    print(
        f"Bronze max:  {latest_date}"
    )

    print(
        f"API period:  "
        f"{start_date} -> {end_date}"
    )

    # -------------------------------------------------------------------------
    # ALREADY UP TO DATE
    # -------------------------------------------------------------------------

    if start_date > end_date:

        print(
            f"[SKIP] {ticker}: "
            f"Bronze je već ažuran."
        )

        return {
            "ticker": ticker,
            "status": (
                "UP_TO_DATE"
            ),
            "latest_date_before": (
                latest_date
            ),
            "latest_date_after": (
                latest_date
            ),
            "rows_received": 0,
            "api_called": False,
        }

    # -------------------------------------------------------------------------
    # FETCH
    # -------------------------------------------------------------------------

    rows = fetch_stock_prices(
        ticker=ticker,
        start_date=start_date,
        end_date=end_date,
    )

    print(
        f"Massive API: "
        f"{len(rows)} trading zapisa"
    )

    # Vikend ili praznik može vratiti 0.
    if not rows:

        print(
            f"[OK] {ticker}: "
            f"nema novih trading podataka."
        )

        return {
            "ticker": ticker,
            "status": (
                "NO_NEW_DATA"
            ),
            "latest_date_before": (
                latest_date
            ),
            "latest_date_after": (
                latest_date
            ),
            "rows_received": 0,
            "api_called": True,
        }

    # -------------------------------------------------------------------------
    # MERGE
    # -------------------------------------------------------------------------

    merged_count = (
        merge_into_bronze(
            rows
        )
    )

    latest_after = (
        get_latest_date(
            ticker
        )
    )

    print(
        f"[OK] {ticker}: "
        f"{merged_count} zapisa "
        f"obrađeno."
    )

    print(
        f"Bronze max poslije: "
        f"{latest_after}"
    )

    return {
        "ticker": ticker,
        "status": "UPDATED",
        "latest_date_before": (
            latest_date
        ),
        "latest_date_after": (
            latest_after
        ),
        "rows_received": len(rows),
        "api_called": True,
    }


# =============================================================================
# MAIN INGESTION
# =============================================================================

def main():

    separator()

    print(
        "AI STOCK MARKET "
        "RESEARCH ASSISTANT"
    )

    print(
        "INCREMENTAL MARKET "
        "DATA INGESTION"
    )

    separator()

    validate_configuration()

    current_date = (
        date.today()
    )

    market_data_target_date = (
        current_date
        - timedelta(
            days=(
                MARKET_DATA_LAG_DAYS
            )
        )
    )

    print(
        f"Current date: "
        f"{current_date}"
    )

    print(
        f"Market data target date: "
        f"{market_data_target_date}"
    )

    print(
        f"Bronze table: "
        f"{BRONZE_TABLE}"
    )

    print(
        f"Tickers: "
        f"{', '.join(TICKERS)}"
    )

    # =========================================================================
    # BRONZE TABLE CHECK
    # =========================================================================

    if not (
        spark.catalog.tableExists(
            BRONZE_TABLE
        )
    ):

        raise RuntimeError(
            f"Bronze tabela ne postoji: "
            f"{BRONZE_TABLE}"
        )

    results = []

    failed = []

    # =========================================================================
    # TICKERS
    # =========================================================================

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
                refresh_ticker(
                    ticker=ticker,
                    end_date=(
                        market_data_target_date
                    ),
                )
            )

            results.append(
                result
            )

        except Exception as exc:

            error_message = str(
                exc
            )

            print(
                f"[FAIL] {ticker}: "
                f"{type(exc).__name__}: "
                f"{error_message}"
            )

            failed.append(
                {
                    "ticker": ticker,
                    "error": (
                        error_message
                    ),
                }
            )

    # =========================================================================
    # FINAL BRONZE STATUS
    # =========================================================================

    print()

    separator()

    print(
        "INCREMENTAL INGESTION "
        "SUMMARY"
    )

    separator()

    for result in results:

        print(
            f"{result['ticker']:<6} "
            f"{result['status']:<15} "
            f"rows="
            f"{result['rows_received']}"
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

        print()

        separator()

        print(
            "[FAIL] INCREMENTAL "
            "STOCK PRICE INGESTION"
        )

        separator()

        raise RuntimeError(
            f"Incremental ingestion "
            f"failed for "
            f"{len(failed)} ticker(s)."
        )

    print()

    print(
        "[PASS] INCREMENTAL "
        "STOCK PRICE INGESTION "
        "COMPLETED"
    )

    separator()


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    main()