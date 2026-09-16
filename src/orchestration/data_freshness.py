from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

from databricks.connect import DatabricksSession
from pyspark.sql import types as T


# =============================================================================
# CONFIGURATION
# =============================================================================

PRICE_TABLE = "workspace.bronze.stock_prices"
NEWS_TABLE = "workspace.bronze.stock_news"

FRESHNESS_TABLE = (
    "workspace.gold.data_freshness"
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

# News ingestion planiramo najmanje jednom dnevno.
# Dajemo malo tolerancije scheduleru.
NEWS_FRESHNESS_HOURS = 30.0


# =============================================================================
# SPARK
# =============================================================================

spark = (
    DatabricksSession
    .builder
    .getOrCreate()
)


# =============================================================================
# OUTPUT
# =============================================================================

def separator(
    character: str = "=",
    width: int = 100,
):
    print(
        character * width
    )


# =============================================================================
# DATE HELPERS
# =============================================================================

def nth_weekday_of_month(
    year: int,
    month: int,
    weekday: int,
    occurrence: int,
) -> date:

    current = date(
        year,
        month,
        1,
    )

    days_until_weekday = (
        weekday
        - current.weekday()
    ) % 7

    return (
        current
        + timedelta(
            days=(
                days_until_weekday
                + 7 * (
                    occurrence - 1
                )
            )
        )
    )


def last_weekday_of_month(
    year: int,
    month: int,
    weekday: int,
) -> date:

    if month == 12:

        next_month = date(
            year + 1,
            1,
            1,
        )

    else:

        next_month = date(
            year,
            month + 1,
            1,
        )

    current = (
        next_month
        - timedelta(days=1)
    )

    while (
        current.weekday()
        != weekday
    ):

        current -= timedelta(
            days=1
        )

    return current


def observed_fixed_holiday(
    holiday: date,
) -> date:

    # Saturday -> Friday
    if holiday.weekday() == 5:

        return (
            holiday
            - timedelta(days=1)
        )

    # Sunday -> Monday
    if holiday.weekday() == 6:

        return (
            holiday
            + timedelta(days=1)
        )

    return holiday


# =============================================================================
# EASTER / GOOD FRIDAY
# =============================================================================

def easter_sunday(
    year: int,
) -> date:
    """
    Gregorian Easter calculation.
    Standard library only.
    """

    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4

    f = (
        b + 8
    ) // 25

    g = (
        b - f + 1
    ) // 3

    h = (
        19 * a
        + b
        - d
        - g
        + 15
    ) % 30

    i = c // 4
    k = c % 4

    l = (
        32
        + 2 * e
        + 2 * i
        - h
        - k
    ) % 7

    m = (
        a
        + 11 * h
        + 22 * l
    ) // 451

    month = (
        h
        + l
        - 7 * m
        + 114
    ) // 31

    day = (
        (
            h
            + l
            - 7 * m
            + 114
        )
        % 31
    ) + 1

    return date(
        year,
        month,
        day,
    )


# =============================================================================
# APPROXIMATE NYSE HOLIDAY CALENDAR
# =============================================================================

def get_market_holidays(
    year: int,
) -> set[date]:
    """
    Standardni NYSE praznici.

    Ovo pokriva redovne zatvorene dane.
    Vanredna zatvaranja tržišta nisu uključena.
    """

    holidays: set[date] = set()

    # New Year's Day
    holidays.add(
        observed_fixed_holiday(
            date(
                year,
                1,
                1,
            )
        )
    )

    # Martin Luther King Jr. Day
    holidays.add(
        nth_weekday_of_month(
            year,
            1,
            0,
            3,
        )
    )

    # Presidents' Day
    holidays.add(
        nth_weekday_of_month(
            year,
            2,
            0,
            3,
        )
    )

    # Good Friday
    holidays.add(
        easter_sunday(
            year
        )
        - timedelta(days=2)
    )

    # Memorial Day
    holidays.add(
        last_weekday_of_month(
            year,
            5,
            0,
        )
    )

    # Juneteenth
    holidays.add(
        observed_fixed_holiday(
            date(
                year,
                6,
                19,
            )
        )
    )

    # Independence Day
    holidays.add(
        observed_fixed_holiday(
            date(
                year,
                7,
                4,
            )
        )
    )

    # Labor Day
    holidays.add(
        nth_weekday_of_month(
            year,
            9,
            0,
            1,
        )
    )

    # Thanksgiving
    holidays.add(
        nth_weekday_of_month(
            year,
            11,
            3,
            4,
        )
    )

    # Christmas
    holidays.add(
        observed_fixed_holiday(
            date(
                year,
                12,
                25,
            )
        )
    )

    return holidays


def is_market_day(
    value: date,
) -> bool:

    # Saturday / Sunday
    if value.weekday() >= 5:

        return False

    holidays = (
        get_market_holidays(
            value.year
        )
    )

    return (
        value not in holidays
    )


def previous_market_day(
    value: date,
) -> date:

    current = value

    while not is_market_day(
        current
    ):

        current -= timedelta(
            days=1
        )

    return current


def expected_price_date(
    current_date: date,
) -> date:
    """
    Pipeline koristi konzervativni T-1 daily pristup.

    Zato počinjemo od jučerašnjeg datuma
    i vraćamo se do posljednjeg validnog
    NYSE trgovačkog dana.
    """

    candidate = (
        current_date
        - timedelta(days=1)
    )

    return previous_market_day(
        candidate
    )


# =============================================================================
# TIME HELPERS
# =============================================================================

def age_hours(
    timestamp: datetime | None,
    now: datetime,
) -> float | None:

    if timestamp is None:
        return None

    return (
        now - timestamp
    ).total_seconds() / 3600.0


# =============================================================================
# PRICE FRESHNESS
# =============================================================================

def check_price_freshness(
    ticker: str,
    expected_date: date,
    now: datetime,
) -> dict:

    rows = spark.sql(
        f"""
        SELECT
            MAX(date)
                AS latest_data_date,

            MAX(_ingested_at)
                AS latest_ingested_at

        FROM {PRICE_TABLE}

        WHERE ticker = '{ticker}'
        """
    ).collect()

    row = rows[0]

    latest_date = row[
        "latest_data_date"
    ]

    latest_ingested_at = row[
        "latest_ingested_at"
    ]

    ingestion_age = age_hours(
        latest_ingested_at,
        now,
    )

    if latest_date is None:

        status = "ERROR"

        lag_days = None

        detail = (
            "Nema stock price podataka."
        )

    elif (
        latest_date
        >= expected_date
    ):

        status = "FRESH"

        lag_days = 0

        detail = (
            f"Latest price date "
            f"{latest_date} odgovara "
            f"očekivanom tržišnom datumu "
            f"{expected_date}."
        )

    else:

        status = "STALE"

        lag_days = (
            expected_date
            - latest_date
        ).days

        detail = (
            f"Latest price date "
            f"{latest_date}; očekivano "
            f"{expected_date}."
        )

    return {
        "checked_at": now,
        "data_type": "stock_prices",
        "ticker": ticker,
        "status": status,
        "latest_data_date": latest_date,
        "expected_data_date": (
            expected_date
        ),
        "latest_ingested_at": (
            latest_ingested_at
        ),
        "age_hours": (
            ingestion_age
        ),
        "lag_days": lag_days,
        "detail": detail,
    }


# =============================================================================
# NEWS FRESHNESS
# =============================================================================

def check_news_freshness(
    ticker: str,
    current_date: date,
    now: datetime,
) -> dict:

    rows = spark.sql(
        f"""
        SELECT
            MAX(
                CAST(
                    published_utc
                    AS DATE
                )
            ) AS latest_data_date,

            MAX(_ingested_at)
                AS latest_ingested_at

        FROM {NEWS_TABLE}

        WHERE requested_ticker = '{ticker}'
        """
    ).collect()

    row = rows[0]

    latest_date = row[
        "latest_data_date"
    ]

    latest_ingested_at = row[
        "latest_ingested_at"
    ]

    ingestion_age = age_hours(
        latest_ingested_at,
        now,
    )

    if latest_ingested_at is None:

        status = "ERROR"

        detail = (
            "Nema Stock News ingestion "
            "timestamp-a."
        )

    elif (
        ingestion_age
        <= NEWS_FRESHNESS_HOURS
    ):

        status = "FRESH"

        detail = (
            f"News ingestion je osvježen "
            f"prije {ingestion_age:.2f} h. "
            f"Najnovija objavljena vijest: "
            f"{latest_date}."
        )

    else:

        status = "STALE"

        detail = (
            f"News ingestion nije osvježen "
            f"{ingestion_age:.2f} h."
        )

    if latest_date is None:

        lag_days = None

    else:

        lag_days = max(
            (
                current_date
                - latest_date
            ).days,
            0,
        )

    return {
        "checked_at": now,
        "data_type": "stock_news",
        "ticker": ticker,
        "status": status,
        "latest_data_date": latest_date,
        "expected_data_date": (
            current_date
        ),
        "latest_ingested_at": (
            latest_ingested_at
        ),
        "age_hours": (
            ingestion_age
        ),
        "lag_days": lag_days,
        "detail": detail,
    }


# =============================================================================
# OUTPUT SCHEMA
# =============================================================================

FRESHNESS_SCHEMA = T.StructType(
    [
        T.StructField(
            "checked_at",
            T.TimestampType(),
            False,
        ),
        T.StructField(
            "data_type",
            T.StringType(),
            False,
        ),
        T.StructField(
            "ticker",
            T.StringType(),
            False,
        ),
        T.StructField(
            "status",
            T.StringType(),
            False,
        ),
        T.StructField(
            "latest_data_date",
            T.DateType(),
            True,
        ),
        T.StructField(
            "expected_data_date",
            T.DateType(),
            True,
        ),
        T.StructField(
            "latest_ingested_at",
            T.TimestampType(),
            True,
        ),
        T.StructField(
            "age_hours",
            T.DoubleType(),
            True,
        ),
        T.StructField(
            "lag_days",
            T.IntegerType(),
            True,
        ),
        T.StructField(
            "detail",
            T.StringType(),
            True,
        ),
    ]
)


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
        "DATA FRESHNESS CHECK"
    )

    separator()

    now = (
        datetime
        .now(timezone.utc)
        .replace(
            tzinfo=None
        )
    )

    current_date = (
        now.date()
    )

    expected_prices = (
        expected_price_date(
            current_date
        )
    )

    print(
        f"Checked at UTC: "
        f"{now}"
    )

    print(
        f"Current date: "
        f"{current_date}"
    )

    print(
        f"Expected latest "
        f"price date: "
        f"{expected_prices}"
    )

    print(
        f"News freshness limit: "
        f"{NEWS_FRESHNESS_HOURS:.0f} h"
    )

    # =========================================================================
    # REQUIRED TABLES
    # =========================================================================

    missing_tables = []

    for table_name in [
        PRICE_TABLE,
        NEWS_TABLE,
    ]:

        if not (
            spark.catalog.tableExists(
                table_name
            )
        ):

            missing_tables.append(
                table_name
            )

    if missing_tables:

        raise RuntimeError(
            "Nedostaju potrebne tabele: "
            + ", ".join(
                missing_tables
            )
        )

    # =========================================================================
    # CHECK ALL TICKERS
    # =========================================================================

    results = []

    for ticker in TICKERS:

        price_result = (
            check_price_freshness(
                ticker=ticker,
                expected_date=(
                    expected_prices
                ),
                now=now,
            )
        )

        results.append(
            price_result
        )

        news_result = (
            check_news_freshness(
                ticker=ticker,
                current_date=(
                    current_date
                ),
                now=now,
            )
        )

        results.append(
            news_result
        )

    # =========================================================================
    # SYSTEM STATUS
    # =========================================================================

    individual_statuses = [
        item["status"]
        for item in results
    ]

    if "ERROR" in individual_statuses:

        overall_status = "ERROR"

    elif "STALE" in individual_statuses:

        overall_status = "STALE"

    else:

        overall_status = "FRESH"

    fresh_count = (
        individual_statuses.count(
            "FRESH"
        )
    )

    stale_count = (
        individual_statuses.count(
            "STALE"
        )
    )

    error_count = (
        individual_statuses.count(
            "ERROR"
        )
    )

    results.append(
        {
            "checked_at": now,
            "data_type": "system",
            "ticker": "ALL",
            "status": (
                overall_status
            ),
            "latest_data_date": None,
            "expected_data_date": (
                expected_prices
            ),
            "latest_ingested_at": None,
            "age_hours": None,
            "lag_days": None,
            "detail": (
                f"FRESH={fresh_count}, "
                f"STALE={stale_count}, "
                f"ERROR={error_count}"
            ),
        }
    )

    # =========================================================================
    # WRITE GOLD TABLE
    # =========================================================================

    spark.sql(
        """
        CREATE SCHEMA IF NOT EXISTS
            workspace.gold
        """
    )

    freshness_df = (
        spark.createDataFrame(
            results,
            schema=(
                FRESHNESS_SCHEMA
            ),
        )
    )

    (
        freshness_df
        .write
        .format("delta")
        .mode("overwrite")
        .option(
            "overwriteSchema",
            "true",
        )
        .saveAsTable(
            FRESHNESS_TABLE
        )
    )

    # =========================================================================
    # DISPLAY RESULTS
    # =========================================================================

    print()

    separator()

    print(
        "DATA FRESHNESS RESULT"
    )

    separator()

    (
        freshness_df
        .select(
            "data_type",
            "ticker",
            "status",
            "latest_data_date",
            "expected_data_date",
            "age_hours",
            "lag_days",
        )
        .orderBy(
            "data_type",
            "ticker",
        )
        .show(
            50,
            truncate=False,
        )
    )

    print()

    print(
        f"SYSTEM STATUS: "
        f"{overall_status}"
    )

    print(
        f"FRESH: {fresh_count}"
    )

    print(
        f"STALE: {stale_count}"
    )

    print(
        f"ERROR: {error_count}"
    )

    print(
        f"Saved table: "
        f"{FRESHNESS_TABLE}"
    )

    print()

    separator()

    if overall_status == "FRESH":

        print(
            "[PASS] MARKET DATA "
            "IS FRESH"
        )

    elif overall_status == "STALE":

        print(
            "[WARN] SOME MARKET DATA "
            "IS STALE"
        )

    else:

        print(
            "[ERROR] DATA FRESHNESS "
            "CHECK DETECTED ERRORS"
        )

    separator()


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    main()