from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from threading import Lock
from typing import Any
import os
import subprocess
import sys

from databricks.connect import DatabricksSession
from fastapi import APIRouter, HTTPException, status


router = APIRouter(
    prefix="/system",
    tags=["System"],
)


DATA_FRESHNESS_TABLE = (
    "workspace.gold.data_freshness"
)


# ============================================================
# PROJECT / REFRESH CONFIGURATION
# ============================================================

PROJECT_ROOT = (
    Path(__file__)
    .resolve()
    .parents[3]
)

REFRESH_SCRIPT = (
    PROJECT_ROOT
    / "src"
    / "orchestration"
    / "refresh_market_data.py"
)

REFRESH_SUMMARY_FILE = (
    PROJECT_ROOT
    / "logs"
    / "market_refresh_last.json"
)

REFRESH_LOG_FILE = (
    PROJECT_ROOT
    / "logs"
    / "market_refresh_api.log"
)

REFRESH_LOCK = Lock()


# ============================================================
# SPARK / DATABRICKS
# ============================================================

spark = (
    DatabricksSession
    .builder
    .getOrCreate()
)


# ============================================================
# SERIALIZATION HELPERS
# ============================================================

def serialize_value(
    value: Any,
):
    """
    Converts Spark values into JSON-safe Python values.
    """

    if value is None:
        return None

    if isinstance(
        value,
        datetime,
    ):
        return value.isoformat()

    if isinstance(
        value,
        date,
    ):
        return value.isoformat()

    if isinstance(
        value,
        Decimal,
    ):
        return float(value)

    return value


def serialize_row(
    row,
) -> dict:
    """
    Converts a Spark Row into a JSON-safe dictionary.
    """

    raw = row.asDict(
        recursive=True
    )

    return {
        key: serialize_value(value)
        for key, value
        in raw.items()
    }


# ============================================================
# DATABRICKS BOOTSTRAP DISCOVERY
# ============================================================

def find_dbconnect_bootstrap() -> Path | None:
    """
    Finds the Databricks VS Code extension bootstrap script.

    The extension version is intentionally not hard-coded so the
    endpoint continues to work after normal extension updates.
    """

    vscode_extensions = (
        Path.home()
        / ".vscode"
        / "extensions"
    )

    if not vscode_extensions.exists():
        return None

    candidates = sorted(
        vscode_extensions.glob(
            "databricks.databricks-*-win32-x64/"
            "resources/python/dbconnect-bootstrap.py"
        ),
        reverse=True,
    )

    if not candidates:
        candidates = sorted(
            vscode_extensions.glob(
                "databricks.databricks-*/"
                "resources/python/dbconnect-bootstrap.py"
            ),
            reverse=True,
        )

    return (
        candidates[0]
        if candidates
        else None
    )


# ============================================================
# REFRESH OUTPUT HELPERS
# ============================================================

def tail_text(
    text: str,
    max_chars: int = 5000,
) -> str:
    """
    Keeps API error responses reasonably small.
    """

    if len(text) <= max_chars:
        return text

    return text[-max_chars:]


# ============================================================
# DATA FRESHNESS
# ============================================================

@router.get(
    "/data-freshness"
)
def get_data_freshness():
    """
    Returns the latest persisted data freshness state from
    workspace.gold.data_freshness.

    The endpoint does NOT run the ingestion pipeline.
    It only exposes the most recently calculated freshness
    results to the application/frontend.
    """

    try:

        rows = (
            spark.sql(
                f"""
                SELECT
                    data_type,
                    ticker,
                    status,
                    latest_data_date,
                    expected_data_date,
                    age_hours,
                    lag_days
                FROM {DATA_FRESHNESS_TABLE}
                ORDER BY
                    CASE
                        WHEN data_type = 'system'
                        THEN 0
                        WHEN data_type = 'stock_prices'
                        THEN 1
                        WHEN data_type = 'stock_news'
                        THEN 2
                        ELSE 3
                    END,
                    ticker
                """
            )
            .collect()
        )

    except Exception as exc:

        print(
            "\n"
            + "=" * 80
        )

        print(
            "DATA FRESHNESS API ERROR"
        )

        print(
            "=" * 80
        )

        print(
            f"Exception: {exc}"
        )

        print(
            "=" * 80
            + "\n"
        )

        raise HTTPException(
            status_code=(
                status.HTTP_503_SERVICE_UNAVAILABLE
            ),
            detail=(
                "Data freshness information "
                "is currently unavailable."
            ),
        ) from exc


    if not rows:

        raise HTTPException(
            status_code=(
                status.HTTP_404_NOT_FOUND
            ),
            detail=(
                "No data freshness results found."
            ),
        )


    # ========================================================
    # SERIALIZE
    # ========================================================

    checks = [
        serialize_row(row)
        for row in rows
    ]


    # ========================================================
    # SYSTEM ROW
    # ========================================================

    system_check = next(
        (
            check
            for check in checks
            if (
                check.get("data_type")
                == "system"
                and check.get("ticker")
                == "ALL"
            )
        ),
        None,
    )


    # ========================================================
    # INDIVIDUAL CHECKS
    # ========================================================

    individual_checks = [
        check
        for check in checks
        if check.get("data_type")
        != "system"
    ]


    total_checks = len(
        individual_checks
    )


    fresh_checks = sum(
        1
        for check in individual_checks
        if (
            str(
                check.get(
                    "status",
                    ""
                )
            )
            .upper()
            == "FRESH"
        )
    )


    stale_checks = sum(
        1
        for check in individual_checks
        if (
            str(
                check.get(
                    "status",
                    ""
                )
            )
            .upper()
            == "STALE"
        )
    )


    error_checks = sum(
        1
        for check in individual_checks
        if (
            str(
                check.get(
                    "status",
                    ""
                )
            )
            .upper()
            == "ERROR"
        )
    )


    # ========================================================
    # OVERALL STATUS
    # ========================================================

    if system_check:

        overall_status = (
            str(
                system_check.get(
                    "status",
                    "UNKNOWN",
                )
            )
            .upper()
        )

    elif error_checks > 0:

        overall_status = "ERROR"

    elif stale_checks > 0:

        overall_status = "STALE"

    elif (
        total_checks > 0
        and fresh_checks
        == total_checks
    ):

        overall_status = "FRESH"

    else:

        overall_status = "UNKNOWN"


    # ========================================================
    # PRICE CHECKS
    # ========================================================

    price_checks = [
        check
        for check in individual_checks
        if (
            check.get("data_type")
            == "stock_prices"
        )
    ]


    price_dates = [
        check.get(
            "latest_data_date"
        )
        for check in price_checks
        if check.get(
            "latest_data_date"
        )
    ]


    latest_market_date = (
        max(price_dates)
        if price_dates
        else None
    )


    expected_price_dates = [
        check.get(
            "expected_data_date"
        )
        for check in price_checks
        if check.get(
            "expected_data_date"
        )
    ]


    expected_market_date = (
        max(expected_price_dates)
        if expected_price_dates
        else None
    )


    # ========================================================
    # NEWS CHECKS
    # ========================================================

    news_checks = [
        check
        for check in individual_checks
        if (
            check.get("data_type")
            == "stock_news"
        )
    ]


    news_dates = [
        check.get(
            "latest_data_date"
        )
        for check in news_checks
        if check.get(
            "latest_data_date"
        )
    ]


    latest_news_date = (
        max(news_dates)
        if news_dates
        else None
    )


    # ========================================================
    # DATA-TYPE STATUS
    # ========================================================

    def group_status(
        group: list[dict],
    ) -> str:

        if not group:
            return "UNKNOWN"

        statuses = {
            str(
                item.get(
                    "status",
                    "UNKNOWN",
                )
            )
            .upper()

            for item in group
        }

        if "ERROR" in statuses:
            return "ERROR"

        if "STALE" in statuses:
            return "STALE"

        if statuses == {
            "FRESH"
        }:
            return "FRESH"

        return "UNKNOWN"


    market_status = group_status(
        price_checks
    )


    news_status = group_status(
        news_checks
    )


    # ========================================================
    # RESPONSE
    # ========================================================

    return {
        "overall_status": (
            overall_status
        ),

        "retrieved_at": (
            datetime
            .now(UTC)
            .isoformat()
        ),

        "summary": {
            "total_checks": (
                total_checks
            ),
            "fresh_checks": (
                fresh_checks
            ),
            "stale_checks": (
                stale_checks
            ),
            "error_checks": (
                error_checks
            ),
        },

        "market_data": {
            "status": (
                market_status
            ),
            "latest_date": (
                latest_market_date
            ),
            "expected_date": (
                expected_market_date
            ),
        },

        "news_data": {
            "status": (
                news_status
            ),
            "latest_date": (
                latest_news_date
            ),
        },

        "system": (
            system_check
        ),

        "checks": (
            individual_checks
        ),
    }


# ============================================================
# FULL MARKET DATA REFRESH
# ============================================================

@router.post(
    "/refresh-market-data"
)
def refresh_market_data():
    """
    Runs the existing end-to-end market refresh orchestrator.

    The request waits until the pipeline finishes. A process-wide
    lock prevents accidental duplicate refresh executions from
    multiple frontend clicks.
    """

    acquired = REFRESH_LOCK.acquire(
        blocking=False
    )

    if not acquired:

        raise HTTPException(
            status_code=(
                status.HTTP_409_CONFLICT
            ),
            detail=(
                "A market data refresh is already running."
            ),
        )


    started_at = datetime.now(UTC)

    try:

        if not REFRESH_SCRIPT.exists():

            raise HTTPException(
                status_code=(
                    status.HTTP_500_INTERNAL_SERVER_ERROR
                ),
                detail=(
                    "Market refresh script was not found."
                ),
            )


        bootstrap = (
            find_dbconnect_bootstrap()
        )

        if bootstrap is None:

            raise HTTPException(
                status_code=(
                    status.HTTP_500_INTERNAL_SERVER_ERROR
                ),
                detail=(
                    "Databricks Connect bootstrap script "
                    "was not found."
                ),
            )


        REFRESH_LOG_FILE.parent.mkdir(
            parents=True,
            exist_ok=True,
        )


        command = [
            sys.executable,
            str(bootstrap),
            str(REFRESH_SCRIPT),
        ]


        environment = os.environ.copy()
        # Force UTF-8 for Python subprocesses on Windows.
        # Without this, Serbian characters such as č, ć, š, ž
        # can cause UnicodeEncodeError when the refresh pipeline
        # is launched from FastAPI.
        environment["PYTHONUTF8"] = "1"
        environment["PYTHONIOENCODING"] = "utf-8"

        with REFRESH_LOG_FILE.open(
            "w",
            encoding="utf-8",
            errors="replace",
        ) as log_file:

            log_file.write(
                "MARKET DATA REFRESH STARTED FROM API\n"
            )

            log_file.write(
                f"Started: {started_at.isoformat()}\n"
            )

            log_file.write(
                f"Command: {' '.join(command)}\n\n"
            )

            log_file.flush()


            process = subprocess.run(
                command,
                cwd=str(PROJECT_ROOT),
                env=environment,
                stdout=log_file,
                stderr=subprocess.STDOUT,
                text=True,
                check=False,
            )


        finished_at = datetime.now(UTC)

        duration_seconds = (
            finished_at
            - started_at
        ).total_seconds()


        if process.returncode != 0:

            try:

                log_tail = tail_text(
                    REFRESH_LOG_FILE.read_text(
                        encoding="utf-8",
                        errors="replace",
                    )
                )

            except Exception:

                log_tail = (
                    "Refresh log could not be read."
                )


            print(
                "\n"
                + "=" * 80
            )

            print(
                "MARKET DATA REFRESH API FAILED"
            )

            print(
                "=" * 80
            )

            print(
                log_tail
            )

            print(
                "=" * 80
                + "\n"
            )


            raise HTTPException(
                status_code=(
                    status.HTTP_500_INTERNAL_SERVER_ERROR
                ),
                detail=(
                    "Market data refresh failed. "
                    "Check logs/market_refresh_api.log "
                    "and logs/market_refresh_last.json."
                ),
            )


        return {
            "status": "completed",

            "message": (
                "Market data refresh completed "
                "successfully."
            ),

            "started_at": (
                started_at.isoformat()
            ),

            "finished_at": (
                finished_at.isoformat()
            ),

            "duration_seconds": round(
                duration_seconds,
                2,
            ),

            "refresh_log": str(
                REFRESH_LOG_FILE
            ),

            "summary_file": (
                str(REFRESH_SUMMARY_FILE)
                if REFRESH_SUMMARY_FILE.exists()
                else None
            ),
        }


    finally:

        REFRESH_LOCK.release()
