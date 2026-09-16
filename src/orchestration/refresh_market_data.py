from __future__ import annotations

import argparse
import json
import runpy
import sys
import time

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


# =============================================================================
# CONFIGURATION
# =============================================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

LOG_DIRECTORY = PROJECT_ROOT / "logs"
LAST_RUN_FILE = LOG_DIRECTORY / "market_refresh_last.json"


@dataclass(frozen=True)
class PipelineStep:
    name: str
    description: str
    relative_path: str
    required: bool = True


# =============================================================================
# PIPELINE DEFINITION
# =============================================================================

CORE_PIPELINE = [
    PipelineStep(
        name="incremental_stock_prices",
        description=(
            "Incremental Massive API -> "
            "Bronze stock prices"
        ),
        relative_path=(
            "src/orchestration/"
            "incremental_market_ingestion.py"
        ),
    ),

    PipelineStep(
        name="incremental_stock_news",
        description=(
            "Incremental Massive API -> "
            "Bronze stock news"
        ),
        relative_path=(
            "src/orchestration/"
            "incremental_stock_news.py"
        ),
    ),

    PipelineStep(
        name="silver_stock_prices",
        description=(
            "Bronze -> Silver stock prices"
        ),
        relative_path=(
            "src/silver/stock_prices.py"
        ),
    ),

    PipelineStep(
        name="silver_stock_news",
        description=(
            "Bronze -> Silver stock news"
        ),
        relative_path=(
            "src/silver/stock_news.py"
        ),
    ),

    PipelineStep(
        name="silver_company_details",
        description=(
            "Bronze -> Silver company details"
        ),
        relative_path=(
            "src/silver/company_details.py"
        ),
    ),

    PipelineStep(
        name="gold_stock_daily_metrics",
        description=(
            "Silver -> Gold daily metrics"
        ),
        relative_path=(
            "src/gold/stock_daily_metrics.py"
        ),
    ),

    PipelineStep(
        name="gold_technical_indicators",
        description=(
            "Build technical indicators"
        ),
        relative_path=(
            "src/gold/technical_indicators.py"
        ),
    ),

    PipelineStep(
        name="gold_company_performance",
        description=(
            "Build company performance"
        ),
        relative_path=(
            "src/gold/company_performance.py"
        ),
    ),

    PipelineStep(
        name="gold_news_analytics",
        description=(
            "Build Gold news analytics"
        ),
        relative_path=(
            "src/gold/news_analytics.py"
        ),
    ),

    PipelineStep(
        name="gold_stock_anomalies",
        description=(
            "Run stock anomaly detection"
        ),
        relative_path=(
            "src/gold/stock_anomalies.py"
        ),
    ),

    PipelineStep(
        name="data_freshness",
        description=(
            "Calculate data freshness status"
        ),
        relative_path=(
            "src/orchestration/"
            "data_freshness.py"
        ),
    ),
]


VALIDATION_PIPELINE = [

    PipelineStep(
        name="validate_silver",
        description="Validate Silver layer",
        relative_path="src/silver/validate_silver.py",
    ),

    PipelineStep(
        name="validate_gold",
        description="Validate Gold layer",
        relative_path="src/gold/validate_gold.py",
    ),
]


# =============================================================================
# HELPERS
# =============================================================================

def separator(
    character: str = "=",
    width: int = 100,
) -> None:
    print(character * width)


def resolve_step_path(
    step: PipelineStep,
) -> Path:
    return PROJECT_ROOT / step.relative_path


def format_duration(
    seconds: float,
) -> str:
    if seconds < 60:
        return f"{seconds:.2f}s"

    minutes = int(seconds // 60)
    remaining_seconds = seconds % 60

    return (
        f"{minutes}m "
        f"{remaining_seconds:.1f}s"
    )


# =============================================================================
# PREFLIGHT
# =============================================================================

def validate_pipeline_files(
    steps: list[PipelineStep],
) -> list[str]:

    missing_files = []

    print()
    separator()
    print("PIPELINE PREFLIGHT")
    separator()

    for index, step in enumerate(
        steps,
        start=1,
    ):

        script_path = resolve_step_path(step)

        if script_path.exists():

            print(
                f"[OK] {index:02d}. "
                f"{step.name:<28} "
                f"{step.relative_path}"
            )

        else:

            status = (
                "MISSING"
                if step.required
                else "OPTIONAL"
            )

            print(
                f"[{status}] {index:02d}. "
                f"{step.name:<28} "
                f"{step.relative_path}"
            )

            if step.required:
                missing_files.append(
                    step.relative_path
                )

    return missing_files


# =============================================================================
# RUN ONE PIPELINE SCRIPT
# =============================================================================

def run_pipeline_step(
    step: PipelineStep,
    index: int,
    total_steps: int,
) -> dict:

    script_path = resolve_step_path(step)

    separator()
    print(
        f"[{index}/{total_steps}] "
        f"{step.description}"
    )
    separator()

    print(f"Step:   {step.name}")
    print(f"Script: {step.relative_path}")
    print()

    started_at = datetime.now()
    started_perf = time.perf_counter()

    original_argv = sys.argv.copy()

    try:

        # Sprjecava da CLI argumenti orchestratora
        # budu proslijedjeni pojedinacnoj ETL skripti.
        sys.argv = [str(script_path)]

        runpy.run_path(
            str(script_path),
            run_name="__main__",
        )

        duration = (
            time.perf_counter()
            - started_perf
        )

        print()
        print(
            f"[PASS] {step.name} "
            f"({format_duration(duration)})"
        )

        return {
            "name": step.name,
            "description": step.description,
            "script": step.relative_path,
            "status": "SUCCESS",
            "started_at": (
                started_at.isoformat(
                    timespec="seconds"
                )
            ),
            "finished_at": (
                datetime.now().isoformat(
                    timespec="seconds"
                )
            ),
            "duration_seconds": round(
                duration,
                3,
            ),
            "error": None,
        }

    except SystemExit as exc:

        duration = (
            time.perf_counter()
            - started_perf
        )

        # SystemExit(0) tretiramo kao uspjesan
        # zavrsetak skripte.
        if exc.code in (
            None,
            0,
        ):

            return {
                "name": step.name,
                "description": (
                    step.description
                ),
                "script": step.relative_path,
                "status": "SUCCESS",
                "started_at": (
                    started_at.isoformat(
                        timespec="seconds"
                    )
                ),
                "finished_at": (
                    datetime.now().isoformat(
                        timespec="seconds"
                    )
                ),
                "duration_seconds": round(
                    duration,
                    3,
                ),
                "error": None,
            }

        raise RuntimeError(
            f"{step.name} exited "
            f"with code {exc.code}"
        ) from exc

    except Exception as exc:

        duration = (
            time.perf_counter()
            - started_perf
        )

        print()
        print(
            f"[FAIL] {step.name}"
        )

        print(
            f"Exception: "
            f"{type(exc).__name__}"
        )

        print(
            f"Message: {exc}"
        )

        return {
            "name": step.name,
            "description": step.description,
            "script": step.relative_path,
            "status": "FAILED",
            "started_at": (
                started_at.isoformat(
                    timespec="seconds"
                )
            ),
            "finished_at": (
                datetime.now().isoformat(
                    timespec="seconds"
                )
            ),
            "duration_seconds": round(
                duration,
                3,
            ),
            "error": (
                f"{type(exc).__name__}: "
                f"{exc}"
            ),
        }

    finally:
        sys.argv = original_argv


# =============================================================================
# SAVE LOCAL RUN SUMMARY
# =============================================================================

def save_run_summary(
    summary: dict,
) -> None:

    LOG_DIRECTORY.mkdir(
        parents=True,
        exist_ok=True,
    )

    with LAST_RUN_FILE.open(
        "w",
        encoding="utf-8",
    ) as file:

        json.dump(
            summary,
            file,
            indent=2,
            ensure_ascii=False,
            default=str,
        )


# =============================================================================
# PIPELINE
# =============================================================================

def run_refresh_pipeline(
    validate: bool = False,
    dry_run: bool = False,
) -> dict:

    pipeline_started = datetime.now()
    performance_started = (
        time.perf_counter()
    )

    steps = list(CORE_PIPELINE)

    if validate:
        steps.extend(
            VALIDATION_PIPELINE
        )

    separator()
    print(
        "AI STOCK MARKET RESEARCH ASSISTANT"
    )
    print(
        "AUTOMATIC MARKET DATA REFRESH"
    )
    separator()

    print(
        f"Started: "
        f"{pipeline_started.isoformat(timespec='seconds')}"
    )

    print(
        f"Project: {PROJECT_ROOT}"
    )

    print(
        f"Validation: "
        f"{'ON' if validate else 'OFF'}"
    )

    print(
        f"Dry run: "
        f"{'YES' if dry_run else 'NO'}"
    )

    missing_files = (
        validate_pipeline_files(
            steps
        )
    )

    # -------------------------------------------------------------------------
    # DRY RUN
    # -------------------------------------------------------------------------

    if dry_run:

        print()
        separator()

        if missing_files:

            print(
                "DRY RUN RESULT: "
                "PIPELINE NOT READY"
            )

            print()
            print(
                "Missing required scripts:"
            )

            for path in missing_files:
                print(f" - {path}")

        else:

            print(
                "DRY RUN RESULT: "
                "PIPELINE READY"
            )

        separator()

        return {
            "status": (
                "NOT_READY"
                if missing_files
                else "READY"
            ),
            "missing_files": (
                missing_files
            ),
        }

    # -------------------------------------------------------------------------
    # NORMAL RUN REQUIRES COMPLETE PIPELINE
    # -------------------------------------------------------------------------

    if missing_files:

        raise FileNotFoundError(
            "Pipeline nije kompletan. "
            "Nedostaju: "
            + ", ".join(
                missing_files
            )
        )

    results = []

    pipeline_status = "SUCCESS"
    failed_step = None

    for index, step in enumerate(
        steps,
        start=1,
    ):

        result = run_pipeline_step(
            step=step,
            index=index,
            total_steps=len(steps),
        )

        results.append(result)

        # Ako jedan dependency korak padne,
        # NE nastavljamo dalje.
        #
        # Primjer:
        # ako Silver prices padne,
        # nema smisla racunati novi Gold.
        if result["status"] != "SUCCESS":

            pipeline_status = "FAILED"
            failed_step = step.name

            print()
            print(
                "Pipeline stopped because "
                "a required step failed."
            )

            break

    pipeline_finished = datetime.now()

    total_duration = (
        time.perf_counter()
        - performance_started
    )

    summary = {
        "pipeline": (
            "market_data_refresh"
        ),
        "status": pipeline_status,
        "failed_step": failed_step,
        "started_at": (
            pipeline_started.isoformat(
                timespec="seconds"
            )
        ),
        "finished_at": (
            pipeline_finished.isoformat(
                timespec="seconds"
            )
        ),
        "duration_seconds": round(
            total_duration,
            3,
        ),
        "validation_enabled": validate,
        "steps": results,
    }

    save_run_summary(
        summary
    )

    print()
    separator()

    if pipeline_status == "SUCCESS":

        print(
            "[PASS] MARKET DATA REFRESH "
            "COMPLETED"
        )

    else:

        print(
            "[FAIL] MARKET DATA REFRESH "
            "FAILED"
        )

        print(
            f"Failed step: {failed_step}"
        )

    print(
        f"Duration: "
        f"{format_duration(total_duration)}"
    )

    print(
        f"Run summary: {LAST_RUN_FILE}"
    )

    separator()

    return summary


# =============================================================================
# CLI
# =============================================================================

def parse_arguments():

    parser = argparse.ArgumentParser(
        description=(
            "Refresh complete market-data "
            "pipeline."
        )
    )

    parser.add_argument(
        "--validate",
        action="store_true",
        help=(
            "Run Silver and Gold validation "
            "after refresh."
        ),
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Check pipeline files without "
            "executing ETL."
        ),
    )

    return parser.parse_args()


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":

    args = parse_arguments()

    result = run_refresh_pipeline(
        validate=args.validate,
        dry_run=args.dry_run,
    )

    if result.get("status") in (
        "FAILED",
        "NOT_READY",
    ):
        sys.exit(1)