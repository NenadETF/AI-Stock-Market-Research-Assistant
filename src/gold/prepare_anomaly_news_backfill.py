from collections import defaultdict
from datetime import timedelta

from databricks.connect import DatabricksSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DateType,
    IntegerType,
)


# =============================================================================
# CONFIGURATION
# =============================================================================

ANOMALY_TABLE = "workspace.gold.stock_anomalies"
NEWS_TABLE = "workspace.silver.stock_news"

TARGET_TABLE = "workspace.gold.stock_anomaly_news_backfill_plan"

NEWS_WINDOW_DAYS = 3


# =============================================================================
# SPARK SESSION
# =============================================================================

spark = DatabricksSession.builder.getOrCreate()


print("=" * 100)
print("ANOMALY NEWS HISTORICAL BACKFILL PLAN")
print("=" * 100)

print(f"Anomaly source: {ANOMALY_TABLE}")
print(f"News source:    {NEWS_TABLE}")
print(f"Plan target:    {TARGET_TABLE}")
print(f"Window:         +/- {NEWS_WINDOW_DAYS} dana")


# =============================================================================
# 1. LOAD FINAL ANOMALIES
# =============================================================================

anomaly_df = (
    spark.table(ANOMALY_TABLE)
    .filter(F.col("is_anomaly"))
    .select(
        "ticker",
        "date"
    )
    .orderBy(
        "ticker",
        "date"
    )
)


anomaly_count = anomaly_df.count()


print()
print(f"Finalnih anomalija: {anomaly_count}")


# =============================================================================
# 2. CURRENT NEWS COVERAGE
# =============================================================================

news_coverage_df = (
    spark.table(NEWS_TABLE)
    .groupBy("requested_ticker")
    .agg(
        F.min("published_date").alias("coverage_start"),
        F.max("published_date").alias("coverage_end")
    )
)


coverage_rows = news_coverage_df.collect()


coverage_by_ticker = {}

for row in coverage_rows:

    coverage_by_ticker[row["requested_ticker"]] = {
        "start": row["coverage_start"],
        "end": row["coverage_end"]
    }


# =============================================================================
# 3. CREATE RAW +/- 3 DAY WINDOWS
# =============================================================================

raw_windows = defaultdict(list)


for row in anomaly_df.collect():

    ticker = row["ticker"]
    anomaly_date = row["date"]

    start_date = (
        anomaly_date
        - timedelta(days=NEWS_WINDOW_DAYS)
    )

    end_date = (
        anomaly_date
        + timedelta(days=NEWS_WINDOW_DAYS)
    )

    raw_windows[ticker].append(
        (
            start_date,
            end_date,
            anomaly_date
        )
    )


# =============================================================================
# 4. REMOVE PERIODS ALREADY COVERED BY NEWS DATA
#
# Ako anomaly window djelimicno ulazi u postojeci news period,
# zadrzavamo samo dio koji nedostaje.
#
# Primjer:
#
# anomaly window:
# 2026-07-28 -> 2026-08-03
#
# news coverage:
# 2026-08-01 -> 2026-09-01
#
# backfill:
# 2026-07-28 -> 2026-07-31
# =============================================================================

missing_windows = defaultdict(list)


for ticker, windows in raw_windows.items():

    coverage = coverage_by_ticker.get(ticker)

    for start_date, end_date, anomaly_date in windows:

        # -------------------------------------------------------------
        # Ako nema news coverage-a za ticker, treba cijeli interval.
        # -------------------------------------------------------------

        if coverage is None:

            missing_windows[ticker].append(
                (
                    start_date,
                    end_date,
                    anomaly_date
                )
            )

            continue


        coverage_start = coverage["start"]
        coverage_end = coverage["end"]


        # -------------------------------------------------------------
        # Window je potpuno izvan postojeceg coverage-a.
        # -------------------------------------------------------------

        if (
            end_date < coverage_start
            or start_date > coverage_end
        ):

            missing_windows[ticker].append(
                (
                    start_date,
                    end_date,
                    anomaly_date
                )
            )

            continue


        # -------------------------------------------------------------
        # Window je potpuno pokriven.
        # Ne treba API poziv.
        # -------------------------------------------------------------

        if (
            start_date >= coverage_start
            and end_date <= coverage_end
        ):

            continue


        # -------------------------------------------------------------
        # Nedostaje lijevi dio window-a.
        # -------------------------------------------------------------

        if start_date < coverage_start:

            missing_left_end = (
                coverage_start
                - timedelta(days=1)
            )

            if start_date <= missing_left_end:

                missing_windows[ticker].append(
                    (
                        start_date,
                        missing_left_end,
                        anomaly_date
                    )
                )


        # -------------------------------------------------------------
        # Nedostaje desni dio window-a.
        # =============================================================================

        if end_date > coverage_end:

            missing_right_start = (
                coverage_end
                + timedelta(days=1)
            )

            if missing_right_start <= end_date:

                missing_windows[ticker].append(
                    (
                        missing_right_start,
                        end_date,
                        anomaly_date
                    )
                )


# =============================================================================
# 5. MERGE OVERLAPPING WINDOWS
#
# Ako imamo:
#
# 2025-04-01 -> 2025-04-07
# 2025-04-05 -> 2025-04-11
#
# radimo samo:
#
# 2025-04-01 -> 2025-04-11
#
# Time drasticno smanjujemo broj API request perioda.
# =============================================================================

merged_plan = []


for ticker in sorted(missing_windows.keys()):

    ticker_windows = sorted(
        missing_windows[ticker],
        key=lambda x: x[0]
    )


    if not ticker_windows:
        continue


    current_start = ticker_windows[0][0]
    current_end = ticker_windows[0][1]

    anomaly_dates = {
        ticker_windows[0][2]
    }


    for (
        next_start,
        next_end,
        anomaly_date
    ) in ticker_windows[1:]:

        # -------------------------------------------------------------
        # Merge i kada se intervali direktno dodiruju.
        # -------------------------------------------------------------

        if (
            next_start
            <= current_end + timedelta(days=1)
        ):

            current_end = max(
                current_end,
                next_end
            )

            anomaly_dates.add(
                anomaly_date
            )

        else:

            merged_plan.append(
                {
                    "ticker": ticker,
                    "start_date": current_start,
                    "end_date": current_end,
                    "days": (
                        current_end
                        - current_start
                    ).days + 1,
                    "anomaly_count": len(
                        anomaly_dates
                    )
                }
            )


            current_start = next_start
            current_end = next_end

            anomaly_dates = {
                anomaly_date
            }


    # -----------------------------------------------------------------
    # Posljednji interval.
    # -----------------------------------------------------------------

    merged_plan.append(
        {
            "ticker": ticker,
            "start_date": current_start,
            "end_date": current_end,
            "days": (
                current_end
                - current_start
            ).days + 1,
            "anomaly_count": len(
                anomaly_dates
            )
        }
    )


# =============================================================================
# 6. CREATE SPARK DATAFRAME
# =============================================================================

schema = StructType(
    [
        StructField(
            "ticker",
            StringType(),
            False
        ),

        StructField(
            "start_date",
            DateType(),
            False
        ),

        StructField(
            "end_date",
            DateType(),
            False
        ),

        StructField(
            "days",
            IntegerType(),
            False
        ),

        StructField(
            "anomaly_count",
            IntegerType(),
            False
        ),
    ]
)


plan_df = spark.createDataFrame(
    merged_plan,
    schema=schema
)


# =============================================================================
# 7. ADD METADATA
# =============================================================================

plan_df = plan_df.withColumn(
    "_planned_at",
    F.current_timestamp()
)


# =============================================================================
# 8. WRITE GOLD PLAN TABLE
# =============================================================================

(
    plan_df.write
    .format("delta")
    .mode("overwrite")
    .option(
        "overwriteSchema",
        "true"
    )
    .saveAsTable(
        TARGET_TABLE
    )
)


print()
print(
    f"[OK] Backfill plan tabela kreirana: "
    f"{TARGET_TABLE}"
)


# =============================================================================
# 9. SUMMARY
# =============================================================================

plan_result_df = spark.table(
    TARGET_TABLE
)


plan_count = plan_result_df.count()


total_days = (
    plan_result_df
    .agg(
        F.sum("days").alias("total_days")
    )
    .first()["total_days"]
)


print()
print("=" * 100)
print("BACKFILL PLAN SUMMARY")
print("=" * 100)

print(f"Finalnih anomalija:     {anomaly_count}")
print(f"API date intervala:     {plan_count}")
print(f"Ukupno ticker-dana:     {total_days}")


# =============================================================================
# 10. PLAN BY TICKER
# =============================================================================

print()
print("BACKFILL PLAN PO TICKERU:")

(
    plan_result_df
    .groupBy("ticker")
    .agg(

        F.count("*")
        .alias(
            "interval_count"
        ),

        F.sum("days")
        .alias(
            "total_days"
        ),

        F.sum("anomaly_count")
        .alias(
            "covered_anomalies"
        ),

        F.min("start_date")
        .alias(
            "oldest_required_date"
        ),

        F.max("end_date")
        .alias(
            "latest_required_date"
        )
    )
    .orderBy("ticker")
    .show(
        n=100,
        truncate=False
    )
)


# =============================================================================
# 11. FULL PLAN
# =============================================================================

print()
print("DETALJAN BACKFILL PLAN:")

(
    plan_result_df
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
    .show(
        n=500,
        truncate=False
    )
)


print()
print("=" * 100)
print("ANOMALY NEWS BACKFILL PLAN COMPLETED")
print("=" * 100)