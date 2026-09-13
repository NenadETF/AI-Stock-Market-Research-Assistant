from databricks.connect import DatabricksSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# =============================================================================
# CONFIGURATION
# =============================================================================

SOURCE_TABLE = "workspace.gold.stock_daily_metrics"
ANOMALY_TABLE = "workspace.gold.stock_anomalies"


# =============================================================================
# SPARK SESSION
# =============================================================================

spark = DatabricksSession.builder.getOrCreate()


print("=" * 100)
print("FINAL STOCK ANOMALY DETECTION VALIDATION")
print("=" * 100)


source_df = spark.table(SOURCE_TABLE)
anomaly_df = spark.table(ANOMALY_TABLE)


# =============================================================================
# HELPER
# =============================================================================

def validation_result(condition, description, details=None):

    if condition:
        print(f"[PASS] {description}")
    else:
        print(f"[FAIL] {description}")

    if details is not None:
        print(f"       {details}")


# =============================================================================
# 1. TABLE COUNTS
# =============================================================================

print()
print("=" * 100)
print("1. TABLE COUNTS")
print("=" * 100)


source_count = source_df.count()
anomaly_count = anomaly_df.count()


print(f"stock_daily_metrics: {source_count}")
print(f"stock_anomalies:     {anomaly_count}")


validation_result(
    source_count > 0,
    "Source Gold tabela nije prazna"
)

validation_result(
    anomaly_count > 0,
    "Anomaly tabela nije prazna"
)

validation_result(
    source_count == anomaly_count,
    "Anomaly tabela ima isti broj redova kao source Gold tabela",
    f"Source: {source_count}, anomaly: {anomaly_count}"
)


# =============================================================================
# 2. BUSINESS KEYS
# =============================================================================

print()
print("=" * 100)
print("2. BUSINESS KEY VALIDATION")
print("=" * 100)


null_keys = (
    anomaly_df
    .filter(
        F.col("ticker").isNull()
        | F.col("date").isNull()
    )
    .count()
)


validation_result(
    null_keys == 0,
    "Nema NULL poslovnih kljuceva",
    f"NULL kljuceva: {null_keys}"
)


duplicates = (
    anomaly_df
    .groupBy(
        "ticker",
        "date"
    )
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)


validation_result(
    duplicates == 0,
    "Nema duplikata po (ticker, date)",
    f"Duplikata: {duplicates}"
)


# =============================================================================
# 3. ANOMALY SCORE RANGE
# =============================================================================

print()
print("=" * 100)
print("3. ANOMALY SCORE VALIDATION")
print("=" * 100)


invalid_scores = (
    anomaly_df
    .filter(
        F.col("anomaly_score").isNull()
        | (F.col("anomaly_score") < 0)
        | (F.col("anomaly_score") > 100)
    )
    .count()
)


validation_result(
    invalid_scores == 0,
    "anomaly_score je uvijek u rasponu 0-100",
    f"Nevalidnih score vrijednosti: {invalid_scores}"
)


score_stats = (
    anomaly_df
    .agg(
        F.round(
            F.min("anomaly_score"),
            2
        ).alias("min_score"),

        F.round(
            F.avg("anomaly_score"),
            2
        ).alias("avg_score"),

        F.round(
            F.max("anomaly_score"),
            2
        ).alias("max_score")
    )
)


print()
print("ANOMALY SCORE STATISTIKA:")

score_stats.show(
    truncate=False
)


# =============================================================================
# 4. BOOLEAN ANOMALY CONSISTENCY
# =============================================================================

print()
print("=" * 100)
print("4. ANOMALY FLAG CONSISTENCY")
print("=" * 100)


invalid_final_flags = (
    anomaly_df
    .filter(
        F.col("is_anomaly")
        != (
            (F.col("anomaly_score") >= 50)
            | (F.col("anomaly_signal_count") >= 2)
        )
    )
    .count()
)


validation_result(
    invalid_final_flags == 0,
    "is_anomaly odgovara definisanom decision pravilu",
    f"Nekonzistentnih redova: {invalid_final_flags}"
)


# =============================================================================
# 5. SIGNAL COUNT VALIDATION
# =============================================================================

print()
print("=" * 100)
print("5. SIGNAL COUNT VALIDATION")
print("=" * 100)


calculated_signal_count = (
    F.col("price_return_anomaly").cast("int")
    + F.col("volume_anomaly").cast("int")
    + F.col("range_anomaly").cast("int")
    + F.col("gap_anomaly").cast("int")
    + F.col("transactions_anomaly").cast("int")
    + F.col("volatility_anomaly").cast("int")
)


invalid_signal_counts = (
    anomaly_df
    .filter(
        F.col("anomaly_signal_count")
        != calculated_signal_count
    )
    .count()
)


validation_result(
    invalid_signal_counts == 0,
    "anomaly_signal_count odgovara broju aktivnih anomaly flagova",
    f"Nekonzistentnih redova: {invalid_signal_counts}"
)


invalid_signal_range = (
    anomaly_df
    .filter(
        (F.col("anomaly_signal_count") < 0)
        | (F.col("anomaly_signal_count") > 6)
    )
    .count()
)


validation_result(
    invalid_signal_range == 0,
    "anomaly_signal_count je u rasponu 0-6",
    f"Nevalidnih vrijednosti: {invalid_signal_range}"
)


# =============================================================================
# 6. ANOMALY TYPE EXPLAINABILITY
# =============================================================================

print()
print("=" * 100)
print("6. ANOMALY EXPLAINABILITY")
print("=" * 100)


unexplained_anomalies = (
    anomaly_df
    .filter(
        F.col("is_anomaly")
        & (
            F.col("anomaly_type").isNull()
            | (F.col("anomaly_type") == "")
            | (F.col("anomaly_type") == "NONE")
        )
    )
    .count()
)


validation_result(
    unexplained_anomalies == 0,
    "Svaka finalna anomalija ima anomaly_type",
    f"Neobjasnjenih anomalija: {unexplained_anomalies}"
)


# =============================================================================
# 7. SEVERITY CONSISTENCY
# =============================================================================

print()
print("=" * 100)
print("7. SEVERITY VALIDATION")
print("=" * 100)


invalid_severity = (
    anomaly_df
    .filter(

        (
            (F.col("anomaly_score") >= 80)
            & (F.col("anomaly_severity") != "CRITICAL")
        )

        |

        (
            (F.col("anomaly_score") >= 65)
            & (F.col("anomaly_score") < 80)
            & (F.col("anomaly_severity") != "HIGH")
        )

        |

        (
            (F.col("anomaly_score") >= 50)
            & (F.col("anomaly_score") < 65)
            & (F.col("anomaly_severity") != "MEDIUM")
        )

        |

        (
            (F.col("anomaly_score") < 50)
            & F.col("is_anomaly")
            & (F.col("anomaly_severity") != "LOW")
        )

        |

        (
            (~F.col("is_anomaly"))
            & (F.col("anomaly_severity") != "NORMAL")
        )
    )
    .count()
)


validation_result(
    invalid_severity == 0,
    "anomaly_severity je konzistentan sa score-om i finalnim flagom",
    f"Nekonzistentnih redova: {invalid_severity}"
)


# =============================================================================
# 8. WARM-UP PERIOD VALIDATION
#
# Prvih 30 trading redova svakog tickera nemaju kompletan
# historical baseline.
# =============================================================================

print()
print("=" * 100)
print("8. HISTORICAL BASELINE WARM-UP")
print("=" * 100)


ticker_window = (
    Window
    .partitionBy("ticker")
    .orderBy("date")
)


warmup_df = (
    anomaly_df
    .withColumn(
        "_ticker_row",
        F.row_number().over(ticker_window)
    )
)


warmup_anomalies = (
    warmup_df
    .filter(
        (F.col("_ticker_row") <= 30)
        & F.col("is_anomaly")
    )
    .count()
)


validation_result(
    warmup_anomalies == 0,
    "Nema anomaly odluka prije formiranja 30-periodnog baseline-a",
    f"Warm-up anomalija: {warmup_anomalies}"
)


# =============================================================================
# 9. ANOMALY DISTRIBUTION
# =============================================================================

print()
print("=" * 100)
print("9. ANOMALY DISTRIBUTION")
print("=" * 100)


total_anomalies = (
    anomaly_df
    .filter(
        F.col("is_anomaly")
    )
    .count()
)


anomaly_percentage = (
    total_anomalies
    / anomaly_count
    * 100
)


print(f"Ukupan broj anomalija: {total_anomalies}")
print(f"Udio anomalija:        {anomaly_percentage:.2f}%")


validation_result(
    anomaly_percentage < 10,
    "Anomaly stopa nije ekstremno visoka",
    f"Anomaly rate: {anomaly_percentage:.2f}%"
)


validation_result(
    anomaly_percentage > 0,
    "Detektor pronalazi anomaly događaje"
)


# =============================================================================
# 10. ANOMALY RATE BY TICKER
# =============================================================================

print()
print("=" * 100)
print("10. ANOMALY RATE BY TICKER")
print("=" * 100)


ticker_stats = (
    anomaly_df
    .groupBy("ticker")
    .agg(

        F.count("*").alias("total_rows"),

        F.sum(
            F.col("is_anomaly").cast("int")
        ).alias("anomaly_count")
    )
    .withColumn(
        "anomaly_pct",

        F.round(
            (
                F.col("anomaly_count")
                / F.col("total_rows")
            ) * 100,
            2
        )
    )
    .orderBy("ticker")
)


ticker_stats.show(
    n=100,
    truncate=False
)


# =============================================================================
# 11. SEVERITY DISTRIBUTION
# =============================================================================

print()
print("=" * 100)
print("11. SEVERITY DISTRIBUTION")
print("=" * 100)


(
    anomaly_df
    .groupBy(
        "anomaly_severity"
    )
    .count()
    .orderBy(
        F.col("count").desc()
    )
    .show(
        truncate=False
    )
)


# =============================================================================
# 12. SIGNAL FREQUENCY
# =============================================================================

print()
print("=" * 100)
print("12. ANOMALY SIGNAL FREQUENCY")
print("=" * 100)


anomaly_df.select(

    F.sum(
        F.col("price_return_anomaly").cast("int")
    ).alias("price_return_signals"),

    F.sum(
        F.col("volume_anomaly").cast("int")
    ).alias("volume_signals"),

    F.sum(
        F.col("range_anomaly").cast("int")
    ).alias("range_signals"),

    F.sum(
        F.col("gap_anomaly").cast("int")
    ).alias("gap_signals"),

    F.sum(
        F.col("transactions_anomaly").cast("int")
    ).alias("transaction_signals"),

    F.sum(
        F.col("volatility_anomaly").cast("int")
    ).alias("volatility_signals")

).show(
    truncate=False
)


# =============================================================================
# 13. COMPONENT CORRELATION
#
# Ovo je vazno jer volume i transactions mogu biti veoma povezani.
#
# Ako dvije komponente imaju skoro savrsenu korelaciju,
# postoji rizik da composite score dvostruko nagradjuje isti događaj.
# =============================================================================

print()
print("=" * 100)
print("13. COMPONENT CORRELATION")
print("=" * 100)


correlation_pairs = [

    (
        "volume_zscore",
        "transactions_zscore"
    ),

    (
        "return_zscore",
        "gap_zscore"
    ),

    (
        "return_zscore",
        "range_zscore"
    ),

    (
        "volume_zscore",
        "range_zscore"
    )
]


for left_column, right_column in correlation_pairs:

    corr_value = (
        anomaly_df
        .select(
            left_column,
            right_column
        )
        .na.drop()
        .stat.corr(
            left_column,
            right_column
        )
    )

    if corr_value is None:
        print(
            f"{left_column} <-> {right_column}: N/A"
        )
    else:
        print(
            f"{left_column} <-> {right_column}: "
            f"{corr_value:.4f}"
        )


# =============================================================================
# 14. COMPOSITE-ONLY ANOMALIES
# =============================================================================

print()
print("=" * 100)
print("14. COMPOSITE MULTI-FACTOR ANOMALIES")
print("=" * 100)


composite_only = (
    anomaly_df
    .filter(
        F.col("is_anomaly")
        & (F.col("anomaly_signal_count") == 0)
    )
)


composite_only_count = composite_only.count()


print(
    f"Composite-only anomalija: "
    f"{composite_only_count}"
)


if composite_only_count > 0:

    composite_only.select(
        "ticker",
        "date",
        "daily_return_pct",

        "return_zscore",
        "volume_zscore",
        "range_zscore",
        "gap_zscore",
        "transactions_zscore",
        "volatility_ratio",

        "anomaly_score",
        "anomaly_type"
    ).orderBy(
        F.col("anomaly_score").desc()
    ).show(
        n=50,
        truncate=False
    )


# =============================================================================
# 15. TOP ANOMALIES
# =============================================================================

print()
print("=" * 100)
print("15. TOP 20 ANOMALIES")
print("=" * 100)


(
    anomaly_df
    .filter(
        F.col("is_anomaly")
    )
    .select(
        "ticker",
        "date",
        "daily_return_pct",

        "historical_volume_ratio",

        "return_zscore",
        "volume_zscore",
        "range_zscore",
        "gap_zscore",
        "transactions_zscore",

        "anomaly_signal_count",
        "anomaly_score",

        "anomaly_severity",
        "anomaly_type",
        "market_condition"
    )
    .orderBy(
        F.col("anomaly_score").desc()
    )
    .show(
        n=20,
        truncate=False
    )
)


# =============================================================================
# 16. FINAL VALIDATION SUMMARY
# =============================================================================

print()
print("=" * 100)
print("FINAL VALIDATION SUMMARY")
print("=" * 100)


hard_failures = [

    source_count != anomaly_count,
    null_keys != 0,
    duplicates != 0,
    invalid_scores != 0,
    invalid_final_flags != 0,
    invalid_signal_counts != 0,
    invalid_signal_range != 0,
    unexplained_anomalies != 0,
    invalid_severity != 0,
    warmup_anomalies != 0
]


failure_count = sum(
    1
    for failure
    in hard_failures
    if failure
)


if failure_count == 0:

    print(
        "[PASS] FINAL STOCK ANOMALY VALIDATION USPJESNA"
    )

else:

    print(
        f"[FAIL] FINAL VALIDATION IMA "
        f"{failure_count} PROBLEMA"
    )


print("=" * 100)