from databricks.connect import DatabricksSession


TABLES = [
    "workspace.silver.company_details",
    "workspace.gold.company_performance",
    "workspace.gold.technical_indicators",
    "workspace.gold.news_summary",
    "workspace.gold.news_daily_analytics",
    "workspace.gold.stock_anomalies",
    "workspace.gold.stock_anomaly_news_summary",
]


def inspect_table(spark, table_name: str):
    print("\n")
    print("=" * 110)
    print(table_name.upper())
    print("=" * 110)

    df = spark.table(table_name)

    print("\nSCHEMA")
    print("-" * 110)

    df.printSchema()

    print("\nSAMPLE DATA")
    print("-" * 110)

    df.show(
        3,
        truncate=False,
    )

    print("\nCOLUMNS")
    print("-" * 110)

    for column in df.columns:
        print(f"- {column}")


def main():
    print("=" * 110)
    print("STOCK RESEARCH CONTEXT SOURCES INSPECTION")
    print("=" * 110)

    spark = DatabricksSession.builder.getOrCreate()

    for table_name in TABLES:
        try:
            inspect_table(
                spark=spark,
                table_name=table_name,
            )

        except Exception as exc:
            print("\n[ERROR]")
            print(f"Table: {table_name}")
            print(f"Error: {exc}")

    print("\n")
    print("=" * 110)
    print("[PASS] RESEARCH CONTEXT SOURCE INSPECTION FINISHED")
    print("=" * 110)


if __name__ == "__main__":
    main()