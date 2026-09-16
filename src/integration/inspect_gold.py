from databricks.connect import DatabricksSession


GOLD_TABLE = "workspace.gold.stock_daily_metrics"


def main():
    print("=" * 90)
    print("GOLD TABLE INSPECTION")
    print("=" * 90)

    spark = DatabricksSession.builder.getOrCreate()

    print(f"\nTable: {GOLD_TABLE}")

    print("\n" + "=" * 90)
    print("SCHEMA")
    print("=" * 90)

    df = spark.table(GOLD_TABLE)

    df.printSchema()

    print("\n" + "=" * 90)
    print("SAMPLE DATA")
    print("=" * 90)

    df.show(5, truncate=False)

    print("\n" + "=" * 90)
    print("AVAILABLE TICKERS")
    print("=" * 90)

    (
        df.select("ticker")
        .distinct()
        .orderBy("ticker")
        .show(100, truncate=False)
    )

    print("\n[PASS] Gold tabela je dostupna.")


if __name__ == "__main__":
    main()