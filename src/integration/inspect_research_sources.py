from databricks.connect import DatabricksSession


def main():
    print("=" * 100)
    print("RESEARCH DATA SOURCES INSPECTION")
    print("=" * 100)

    spark = DatabricksSession.builder.getOrCreate()

    # ==========================================================
    # GOLD TABLES
    # ==========================================================

    print("\n" + "=" * 100)
    print("WORKSPACE.GOLD TABLES")
    print("=" * 100)

    gold_tables = spark.sql(
        "SHOW TABLES IN workspace.gold"
    )

    gold_tables.show(
        100,
        truncate=False,
    )

    # ==========================================================
    # SILVER TABLES
    # ==========================================================

    print("\n" + "=" * 100)
    print("WORKSPACE.SILVER TABLES")
    print("=" * 100)

    silver_tables = spark.sql(
        "SHOW TABLES IN workspace.silver"
    )

    silver_tables.show(
        100,
        truncate=False,
    )

    print("\n[PASS] Research source inspection završen.")


if __name__ == "__main__":
    main()