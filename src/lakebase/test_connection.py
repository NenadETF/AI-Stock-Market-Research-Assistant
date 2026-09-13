from src.lakebase.connection import get_connection


def main():
    print("=" * 80)
    print("LAKEBASE CONNECTION TEST")
    print("=" * 80)

    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    current_database() AS database_name,
                    current_user AS database_user,
                    version() AS postgres_version;
                """
            )

            result = cursor.fetchone()

    print(f"Database: {result['database_name']}")
    print(f"User:     {result['database_user']}")
    print(f"Postgres: {result['postgres_version']}")
    print("\n[PASS] Lakebase konekcija radi.")


if __name__ == "__main__":
    main()