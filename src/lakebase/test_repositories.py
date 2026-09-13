from src.lakebase.user_repository import (
    get_user_by_id,
    get_user_by_username,
    list_users,
)

from src.lakebase.watchlist_repository import (
    get_watchlist,
    get_watchlist_item,
)


def main():
    print("=" * 90)
    print("LAKEBASE REPOSITORY TEST")
    print("=" * 90)

    print("\n1. USER BY ID")
    print("-" * 90)

    user = get_user_by_id(1)

    if user:
        print(f"ID:        {user['user_id']}")
        print(f"Username:  {user['username']}")
        print(f"Email:     {user['email']}")
        print(f"Full name: {user['full_name']}")
        print(f"Active:    {user['is_active']}")
    else:
        print("Korisnik nije pronađen.")

    print("\n2. USER BY USERNAME")
    print("-" * 90)

    user = get_user_by_username("demo_user")

    print(user)

    print("\n3. ALL USERS")
    print("-" * 90)

    users = list_users()

    for item in users:
        print(
            f"{item['user_id']} | "
            f"{item['username']} | "
            f"{item['email']}"
        )

    print("\n4. USER WATCHLIST")
    print("-" * 90)

    watchlist = get_watchlist(1)

    for item in watchlist:
        print(
            f"{item['ticker']:<6} | "
            f"{item['investment_thesis']}"
        )

    print("\n5. SINGLE WATCHLIST ITEM")
    print("-" * 90)

    apple = get_watchlist_item(
        user_id=1,
        ticker="aapl",
    )

    print(apple)

    print("\n" + "=" * 90)
    print("[PASS] LAKEBASE REPOSITORY READ TEST")
    print("=" * 90)


if __name__ == "__main__":
    main()