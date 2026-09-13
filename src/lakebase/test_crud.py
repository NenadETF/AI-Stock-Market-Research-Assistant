from uuid import uuid4

from src.lakebase.user_repository import (
    create_user,
    delete_user,
    get_user_by_id,
    update_user,
)

from src.lakebase.watchlist_repository import (
    add_to_watchlist,
    get_watchlist,
    get_watchlist_item,
    remove_from_watchlist,
    update_watchlist_item,
)


def main():
    print("=" * 90)
    print("LAKEBASE FULL CRUD TEST")
    print("=" * 90)

    suffix = uuid4().hex[:8]

    username = f"crud_test_{suffix}"
    email = f"crud.{suffix}@example.com"

    user_id = None

    try:
        # ==========================================================
        # 1. CREATE USER
        # ==========================================================

        print("\n1. CREATE USER")
        print("-" * 90)

        user = create_user(
            username=username,
            email=email,
            full_name="CRUD Test User",
        )

        user_id = user["user_id"]

        print(f"[PASS] Kreiran korisnik ID={user_id}")
        print(f"Username: {user['username']}")
        print(f"Email:    {user['email']}")

        # ==========================================================
        # 2. UPDATE USER
        # ==========================================================

        print("\n2. UPDATE USER")
        print("-" * 90)

        updated_user = update_user(
            user_id=user_id,
            full_name="Updated CRUD Test User",
        )

        print(f"[PASS] Korisnik ažuriran")
        print(f"Full name: {updated_user['full_name']}")
        print(f"Updated:   {updated_user['updated_at']}")

        # ==========================================================
        # 3. ADD WATCHLIST ITEMS
        # ==========================================================

        print("\n3. ADD TO WATCHLIST")
        print("-" * 90)

        apple = add_to_watchlist(
            user_id=user_id,
            ticker="aapl",
            investment_thesis="Test thesis for Apple.",
            notes="Initial Apple note.",
        )

        tesla = add_to_watchlist(
            user_id=user_id,
            ticker="tsla",
            investment_thesis="Test thesis for Tesla.",
            notes="Initial Tesla note.",
        )

        print(
            f"[PASS] Dodano: "
            f"{apple['ticker']}, {tesla['ticker']}"
        )

        # ==========================================================
        # 4. READ WATCHLIST
        # ==========================================================

        print("\n4. READ WATCHLIST")
        print("-" * 90)

        watchlist = get_watchlist(user_id)

        for item in watchlist:
            print(
                f"{item['ticker']:<6} | "
                f"{item['investment_thesis']}"
            )

        assert len(watchlist) == 2

        print("[PASS] Watchlista sadrži 2 akcije.")

        # ==========================================================
        # 5. UPDATE WATCHLIST ITEM
        # ==========================================================

        print("\n5. UPDATE WATCHLIST ITEM")
        print("-" * 90)

        updated_apple = update_watchlist_item(
            user_id=user_id,
            ticker="aapl",
            notes="Updated Apple note from CRUD test.",
        )

        print(f"Ticker:     {updated_apple['ticker']}")
        print(f"Notes:      {updated_apple['notes']}")
        print(f"Updated at: {updated_apple['updated_at']}")

        assert (
            updated_apple["notes"]
            == "Updated Apple note from CRUD test."
        )

        print("[PASS] AAPL zapis uspješno ažuriran.")

        # ==========================================================
        # 6. DELETE WATCHLIST ITEM
        # ==========================================================

        print("\n6. REMOVE FROM WATCHLIST")
        print("-" * 90)

        removed = remove_from_watchlist(
            user_id=user_id,
            ticker="tsla",
        )

        assert removed is True

        tesla_after_delete = get_watchlist_item(
            user_id=user_id,
            ticker="TSLA",
        )

        assert tesla_after_delete is None

        print("[PASS] TSLA uklonjen iz watchliste.")

        # ==========================================================
        # 7. DELETE USER
        # ==========================================================

        print("\n7. DELETE USER")
        print("-" * 90)

        deleted = delete_user(user_id)

        assert deleted is True

        user_after_delete = get_user_by_id(user_id)

        assert user_after_delete is None

        print("[PASS] Privremeni korisnik obrisan.")

        # ==========================================================
        # 8. TEST ON DELETE CASCADE
        # ==========================================================

        print("\n8. ON DELETE CASCADE TEST")
        print("-" * 90)

        apple_after_user_delete = get_watchlist_item(
            user_id=user_id,
            ticker="AAPL",
        )

        assert apple_after_user_delete is None

        print(
            "[PASS] AAPL zapis automatski obrisan "
            "zajedno sa korisnikom."
        )

        user_id = None

        print("\n" + "=" * 90)
        print("[PASS] FULL LAKEBASE CRUD TEST SUCCESSFUL")
        print("=" * 90)

    finally:
        # Sigurnosno čišćenje ako test pukne prije kraja.
        if user_id is not None:
            existing = get_user_by_id(user_id)

            if existing is not None:
                delete_user(user_id)

                print(
                    "\n[CLEANUP] Privremeni test korisnik je obrisan."
                )


if __name__ == "__main__":
    main()