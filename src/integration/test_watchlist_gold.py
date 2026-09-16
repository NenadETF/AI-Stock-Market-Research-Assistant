from src.integration.watchlist_gold_service import (
    get_latest_gold_metrics_for_watchlist,
    get_watchlist_tickers,
)

from src.lakebase.user_repository import get_user_by_id


USER_ID = 1


def format_number(value, decimals=2):
    if value is None:
        return "-"

    return f"{value:,.{decimals}f}"


def main():
    print("=" * 100)
    print("LAKEBASE -> GOLD INTEGRATION TEST")
    print("=" * 100)

    # ==========================================================
    # USER
    # ==========================================================

    user = get_user_by_id(USER_ID)

    if user is None:
        raise RuntimeError(
            f"Korisnik sa ID={USER_ID} nije pronađen."
        )

    print("\nUSER")
    print("-" * 100)

    print(f"ID:        {user['user_id']}")
    print(f"Username:  {user['username']}")
    print(f"Full name: {user['full_name']}")

    # ==========================================================
    # WATCHLIST
    # ==========================================================

    tickers = get_watchlist_tickers(USER_ID)

    print("\nWATCHLIST FROM LAKEBASE")
    print("-" * 100)

    if not tickers:
        print("Watchlista je prazna.")
        return

    for ticker in tickers:
        print(f"- {ticker}")

    # ==========================================================
    # GOLD DATA
    # ==========================================================

    print("\nLATEST GOLD METRICS")
    print("-" * 100)

    metrics = get_latest_gold_metrics_for_watchlist(
        USER_ID
    )

    for item in metrics:

        if item.get("status") == "NOT_FOUND_IN_GOLD":
            print(
                f"\n{item['ticker']}: "
                f"nije pronađen u Gold tabeli."
            )
            continue

        print()
        print(f"Ticker:        {item['ticker']}")
        print(f"Date:          {item['date']}")
        print(
            f"Close:         "
            f"{format_number(item['close'])}"
        )
        print(
            f"Daily return:  "
            f"{format_number(item['daily_return_pct'])}%"
        )
        print(
            f"Direction:     "
            f"{item['daily_direction'] or '-'}"
        )
        print(
            f"MA 7d:         "
            f"{format_number(item['moving_avg_7d'])}"
        )
        print(
            f"MA 30d:        "
            f"{format_number(item['moving_avg_30d'])}"
        )
        print(
            f"Volatility:    "
            f"{format_number(
                item['annualized_volatility_30d_pct']
            )}%"
        )
        print(
            f"Volume ratio:  "
            f"{format_number(item['volume_ratio'])}"
        )
        print(
            f"Close vs VWAP: "
            f"{format_number(item['close_vs_vwap_pct'])}%"
        )

    # ==========================================================
    # VALIDATION
    # ==========================================================

    found = {
        item["ticker"]
        for item in metrics
        if item.get("status") != "NOT_FOUND_IN_GOLD"
    }

    expected = set(tickers)

    assert found == expected, (
        f"Gold tickers {found} "
        f"ne odgovaraju watchlisti {expected}"
    )

    print("\n" + "=" * 100)
    print(
        "[PASS] LAKEBASE -> GOLD INTEGRATION SUCCESSFUL"
    )
    print("=" * 100)


if __name__ == "__main__":
    main()