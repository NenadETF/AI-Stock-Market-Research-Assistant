from src.integration.research_context_service import (
    get_research_context,
)

from src.lakebase.user_repository import get_user_by_id
from src.lakebase.watchlist_repository import get_watchlist


USER_ID = 1


def main():
    print("=" * 100)
    print("CHAT 6 - FINAL LAKEBASE / WATCHLIST VALIDATION")
    print("=" * 100)

    # ==========================================================
    # USER
    # ==========================================================

    user = get_user_by_id(USER_ID)

    assert user is not None

    print("\nUSER")
    print("-" * 100)

    print(f"ID:        {user['user_id']}")
    print(f"Username:  {user['username']}")
    print(f"Full name: {user['full_name']}")

    # ==========================================================
    # WATCHLIST
    # ==========================================================

    watchlist = get_watchlist(USER_ID)

    assert watchlist

    print("\nWATCHLIST")
    print("-" * 100)

    for item in watchlist:
        print(f"- {item['ticker']}")

    # ==========================================================
    # RESEARCH CONTEXT FOR EVERY TICKER
    # ==========================================================

    print("\nRESEARCH CONTEXT VALIDATION")
    print("-" * 100)

    successful = 0

    for item in watchlist:
        ticker = item["ticker"]

        print(f"\nTesting {ticker}...")

        context = get_research_context(
            user_id=USER_ID,
            ticker=ticker,
        )

        assert context["company"] is not None
        assert context["market"] is not None
        assert context["performance"] is not None
        assert context["technical"] is not None
        assert context["news"]["summary"] is not None

        assert context["company"]["ticker"] == ticker
        assert context["market"]["ticker"] == ticker
        assert context["performance"]["ticker"] == ticker
        assert context["technical"]["ticker"] == ticker

        market = context["market"]
        performance = context["performance"]
        news = context["news"]["summary"]

        print(f"[PASS] {ticker}")
        print(f"       Date:        {market['date']}")
        print(f"       Close:       {market['close']}")
        print(
            f"       30d return:  "
            f"{performance['return_30d_pct']}%"
        )
        print(
            f"       Trend:       "
            f"{performance['trend_signal']}"
        )
        print(
            f"       News:        "
            f"{news['dominant_sentiment_30d']}"
        )
        print(
            f"       Anomalies:   "
            f"{len(context['anomalies']['recent'])}"
        )

        successful += 1

    # ==========================================================
    # RESULT
    # ==========================================================

    assert successful == len(watchlist)

    print("\n" + "=" * 100)
    print("FINAL RESULT")
    print("=" * 100)

    print(f"Users tested:           1")
    print(f"Watchlist items:        {len(watchlist)}")
    print(f"Research contexts:      {successful}")

    print("\n[PASS] Lakebase connection")
    print("[PASS] Users repository")
    print("[PASS] Watchlist repository")
    print("[PASS] User -> Watchlist relation")
    print("[PASS] Lakebase -> Gold integration")
    print("[PASS] Company data integration")
    print("[PASS] Market data integration")
    print("[PASS] Technical analysis integration")
    print("[PASS] News analytics integration")
    print("[PASS] Anomaly analytics integration")
    print("[PASS] Research Context service")

    print("\n" + "=" * 100)
    print("[PASS] CHAT 6 COMPLETED SUCCESSFULLY")
    print("=" * 100)


if __name__ == "__main__":
    main()