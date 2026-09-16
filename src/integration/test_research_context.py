from src.integration.research_context_service import (
    get_research_context,
)


USER_ID = 1
TICKER = "AAPL"


def value_or_dash(value):
    if value is None:
        return "-"

    return value


def main():
    print("=" * 110)
    print("STOCK RESEARCH CONTEXT TEST")
    print("=" * 110)

    context = get_research_context(
        user_id=USER_ID,
        ticker=TICKER,
    )

    # ==========================================================
    # USER / WATCHLIST
    # ==========================================================

    print("\nUSER CONTEXT")
    print("-" * 110)

    print(
        f"User:   "
        f"{context['user']['username']}"
    )

    print(
        f"Ticker: "
        f"{context['watchlist']['ticker']}"
    )

    print(
        f"Thesis: "
        f"{context['watchlist']['investment_thesis']}"
    )

    print(
        f"Notes:  "
        f"{context['watchlist']['notes']}"
    )

    # ==========================================================
    # COMPANY
    # ==========================================================

    company = context["company"]

    print("\nCOMPANY")
    print("-" * 110)

    if company:
        print(f"Name:       {company['name']}")
        print(
            f"Exchange:   "
            f"{company['primary_exchange']}"
        )
        print(
            f"Market cap: "
            f"{company['market_cap_billions']} B"
        )
        print(
            f"Employees:  "
            f"{company['total_employees']}"
        )
        print(
            f"Industry:   "
            f"{company['sic_description']}"
        )

    # ==========================================================
    # MARKET
    # ==========================================================

    market = context["market"]

    print("\nLATEST MARKET")
    print("-" * 110)

    if market:
        print(f"Date:          {market['date']}")
        print(f"Close:         {market['close']}")
        print(
            f"Daily return:  "
            f"{market['daily_return_pct']}%"
        )
        print(
            f"Direction:     "
            f"{market['daily_direction']}"
        )
        print(
            f"Volatility:    "
            f"{market['annualized_volatility_30d_pct']}%"
        )

    # ==========================================================
    # PERFORMANCE
    # ==========================================================

    performance = context["performance"]

    print("\nPERFORMANCE")
    print("-" * 110)

    if performance:
        print(
            f"7d return:     "
            f"{performance['return_7d_pct']}%"
        )
        print(
            f"30d return:    "
            f"{performance['return_30d_pct']}%"
        )
        print(
            f"90d return:    "
            f"{performance['return_90d_pct']}%"
        )
        print(
            f"1y return:     "
            f"{performance['return_1y_pct']}%"
        )
        print(
            f"Trend:         "
            f"{performance['trend_signal']}"
        )
        print(
            f"Score:         "
            f"{performance['performance_score']}"
        )
        print(
            f"Rank:          "
            f"{performance['performance_rank']}"
        )
        print(
            f"Category:      "
            f"{performance['performance_category']}"
        )

    # ==========================================================
    # TECHNICAL
    # ==========================================================

    technical = context["technical"]

    print("\nTECHNICAL")
    print("-" * 110)

    if technical:
        print(
            f"RSI 14:        "
            f"{technical['rsi_14']}"
        )
        print(
            f"RSI signal:    "
            f"{technical['rsi_signal']}"
        )
        print(
            f"MACD state:    "
            f"{technical['macd_state']}"
        )
        print(
            f"Trend signal:  "
            f"{technical['trend_signal']}"
        )
        print(
            f"SMA 20:        "
            f"{technical['sma_20']}"
        )
        print(
            f"SMA 50:        "
            f"{technical['sma_50']}"
        )

    # ==========================================================
    # NEWS
    # ==========================================================

    news_summary = context["news"]["summary"]

    print("\nNEWS")
    print("-" * 110)

    if news_summary:
        print(
            f"News 7d:       "
            f"{news_summary['news_7d_count']}"
        )
        print(
            f"News 30d:      "
            f"{news_summary['news_30d_count']}"
        )
        print(
            f"Sentiment:     "
            f"{news_summary['dominant_sentiment_30d']}"
        )
        print(
            f"Score:         "
            f"{news_summary['sentiment_score_30d']}"
        )
        print(
            f"Positive:      "
            f"{news_summary['positive_share_30d_pct']}%"
        )
        print(
            f"Negative:      "
            f"{news_summary['negative_share_30d_pct']}%"
        )
        print(
            f"Latest title:  "
            f"{news_summary['latest_news_title']}"
        )

    print(
        f"Daily rows:    "
        f"{len(context['news']['recent_daily'])}"
    )

    # ==========================================================
    # ANOMALIES
    # ==========================================================

    anomalies = context["anomalies"]["recent"]

    print("\nRECENT ANOMALIES")
    print("-" * 110)

    print(
        f"Loaded anomalies: {len(anomalies)}"
    )

    for anomaly in anomalies:
        print(
            f"{anomaly['date']} | "
            f"{anomaly['anomaly_severity']:<8} | "
            f"score={anomaly['anomaly_score']:<6} | "
            f"{anomaly['anomaly_type']}"
        )

    anomaly_news = (
        context["anomalies"]["news_context"]
    )

    print("\nANOMALY NEWS CONTEXT")
    print("-" * 110)

    print(
        f"Loaded summaries: {len(anomaly_news)}"
    )

    for item in anomaly_news:
        print(
            f"{item['anomaly_date']} | "
            f"{item['anomaly_severity']:<8} | "
            f"news={item['related_news_count']:<3} | "
            f"sentiment={item['dominant_sentiment']}"
        )

    # ==========================================================
    # VALIDATION
    # ==========================================================

    assert context["user"]["user_id"] == USER_ID
    assert context["watchlist"]["ticker"] == TICKER
    assert context["company"]["ticker"] == TICKER
    assert context["market"]["ticker"] == TICKER
    assert context["performance"]["ticker"] == TICKER
    assert context["technical"]["ticker"] == TICKER

    print("\n" + "=" * 110)
    print(
        "[PASS] STOCK RESEARCH CONTEXT CREATED SUCCESSFULLY"
    )
    print("=" * 110)


if __name__ == "__main__":
    main()