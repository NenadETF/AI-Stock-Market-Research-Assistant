from src.agent.research_agent import (
    analyze_stock,
)
from src.integration.research_history import (
    get_latest_research,
    get_research_history,
    get_research_report,
    get_ticker_history,
)
from src.lakebase.analysis_report_repository import (
    get_analysis_report,
)


USER_ID = 1
TICKER = "AAPL"

QUESTION = (
    "Does the current market performance, technical momentum, "
    "recent news sentiment and anomaly activity support my "
    "investment thesis for this stock?"
)


def print_list(items):
    if not items:
        print("  - None")
        return

    for item in items:
        print(f"  - {item}")


def main():
    print("=" * 110)
    print(
        "AI STOCK MARKET RESEARCH AGENT TEST"
    )
    print("=" * 110)

    print(
        f"\nUser ID:  {USER_ID}"
    )

    print(
        f"Ticker:   {TICKER}"
    )

    print(
        f"Question: {QUESTION}"
    )

    print(
        "\nGenerating personalized "
        "research report..."
    )

    print(
        "This may take several seconds.\n"
    )

    # ==========================================================
    # 1. GENERATE RESEARCH REPORT
    # ==========================================================

    result = analyze_stock(
        user_id=USER_ID,
        ticker=TICKER,
        question=QUESTION,
    )

    report = result["report"]
    report_id = result["report_id"]

    # ==========================================================
    # 2. META
    # ==========================================================

    print("=" * 110)
    print("RESEARCH REPORT")
    print("=" * 110)

    print(
        f"\nReport ID:   {report_id}"
    )

    print(
        f"User:        "
        f"{result['username']}"
    )

    print(
        f"Ticker:      "
        f"{result['ticker']}"
    )

    print(
        f"Model:       "
        f"{result['model']}"
    )

    print(
        f"Data as of:  "
        f"{result['data_as_of']}"
    )

    if result.get("created_at"):
        print(
            f"Created at:  "
            f"{result['created_at']}"
        )

    # ==========================================================
    # 3. INVESTMENT THESIS
    # ==========================================================

    print(
        "\nINVESTMENT THESIS"
    )

    print("-" * 110)

    print(
        result["investment_thesis"]
        or "No investment thesis provided."
    )

    # ==========================================================
    # 4. RESEARCH QUESTION
    # ==========================================================

    print(
        "\nRESEARCH QUESTION"
    )

    print("-" * 110)

    print(
        result["question"]
    )

    # ==========================================================
    # 5. EXECUTIVE SUMMARY
    # ==========================================================

    print(
        "\nEXECUTIVE SUMMARY"
    )

    print("-" * 110)

    print(
        report["executive_summary"]
    )

    # ==========================================================
    # 6. MARKET
    # ==========================================================

    print(
        "\nMARKET ANALYSIS"
    )

    print("-" * 110)

    print(
        report["market_analysis"]
    )

    # ==========================================================
    # 7. TECHNICAL
    # ==========================================================

    print(
        "\nTECHNICAL ANALYSIS"
    )

    print("-" * 110)

    print(
        report["technical_analysis"]
    )

    # ==========================================================
    # 8. NEWS
    # ==========================================================

    print(
        "\nNEWS ANALYSIS"
    )

    print("-" * 110)

    print(
        report["news_analysis"]
    )

    # ==========================================================
    # 9. ANOMALIES
    # ==========================================================

    print(
        "\nANOMALY ANALYSIS"
    )

    print("-" * 110)

    print(
        report["anomaly_analysis"]
    )

    # ==========================================================
    # 10. THESIS ALIGNMENT
    # ==========================================================

    thesis = (
        report["thesis_alignment"]
    )

    print(
        "\nTHESIS ALIGNMENT"
    )

    print("-" * 110)

    print(
        f"Assessment: "
        f"{thesis.get('assessment', '-')}"
    )

    print(
        "\nSupporting evidence:"
    )

    print_list(
        thesis.get(
            "supporting_evidence",
            [],
        )
    )

    print(
        "\nContradicting evidence:"
    )

    print_list(
        thesis.get(
            "contradicting_evidence",
            [],
        )
    )

    # ==========================================================
    # 11. CATALYSTS
    # ==========================================================

    print(
        "\nKEY CATALYSTS"
    )

    print("-" * 110)

    print_list(
        report["key_catalysts"]
    )

    # ==========================================================
    # 12. RISKS
    # ==========================================================

    print(
        "\nKEY RISKS"
    )

    print("-" * 110)

    print_list(
        report["key_risks"]
    )

    # ==========================================================
    # 13. DATA LIMITATIONS
    # ==========================================================

    print(
        "\nDATA LIMITATIONS"
    )

    print("-" * 110)

    print_list(
        report["data_limitations"]
    )

    # ==========================================================
    # 14. CONCLUSION
    # ==========================================================

    print(
        "\nCONCLUSION"
    )

    print("-" * 110)

    print(
        report["conclusion"]
    )

    # ==========================================================
    # 15. DISCLAIMER
    # ==========================================================

    print(
        "\nDISCLAIMER"
    )

    print("-" * 110)

    print(
        report["disclaimer"]
    )

    # ==========================================================
    # 16. BASIC AGENT VALIDATION
    # ==========================================================

    print(
        "\nAGENT VALIDATION"
    )

    print("-" * 110)

    assert (
        result["user_id"]
        == USER_ID
    )

    assert (
        result["ticker"]
        == TICKER
    )

    assert (
        report_id is not None
    )

    assert (
        report_id > 0
    )

    assert (
        report["ticker"].upper()
        == TICKER
    )

    assert (
        report["executive_summary"]
    )

    assert (
        report["conclusion"]
    )

    assert (
        report["thesis_alignment"]
    )

    assert (
        report["thesis_alignment"]
        .get("assessment")
    )

    print(
        "[PASS] Agent result structure is valid"
    )

    # ==========================================================
    # 17. GROUNDING VALIDATION
    # ==========================================================

    print(
        "\nGROUNDING VALIDATION"
    )

    print("-" * 110)

    grounding = (
        result["grounding_validation"]
    )

    print(
        f"Status:              "
        f"{grounding['status']}"
    )

    print(
        f"Report numbers:      "
        f"{grounding['checked_report_numbers']}"
    )

    print(
        f"Source numbers:      "
        f"{grounding['source_numeric_values']}"
    )

    print(
        f"Report dates:        "
        f"{grounding['checked_report_dates']}"
    )

    if grounding["errors"]:
        print(
            "\nErrors:"
        )

        for error in grounding["errors"]:
            print(
                f"  - {error}"
            )

    if grounding["warnings"]:
        print(
            "\nWarnings:"
        )

        for warning in grounding["warnings"]:
            print(
                f"  - {warning}"
            )

    assert (
        grounding["status"]
        == "PASS"
    )

    assert not grounding["errors"]

    print(
        "[PASS] AI report is grounded "
        "in Research Context"
    )

    # ==========================================================
    # 18. LAKEBASE PERSISTENCE VALIDATION
    # ==========================================================

    print(
        "\nLAKEBASE PERSISTENCE"
    )

    print("-" * 110)

    saved = get_analysis_report(
        report_id
    )

    assert (
        saved is not None
    )

    assert (
        saved["report_id"]
        == report_id
    )

    assert (
        saved["user_id"]
        == USER_ID
    )

    assert (
        saved["ticker"]
        == TICKER
    )

    assert (
        saved["model"]
        == result["model"]
    )

    assert (
        saved["question"]
        == QUESTION
    )

    assert (
        saved["content"]["ticker"].upper()
        == TICKER
    )

    assert (
        saved["content"]
        ["thesis_alignment"]
        ["assessment"]
        ==
        report
        ["thesis_alignment"]
        ["assessment"]
    )

    assert (
        saved["content"]
        ["executive_summary"]
        ==
        report["executive_summary"]
    )

    print(
        f"[PASS] Report saved permanently "
        f"with ID={report_id}"
    )

    print(
        f"       User:       "
        f"{saved['username']}"
    )

    print(
        f"       Ticker:     "
        f"{saved['ticker']}"
    )

    print(
        f"       Assessment: "
        f"{saved['thesis_assessment']}"
    )

    print(
        f"       Created at: "
        f"{saved['created_at']}"
    )

    # ==========================================================
    # 19. RESEARCH HISTORY - ALL USER REPORTS
    # ==========================================================

    print(
        "\nRESEARCH HISTORY"
    )

    print("-" * 110)

    history = get_research_history(
        user_id=USER_ID,
        limit=50,
    )

    assert history

    assert any(
        item["report_id"] == report_id
        for item in history
    )

    print(
        "[PASS] Generated report exists "
        "in user research history"
    )

    print(
        f"       Total loaded reports: "
        f"{len(history)}"
    )

    # ==========================================================
    # 20. RESEARCH HISTORY - TICKER FILTER
    # ==========================================================

    ticker_history = get_ticker_history(
        user_id=USER_ID,
        ticker=TICKER,
        limit=50,
    )

    assert ticker_history

    assert all(
        item["ticker"] == TICKER
        for item in ticker_history
    )

    assert any(
        item["report_id"] == report_id
        for item in ticker_history
    )

    print(
        f"[PASS] Generated report exists "
        f"in {TICKER} history"
    )

    print(
        f"       {TICKER} reports loaded: "
        f"{len(ticker_history)}"
    )

    # ==========================================================
    # 21. RESEARCH HISTORY - COMPLETE REPORT
    # ==========================================================

    history_report = get_research_report(
        user_id=USER_ID,
        report_id=report_id,
    )

    assert (
        history_report is not None
    )

    assert (
        history_report["report_id"]
        == report_id
    )

    assert (
        history_report["user_id"]
        == USER_ID
    )

    assert (
        history_report["ticker"]
        == TICKER
    )

    assert (
        history_report["question"]
        == QUESTION
    )

    assert (
        history_report["content"]
        ["executive_summary"]
        ==
        report["executive_summary"]
    )

    assert (
        history_report["content"]
        ["thesis_alignment"]
        ["assessment"]
        ==
        report["thesis_alignment"]
        ["assessment"]
    )

    print(
        "[PASS] Complete report loaded "
        "through Research History service"
    )

    # ==========================================================
    # 22. RESEARCH HISTORY - LATEST REPORT
    # ==========================================================

    latest = get_latest_research(
        user_id=USER_ID,
        ticker=TICKER,
    )

    assert (
        latest is not None
    )

    assert (
        latest["report_id"]
        == report_id
    )

    assert (
        latest["ticker"]
        == TICKER
    )

    print(
        f"[PASS] Newly generated report "
        f"is latest {TICKER} research"
    )

    print(
        f"       Latest report ID: "
        f"{latest['report_id']}"
    )

    # ==========================================================
    # 23. RESEARCH HISTORY OWNERSHIP PROTECTION
    # ==========================================================

    unauthorized_report = (
        get_research_report(
            user_id=999999,
            report_id=report_id,
        )
    )

    assert (
        unauthorized_report is None
    )

    print(
        "[PASS] Research History ownership "
        "protection works correctly"
    )

    # ==========================================================
    # FINAL VALIDATION
    # ==========================================================

    print(
        "\n" + "=" * 110
    )

    print(
        "[PASS] PERSONALIZED AI RESEARCH REPORT "
        "GENERATED, GROUNDED, SAVED AND VERIFIED "
        "IN RESEARCH HISTORY SUCCESSFULLY"
    )

    print("=" * 110)


if __name__ == "__main__":
    main()