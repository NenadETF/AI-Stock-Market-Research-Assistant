from src.lakebase.analysis_report_repository import (
    count_analysis_reports,
    delete_analysis_report,
    get_analysis_report,
    list_analysis_reports,
    save_analysis_report,
)


USER_ID = 1
TICKER = "AAPL"


TEST_REPORT = {
    "ticker": "AAPL",
    "data_as_of": "2026-08-31",
    "executive_summary": (
        "Test AI research report used for Lakebase persistence validation."
    ),
    "market_analysis": (
        "Market analysis test."
    ),
    "technical_analysis": (
        "Technical analysis test."
    ),
    "news_analysis": (
        "News analysis test."
    ),
    "anomaly_analysis": (
        "Anomaly analysis test."
    ),
    "thesis_alignment": {
        "assessment": "PARTIALLY_SUPPORTS",
        "supporting_evidence": [
            "Test supporting evidence."
        ],
        "contradicting_evidence": [
            "Test contradicting evidence."
        ],
    },
    "key_catalysts": [
        "Test catalyst."
    ],
    "key_risks": [
        "Test risk."
    ],
    "data_limitations": [
        "Test limitation."
    ],
    "conclusion": (
        "Test conclusion."
    ),
    "disclaimer": (
        "Research information only. Not investment advice."
    ),
}


def main():
    print("=" * 100)
    print("LAKEBASE ANALYSIS REPORT REPOSITORY TEST")
    print("=" * 100)

    # ==========================================================
    # COUNT BEFORE
    # ==========================================================

    before_count = count_analysis_reports(
        USER_ID
    )

    print(
        f"\nReports before test: {before_count}"
    )

    # ==========================================================
    # CREATE
    # ==========================================================

    print("\n1. SAVE REPORT")
    print("-" * 100)

    created = save_analysis_report(
        user_id=USER_ID,
        ticker=TICKER,
        title="AAPL Research Report - Repository Test",
        question=(
            "Does current evidence support "
            "the investment thesis?"
        ),
        investment_thesis=(
            "Long-term growth driven by ecosystem "
            "strength, services and recurring revenue."
        ),
        model="openai/gpt-oss-120b",
        thesis_assessment="PARTIALLY_SUPPORTS",
        data_as_of="2026-08-31",
        content=TEST_REPORT,
    )

    report_id = created["report_id"]

    print(
        f"[PASS] Created report ID: {report_id}"
    )

    assert created["user_id"] == USER_ID
    assert created["ticker"] == TICKER

    # ==========================================================
    # READ ONE
    # ==========================================================

    print("\n2. GET REPORT")
    print("-" * 100)

    loaded = get_analysis_report(
        report_id
    )

    assert loaded is not None
    assert loaded["report_id"] == report_id
    assert loaded["user_id"] == USER_ID
    assert loaded["ticker"] == TICKER

    assert isinstance(
        loaded["content"],
        dict,
    )

    assert (
        loaded["content"]
        ["thesis_alignment"]
        ["assessment"]
        == "PARTIALLY_SUPPORTS"
    )

    print(
        f"[PASS] Loaded report: "
        f"{loaded['title']}"
    )

    print(
        f"       User:       "
        f"{loaded['username']}"
    )

    print(
        f"       Ticker:     "
        f"{loaded['ticker']}"
    )

    print(
        f"       Assessment: "
        f"{loaded['thesis_assessment']}"
    )

    # ==========================================================
    # LIST
    # ==========================================================

    print("\n3. LIST USER REPORTS")
    print("-" * 100)

    reports = list_analysis_reports(
        user_id=USER_ID,
        ticker=TICKER,
    )

    assert any(
        row["report_id"] == report_id
        for row in reports
    )

    print(
        f"[PASS] Loaded {len(reports)} "
        f"{TICKER} report(s)"
    )

    # ==========================================================
    # COUNT
    # ==========================================================

    print("\n4. COUNT")
    print("-" * 100)

    during_count = count_analysis_reports(
        USER_ID
    )

    assert during_count == before_count + 1

    print(
        f"[PASS] Report count increased: "
        f"{before_count} -> {during_count}"
    )

    # ==========================================================
    # DELETE TEST RECORD
    # ==========================================================

    print("\n5. DELETE TEST REPORT")
    print("-" * 100)

    deleted = delete_analysis_report(
        report_id=report_id,
        user_id=USER_ID,
    )

    assert deleted is True

    print(
        f"[PASS] Deleted test report ID: "
        f"{report_id}"
    )

    # ==========================================================
    # FINAL COUNT
    # ==========================================================

    after_count = count_analysis_reports(
        USER_ID
    )

    assert after_count == before_count

    print()
    print("=" * 100)
    print(
        "[PASS] ANALYSIS REPORT REPOSITORY "
        "WORKS CORRECTLY"
    )
    print("=" * 100)


if __name__ == "__main__":
    main()