from src.lakebase.analysis_report_repository import (
    delete_analysis_report,
    get_analysis_report,
    list_analysis_reports,
)


def get_research_history(
    user_id: int,
    limit: int = 50,
):
    """
    Returns saved research history for one user,
    ordered from newest to oldest.
    """

    if user_id < 1:
        raise ValueError(
            "user_id mora biti veći od 0."
        )

    if limit < 1:
        raise ValueError(
            "Limit mora biti veći od 0."
        )

    return list_analysis_reports(
        user_id=user_id,
        limit=limit,
    )


def get_ticker_history(
    user_id: int,
    ticker: str,
    limit: int = 50,
):
    """
    Returns saved research history for one ticker
    belonging to one user.
    """

    if user_id < 1:
        raise ValueError(
            "user_id mora biti veći od 0."
        )

    if limit < 1:
        raise ValueError(
            "Limit mora biti veći od 0."
        )

    ticker = ticker.strip().upper()

    if not ticker:
        raise ValueError(
            "Ticker ne može biti prazan."
        )

    return list_analysis_reports(
        user_id=user_id,
        ticker=ticker,
        limit=limit,
    )


def get_research_report(
    user_id: int,
    report_id: int,
):
    """
    Returns one complete research report only if
    it belongs to the supplied user.

    Returns None if:
    - the report does not exist
    - the report belongs to another user
    """

    if user_id < 1:
        raise ValueError(
            "user_id mora biti veći od 0."
        )

    if report_id < 1:
        raise ValueError(
            "report_id mora biti veći od 0."
        )

    report = get_analysis_report(
        report_id=report_id,
    )

    if report is None:
        return None

    if report["user_id"] != user_id:
        return None

    return report


def get_latest_research(
    user_id: int,
    ticker: str,
):
    """
    Returns the newest complete saved research report
    for one ticker and one user.

    Returns None if no saved report exists.
    """

    if user_id < 1:
        raise ValueError(
            "user_id mora biti veći od 0."
        )

    ticker = ticker.strip().upper()

    if not ticker:
        raise ValueError(
            "Ticker ne može biti prazan."
        )

    reports = get_ticker_history(
        user_id=user_id,
        ticker=ticker,
        limit=1,
    )

    if not reports:
        return None

    latest_report_id = (
        reports[0]["report_id"]
    )

    return get_research_report(
        user_id=user_id,
        report_id=latest_report_id,
    )


def delete_research_report(
    user_id: int,
    report_id: int,
) -> bool:
    """
    Deletes a research report only if it belongs
    to the supplied user.
    """

    if user_id < 1:
        raise ValueError(
            "user_id mora biti veći od 0."
        )

    if report_id < 1:
        raise ValueError(
            "report_id mora biti veći od 0."
        )

    return delete_analysis_report(
        report_id=report_id,
        user_id=user_id,
    )