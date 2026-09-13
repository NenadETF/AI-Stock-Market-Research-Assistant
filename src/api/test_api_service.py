import json
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


BASE_URL = "http://127.0.0.1:8000"

USER_ID = 1
TICKER = "AAPL"


def request_json(
    path: str,
    expected_status: int = 200,
):
    url = f"{BASE_URL}{path}"

    request = Request(
        url=url,
        method="GET",
        headers={
            "Accept": "application/json",
        },
    )

    try:
        with urlopen(
            request,
            timeout=180,
        ) as response:
            status_code = response.status
            body = response.read().decode("utf-8")

            data = (
                json.loads(body)
                if body
                else None
            )

    except HTTPError as exc:
        status_code = exc.code

        body = exc.read().decode("utf-8")

        data = (
            json.loads(body)
            if body
            else None
        )

    if status_code != expected_status:
        raise AssertionError(
            f"{path}: expected HTTP "
            f"{expected_status}, received "
            f"{status_code}. Response: {data}"
        )

    return data


def print_pass(name: str):
    print(f"[PASS] {name}")


def main():
    print("=" * 90)
    print("AI STOCK MARKET RESEARCH ASSISTANT - API SMOKE TEST")
    print("=" * 90)

    # ----------------------------------------------------------
    # HEALTH
    # ----------------------------------------------------------

    health = request_json(
        "/health"
    )

    assert health["status"] == "ok"

    print_pass(
        "API health check"
    )

    # ----------------------------------------------------------
    # WATCHLIST
    # ----------------------------------------------------------

    watchlist = request_json(
        f"/users/{USER_ID}/watchlist"
    )

    assert isinstance(
        watchlist,
        list,
    )

    print_pass(
        "Lakebase watchlist API"
    )

    # ----------------------------------------------------------
    # LATEST MARKET ANALYTICS
    # ----------------------------------------------------------

    latest = request_json(
        f"/analytics/{TICKER}/latest"
    )

    assert latest["ticker"] == TICKER

    print_pass(
        "Latest Gold analytics API"
    )

    # ----------------------------------------------------------
    # MARKET HISTORY
    # ----------------------------------------------------------

    params = urlencode({
        "limit": 3,
    })

    history = request_json(
        f"/analytics/{TICKER}/history?{params}"
    )

    assert isinstance(
        history,
        list,
    )

    assert len(history) > 0

    print_pass(
        "Historical Gold analytics API"
    )

    # ----------------------------------------------------------
    # NEWS SUMMARY
    # ----------------------------------------------------------

    news_summary = request_json(
        f"/analytics/{TICKER}/news/summary"
    )

    assert (
        news_summary["ticker"]
        == TICKER
    )

    print_pass(
        "News summary API"
    )

    # ----------------------------------------------------------
    # NEWS HISTORY
    # ----------------------------------------------------------

    news_history = request_json(
        f"/analytics/{TICKER}/news/history?limit=3"
    )

    assert isinstance(
        news_history,
        list,
    )

    print_pass(
        "News history API"
    )

    # ----------------------------------------------------------
    # ANOMALIES
    # ----------------------------------------------------------

    anomalies = request_json(
        f"/analytics/{TICKER}/anomalies?limit=3"
    )

    assert isinstance(
        anomalies,
        list,
    )

    print_pass(
        "Stock anomalies API"
    )

    # ----------------------------------------------------------
    # ANOMALY SUMMARY
    # ----------------------------------------------------------

    anomaly_summary = request_json(
        f"/analytics/{TICKER}/anomalies/summary"
    )

    assert (
        anomaly_summary["ticker"]
        == TICKER
    )

    print_pass(
        "Anomaly summary API"
    )

    # ----------------------------------------------------------
    # RESEARCH HISTORY
    # ----------------------------------------------------------

    research_history = request_json(
        f"/users/{USER_ID}/research?limit=3"
    )

    assert isinstance(
        research_history,
        list,
    )

    print_pass(
        "Research history API"
    )

    # ----------------------------------------------------------
    # LATEST SAVED RESEARCH REPORT
    # ----------------------------------------------------------

    if research_history:
        report_id = (
            research_history[0]["report_id"]
        )

        report = request_json(
            f"/users/{USER_ID}/research/{report_id}"
        )

        assert (
            report["report_id"]
            == report_id
        )

        assert "content" in report

        print_pass(
            "Research report detail API"
        )

    # ----------------------------------------------------------
    # EXPECTED 404 TEST
    # ----------------------------------------------------------

    missing_user = request_json(
        "/users/999999/watchlist",
        expected_status=404,
    )

    assert "detail" in missing_user

    print_pass(
        "Expected 404 handling"
    )

    print()
    print("=" * 90)
    print("FINAL RESULT: ALL API SMOKE TESTS PASSED")
    print("=" * 90)


if __name__ == "__main__":
    main()