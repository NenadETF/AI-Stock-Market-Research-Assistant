import os
import time
import requests
from datetime import datetime, timezone


MASSIVE_BASE_URL = "https://api.massive.com"

REQUEST_DELAY_SECONDS = 13
MAX_RETRIES = 3


def fetch_company_details(ticker: str) -> dict:
    """
    Preuzima detaljne podatke o kompaniji za zadati ticker
    sa Massive API-ja.
    """

    api_key = os.getenv("MASSIVE_API_KEY")

    if not api_key:
        raise ValueError(
            "MASSIVE_API_KEY environment variable nije postavljen."
        )

    ticker = ticker.upper()

    url = (
        f"{MASSIVE_BASE_URL}/v3/reference/tickers/{ticker}"
    )

    params = {
        "apiKey": api_key
    }

    response = None

    for attempt in range(1, MAX_RETRIES + 1):

        response = requests.get(
            url,
            params=params,
            timeout=30
        )

        if response.status_code == 200:
            break

        if response.status_code == 429:

            retry_after = response.headers.get("Retry-After")

            if retry_after:
                try:
                    delay = int(retry_after)
                except ValueError:
                    delay = 60
            else:
                delay = 60 * attempt

            print(
                f"[429] Rate limit za {ticker}. "
                f"Pokusaj {attempt}/{MAX_RETRIES}. "
                f"Novi pokusaj nakon {delay} sekundi."
            )

            time.sleep(delay)

            continue

        response.raise_for_status()

    else:
        raise RuntimeError(
            f"Nije moguce preuzeti company details "
            f"za ticker {ticker} nakon {MAX_RETRIES} pokusaja."
        )

    response.raise_for_status()

    payload = response.json()

    result = payload.get("results")

    if not result:
        raise ValueError(
            f"Massive API nije vratio company details za {ticker}."
        )

    address = result.get("address") or {}
    branding = result.get("branding") or {}

    ingested_at = datetime.now(
        timezone.utc
    ).isoformat()

    return {
        "ticker": result.get("ticker", ticker),
        "name": result.get("name"),
        "market": result.get("market"),
        "locale": result.get("locale"),
        "primary_exchange": result.get("primary_exchange"),
        "type": result.get("type"),
        "active": result.get("active"),
        "currency_name": result.get("currency_name"),

        "cik": result.get("cik"),
        "composite_figi": result.get("composite_figi"),
        "share_class_figi": result.get("share_class_figi"),

        "market_cap": result.get("market_cap"),

        "description": result.get("description"),
        "sic_code": result.get("sic_code"),
        "sic_description": result.get("sic_description"),

        "homepage_url": result.get("homepage_url"),
        "phone_number": result.get("phone_number"),

        "total_employees": result.get("total_employees"),
        "list_date": result.get("list_date"),

        "ticker_root": result.get("ticker_root"),
        "ticker_suffix": result.get("ticker_suffix"),

        "weighted_shares_outstanding":
            result.get("weighted_shares_outstanding"),

        "share_class_shares_outstanding":
            result.get("share_class_shares_outstanding"),

        "round_lot": result.get("round_lot"),

        "address1": address.get("address1"),
        "city": address.get("city"),
        "state": address.get("state"),
        "postal_code": address.get("postal_code"),

        "logo_url": branding.get("logo_url"),
        "icon_url": branding.get("icon_url"),

        "_ingested_at": ingested_at,
        "_source": "massive_api",
    }


def fetch_company_details_for_tickers(
    tickers: list[str]
) -> list[dict]:
    """
    Preuzima company details za više tickera uz zaštitu
    od Massive API rate limita.
    """

    records = []

    total = len(tickers)

    for index, ticker in enumerate(
        tickers,
        start=1
    ):

        ticker = ticker.upper()

        print(
            f"\n[{index}/{total}] "
            f"Preuzimam company details za {ticker}..."
        )

        company = fetch_company_details(
            ticker=ticker
        )

        records.append(company)

        print(
            f"{ticker}: {company.get('name')}"
        )

        if index < total:

            print(
                f"Rate-limit zastita: "
                f"{REQUEST_DELAY_SECONDS}s do sljedeceg API poziva."
            )

            time.sleep(
                REQUEST_DELAY_SECONDS
            )

    return records