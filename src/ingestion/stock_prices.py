import os
import time
import requests
from datetime import datetime, timezone


MASSIVE_BASE_URL = "https://api.massive.com"

# Massive Basic plan trenutno dozvoljava 5 API poziva/min.
# 13 sekundi između poziva daje nam malu sigurnosnu marginu.
REQUEST_DELAY_SECONDS = 13

MAX_RETRIES = 3


def fetch_stock_prices(
    ticker: str,
    start_date: str,
    end_date: str
) -> list[dict]:
    """
    Preuzima istorijske dnevne OHLC podatke za zadati ticker
    sa Massive API-ja.

    Ako API vrati HTTP 429, pokušava ponovo nakon pauze.
    """

    api_key = os.getenv("MASSIVE_API_KEY")

    if not api_key:
        raise ValueError(
            "MASSIVE_API_KEY environment variable nije postavljen."
        )

    ticker = ticker.upper()

    url = (
        f"{MASSIVE_BASE_URL}/v2/aggs/ticker/"
        f"{ticker}/range/1/day/"
        f"{start_date}/{end_date}"
    )

    params = {
        "adjusted": "true",
        "sort": "asc",
        "limit": 50000,
        "apiKey": api_key,
    }

    # ========================================================
    # API poziv sa retry logikom
    # ========================================================

    response = None

    for attempt in range(1, MAX_RETRIES + 1):

        response = requests.get(
            url,
            params=params,
            timeout=30
        )

        # Uspješan odgovor
        if response.status_code == 200:
            break

        # Rate limit
        if response.status_code == 429:

            retry_after = response.headers.get("Retry-After")

            if retry_after:
                try:
                    delay = int(retry_after)
                except ValueError:
                    delay = 60
            else:
                # Ako server nije rekao kada da pokušamo ponovo,
                # koristimo konzervativni backoff.
                delay = 60 * attempt

            print(
                f"[429] Rate limit za {ticker}. "
                f"Pokusaj {attempt}/{MAX_RETRIES}. "
                f"Novi pokusaj nakon {delay} sekundi."
            )

            time.sleep(delay)

            continue

        # Sve druge HTTP greške
        response.raise_for_status()

    else:
        raise RuntimeError(
            f"Massive API rate limit nije prevazidjen "
            f"nakon {MAX_RETRIES} pokusaja za ticker {ticker}."
        )

    response.raise_for_status()

    payload = response.json()

    results = payload.get("results", [])

    records = []

    ingested_at = datetime.now(
        timezone.utc
    ).isoformat()

    for item in results:

        timestamp_ms = item.get("t")

        trading_date = (
            datetime
            .fromtimestamp(
                timestamp_ms / 1000,
                tz=timezone.utc
            )
            .date()
            .isoformat()
            if timestamp_ms is not None
            else None
        )

        records.append(
            {
                "ticker": ticker,
                "date": trading_date,
                "open": item.get("o"),
                "high": item.get("h"),
                "low": item.get("l"),
                "close": item.get("c"),
                "volume": item.get("v"),
                "vwap": item.get("vw"),
                "transactions": item.get("n"),
                "_ingested_at": ingested_at,
                "_source": "massive_api",
            }
        )

    return records


def fetch_stock_prices_for_tickers(
    tickers: list[str],
    start_date: str,
    end_date: str
) -> list[dict]:
    """
    Preuzima istorijske dnevne podatke za više tickera.

    Pozivi se namjerno razdvajaju kako ne bismo prekoračili
    Massive API rate limit.
    """

    all_records = []

    total_tickers = len(tickers)

    for index, ticker in enumerate(tickers, start=1):

        ticker = ticker.upper()

        print(
            f"\n[{index}/{total_tickers}] "
            f"Preuzimam {ticker}..."
        )

        records = fetch_stock_prices(
            ticker=ticker,
            start_date=start_date,
            end_date=end_date
        )

        print(
            f"{ticker}: preuzeto {len(records)} zapisa."
        )

        all_records.extend(records)

        # Ne pravimo pauzu nakon posljednjeg tickera.
        if index < total_tickers:

            print(
                f"Rate-limit zastita: "
                f"{REQUEST_DELAY_SECONDS}s do sljedeceg API poziva."
            )

            time.sleep(
                REQUEST_DELAY_SECONDS
            )

    return all_records