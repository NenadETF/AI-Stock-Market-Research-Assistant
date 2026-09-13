import json
import os
import time
import requests
from datetime import datetime, timezone


MASSIVE_BASE_URL = "https://api.massive.com"

REQUEST_DELAY_SECONDS = 13
MAX_RETRIES = 3


def fetch_stock_news(
    ticker: str,
    limit: int = 25
) -> list[dict]:
    """
    Preuzima najnovije vijesti povezane sa zadatim tickerom
    koristeći Massive News API.

    Za Bronze sloj složenije strukture:
    - tickers
    - keywords
    - insights

    čuvamo kao JSON stringove.
    """

    api_key = os.getenv("MASSIVE_API_KEY")

    if not api_key:
        raise ValueError(
            "MASSIVE_API_KEY environment variable nije postavljen."
        )

    ticker = ticker.upper()

    url = (
        f"{MASSIVE_BASE_URL}/v2/reference/news"
    )

    params = {
        "ticker": ticker,
        "limit": limit,
        "order": "desc",
        "sort": "published_utc",
        "apiKey": api_key,
    }

    response = None

    # ========================================================
    # HTTP + retry
    # ========================================================

    for attempt in range(1, MAX_RETRIES + 1):

        response = requests.get(
            url,
            params=params,
            timeout=30
        )

        if response.status_code == 200:
            break

        if response.status_code == 429:

            retry_after = response.headers.get(
                "Retry-After"
            )

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
                f"Novi pokusaj nakon {delay}s."
            )

            time.sleep(delay)

            continue

        response.raise_for_status()

    else:

        raise RuntimeError(
            f"Nije moguce preuzeti stock news "
            f"za {ticker} nakon "
            f"{MAX_RETRIES} pokusaja."
        )

    response.raise_for_status()

    payload = response.json()

    results = payload.get(
        "results",
        []
    )

    ingested_at = datetime.now(
        timezone.utc
    ).isoformat()

    records = []

    # ========================================================
    # Transformacija API odgovora u Bronze zapise
    # ========================================================

    for item in results:

        publisher = (
            item.get("publisher")
            or {}
        )

        tickers = (
            item.get("tickers")
            or []
        )

        keywords = (
            item.get("keywords")
            or []
        )

        insights = (
            item.get("insights")
            or []
        )

        records.append(
            {
                "article_id":
                    item.get("id"),

                "requested_ticker":
                    ticker,

                "title":
                    item.get("title"),

                "author":
                    item.get("author"),

                "description":
                    item.get("description"),

                "article_url":
                    item.get("article_url"),

                "amp_url":
                    item.get("amp_url"),

                "image_url":
                    item.get("image_url"),

                "published_utc":
                    item.get("published_utc"),

                "publisher_name":
                    publisher.get("name"),

                "publisher_homepage_url":
                    publisher.get("homepage_url"),

                "publisher_logo_url":
                    publisher.get("logo_url"),

                "publisher_favicon_url":
                    publisher.get("favicon_url"),

                # Za Bronze zadržavamo kompletnu strukturu.
                "tickers_json":
                    json.dumps(tickers),

                "keywords_json":
                    json.dumps(keywords),

                "insights_json":
                    json.dumps(insights),

                "_ingested_at":
                    ingested_at,

                "_source":
                    "massive_api",
            }
        )

    return records

def fetch_stock_news_for_tickers(
    tickers: list[str],
    limit_per_ticker: int = 25
) -> list[dict]:
    """
    Preuzima najnovije vijesti za više tickera uz
    Massive API rate-limit zaštitu.
    """

    all_records = []

    total = len(tickers)

    for index, ticker in enumerate(
        tickers,
        start=1
    ):
        ticker = ticker.upper()

        print(
            f"\n[{index}/{total}] "
            f"Preuzimam vijesti za {ticker}..."
        )

        records = fetch_stock_news(
            ticker=ticker,
            limit=limit_per_ticker
        )

        all_records.extend(records)

        print(
            f"{ticker}: preuzeto {len(records)} clanaka."
        )

        if index < total:
            print(
                f"Rate-limit zastita: "
                f"{REQUEST_DELAY_SECONDS}s do sljedeceg API poziva."
            )

            time.sleep(
                REQUEST_DELAY_SECONDS
            )

    return all_records

def fetch_historical_stock_news(
    ticker: str,
    start_date: str,
    end_date: str,
    limit_per_page: int = 1000,
    max_pages: int | None = None
) -> list[dict]:
    """
    Preuzima istorijske vijesti za jedan ticker uz paginaciju.

    start_date / end_date:
        YYYY-MM-DD

    Ako Massive vrati next_url, funkcija nastavlja
    sa sljedećom stranicom dok ne preuzme sve rezultate
    ili dok ne dostigne max_pages.
    """

    api_key = os.getenv("MASSIVE_API_KEY")

    if not api_key:
        raise ValueError(
            "MASSIVE_API_KEY environment variable nije postavljen."
        )

    ticker = ticker.upper()

    url = f"{MASSIVE_BASE_URL}/v2/reference/news"

    params = {
        "ticker": ticker,
        "published_utc.gte": start_date,
        "published_utc.lte": end_date,
        "sort": "published_utc",
        "order": "asc",
        "limit": limit_per_page,
        "apiKey": api_key,
    }

    all_records = []

    page_number = 1

    while url:

        print(
            f"{ticker}: preuzimam stranicu {page_number}..."
        )

        response = None

        # ====================================================
        # Retry logika
        # ====================================================

        for attempt in range(1, MAX_RETRIES + 1):

            # Prvi request koristi pune parametre.
            # next_url već sadrži cursor i ostale parametre.
            if page_number == 1:

                response = requests.get(
                    url,
                    params=params,
                    timeout=30
                )

            else:

                response = requests.get(
                    url,
                    params={
                        "apiKey": api_key
                    },
                    timeout=30
                )

            if response.status_code == 200:
                break

            if response.status_code == 429:

                retry_after = response.headers.get(
                    "Retry-After"
                )

                if retry_after:

                    try:
                        delay = int(retry_after)

                    except ValueError:
                        delay = 60

                else:
                    delay = 60 * attempt

                print(
                    f"[429] Rate limit za {ticker}, "
                    f"stranica {page_number}. "
                    f"Novi pokusaj za {delay}s."
                )

                time.sleep(delay)

                continue

            response.raise_for_status()

        else:

            raise RuntimeError(
                f"Historical News API nije uspio za "
                f"{ticker}, stranica {page_number}."
            )

        response.raise_for_status()

        payload = response.json()

        results = payload.get(
            "results",
            []
        )

        ingested_at = datetime.now(
            timezone.utc
        ).isoformat()

        # ====================================================
        # Pretvaranje API rezultata u Bronze records
        # ====================================================

        for item in results:

            publisher = (
                item.get("publisher")
                or {}
            )

            tickers = (
                item.get("tickers")
                or []
            )

            keywords = (
                item.get("keywords")
                or []
            )

            insights = (
                item.get("insights")
                or []
            )

            all_records.append(
                {
                    "article_id":
                        item.get("id"),

                    "requested_ticker":
                        ticker,

                    "title":
                        item.get("title"),

                    "author":
                        item.get("author"),

                    "description":
                        item.get("description"),

                    "article_url":
                        item.get("article_url"),

                    "amp_url":
                        item.get("amp_url"),

                    "image_url":
                        item.get("image_url"),

                    "published_utc":
                        item.get("published_utc"),

                    "publisher_name":
                        publisher.get("name"),

                    "publisher_homepage_url":
                        publisher.get("homepage_url"),

                    "publisher_logo_url":
                        publisher.get("logo_url"),

                    "publisher_favicon_url":
                        publisher.get("favicon_url"),

                    "tickers_json":
                        json.dumps(tickers),

                    "keywords_json":
                        json.dumps(keywords),

                    "insights_json":
                        json.dumps(insights),

                    "_ingested_at":
                        ingested_at,

                    "_source":
                        "massive_api",
                }
            )

        print(
            f"{ticker}: stranica {page_number} -> "
            f"{len(results)} clanaka."
        )

        next_url = payload.get(
            "next_url"
        )

        if not next_url:
            break

        if (
            max_pages is not None
            and page_number >= max_pages
        ):
            print(
                f"{ticker}: dostignut max_pages={max_pages}."
            )
            break

        # Svaka stranica je novi API poziv.
        print(
            f"Rate-limit zastita: "
            f"{REQUEST_DELAY_SECONDS}s do sljedece stranice."
        )

        time.sleep(
            REQUEST_DELAY_SECONDS
        )

        url = next_url

        # Od druge stranice cursor je već unutar next_url.
        params = None

        page_number += 1

    return all_records