from src.ingestion.stock_news import fetch_stock_news


news = fetch_stock_news(
    ticker="AAPL",
    limit=10
)


print("\n==============================")
print(" AAPL STOCK NEWS")
print("==============================")


print(
    "\nBroj preuzetih clanaka:",
    len(news)
)


for index, article in enumerate(
    news,
    start=1
):

    print("\n--------------------------------")

    print(
        f"CLANAK {index}"
    )

    print(
        "ID:",
        article["article_id"]
    )

    print(
        "Naslov:",
        article["title"]
    )

    print(
        "Datum:",
        article["published_utc"]
    )

    print(
        "Publisher:",
        article["publisher_name"]
    )

    print(
        "Autor:",
        article["author"]
    )

    print(
        "URL:",
        article["article_url"]
    )

    print(
        "Tickers:",
        article["tickers_json"]
    )

    print(
        "Keywords:",
        article["keywords_json"]
    )

    print(
        "Insights:",
        article["insights_json"]
    )


print(
    "\nStock News API test zavrsen."
)