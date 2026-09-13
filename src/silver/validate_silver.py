from databricks.connect import DatabricksSession
from pyspark.sql import functions as F


# ============================================================
# SPARK SESSION
# ============================================================

spark = DatabricksSession.builder.getOrCreate()


PRICE_TABLE = "workspace.silver.stock_prices"
NEWS_TABLE = "workspace.silver.stock_news"
COMPANY_TABLE = "workspace.silver.company_details"


print("=" * 90)
print("FINAL SILVER LAYER VALIDATION")
print("=" * 90)


all_checks_passed = True


def check(name, condition, details=""):
    global all_checks_passed

    status = "PASS" if condition else "FAIL"

    print(f"[{status}] {name}")

    if details:
        print(f"       {details}")

    if not condition:
        all_checks_passed = False


# ============================================================
# 1. LOAD TABLES
# ============================================================

prices_df = spark.table(PRICE_TABLE)
news_df = spark.table(NEWS_TABLE)
companies_df = spark.table(COMPANY_TABLE)


price_count = prices_df.count()
news_count = news_df.count()
company_count = companies_df.count()


print("\n" + "=" * 90)
print("1. TABLE COUNTS")
print("=" * 90)

print(f"stock_prices:     {price_count}")
print(f"stock_news:       {news_count}")
print(f"company_details:  {company_count}")


check(
    "stock_prices tabela nije prazna",
    price_count > 0
)

check(
    "stock_news tabela nije prazna",
    news_count > 0
)

check(
    "company_details tabela nije prazna",
    company_count > 0
)


# ============================================================
# 2. STOCK PRICES VALIDATION
# ============================================================

print("\n" + "=" * 90)
print("2. STOCK PRICES VALIDATION")
print("=" * 90)


price_null_keys = (
    prices_df
    .filter(
        F.col("ticker").isNull()
        | F.col("date").isNull()
    )
    .count()
)


price_duplicates = (
    prices_df
    .groupBy(
        "ticker",
        "date"
    )
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)


invalid_ohlc = (
    prices_df
    .filter(
        (F.col("open") <= 0)
        | (F.col("high") <= 0)
        | (F.col("low") <= 0)
        | (F.col("close") <= 0)

        | (F.col("high") < F.col("open"))
        | (F.col("high") < F.col("close"))
        | (F.col("high") < F.col("low"))

        | (F.col("low") > F.col("open"))
        | (F.col("low") > F.col("close"))
        | (F.col("low") > F.col("high"))
    )
    .count()
)


invalid_volume = (
    prices_df
    .filter(
        F.col("volume").isNotNull()
        & (F.col("volume") < 0)
    )
    .count()
)


invalid_transactions = (
    prices_df
    .filter(
        F.col("transactions").isNotNull()
        & (F.col("transactions") < 0)
    )
    .count()
)


check(
    "stock_prices nema NULL poslovne ključeve",
    price_null_keys == 0,
    f"NULL ključeva: {price_null_keys}"
)

check(
    "stock_prices nema duplikate po (ticker, date)",
    price_duplicates == 0,
    f"Duplikata: {price_duplicates}"
)

check(
    "stock_prices OHLC vrijednosti su validne",
    invalid_ohlc == 0,
    f"Nevalidnih OHLC redova: {invalid_ohlc}"
)

check(
    "stock_prices volume je validan",
    invalid_volume == 0,
    f"Nevalidnih volume vrijednosti: {invalid_volume}"
)

check(
    "stock_prices transactions je validan",
    invalid_transactions == 0,
    f"Nevalidnih vrijednosti: {invalid_transactions}"
)


print("\nSTOCK PRICE PERIOD:")

prices_df.select(
    F.min("date").alias("najstariji"),
    F.max("date").alias("najnoviji")
).show()


print("STOCK PRICE TICKERS:")

prices_df.select(
    "ticker"
).distinct().orderBy(
    "ticker"
).show(
    100,
    truncate=False
)


# ============================================================
# 3. STOCK NEWS VALIDATION
# ============================================================

print("\n" + "=" * 90)
print("3. STOCK NEWS VALIDATION")
print("=" * 90)


news_null_keys = (
    news_df
    .filter(
        F.col("article_id").isNull()
        | F.col("requested_ticker").isNull()
    )
    .count()
)


news_duplicates = (
    news_df
    .groupBy(
        "article_id",
        "requested_ticker"
    )
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)


invalid_sentiment = (
    news_df
    .filter(
        F.col(
            "requested_ticker_sentiment"
        ).isNotNull()
        & ~F.col(
            "requested_ticker_sentiment"
        ).isin(
            "positive",
            "neutral",
            "negative"
        )
    )
    .count()
)


null_published_date = (
    news_df
    .filter(
        F.col("published_date").isNull()
    )
    .count()
)


check(
    "stock_news nema NULL poslovne ključeve",
    news_null_keys == 0,
    f"NULL ključeva: {news_null_keys}"
)

check(
    "stock_news nema duplikate po "
    "(article_id, requested_ticker)",
    news_duplicates == 0,
    f"Duplikata: {news_duplicates}"
)

check(
    "stock_news sentiment vrijednosti su validne",
    invalid_sentiment == 0,
    f"Nevalidnih sentimenta: {invalid_sentiment}"
)

check(
    "stock_news svaki članak ima published_date",
    null_published_date == 0,
    f"NULL published_date: {null_published_date}"
)


print("\nNEWS PERIOD:")

news_df.select(
    F.min("published_utc").alias("najstariji"),
    F.max("published_utc").alias("najnoviji")
).show(
    truncate=False
)


print("NEWS SENTIMENT DISTRIBUCIJA:")

news_df.groupBy(
    "requested_ticker_sentiment"
).count().orderBy(
    F.desc("count")
).show()


# ============================================================
# 4. COMPANY DETAILS VALIDATION
# ============================================================

print("\n" + "=" * 90)
print("4. COMPANY DETAILS VALIDATION")
print("=" * 90)


company_null_keys = (
    companies_df
    .filter(
        F.col("ticker").isNull()
        | F.col("name").isNull()
    )
    .count()
)


company_duplicates = (
    companies_df
    .groupBy("ticker")
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)


invalid_market_cap = (
    companies_df
    .filter(
        F.col("market_cap").isNotNull()
        & (F.col("market_cap") < 0)
    )
    .count()
)


invalid_employees = (
    companies_df
    .filter(
        F.col("total_employees").isNotNull()
        & (F.col("total_employees") < 0)
    )
    .count()
)


check(
    "company_details nema NULL ključne vrijednosti",
    company_null_keys == 0,
    f"NULL ključeva: {company_null_keys}"
)

check(
    "company_details nema duplikate po tickeru",
    company_duplicates == 0,
    f"Duplikata: {company_duplicates}"
)

check(
    "company_details market_cap je validan",
    invalid_market_cap == 0,
    f"Nevalidnih market cap vrijednosti: {invalid_market_cap}"
)

check(
    "company_details total_employees je validan",
    invalid_employees == 0,
    f"Nevalidnih vrijednosti: {invalid_employees}"
)


# ============================================================
# 5. CROSS-TABLE CONSISTENCY
# ============================================================

print("\n" + "=" * 90)
print("5. CROSS-TABLE CONSISTENCY")
print("=" * 90)


company_tickers = (
    companies_df
    .select("ticker")
    .distinct()
)


price_tickers = (
    prices_df
    .select("ticker")
    .distinct()
)


news_tickers = (
    news_df
    .select(
        F.col(
            "requested_ticker"
        ).alias("ticker")
    )
    .distinct()
)


prices_without_company = (
    price_tickers
    .join(
        company_tickers,
        "ticker",
        "left_anti"
    )
)


news_without_company = (
    news_tickers
    .join(
        company_tickers,
        "ticker",
        "left_anti"
    )
)


prices_without_company_count = (
    prices_without_company.count()
)

news_without_company_count = (
    news_without_company.count()
)


check(
    "Svaki stock_prices ticker postoji u company_details",
    prices_without_company_count == 0,
    f"Nedostajućih tickera: {prices_without_company_count}"
)

check(
    "Svaki stock_news requested_ticker postoji u company_details",
    news_without_company_count == 0,
    f"Nedostajućih tickera: {news_without_company_count}"
)


if prices_without_company_count > 0:
    print("\nPRICE TICKERI BEZ COMPANY DETAILS:")
    prices_without_company.show(truncate=False)


if news_without_company_count > 0:
    print("\nNEWS TICKERI BEZ COMPANY DETAILS:")
    news_without_company.show(truncate=False)


# ============================================================
# 6. TABLE SUMMARY
# ============================================================

print("\n" + "=" * 90)
print("6. SILVER SUMMARY")
print("=" * 90)


summary_df = spark.createDataFrame(
    [
        (
            "stock_prices",
            price_count
        ),
        (
            "stock_news",
            news_count
        ),
        (
            "company_details",
            company_count
        )
    ],
    [
        "table_name",
        "row_count"
    ]
)


summary_df.show(
    truncate=False
)


# ============================================================
# 7. FINAL RESULT
# ============================================================

print("=" * 90)

if all_checks_passed:

    print(
        "FINAL RESULT: ALL SILVER DATA QUALITY CHECKS PASSED"
    )

    print(
        "SILVER LAYER JE USPJESNO VALIDIRAN."
    )

else:

    print(
        "FINAL RESULT: SILVER VALIDATION FAILED"
    )

    print(
        "Jedna ili više provjera nije prošla."
    )

print("=" * 90)