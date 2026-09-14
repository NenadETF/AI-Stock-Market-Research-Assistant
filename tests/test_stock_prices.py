import os
from datetime import datetime

import requests
from dotenv import load_dotenv
from databricks.connect import DatabricksSession


# ============================================================
# 1. Ucitavanje Massive API kljuca
# ============================================================

load_dotenv()

api_key = os.getenv("MASSIVE_API_KEY")

if not api_key:
    raise ValueError("MASSIVE_API_KEY nije pronađen u .env fajlu.")


# ============================================================
# 2. Parametri
# ============================================================

ticker = "AAPL"
date_from = "2026-08-01"
date_to = "2026-08-31"


# ============================================================
# 3. Massive API poziv
# ============================================================

url = (
    f"https://api.massive.com/v2/aggs/ticker/{ticker}"
    f"/range/1/day/{date_from}/{date_to}"
)

headers = {
    "Authorization": f"Bearer {api_key}"
}

params = {
    "adjusted": "true",
    "sort": "asc",
    "limit": 5000
}

response = requests.get(
    url,
    headers=headers,
    params=params,
    timeout=30
)

print("HTTP status:", response.status_code)

response.raise_for_status()

data = response.json()

results = data.get("results", [])

print("Ticker:", ticker)
print("Broj trading dana:", len(results))


if not results:
    raise ValueError("Massive API nije vratio podatke.")


# ============================================================
# 4. Pretvaranje Massive rezultata u citljiv format
# ============================================================

rows = []

for item in results:
    rows.append(
        {
            "ticker": ticker,
            "date": datetime.fromtimestamp(
                item["t"] / 1000
            ).strftime("%Y-%m-%d"),
            "open": float(item["o"]),
            "high": float(item["h"]),
            "low": float(item["l"]),
            "close": float(item["c"]),
            "volume": float(item["v"]),
            "vwap": (
                float(item["vw"])
                if item.get("vw") is not None
                else None
            ),
            "transactions": (
                int(item["n"])
                if item.get("n") is not None
                else None
            ),
        }
    )


print("\nPrvih 5 zapisa iz Massive API-ja:")

for row in rows[:5]:
    print(row)


# ============================================================
# 5. Databricks Spark sesija
# ============================================================

spark = DatabricksSession.builder.getOrCreate()


# ============================================================
# 6. Kreiranje Spark DataFrame-a
# ============================================================

df = spark.createDataFrame(rows)


print("\nSpark schema:")
df.printSchema()


print("\nSpark DataFrame:")
df.show(30, truncate=False)


print("Ukupan broj redova:", df.count())

# ============================================================
# 7. Provjera Databricks kataloga
# ============================================================

print("\nDostupni katalozi:")
spark.sql("SHOW CATALOGS").show(truncate=False)

print("\nTrenutni katalog i schema:")
spark.sql("""
    SELECT
        current_catalog() AS catalog,
        current_schema() AS schema
""").show(truncate=False)


# ============================================================
# 8. Kreiranje Bronze schema-e
# ============================================================

spark.sql("""
    CREATE SCHEMA IF NOT EXISTS bronze
""")

print("Bronze schema postoji.")


# ============================================================
# 9. Upis u Delta tabelu
# ============================================================

table_name = "bronze.stock_prices"

(
    df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(table_name)
)

print(f"Delta tabela kreirana: {table_name}")


# ============================================================
# 10. Provjera podataka iz Delta tabele
# ============================================================

delta_df = spark.table(table_name)

print("\nPodaci pročitani iz Delta tabele:")

delta_df.show(30, truncate=False)

print(
    "Broj redova u Delta tabeli:",
    delta_df.count()
)