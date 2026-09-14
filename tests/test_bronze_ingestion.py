from src.ingestion.stock_prices import fetch_stock_prices


records = fetch_stock_prices(
    ticker="AAPL",
    start_date="2026-08-01",
    end_date="2026-08-31"
)

print("Broj zapisa:", len(records))

print("\nPrvih 5 zapisa:")

for record in records[:5]:
    print(record)