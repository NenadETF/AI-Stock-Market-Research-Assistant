from src.ingestion.company_details import fetch_company_details


company = fetch_company_details(
    ticker="AAPL"
)

print("\n=== COMPANY DETAILS ===")

for key, value in company.items():
    print(f"{key}: {value}")