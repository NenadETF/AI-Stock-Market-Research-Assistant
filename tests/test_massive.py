import os

import requests
from dotenv import load_dotenv


load_dotenv()

api_key = os.getenv("MASSIVE_API_KEY")

if not api_key:
    raise ValueError("MASSIVE_API_KEY nije pronađen u .env fajlu.")

url = "https://api.massive.com/v3/reference/tickers/AAPL"

headers = {
    "Authorization": f"Bearer {api_key}"
}

response = requests.get(url, headers=headers, timeout=30)

print("HTTP status:", response.status_code)

if response.ok:
    data = response.json()

    print("Massive API radi!")
    print("Status:", data.get("status"))

    result = data.get("results", {})

    print("Ticker:", result.get("ticker"))
    print("Naziv:", result.get("name"))
    print("Market:", result.get("market"))
    print("Valuta:", result.get("currency_name"))
    print("Berza:", result.get("primary_exchange"))

else:
    print("Greška:")
    print(response.text)