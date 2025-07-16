import json
from google.cloud import storage

# CONFIG
BUCKET_NAME = "historical_data_evoke"
BLOB_PATH = "market_data/earnings_calendar/ALL_EARNINGS_2025.json"

# Initialize GCS client
client = storage.Client()
bucket = client.bucket(BUCKET_NAME)
blob = bucket.blob(BLOB_PATH)

# Download and parse
print(f"📥 Downloading: {BLOB_PATH}")
content = blob.download_as_text()
data = json.loads(content)

dot_tickers = set()

for entry in data.get("earningsCalendar", []):
    symbol = entry.get("symbol", "")
    if "." in symbol:
        dot_tickers.add(symbol)

# Output results
print(f"\n🔍 Found {len(dot_tickers)} tickers with '.' in symbol:\n")
for ticker in sorted(dot_tickers):
    print(f" - {ticker}")
