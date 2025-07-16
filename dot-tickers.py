import json
from google.cloud import storage

# CONFIG
BUCKET_NAME = "historical_data_evoke"
SOURCE_PREFIX = "market_data/earnings_calendar/2025-01-01_to_2025-12-01/"

# Init GCS
client = storage.Client()
bucket = client.bucket(BUCKET_NAME)
blobs = list(client.list_blobs(BUCKET_NAME, prefix=SOURCE_PREFIX))

dot_tickers = {}

print(f"🔍 Scanning files in: {SOURCE_PREFIX}")
for blob in blobs:
    if not blob.name.endswith(".json"):
        continue
    try:
        content = blob.download_as_text()
        data = json.loads(content)
        entries = data.get("earningsCalendar", [])
        for entry in entries:
            symbol = entry.get("symbol", "")
            if "." in symbol:
                if symbol not in dot_tickers:
                    dot_tickers[symbol] = []
                dot_tickers[symbol].append(blob.name)
    except Exception as e:
        print(f"⚠️ Error reading {blob.name}: {e}")

# Output
if dot_tickers:
    print(f"\n✅ Found {len(dot_tickers)} dotted tickers:\n")
    for symbol, files in sorted(dot_tickers.items()):
        print(f" - {symbol}:")
        for f in files:
            print(f"    📄 {f}")
else:
    print("✅ No tickers with '.' found.")
