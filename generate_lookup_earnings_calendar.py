import json
from google.cloud import storage
from pathlib import Path

# CONFIG
BUCKET_NAME = "historical_data_evoke"
SOURCE_PREFIX = "market_data/earnings_calendar/2025-01-01_to_2025-12-01/"
DEST_BLOB_NAME = "market_data/earnings_calendar/ALL_EARNINGS_2025.json"

# TEMP LOCAL
local_dir = Path("/tmp/earnings_merge")
local_dir.mkdir(exist_ok=True)
merged_path = local_dir / "ALL_EARNINGS_2025.json"

# INIT GCS
client = storage.Client()
bucket = client.bucket(BUCKET_NAME)
blobs = list(client.list_blobs(BUCKET_NAME, prefix=SOURCE_PREFIX))

all_earnings = []

print(f"📂 Found {len(blobs)} files in {SOURCE_PREFIX}")

for blob in blobs:
    if not blob.name.endswith(".json"):
        continue

    try:
        content = blob.download_as_text()
        data = json.loads(content)
        entries = data.get("earningsCalendar", [])
        if isinstance(entries, list):
            all_earnings.extend(entries)
        else:
            print(f"⚠️ Skipped malformed file: {blob.name}")
    except Exception as e:
        print(f"❌ Error processing {blob.name}: {e}")

print(f"✅ Merged {len(all_earnings)} earnings entries")

# Save locally
with open(merged_path, "w") as f:
    json.dump({"earningsCalendar": all_earnings}, f, indent=2)

print(f"💾 Saved merged file to: {merged_path}")

# Upload to GCS
dest_blob = bucket.blob(DEST_BLOB_NAME)
dest_blob.upload_from_filename(str(merged_path))

print(f"✅ Uploaded to GCS: {DEST_BLOB_NAME}")
