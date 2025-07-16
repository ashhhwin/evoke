from google.cloud import storage
import pandas as pd
import io

# GCS bucket and folder
BUCKET_NAME = "historical_data_evoke"
PREFIX = "Final_data_v2/"

def convert_all_csvs_to_parquet():
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blobs = list(bucket.list_blobs(prefix=PREFIX))

    for blob in blobs:
        if blob.name.endswith(".csv"):
            print(f"Processing: {blob.name}")
            try:
                # 1. Download CSV from GCS
                csv_bytes = blob.download_as_bytes()
                df = pd.read_csv(io.BytesIO(csv_bytes), low_memory=False)

                # 2. Rename date column
                if "Trade_Date" not in df.columns or "Symbol" not in df.columns:
                    raise ValueError("Required columns missing")

                df.rename(columns={"Trade_Date": "timestamp"}, inplace=True)
                df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

                # 3. Clean column names
                df.columns = df.columns.str.strip()

                # 4. Identify non-numeric columns to preserve
                string_cols = [
                    "timestamp", "Symbol", "Company_Name", "Type", "Sector",
                    "Industry", "Earnings_Date"
                ]

                for col in df.columns:
                    if col not in string_cols:
                        df[col] = pd.to_numeric(df[col], errors="coerce")

                # 5. Drop rows missing essential info
                df.dropna(subset=["timestamp", "Symbol"], inplace=True)

                # 6. Save as Parquet in memory
                parquet_buffer = io.BytesIO()
                df.to_parquet(parquet_buffer, index=False)
                parquet_buffer.seek(0)

                # 7. Upload to GCS with .parquet extension
                parquet_blob_name = blob.name.replace(".csv", ".parquet")
                bucket.blob(parquet_blob_name).upload_from_file(
                    parquet_buffer, content_type="application/octet-stream"
                )
                print(f"✅ Uploaded: {parquet_blob_name}")

            except Exception as e:
                print(f"❌ Failed to process {blob.name}: {e}")

if __name__ == "__main__":
    convert_all_csvs_to_parquet()
