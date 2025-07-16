from google.cloud import storage
import pandas as pd
import io

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

                # 2. Attempt to convert Trade_Date to datetime (optional, keeps original column name)
                if "Trade_Date" in df.columns:
                    df["Trade_Date"] = pd.to_datetime(df["Trade_Date"], errors="coerce")

                # 3. Attempt numeric conversion (non-destructive)
                for col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="ignore")

                # 4. Save as Parquet in memory
                parquet_buffer = io.BytesIO()
                df.to_parquet(parquet_buffer, index=False)
                parquet_buffer.seek(0)

                # 5. Upload to GCS
                parquet_blob_name = blob.name.replace(".csv", ".parquet")
                bucket.blob(parquet_blob_name).upload_from_file(
                    parquet_buffer, content_type="application/octet-stream"
                )
                print(f"✅ Uploaded: {parquet_blob_name}")

            except Exception as e:
                print(f"❌ Failed to process {blob.name}: {e}")

if __name__ == "__main__":
    convert_all_csvs_to_parquet()
