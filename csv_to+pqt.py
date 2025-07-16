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
                # TSV reader with proper header
                csv_bytes = blob.download_as_bytes()
                df = pd.read_csv(io.BytesIO(csv_bytes), sep="\t", low_memory=False)

                # Clean column names
                df.columns = df.columns.str.strip()

                # Validate essential columns
                if "Trade_Date" not in df.columns or "Symbol" not in df.columns:
                    print(f"Skipping {blob.name}: missing Trade_Date or Symbol")
                    continue

                # Rename for downstream consistency
                df.rename(columns={"Trade_Date": "timestamp"}, inplace=True)

                # Parse datetime
                df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

                # Identify columns to preserve as strings
                string_cols = ["timestamp", "Symbol", "Company_Name", "Type", "Sector", "Industry", "Earnings_Date"]
                for col in df.columns:
                    if col not in string_cols:
                        df[col] = pd.to_numeric(df[col], errors="coerce")

                # Drop fully empty rows
                df.dropna(how="all", inplace=True)

                # Write to Parquet
                parquet_buffer = io.BytesIO()
                df.to_parquet(parquet_buffer, index=False)
                parquet_buffer.seek(0)

                # Upload to GCS
                parquet_blob_name = blob.name.rsplit(".", 1)[0] + ".parquet"
                bucket.blob(parquet_blob_name).upload_from_file(parquet_buffer, content_type="application/octet-stream")
                print(f"✅ Uploaded: {parquet_blob_name}")

            except Exception as e:
                print(f"❌ Failed on {blob.name}: {e}")

if __name__ == "__main__":
    convert_all_csvs_to_parquet()
