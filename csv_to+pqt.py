from google.cloud import storage
import pandas as pd
import io

BUCKET_NAME = "historical_data_evoke"
PREFIX = "Final_data_v2/"
PARQUET_SUFFIX = ".parquet"

def convert_all_csvs_to_parquet():
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blobs = list(bucket.list_blobs(prefix=PREFIX))

    for blob in blobs:
        if blob.name.endswith(".csv"):
            print(f"Processing: {blob.name}")

            # Read CSV (low_memory=False for better dtype inference)
            csv_bytes = blob.download_as_bytes()
            df = pd.read_csv(io.BytesIO(csv_bytes), low_memory=False)

            # Clean column names
            df.columns = df.columns.str.strip()

            # Make sure required columns exist
            required_cols = ["timestamp", "symbol"]
            if not all(col in df.columns for col in required_cols):
                print(f"Skipping {blob.name}: missing columns")
                continue

            # Convert timestamp column to datetime
            df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

            # Identify and convert only numeric columns
            for col in df.columns:
                if col not in ["timestamp", "symbol"]:  # preserve non-numeric
                    df[col] = pd.to_numeric(df[col], errors="coerce")

            # Drop fully empty rows if any
            df.dropna(how="all", inplace=True)

            # Save as Parquet
            parquet_buffer = io.BytesIO()
            df.to_parquet(parquet_buffer, index=False)
            parquet_buffer.seek(0)

            # Upload to GCS
            parquet_blob_name = blob.name.rsplit(".", 1)[0] + ".parquet"
            bucket.blob(parquet_blob_name).upload_from_file(parquet_buffer, content_type="application/octet-stream")
            print(f"Uploaded: {parquet_blob_name}")
            
if __name__ == "__main__":
    convert_all_csvs_to_parquet()
