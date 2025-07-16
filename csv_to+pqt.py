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
                csv_bytes = blob.download_as_bytes()
                
                # Step 1: Read TSV
                df = pd.read_csv(io.BytesIO(csv_bytes), sep="\t", low_memory=False, dtype=str)
                df.columns = df.columns.str.strip()

                # Step 2: Drop any rows that repeat header (like last row)
                df = df[df["Trade_Date"] != "Trade_Date"]

                # Step 3: Strip whitespace and reset dtypes
                df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

                # Step 4: Rename and convert date
                df.rename(columns={"Trade_Date": "timestamp"}, inplace=True)
                df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

                # Step 5: Coerce numerics (but keep important string columns)
                non_numeric_cols = ["timestamp", "Symbol", "Company_Name", "Type", "Sector", "Industry", "Earnings_Date"]
                for col in df.columns:
                    if col not in non_numeric_cols:
                        df[col] = pd.to_numeric(df[col], errors="coerce")

                df.dropna(subset=["timestamp", "Symbol"], inplace=True)

                # Step 6: Save as Parquet
                parquet_buffer = io.BytesIO()
                df.to_parquet(parquet_buffer, index=False)
                parquet_buffer.seek(0)

                parquet_name = blob.name.replace(".csv", ".parquet")
                bucket.blob(parquet_name).upload_from_file(parquet_buffer, content_type="application/octet-stream")
                print(f"✅ Uploaded: {parquet_name}")

            except Exception as e:
                print(f"❌ Failed to process {blob.name}: {e}")

if __name__ == "__main__":
    convert_all_csvs_to_parquet()
