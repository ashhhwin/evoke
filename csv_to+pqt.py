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

            # Read CSV from GCS
            csv_bytes = blob.download_as_bytes()
            df = pd.read_csv(io.BytesIO(csv_bytes))

            # Convert to Parquet in-memory
            parquet_buffer = io.BytesIO()
            df.to_parquet(parquet_buffer, index=False)
            parquet_buffer.seek(0)

            # Define new Parquet blob name
            parquet_blob_name = blob.name.rsplit(".", 1)[0] + PARQUET_SUFFIX
            parquet_blob = bucket.blob(parquet_blob_name)

            # Upload Parquet file
            parquet_blob.upload_from_file(parquet_buffer, content_type="application/octet-stream")
            print(f"Uploaded: {parquet_blob_name}")

if __name__ == "__main__":
    convert_all_csvs_to_parquet()
