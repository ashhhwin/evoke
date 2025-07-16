from pathlib import Path
from datetime import date, datetime
import pandas as pd
import pandas_market_calendars as mcal
from concurrent.futures import ThreadPoolExecutor
from script import (
    load_tickers,
    run_finnhub_data_pipeline,
    run_daily_bulk_download,
    detect_eps_revenue_changes,
    run_historical_bulk_download,
    load_master_tickers
)
from google.cloud import secretmanager
from google.cloud import storage
import io
import os
import gcsfs # Anu's class
GCS_BUCKET = "historical_data_evoke" 

PROGRESS_LOG = Path("market_data/progress.log")

def log_progress(message: str):
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)
    blob = bucket.blob(str(PROGRESS_LOG))
    if blob.exists():
        current_log = blob.download_as_text()
    else:
        current_log = ""
    updated_log = current_log + f"{message}\n"
    blob.upload_from_string(updated_log, content_type="text/plain")
##### HELPERS    
def upload_dataframe_to_gcs(df: pd.DataFrame, blob_path: str):
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)
    blob = bucket.blob(blob_path)
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    blob.upload_from_string(csv_buffer.getvalue(), content_type="text/csv")

def read_csv_from_gcs(blob_path: str) -> pd.DataFrame:
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)
    blob = bucket.blob(blob_path)
    content = blob.download_as_text()
    return pd.read_csv(io.StringIO(content),infer_datetime_format=True,keep_default_na = False,na_values=[''])

def get_latest_transformed_folder():
    client = storage.Client()
    blobs = client.list_blobs(GCS_BUCKET, prefix="market_data/daily/")
    dates = sorted({blob.name.split("/")[2] for blob in blobs if len(blob.name.split("/")) > 2}, reverse=True)
    for date_str in dates:
        prefix = f"market_data/daily/{date_str}/FINNHUB/transformed/"
        files = list(client.list_blobs(GCS_BUCKET, prefix=prefix))
        if any("eps_transformed" in b.name or "revenue_transformed" in b.name for b in files):
            return prefix
    raise FileNotFoundError("No valid transformed EPS/Revenue folder found.")

def load_eps_and_revenue_data():
    try:
        folder_prefix = get_latest_transformed_folder()
        client = storage.Client()
        blobs = list(client.list_blobs(GCS_BUCKET, prefix=folder_prefix))
        eps_blob = next((b.name for b in blobs if "eps_transformed_" in b.name and b.name.endswith(".csv")),None)
        rev_blob = next((b.name for b in blobs if "revenue_transformed_" in b.name and b.name.endswith(".csv")),None)
        if not eps_blob or not rev_blob:
            raise FileNotFoundError("EPS or Revenue CSV file not found in GCS folder.")
        eps_df = read_csv_from_gcs(eps_blob)
        rev_df = read_csv_from_gcs(rev_blob)
        return eps_df, rev_df
    except Exception as e:
        raise FileNotFoundError(f"Error loading EPS/Revenue data from GCS: {e}")


def load_latest_eodhd_merged(ticker: str) -> pd.Series:
    client = storage.Client()
    blobs = client.list_blobs(GCS_BUCKET, prefix="market_data/daily/")
    dates = sorted({blob.name.split("/")[2] for blob in blobs if len(blob.name.split("/")) > 2}, reverse=True)

    for date_str in dates:
        blob_path = f"market_data/daily/{date_str}/EODHD/eod_us_{date_str}_merged.csv"
        try:
            df = read_csv_from_gcs(blob_path)
            match = df[df["Symbol"].str.upper() == ticker.upper()]
            if not match.empty:
                return match.iloc[0]
        except:
            continue
    raise FileNotFoundError(f"No EODHD data found in GCS for ticker {ticker}")


def get_ticker_data(ticker: str, eps_df: pd.DataFrame, rev_df: pd.DataFrame):
    try:
        eps_row = eps_df[eps_df["ticker"].str.upper() == ticker.upper()].drop(columns=["api_run_date"], errors="ignore")
        rev_row = rev_df[rev_df["ticker"].str.upper() == ticker.upper()].drop(columns=["api_run_date"], errors="ignore")
        if eps_row.empty or rev_row.empty:
            raise ValueError(f"No data found for ticker: {ticker}")
        return eps_row.set_index("ticker").T, rev_row.set_index("ticker").T
    except Exception as e:
        raise ValueError(f"Error getting ticker data: {e}")


def run_finnhub_pipeline():
    try:
        log_progress("Running Finnhub pipeline...")
        try:
            tickers = load_tickers()
        except Exception as e:
            log_progress(f"Finnhub pipeline failed: {e}")
            return f"Failed: {e}"
        run_finnhub_data_pipeline(tickers)
        log_progress("Finnhub pipeline complete.")

        # Automatically run EPS/Revenue change detection
        log_progress("Detecting EPS/Revenue changes...")
        detect_eps_revenue_changes()
        log_progress("EPS/Revenue detection complete.")

    except Exception as e:
        log_progress(f"Finnhub + EPS detection failed: {e}")
        return f"Failed: {e}"

def run_eodhd_pipeline():
    try:
        today = date.today()
        #date_str = today.strftime("%Y-%m-%d")  # Use today's date for the schedule check
        #today = datetime.strptime(date_str, "%Y-%m-%d")
        nyse = mcal.get_calendar('NYSE')
        schedule = nyse.schedule(start_date=today, end_date=today)
        trading_days = schedule.index.date.tolist()

        if today not in trading_days:
            message = f"**Today ({today}) is not a trading day. Skipping EODHD download.**"
            log_progress(message)
            return message  # <-- Return to Gradio

        try:
            tickers = load_tickers()
        except Exception as e:
            log_progress(f"EODHD pipeline failed: {e}")
            return f"Failed: {e}"
        run_daily_bulk_download(tickers)
        log_progress("EODHD pipeline complete.")
        return "EODHD pipeline complete."

    except Exception as e:
        log_progress(f"EODHD failed: {e}")
        return f"EODHD failed: {e}"


def run_historical_pipeline(start: str, end: str):
    try:
        log_progress(f"Running historical download from {start} to {end}...")
        start_date = pd.to_datetime(start).date()
        end_date = pd.to_datetime(end).date()
        try:
            stock_df = load_master_tickers()
        except Exception as e:
            log_progress(f"Historical download failed: {e}")
            return f"Failed: {e}"
        msg = run_historical_bulk_download(start_date, end_date, stock_df)
        log_progress(f"Historical download {start} to {end} complete.")
        return msg  
    except Exception as e:
        log_progress(f"Historical download failed: {e}")
        return f"Failed: {e}"  
'''   
def load_historical_close_prices(ticker: str) -> pd.DataFrame:
    blob_path = "Final_data_1year.csv"
    df = read_csv_from_gcs(blob_path)

    df = df[df["Symbol"].str.upper() == ticker.upper()]
    required_columns = ["Trade_Date", "P_Close", "volume", "Close_to_Close (%)", "V_14D_MA", "V_50D_MA"]
    existing_columns = [col for col in required_columns if col in df.columns]

    if not df.empty and existing_columns:
        return df[existing_columns].dropna().sort_values("Trade_Date")
    else:
        raise ValueError(f"No data found for ticker '{ticker}' in {blob_path}")
'''

def load_historical_close_prices(ticker: str, bucket_name="historical_data_evoke", folder="Final_data_v2") -> pd.DataFrame:
    fs = gcsfs.GCSFileSystem()
    all_files = fs.ls(f"{bucket_name}/{folder}")
    csv_files = [f.replace(f"{bucket_name}/", "") for f in all_files if f.endswith(".csv")]

    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in gs://{bucket_name}/{folder}")

    full_df = pd.concat(
        [read_csv_from_gcs(f) for f in csv_files],
        ignore_index=True
    )

    df = full_df[full_df["Symbol"].str.upper() == ticker.upper()]

    required_columns = ["Trade_Date", "P_Close", "volume", "Close_to_Close (%)", "V_14D_MA", "V_50D_MA"]
    existing_columns = [col for col in required_columns if col in df.columns]

    if not df.empty and existing_columns:
        df["Trade_Date"] = pd.to_datetime(df["Trade_Date"], errors="coerce")
        return df[existing_columns].dropna().sort_values("Trade_Date")
    else:
        raise ValueError(f"No data found for ticker '{ticker}' in any file from {folder}")

    
def load_eps_revenue_changes() -> pd.DataFrame:
    blob_path = "market_data/eps_revenue_changes.csv"
    try:
        return read_csv_from_gcs(blob_path)
    except Exception as e:
        raise FileNotFoundError(f"Error loading EPS/Revenue changes from GCS: {e}")

    
def get_latest_daily_date() -> str:
    client = storage.Client()
    blobs = client.list_blobs(GCS_BUCKET, prefix="market_data/daily/", delimiter="/")
    dates = sorted({blob.name.split("/")[2] for blob in blobs if len(blob.name.split("/")) > 2}, reverse=True)
    if not dates:
        return "No data available"
    return dates[0]


def run_pipelines_concurrently(tickers: list[str]) -> None:
    """
    Run EODHD and Finnhub pipelines concurrently using ThreadPoolExecutor
    """
    if not tickers or len(tickers) == 0:
        raise FileNotFoundError("No tickers found. Please check tickers.txt file.")
    with ThreadPoolExecutor(max_workers=2) as executor:
        # Submit both pipeline tasks
        eodhd_future = executor.submit(run_daily_bulk_download, tickers)
        finnhub_future = executor.submit(run_finnhub_data_pipeline, tickers)
        # Wait for both to complete
        eodhd_future.result()
        finnhub_future.result()

# Helper to fetch secret from Google Secret Manager
# (reuse the same as in script.py, or import if you modularize)
def get_secret(project_id: str, secret_id: str, version_id: str = "latest") -> str:
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

GCP_PROJECT_ID = "tonal-nucleus-464617-n2"
FINNHUB_SECRET_NAME = "finnhub_api_key"
EODHD_SECRET_NAME = "eodhd_api_key"

if __name__ == "__main__":
    run_eodhd_pipeline()
