from pathlib import Path
from datetime import date , datetime
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
import os

PROGRESS_LOG = Path("market_data/progress.log")

def log_progress(message: str):
    PROGRESS_LOG.parent.mkdir(parents=True, exist_ok=True) 
    PROGRESS_LOG.open("a").write(f"{message}\n")

def get_latest_transformed_folder(base_dir=Path("market_data/daily")):
    all_dates = sorted([d.name for d in base_dir.iterdir() if d.is_dir()], reverse=True)
    for date_str in all_dates:
        tx_dir = base_dir / date_str / "FINNHUB" / "transformed"
        if tx_dir.exists():
            return tx_dir
    raise FileNotFoundError("No valid transformed EPS/Revenue folder found.")

def load_eps_and_revenue_data():
    try:
        tx_dir = get_latest_transformed_folder()
        eps_file = next(tx_dir.glob("eps_transformed_*.csv"), None)
        rev_file = next(tx_dir.glob("revenue_transformed_*.csv"), None)
        if not eps_file or not rev_file:
            raise FileNotFoundError("EPS or Revenue file not found in latest folder.")
        eps_df = pd.read_csv(eps_file)
        rev_df = pd.read_csv(rev_file)
        return eps_df, rev_df
    except Exception as e:
        raise FileNotFoundError(f"Error loading EPS/Revenue data: {e}")

def load_latest_eodhd_merged(ticker: str, base_dir=Path("market_data/daily")) -> pd.Series:
    all_dates = sorted([d.name for d in base_dir.iterdir() if d.is_dir()], reverse=True)
    for date_str in all_dates:
        file_path = base_dir / date_str / "EODHD" / f"eod_us_{date_str}_merged.csv"
        if file_path.exists():
            df = pd.read_csv(file_path)
            match = df[df["Symbol"].str.upper() == ticker.upper()]
            if not match.empty:
                return match.iloc[0]  # Return the first matching row as a Series
    raise FileNotFoundError(f"No EODHD data found for ticker {ticker}")


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
    

def load_historical_close_prices(ticker: str, data_file=Path("market_data/historical/Final/last_1_year_data.csv")) -> pd.DataFrame:
    if not data_file.exists():
        raise FileNotFoundError(f"{data_file} not found.")
    # Read safely: keep 'NA' symbol intact, treat empty strings as NaN
    df = pd.read_csv(
        data_file,
        parse_dates=["Trade_Date"],
        dayfirst=False,
        infer_datetime_format=True,
        keep_default_na=False,
        na_values=[""]
    )
    # Filter for the specific ticker (case-insensitive)
    df = df[df["Symbol"].str.upper() == ticker.upper()]
    # Keep only required columns if available
    required_columns = ["Trade_Date", "P_Close", "volume", "Close_to_Close (%)", "V_14D_MA", "V_50D_MA"]
    existing_columns = [col for col in required_columns if col in df.columns]
    if not df.empty and existing_columns:
        return df[existing_columns].dropna().sort_values("Trade_Date")
    else:
        raise ValueError(f"No data found for ticker '{ticker}' in {data_file}")
    
def load_eps_revenue_changes(path=Path("market_data/eps_revenue_cha8nges.csv")) -> pd.DataFrame:
    try:
        if path.exists():
            return pd.read_csv(path)
        else:
            raise FileNotFoundError("eps_revenue_changes.csv not found.")
    except Exception as e:
        raise FileNotFoundError(f"Error loading EPS/Revenue changes: {e}")
    
def get_latest_daily_date(base_dir=Path("market_data/daily")) -> str:
    all_dates = sorted([d.name for d in base_dir.iterdir() if d.is_dir()], reverse=True)
    if not all_dates:
        return "No data available"
    return all_dates[0]

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