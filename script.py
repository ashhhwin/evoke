# This script is intended to be run as a cron job every day at 8pm.
# Example crontab entry (edit with `crontab -e`):
# 0 20 * * * /usr/bin/python3 /path/to/GCP-SCRIPT/script.py >> /path/to/GCP-SCRIPT/cron.log 2>&1

from __future__ import annotations


import gcsfs
import os, time, shutil, logging, requests, json
from datetime import date, timedelta, datetime
from pathlib import Path
from typing import List, Dict, Callable, Optional
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage
import io
import pandas as pd
import pandas_market_calendars as mcal
import finnhub as fb
from tqdm import tqdm
from dotenv import load_dotenv
import glob
from google.cloud import secretmanager, storage
import tempfile



# ──────────────────────────────────────────────────────────────────────────────
# Environment / logging
# ──────────────────────────────────────────────────────────────────────────────
# load_dotenv()  # read .env if present

import os

# Helper to fetch secret from Google Secret Manager
def get_secret(project_id: str, secret_id: str, version_id: str = "latest") -> str:
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

# Set your GCP project ID and secret names here or via environment variables
GCP_PROJECT_ID = "tonal-nucleus-464617-n2"
FINNHUB_SECRET_NAME = "finnhub_api_key"
EODHD_SECRET_NAME = "eodhd_api_key"

# Fetch API keys from Google Secret Manager
FINNHUB_API_KEY = get_secret(GCP_PROJECT_ID, FINNHUB_SECRET_NAME)
EOD_API_TOKEN = get_secret(GCP_PROJECT_ID, EODHD_SECRET_NAME)

BASE_EOD_URL    = "https://eodhd.com/api/eod-bulk-last-day/US"
URL_FUNDAMENTAL = "https://eodhd.com/api/fundamentals"
DATA_DIR        = Path("market_data")
RATE_LIMIT_SEC   = 1


start_date = date(2025, 1, 1)
end_date = date(2025, 12, 31)
YEARS_OF_HISTORY = 5

GCS_BUCKET = "historical_data_evoke"
PROGRESS_LOG = "market_data/progress.log"
BUCKET_NAME = "historical_data_evoke"

def gcs_path(path: str) -> str:
    return f"market_data/{path}" if not path.startswith("market_data/") else path

def upload_to_gcs(bucket_name: str, destination_blob_name: str, source_file: str):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file)

def upload_string_to_gcs(bucket_name: str, destination_blob_name: str, data: str):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_string(data)

def upload_dataframe_to_gcs(df: pd.DataFrame, blob_path: str):
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(blob_path)
    buffer = io.StringIO()
    df.to_csv(buffer, index=False)
    blob.upload_from_string(buffer.getvalue(), content_type="text/csv")

def read_csv_from_gcs(blob_path: str) -> pd.DataFrame:
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(blob_path)
    content = blob.download_as_text()
    return pd.read_csv(io.StringIO(content), keep_default_na=False, na_values=[""])

def log_progress(msg: str):
    log_path = "market_data/progress.log"
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(log_path)
    try:
        current_log = blob.download_as_text() if blob.exists() else ""
    except:
        current_log = ""
    updated_log = current_log + f"{msg}\n"
    blob.upload_from_string(updated_log)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def get_previous_trading_day(today: date) -> Optional[date]:
    nyse = mcal.get_calendar("NYSE")
    sched = nyse.schedule(start_date=today - timedelta(days=10), end_date=today)
    days  = sched.index.date.tolist()
    if today in days:
        days.pop()
    return days[-1] if days else None
    
def process_chunk(chunk):
    if 'MarketCapitalization' in chunk.columns:
        chunk['MarketCapitalization'] = pd.to_numeric(chunk['MarketCapitalization'], errors='coerce').round(0).astype('Int64')
    if 'volume' in chunk.columns:
        chunk['volume'] = pd.to_numeric(chunk['volume'], errors='coerce').round(0).astype('Int64')
    exclude = ['MarketCapitalization', 'volume']
    numeric_cols = chunk.select_dtypes(include='number').columns
    to_round = [col for col in numeric_cols if col not in exclude]
    chunk[to_round] = chunk[to_round].round(2)
    return chunk
    
def format_data(df: pd.DataFrame) -> pd.DataFrame:
    column_map = {
        "date": "Trade_Date",
        "Company Name_x": "Company_Name",
        "Company Name_y": "Company_Name",
        "open": "P_Open",
        "high": "P_High",
        "low": "P_Low",
        "close": "P_Close",
        "prev_close": "Prev_Close (Price)",
        "ema_50d": "P_50D_MA",
        "ema_200d": "P_200D_MA",
        "avgvol_14d": "V_14D_MA",
        "avgvol_50d": "V_50D_MA"
    }

    df = df.rename(columns=column_map)

    final_cols = [
        "Trade_Date", "Symbol", "Company_Name", "Type","Sector", "Industry", "MarketCapitalization", "Beta",
        "P_Open", "P_High", "P_Low", "P_Close", "volume", "Prev_Close (Price)",
        "P_50D_MA", "P_200D_MA", "V_14D_MA", "V_50D_MA", "Options", "hi_250d", "lo_250d",
        "Close_to_Open (% from Prev Day Close)", "Open_Close (%)", "High_Close(%)", "Low_Close(%)",
        "Close_to_Close (%)", "Shares_Out", "Shares_Float", "Short_Ratio", "Short_Percent_Float",
        "Earnings_Date", "Shares_Insiders", "Shares_Institutions"
    ]

   
    for col in final_cols:
        if col not in df.columns:
            df[col] = pd.NA

    # Return DataFrame with all final columns in order
    return df[final_cols]



def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    rename = {"code": "Symbol", "name": "Company Name"}
    df.rename(columns={k: v for k, v in rename.items() if k in df.columns}, inplace=True)
    return df

# Simple retry wrapper for GET calls

def fetch_json(url: str, timeout: int = 30, retries: int = 3, wait: int = 3):
    for attempt in range(retries):
        try:
            r = requests.get(url, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.RequestException as e:
            if attempt == retries - 1:
                raise
            logger.warning("Retrying %s (%s)…", url, e)
            time.sleep(wait * (attempt + 1))

# ──────────────────────────────────────────────────────────────────────────────
# Finnhub pipeline
# ──────────────────────────────────────────────────────────────────────────────

def get_finnhub_df(client, func: Callable, ticker: str, freq: str) -> pd.DataFrame:
    try:
        data = func(ticker, freq=freq) or {}
        return pd.DataFrame(data.get("data", []))
    except Exception as e:
        logger.error("Finnhub error %s / %s : %s", ticker, func.__name__, e)
        return pd.DataFrame()
        
def get_finnhub_news(client, func: Callable, ticker: str, from_date: datetime, to_date: datetime ):
    try:
        data = func(ticker,from_date,to_date) or {}
        return data
    except Exception as e:
        logger.error("Finnhub error %s / %s : %s", ticker, func.__name__, e)
        return {}

def run_finnhub_data_pipeline(tickers: List[str]):
    today_iso = date.today().isoformat()
    raw_dir = f"daily/{today_iso}/FINNHUB/raw_data"
    tx_dir = f"daily/{today_iso}/FINNHUB/transformed"
    news_dir =f"news/{today_iso}"
    #Path(news_dir).mkdir(parents=True, exist_ok=True)
    
    client = fb.Client(api_key=FINNHUB_API_KEY)

    funcs: Dict[str, Callable[[str], pd.DataFrame]] = {
        "revenue_estimates_quarterly": lambda t: get_finnhub_df(client, client.company_revenue_estimates, t, "quarterly"),
        "revenue_estimates_annual":    lambda t: get_finnhub_df(client, client.company_revenue_estimates, t, "annual"),
        "eps_estimates_quarterly":     lambda t: get_finnhub_df(client, client.company_eps_estimates,     t, "quarterly"),
        "eps_estimates_annual":        lambda t: get_finnhub_df(client, client.company_eps_estimates,     t, "annual"),
        "news_data":                   lambda t: get_finnhub_news(client, client.company_news,t,today_iso,today_iso),
    }

    collected: Dict[str, List[pd.DataFrame]] = {k: [] for k in funcs if k != "news_data"}
    for i, tk in enumerate(tqdm(tickers, desc="Finnhub", unit="ticker")):
        log_progress(f"[{i+1}/{len(tickers)}] Fetching from Finnhub: {tk}")
        for name, fn in funcs.items():
            try: 
                 df = fn(tk)
                 if name == "news_data":          
                        json_str = json.dumps(df, indent=2)
                        gcs_dest = gcs_path(f"{news_dir}/{tk}.json")
                        upload_string_to_gcs("historical_data_evoke", gcs_dest, json_str)          
                 else:
                        if not df.empty:
                            df.insert(0, "ticker", tk)
                            df.insert(1, "api_run_date", today_iso)
                            collected[name].append(df)
            except Exception as e:
                log_progress(f"[{i+1}/{len(tickers)}] ERROR {name} for {tk}: {e}")
        time.sleep(RATE_LIMIT_SEC)
    
    # Save raw CSVs to GCS
    for name, lst in collected.items():
        if lst:
            full_df = pd.concat(lst, ignore_index=True)
            upload_dataframe_to_gcs(full_df, gcs_path(f"{raw_dir}/{name}.csv"))
            
    # Transform helper
    def _pivot(df_q, df_y, value_col, out_name):
        START_YEAR = date.today().year - YEARS_OF_HISTORY
        fmt_quarter = lambda r: None if pd.isna(r["quarter"]) else f"Q{int(r['quarter'])}-{str(int(r['year']))[-2:]}" if r["year"] >= START_YEAR else None
        df_q["col"] = df_q.apply(fmt_quarter, axis=1)
        df_q = df_q[df_q["col"].notna()].drop_duplicates(["ticker", "col"])
        df_y = df_y[df_y["year"] >= START_YEAR]
        df_y["col"] = df_y["year"].astype(str)
        df_y = df_y.drop_duplicates(["ticker", "col"])
        pv = pd.concat([
            df_q.pivot(index="ticker", columns="col", values=value_col),
            df_y.pivot(index="ticker", columns="col", values=value_col)
        ], axis=1)
        sorter = lambda c: (int(c[1:].split("-")[1]), int(c[1]) ) if c.startswith("Q") else (int(c), 0)
        final = pv.loc[:, sorted(pv.columns, key=sorter)].reset_index()
        final.insert(1, "api_run_date", today_iso)
        # If this is a revenue transformation, divide all period columns by 1e6
        if "revenue" in out_name:
            num_cols = [col for col in final.columns if col not in ["ticker", "api_run_date"]]
            final[num_cols] = final[num_cols].astype(float) / 1e6
        upload_dataframe_to_gcs(final, gcs_path(f"{tx_dir}/{out_name}"))

    # Revenue transform
    try:
        df_q = read_csv_from_gcs(gcs_path(f"{raw_dir}/revenue_estimates_quarterly.csv"))
        df_y = read_csv_from_gcs(gcs_path(f"{raw_dir}/revenue_estimates_annual.csv"))
        _pivot(df_q, df_y, "revenueAvg", f"revenue_transformed_{today_iso}.csv")
    except Exception as e:
        log_progress(f"Revenue transform failed: {e}")

    # EPS transform
    try:
        df_q = read_csv_from_gcs(gcs_path(f"{raw_dir}/eps_estimates_quarterly.csv"))
        df_y = read_csv_from_gcs(gcs_path(f"{raw_dir}/eps_estimates_annual.csv"))
        _pivot(df_q, df_y, "epsAvg", f"eps_transformed_{today_iso}.csv")
    except Exception as e:
        log_progress(f"EPS transform failed: {e}")
# ──────────────────────────────────────────────────────────────────────────────
# EODHD daily download
# ──────────────────────────────────────────────────────────────────────────────
def append_daily_chunk_to_latest(daily_chunk, date_column, bucket_name, final_data_folder, base_name, max_rows, project=None):

    fs = gcsfs.GCSFileSystem(project=project)
    client = storage.Client(project=project)
    bucket = client.bucket(bucket_name)

    def get_latest_file(fs, folder_path, prefix="eodhd_", suffix=".csv"):
        files = fs.ls(folder_path)
        csv_files = [f for f in files if f.endswith(suffix) and prefix in f]
        return sorted(csv_files)[-1] if csv_files else None

    latest_path = get_latest_file(fs, f"{bucket_name}/{final_data_folder}", prefix=base_name)
    if not latest_path:
        raise ValueError("No existing file found in Final_data_v2!")

    with fs.open(latest_path, 'r') as f:
        master_df = pd.read_csv(f, low_memory=False)
        master_df[date_column] = pd.to_datetime(master_df[date_column], errors='coerce')
        original_df = master_df.copy()

    # Format daily chunk
    daily_chunk[date_column] = pd.to_datetime(daily_chunk[date_column], errors="coerce")
    master_df = pd.concat([master_df, daily_chunk], ignore_index=True)
    master_df.sort_values(by=date_column, inplace=True)
    master_df.reset_index(drop=True, inplace=True)

    if len(master_df) == len(original_df):
        print("⚠️ No new data appended. Skipping upload to avoid overwriting.")
        return


    while not master_df.empty:
        chunk = master_df.iloc[:max_rows]
        master_df = master_df.iloc[max_rows:]

        min_date = chunk[date_column].min().strftime('%Y%m%d')
        max_date = chunk[date_column].max().strftime('%Y%m%d')
        filename = f"{base_name}{min_date}_to{max_date}.csv"

        local_path = f"/tmp/{filename}"
        chunk.to_csv(local_path, index=False)

        destination_path = f"{final_data_folder}/{filename}"
        blob = bucket.blob(destination_path)
        blob.upload_from_filename(local_path)
        print(f"📤 Uploaded: gs://{bucket_name}/{destination_path} ({len(chunk)} rows)")
        os.remove(local_path)


def run_daily_bulk_download(tickers: List[str]):
    
    date_str = "2025-07-23"
    today = datetime.strptime(date_str, "%Y-%m-%d").date()
    #today= date.today()
    #date_str = today.isoformat()
    nyse = mcal.get_calendar('NYSE')
    schedule = nyse.schedule(start_date=today, end_date=today)
    trading_days = schedule.index.date.tolist()
    if today not in trading_days:
        message = f"**Today ({today}) is not a trading day. Skipping EODHD download.**"
        log_progress(message)
        return message
        
    raw_folder = f"daily/{date_str}/EODHD/raw_jsons"
    base_folder = f"daily/{date_str}/EODHD"
    log_lines: List[str] = []
    stock9k = load_master_tickers()
    # Download bulk JSON
    bulk_json_gcs_path = gcs_path(f"{raw_folder}/eod_us_{date_str}.json")
    fundamentals_json_gcs_path = gcs_path(f"{base_folder}/fundamentals_{date_str}.json")
    merged_csv_gcs_path = gcs_path(f"{base_folder}/eod_us_{date_str}_merged.csv")
    try:
        logger.info("[EODHD] Downloading bulk for %s…", date_str)
        js = fetch_json(
            f"{BASE_EOD_URL}?api_token={EOD_API_TOKEN}&date={date_str}&fmt=json&filter=extended",
            timeout=60
        )
        upload_string_to_gcs(BUCKET_NAME, bulk_json_gcs_path, json.dumps(js))
    except Exception as e:
        logger.exception("Bulk download failed")
        return

    df = normalize_columns(pd.DataFrame(js))
    df = df.merge(stock9k, on='Symbol', how='right')
    df = df.drop(columns=['Company Name_x'], errors='ignore')

    if "MarketCapitalization" in df.columns:
        df["MarketCapitalization"] = (pd.to_numeric(df["MarketCapitalization"], errors="coerce") / 1e6).round(2)
    
    df['date'] = date_str
    df["date"] = pd.to_datetime(df["date"])
    #df["Close_to_Close (%)"]=0.0
    #df["Close_to_Open (% from Prev Day Close)"]=0.0
    # Merge with previous day's close and volume
    # ashwin making changes to fix previous close price
    # (skip prev_path.exists() logic for GCS version for now)
    prev_day = get_previous_trading_day(today)
    if prev_day:
        prev_str = prev_day.isoformat()
        prev_blob_path = gcs_path(f"daily/{prev_str}/EODHD/eod_us_{prev_str}_merged.csv")
    try:
        prev_df = read_csv_from_gcs(prev_blob_path)[["Symbol", "P_Close"]]
        prev_df.rename(columns={"P_Close": "prev_close"}, inplace=True)
        df = df.merge(prev_df, on="Symbol", how="left")
        df["Close_to_Close (%)"] = (((df["close"] - df["prev_close"]) / df["prev_close"]) * 100).round(2)
        df["Close_to_Open (% from Prev Day Close)"] = (((df["open"] - df["prev_close"]) / df["prev_close"]) * 100).round(2)
    except Exception as e:
        log_progress(f"[WARNING] Failed to merge previous day data from GCS: {e}")
        df["prev_close"] = pd.NA
        df["Close_to_Close (%)"] = pd.NA
        df["% Prev Close to Open"] = pd.NA
        
    
    # end of ashwin changes
    # Intraday ratios
    df["High_Close(%)"] = ((df["high"] - df["close"]) / df["close"] * 100).round(2)
    df["Low_Close(%)"]  = ((df["low"]  - df["close"]) / df["close"] * 100).round(2)
    df["Open_Close (%)"] = ((df["close"] - df["open"]) / df["open"] * 100).round(2)
    
    # Fundamentals enrichment
    extra_rows = []
    fundamentals_json_gcs_path = gcs_path(f"{base_folder}/fundamentals_{date_str}.json")
    fundamentals_json_list = []
    for i, tk in enumerate(tqdm(tickers, desc="Fundamentals", unit="tk")):
        try:
            log_progress(f"[{i+1}/{len(tickers)}] Fetching fundamentals for: {tk}")
            f_json = fetch_json(
                f"{URL_FUNDAMENTAL}/{tk}?filter=General::Code,SharesStats,Technicals,Earnings::Trend&api_token={EOD_API_TOKEN}&fmt=json",
                timeout=30
            )
            earnings_trend = f_json.get("Earnings::Trend", {})
            future_dates = []
            for k, v in earnings_trend.items():
                try:
                    rep_date = datetime.strptime(k, "%Y-%m-%d").date()
                    if rep_date > today:
                        future_dates.append(rep_date)
                except:
                    continue
            next_earnings_date = min(future_dates).isoformat() if future_dates else None
            extra_rows.append({
                "Symbol": f_json.get("General::Code", tk),
                "Shares_Out": f_json.get("SharesStats", {}).get("SharesOutstanding"),
                "Shares_Float": f_json.get("SharesStats", {}).get("SharesFloat"),
                "Shares_Insiders": f_json.get("SharesStats", {}).get("PercentInsiders"),
                "Shares_Institutions": f_json.get("SharesStats", {}).get("PercentInstitutions"),
                "Short_Ratio": f_json.get("Technicals", {}).get("ShortRatio"),
                "Short_Percent_Float": f_json.get("Technicals", {}).get("ShortPercent"),
                "Earnings_Date": next_earnings_date
            })
            f_json["Symbol"] = tk
            fundamentals_json_list.append(f_json)
        except Exception as e:
            log_progress(f"[ERROR] fundamentals {tk}: {e}")
        time.sleep(0.6)

    # Upload full fundamentals JSON
    upload_string_to_gcs(BUCKET_NAME, fundamentals_json_gcs_path, json.dumps(fundamentals_json_list, indent=2))

    # Merge enriched fundamentals
    if extra_rows:
        df = df.merge(pd.DataFrame(extra_rows), on="Symbol", how="left")
        df['Shares_Out'] = (df['Shares_Out'] / 1e6).round(2)
        df['Shares_Float'] = (df['Shares_Float'] / 1e6).round(2)
        logger.info(f"Final DataFrame shape before enrichment: {df.shape}")

    for col in df.columns:
        if col not in ["Symbol", "date", "MarketCapCategory"]:
            df[col] = pd.to_numeric(df[col], errors="ignore")

    final_df = format_data(df)
    upload_dataframe_to_gcs(final_df, merged_csv_gcs_path)
    log_progress(f"✅ EODHD data pipeline complete for {date_str}")
    log_progress(f" Merging {date_str} with the historical data")
    final_df = process_chunk(final_df)
    append_daily_chunk_to_latest(daily_chunk=final_df,date_column="Trade_Date",bucket_name="historical_data_evoke",final_data_folder="Final_data_v2",base_name="eodhd_",max_rows=1_000_000)
    log_progress(f"✅ EODHD data pipeline completed mergeing for {date_str}")


# ──────────────────────────────────────────────────────────────────────────────
# Convenience loader
# ──────────────────────────────────────────────────────────────────────────────

def load_tickers(path: str = None, limit: int | None = None) -> List[str]:
    # Only look in project root
    paths_to_try = [
        path,
        "./tickers.txt"
    ] if path else ["./tickers.txt"]
    for p in paths_to_try:
        if p and Path(p).exists():
            tickers = [l.strip() for l in Path(p).read_text().splitlines() if l.strip()]
            return tickers[:limit] if limit else tickers
    raise FileNotFoundError("Could not find tickers.txt in project root.")

# ──────────────────────────────────────────────────────────────────────────────
# Historical data collection
# ──────────────────────────────────────────────────────────────────────────────

def run_historical_bulk_download(start_date: date, end_date: date , stock9k: pd.DataFrame):
    """Collect historical data for the given date range."""

    nyse = mcal.get_calendar('NYSE')
    trading_days = nyse.schedule(start_date=start_date, end_date=end_date).index.date.tolist()
      
    if not trading_days:
        msg = "No trading days found in the specified date range."
        logger.warning(msg)
        return msg  


    # Create historical folder with date range in name (format: MM-DD-YY to MM-DD-YY)
    folder_name = f"{start_date.strftime('%m-%d-%y')} to {end_date.strftime('%m-%d-%y')}"
    hist_prefix = f"historical/{folder_name}"
    hist_folder = DATA_DIR / "historical" / folder_name
    hist_folder.mkdir(parents=True, exist_ok=True)
    
    log_lines = []
    all_data = []  # List to store all daily dataframes

    for current in trading_days:
        date_str = current.strftime('%m-%d-%y')  # Format: MM-DD-YY
        url = f"{BASE_EOD_URL}?api_token={EOD_API_TOKEN}&date={current.isoformat()}&fmt=json&filter=extended"
        
        try:
            logger.info(f"Downloading data for {date_str}...")
            js = fetch_json(url, timeout=60)
            
            if not js:
                logger.warning(f"No data found for {date_str}")
                continue
                
            logger.info(f"Processing {len(js)} records for {date_str}")
            
            #for entry in js:
              #  entry['date'] = current.isoformat()
            
            df = normalize_columns(pd.DataFrame(js))
            df = df.merge(stock9k,on='Symbol', how='right')

            df = df.drop(columns=['Company Name_x'], errors='ignore')
            if "MarketCapitalization" in df.columns:
                df["MarketCapitalization"] = pd.to_numeric(df["MarketCapitalization"], errors="coerce") / 1e6
                df["MarketCapitalization"] = df["MarketCapitalization"].round(2)
            logger.info(f"Calculating metrics for {date_str}")
            df['date'] = current.isoformat()
            df['date'] = pd.to_datetime(df['date'])

            # Calculate daily metrics
            df["High_Close(%)"] = (df["high"] - df["close"]) / df["close"] * 100
            df["Low_Close(%)"]  = (df["low"]  - df["close"]) / df["close"] * 100
            df["Open_Close (%)"] = (df["close"] - df["open"]) / df["open"] * 100

            # Ensure all numeric columns are stored as numbers (not strings)
            for col in df.columns:
                if col not in ["Symbol", "date", "MarketCapCategory"]:
                    df[col] = pd.to_numeric(df[col], errors="ignore")

            # Save individual daily file
            output_file = hist_folder / f"{date_str}.csv"
            df = format_data(df)  # Apply final formatting
            #df.to_csv(output_file, index=False)
            upload_dataframe_to_gcs(df, gcs_path(f"{hist_prefix}/{date_str}.csv"))
            logger.info(f"Saved data for {date_str} to {output_file}")
            
            # Add to list of all data
            all_data.append(df)
            
            time.sleep(RATE_LIMIT_SEC)

        except Exception as e:
            error_msg = f"[ERROR] Exception on {date_str}: {e}"
            logger.error(error_msg)
            log_lines.append(error_msg)

    # Create and save merged file if we have data
    if all_data:
        try:
            # Combine all data
            merged_df = pd.concat(all_data, ignore_index=True)
            
            # Sort by Symbol and Trade_Date to ensure correct order for calculations
            merged_df = merged_df.sort_values(['Symbol', 'Trade_Date'])

            # Calculate percentage changes using shift within each Symbol group
            merged_df['prev_close'] = merged_df.groupby('Symbol')['P_Close'].shift(1)
            merged_df['prev_volume'] = merged_df.groupby('Symbol')['volume'].shift(1)
            
            # Calculate percentage changes
            merged_df['Close_to_Close (%)'] = (((merged_df['P_Close'] - merged_df['prev_close']) / merged_df['prev_close']) * 100).round(3)
            merged_df['Close_to_Open (% from Prev Day Close)'] = (((merged_df['P_Open'] - merged_df['prev_close']) / merged_df['prev_close']) * 100).round(3)
            #merged_df['% Volume Change'] = (((merged_df['volume'] - merged_df['prev_volume']) / merged_df['prev_volume']) * 100).round(3)
            

            # Ensure all numeric columns are stored as numbers (not strings)
            for col in merged_df.columns:
                if col not in ["Symbol", "Trade_Date", "MarketCapCategory"]:
                    merged_df[col] = pd.to_numeric(merged_df[col], errors="ignore")

            merged_path = gcs_path(f"{hist_prefix}/{folder_name}.csv")
            tableau_path = gcs_path("historical/for_tableau_db.csv")
            # Save merged file
            merged_file = hist_folder / f"{start_date.strftime('%m-%d-%y')} to {end_date.strftime('%m-%d-%y')}.csv"
            merged_df = format_data(merged_df)  # Apply final formatting
            #merged_df.to_csv(merged_file, index=False)
            upload_dataframe_to_gcs(merged_df, merged_path)
            upload_dataframe_to_gcs(merged_df, tableau_path)
            #logger.info(f"Saved merged data to {merged_file}")
            log_progress(f"✅ Uploaded merged historical data for {folder_name}")
            # Also save/overwrite for_tableau_db.csv in historical/
            #tableau_path = DATA_DIR / "historical" / "for_tableau_db.csv"
            #merged_df.to_csv(tableau_path, index=False)
            #logger.info(f"Saved Tableau-ready data to {tableau_path}")
        except Exception as e:
            error_msg = f"[ERROR] Failed to create merged file: {e}"
            logger.error(error_msg)
            log_lines.append(error_msg)

    if log_lines:
        log_blob = gcs_path(f"{hist_prefix}/log.txt")
        upload_string_to_gcs(BUCKET_NAME, log_blob, "\n".join(log_lines))
        log_progress(f"Historical data finished with some errors (logged to {log_blob})")
    else:
        log_progress(f"Historical data from {start_date} to {end_date} downloaded successfully.")
        return f"Historical data from {start_date} to {end_date} downloaded successfully."

# ──────────────────────────────────────────────────────────────────────────────
# TICKER WISE EPS/REVENUE LOADER
# ──────────────────────────────────────────────────────────────────────────────
def get_latest_transformed_folder():
    client = storage.Client()
    blobs = list(client.list_blobs(BUCKET_NAME, prefix="market_data/daily/"))
    date_folders = sorted({b.name.split("/")[2] for b in blobs if "daily/" in b.name and len(b.name.split("/")) > 2}, reverse=True)
    for date in date_folders:
        prefix = f"market_data/daily/{date}/FINNHUB/transformed/"
        files = list(client.list_blobs(BUCKET_NAME, prefix=prefix))
        if any("eps_transformed" in b.name for b in files):
            return prefix
    raise FileNotFoundError("No valid transformed EPS/Revenue folder found in GCS.")

def load_eps_and_revenue_data():
    folder_prefix = get_latest_transformed_folder()
    client = storage.Client()
    blobs = list(client.list_blobs(BUCKET_NAME, prefix=folder_prefix))

    eps_blob = next((b.name for b in blobs if "eps_transformed" in b.name), None)
    rev_blob = next((b.name for b in blobs if "revenue_transformed" in b.name), None)

    if not eps_blob or not rev_blob:
        raise FileNotFoundError("EPS or Revenue file not found in latest GCS folder.")

    eps_df = read_csv_from_gcs(eps_blob)
    rev_df = read_csv_from_gcs(rev_blob)
    return eps_df, rev_df


def get_ticker_data(ticker: str, eps_df: pd.DataFrame, rev_df: pd.DataFrame):
    eps_row = eps_df[eps_df["ticker"] == ticker].drop(columns=["api_run_date"], errors="ignore")
    rev_row = rev_df[rev_df["ticker"] == ticker].drop(columns=["api_run_date"], errors="ignore")
    if eps_row.empty or rev_row.empty:
        raise ValueError(f"Data not found for ticker: {ticker}")
    return eps_row.set_index("ticker").T, rev_row.set_index("ticker").T  # transpose for plotting

# ──────────────────────────────────────────────────────────────────────────────
# CRON JOB STATS
# ──────────────────────────────────────────────────────────────────────────────

def load_cron_stats():
    """Load cron job statistics from log file"""
    cron_log = DATA_DIR / "cron_log.json"
    if cron_log.exists():
        with open(cron_log) as f:
            return json.load(f)
    return {
        "total_runs": 0,
        "successful_runs": 0,
        "failed_runs": 0,
        "last_run": None,
        "next_run": None,
        "history": []
    }

def save_cron_stats(stats):
    """Save cron job statistics to log file"""
    cron_log = DATA_DIR / "cron_log.json"
    cron_log.parent.mkdir(parents=True, exist_ok=True)
    with open(cron_log, 'w') as f:
        json.dump(stats, f, indent=2)

def update_cron_stats(success: bool, error_msg: str = None):
    """Update cron job statistics"""
    stats = load_cron_stats()
    now = datetime.now().isoformat()
    
    # Update counts
    stats["total_runs"] += 1
    if success:
        stats["successful_runs"] += 1
    else:
        stats["failed_runs"] += 1
    
    # Update last run
    stats["last_run"] = now
    
    # Calculate next run (assuming daily at 4 PM)
    next_run = datetime.now().replace(hour=16, minute=0, second=0, microsecond=0)
    if next_run < datetime.now():
        next_run += timedelta(days=1)
    stats["next_run"] = next_run.isoformat()
    
    # Add to history
    stats["history"].append({
        "timestamp": now,
        "success": success,
        "error": error_msg
    })
    
    # Keep only last 100 history entries
    stats["history"] = stats["history"][-100:]
    
    save_cron_stats(stats)

def detect_eps_revenue_changes():
    """
    Compare the two most recent Finnhub transformed EPS and revenue files and output changes.
    Appends changes to: market_data/eps_revenue_changes.csv
    Appends a summary to: market_data/eps_revenue_changes_summary.csv
    Only logs changes if values differ at 5 decimal places or more, and stores rounded values as numbers. Only stores change_date (not timestamp).
    """
    finnhub_dir = DATA_DIR / "daily"
    all_dates = sorted([d.name for d in finnhub_dir.iterdir() if d.is_dir()])
    if len(all_dates) < 2:
        logger.warning("Not enough daily folders to compare changes.")
        return
    prev_date, curr_date = all_dates[-2], all_dates[-1]
    prev_tx = finnhub_dir / prev_date / "FINNHUB" / "transformed"
    curr_tx = finnhub_dir / curr_date / "FINNHUB" / "transformed"

    changes = []
    for metric, fname in [
        ("Revenue", "revenue_transformed_"),
        ("EPS", "eps_transformed_")
    ]:
        # Find the correct file for each date (since the date is in the filename)
        prev_files = list(prev_tx.glob(f"{fname}*.csv"))
        curr_files = list(curr_tx.glob(f"{fname}*.csv"))
        if not prev_files or not curr_files:
            continue
        prev_df = pd.read_csv(prev_files[0])
        curr_df = pd.read_csv(curr_files[0])
        # Set index to ticker for easy comparison
        prev_df = prev_df.set_index("ticker")
        curr_df = curr_df.set_index("ticker")
        # Compare only columns that are in both
        common_cols = [c for c in prev_df.columns if c in curr_df.columns and c not in ["api_run_date"]]
        for ticker in prev_df.index:
            if ticker not in curr_df.index:
                continue
            for period in common_cols:
                prev_val = prev_df.at[ticker, period]
                curr_val = curr_df.at[ticker, period]
                # Only report if both are not NaN and values are different at 5 decimal places
                try:
                    prev_val_rounded = round(float(prev_val), 5)
                    curr_val_rounded = round(float(curr_val), 5)
                except Exception:
                    prev_val_rounded = prev_val
                    curr_val_rounded = curr_val
                if pd.notna(prev_val_rounded) and pd.notna(curr_val_rounded) and prev_val_rounded != curr_val_rounded:
                    changes.append({
                        "ticker": ticker,
                        "period": period,
                        "metric": metric,
                        "previous_estimate": prev_val_rounded,
                        "new_estimate": curr_val_rounded,
                        "change_date": curr_date
                    })

    if not changes:
        logger.info("No changes detected in EPS/Revenue estimates.")
        # Still log a summary row with zero changes
        summary_path = DATA_DIR / "eps_revenue_changes_summary.csv"
        summary_row = {
            'run_date': curr_date,
            'num_changes': 0,
            'eps_changes': 0,
            'revenue_changes': 0
        }
        import os
        import csv
        write_header = not os.path.exists(summary_path)
        with open(summary_path, 'a', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=summary_row.keys())
            if write_header:
                writer.writeheader()
            writer.writerow(summary_row)
        return

    # Create DataFrame from changes
    changes_df = pd.DataFrame(changes)
    
    # Path for the changes file
    changes_path = DATA_DIR / "eps_revenue_changes.csv"
    
    # Append to existing file or create new one
    if changes_path.exists():
        # Read existing file
        existing_df = pd.read_csv(changes_path)
        # Append new changes
        combined_df = pd.concat([existing_df, changes_df], ignore_index=True)
        # Remove any duplicates (in case script runs multiple times)
        combined_df = combined_df.drop_duplicates(
            subset=['ticker', 'period', 'metric', 'change_date'],
            keep='last'
        )
    else:
        combined_df = changes_df

    # Save updated file, ensuring numbers are stored as numbers
    combined_df['previous_estimate'] = pd.to_numeric(combined_df['previous_estimate'], errors='coerce')
    combined_df['new_estimate'] = pd.to_numeric(combined_df['new_estimate'], errors='coerce')
    combined_df.to_csv(changes_path, index=False, float_format='%.5f')
    
    # Log summary of changes
    summary = changes_df.groupby('metric').size()
    logger.info(f"Added {len(changes)} new changes to {changes_path}")
    logger.info(f"Changes by metric: {summary.to_dict()}")

    # Write summary row to summary CSV
    summary_path = DATA_DIR / "eps_revenue_changes_summary.csv"
    eps_changes = int(summary.get('EPS', 0))
    revenue_changes = int(summary.get('Revenue', 0))
    summary_row = {
        'run_date': curr_date,
        'num_changes': len(changes),
        'eps_changes': eps_changes,
        'revenue_changes': revenue_changes
    }
    import os
    import csv
    write_header = not os.path.exists(summary_path)
    with open(summary_path, 'a', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=summary_row.keys())
        if write_header:
            writer.writeheader()
        writer.writerow(summary_row)
#old function
'''
def run_pipelines_concurrently(tickers: List[str]):
    """
    Run EODHD and Finnhub pipelines concurrently using ThreadPoolExecutor
    """
    with ThreadPoolExecutor(max_workers=2) as executor:
        # Submit both pipeline tasks
        eodhd_future = executor.submit(run_daily_bulk_download, tickers)
        finnhub_future = executor.submit(run_finnhub_data_pipeline, tickers)
        
        # Wait for both to complete
        eodhd_future.result()
        finnhub_future.result()
'''
#new function
def run_pipelines_concurrently(tickers: List[str]):
    success = True
    error_messages = []

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = {
            executor.submit(run_daily_bulk_download, tickers): "EODHD",
            executor.submit(run_finnhub_data_pipeline, tickers): "FINNHUB"
        }

        for future in futures:
            try:
                future.result()
            except Exception as e:
                success = False
                error_messages.append(f"{futures[future]} failed: {e}")
                logger.exception(f"{futures[future]} pipeline failed")

    update_cron_stats(success, error_msg="\n".join(error_messages) if error_messages else None)

    if not success:
        raise RuntimeError("One or more pipelines failed. See logs for details.")
        
# Helper to robustly load master_tickers_with_flags_types.csv

def load_master_tickers(path: str = None) -> pd.DataFrame:
    paths_to_try = [
        path,
        "./master_tickers_with_flags_types.csv"
    ] if path else ["./master_tickers_with_flags_types.csv"]
    for p in paths_to_try:
        if p and Path(p).exists():
            return pd.read_csv(p, keep_default_na=False)
    raise FileNotFoundError("Could not find master_tickers_with_flags_types.csv in project root.")


def run_earnings_calendar_upload(tickers: List[str], from_date: str, to_date: str):
    """
    Fetch earnings calendar from Finnhub for each ticker and upload as JSON to GCS
    """
    client = fb.Client(api_key=FINNHUB_API_KEY)
    folder_name = f"earnings_calendar/{from_date}_to_{to_date}"
    log_progress(f"Starting earnings calendar upload for {len(tickers)} tickers")

    for i, ticker in enumerate(tqdm(tickers, desc="Earnings Calendar", unit="ticker")):
        try:
            data = client.earnings_calendar(
                _from=from_date,
                to=to_date,
                symbol=ticker,
                international=False
            )
            json_str = json.dumps(data, indent=2)
            blob_path = gcs_path(f"{folder_name}/{ticker}.json")
            upload_string_to_gcs(BUCKET_NAME, blob_path, json_str)
            log_progress(f"[{i+1}/{len(tickers)}] Uploaded earnings for {ticker}")
        except Exception as e:
            log_progress(f"[{i+1}/{len(tickers)}] ERROR {ticker}: {e}")
        time.sleep(0.25)  # Respect Finnhub rate limits

    log_progress(f"Finished earnings calendar upload for date range: {from_date} to {to_date}")

import sys
if __name__ == "__main__":
    try:
        tickers = load_tickers(limit=None)
        run_daily_bulk_download(tickers)
        #run_pipelines_concurrently(tickers)
        sys.exit(0)
    except:
        logger.error(f"Fatal error: {e}")
        update_cron_stats(False, str(e))
        sys.exit(1)  # failure triggers job restart
    '''
    #run_finnhub_data_pipeline(tickers)
    run_pipelines_concurrently(tickers)
    #run_earnings_calendar_upload(tickers, from_date="2025-01-01", to_date="2025-12-01")
    #run_daily_bulk_download(tickers)
    #detect_eps_revenue_changes()
    #update_cron_stats(True)
    
    #run_historical_bulk_download(date(2024,3,1),date(2024,3,10),pd.read_csv("master_tickers_with_flags_types.csv",keep_default_na=False))
    
    '''
