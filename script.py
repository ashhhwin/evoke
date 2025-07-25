# This script is intended to be run as a cron job every day at 8pm.
# Example crontab entry (edit with `crontab -e`):
# 0 20 * * * /usr/bin/python3 /path/to/GCP-SCRIPT/script.py >> /path/to/GCP-SCRIPT/cron.log 2>&1

from __future__ import annotations

import os, time, shutil, logging, requests, json
from datetime import date, timedelta, datetime
from pathlib import Path
from typing import List, Dict, Callable, Optional
from concurrent.futures import ThreadPoolExecutor

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
RATE_LIMIT_SEC   = 0.8
PROGRESS_LOG = Path("market_data/progress.log")

start_date = date(2025, 1, 1)
end_date = date(2025, 12, 31)
YEARS_OF_HISTORY = 5
def log_progress(msg: str):
    PROGRESS_LOG.parent.mkdir(parents=True, exist_ok=True)
    with PROGRESS_LOG.open("a") as f:
        f.write(f"{msg}\n")
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


def run_finnhub_data_pipeline(tickers: List[str]):
    today_iso = date.today().isoformat()
    raw_dir = f"daily/{today_iso}/FINNHUB/raw_data"
    tx_dir = f"daily/{today_iso}/FINNHUB/transformed"

    client = fb.Client(api_key=FINNHUB_API_KEY)

    funcs: Dict[str, Callable[[str], pd.DataFrame]] = {
        "revenue_estimates_quarterly": lambda t: get_finnhub_df(client, client.company_revenue_estimates, t, "quarterly"),
        "revenue_estimates_annual":    lambda t: get_finnhub_df(client, client.company_revenue_estimates, t, "annual"),
        "eps_estimates_quarterly":     lambda t: get_finnhub_df(client, client.company_eps_estimates,     t, "quarterly"),
        "eps_estimates_annual":        lambda t: get_finnhub_df(client, client.company_eps_estimates,     t, "annual"),
    }

    collected: Dict[str, List[pd.DataFrame]] = {k: [] for k in funcs}
    for i, tk in enumerate(tqdm(tickers, desc="Finnhub", unit="ticker")):
        log_progress(f"[{i+1}/{len(tickers)}] Fetching from Finnhub: {tk}")
        for name, fn in funcs.items():
            try:
                df = fn(tk)
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
            with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmpf:
                pd.concat(lst, ignore_index=True).to_csv(tmpf.name, index=False)
                upload_to_gcs(BUCKET_NAME, gcs_path(f"{raw_dir}/{name}.csv"), tmpf.name)
            os.remove(tmpf.name)

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
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmpf:
            final.to_csv(tmpf.name, index=False)
            upload_to_gcs(BUCKET_NAME, gcs_path(f"{tx_dir}/{out_name}"), tmpf.name)
        os.remove(tmpf.name)

    # Revenue transform
    try:
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as qf, tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as yf:
            upload_path_q = gcs_path(f"{raw_dir}/revenue_estimates_quarterly.csv")
            upload_path_y = gcs_path(f"{raw_dir}/revenue_estimates_annual.csv")
            # Download from GCS to temp files
            storage_client = storage.Client()
            bucket = storage_client.bucket(BUCKET_NAME)
            bucket.blob(upload_path_q).download_to_filename(qf.name)
            bucket.blob(upload_path_y).download_to_filename(yf.name)
            _pivot(pd.read_csv(qf.name), pd.read_csv(yf.name), "revenueAvg", gcs_path(f"revenue_transformed_{today_iso}.csv"))
        os.remove(qf.name)
        os.remove(yf.name)
    except Exception:
        logger.warning("Revenue raw files missing – skipping transform")

    # EPS transform
    try:
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as qf, tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as yf:
            upload_path_q = gcs_path(f"{raw_dir}/eps_estimates_quarterly.csv")
            upload_path_y = gcs_path(f"{raw_dir}/eps_estimates_annual.csv")
            storage_client = storage.Client()
            bucket = storage_client.bucket(BUCKET_NAME)
            bucket.blob(upload_path_q).download_to_filename(qf.name)
            bucket.blob(upload_path_y).download_to_filename(yf.name)
            _pivot(pd.read_csv(qf.name), pd.read_csv(yf.name), "epsAvg", gcs_path(f"eps_transformed_{today_iso}.csv"))
        os.remove(qf.name)
        os.remove(yf.name)
    except Exception:
        logger.warning("EPS raw files missing – skipping transform")

# ──────────────────────────────────────────────────────────────────────────────
# EODHD daily download
# ──────────────────────────────────────────────────────────────────────────────


def run_daily_bulk_download(tickers: List[str]):
    
    today= date.today()
    date_str = today.isoformat()
    raw_folder = f"daily/{date_str}/EODHD/raw_jsons"
    base_folder = f"daily/{date_str}/EODHD"
    log_lines: List[str] = []
    stock9k = load_master_tickers()
    # Download bulk JSON
    bulk_json_gcs_path = gcs_path(f"{raw_folder}/eod_us_{date_str}.json")
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
        df["MarketCapitalization"] = pd.to_numeric(df["MarketCapitalization"], errors="coerce") / 1e6
    df['date'] = date_str
    df["date"] = pd.to_datetime(df["date"])
    df["Close_to_Close (%)"]=0.0
    df["Close_to_Open (% from Prev Day Close)"]=0.0
    # Merge with previous day's close and volume
    # (skip prev_path.exists() logic for GCS version for now)

    # Intraday ratios
    df["High_Close(%)"] = (df["high"] - df["close"]) / df["close"] * 100
    df["Low_Close(%)"]  = (df["low"]  - df["close"]) / df["close"] * 100
    df["Open_Close (%)"] = (df["close"] - df["open"]) / df["open"] * 100
    
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
            log_lines.append(f"[ERROR] fundamentals {tk}: {e}")
            log_progress(f"[{i+1}/{len(tickers)}] ERROR for {tk}: {e}")
        time.sleep(0.6)
    # Save fundamentals to GCS
    upload_string_to_gcs(BUCKET_NAME, fundamentals_json_gcs_path, json.dumps(fundamentals_json_list, indent=2))

    # Merge enriched fundamentals
    if extra_rows:
        df = df.merge(pd.DataFrame(extra_rows), on="Symbol", how="left")
        logger.info(f"Final DataFrame shape before enrichment: {df.shape}")

    for col in df.columns:
        if col not in ["Symbol", "date", "MarketCapCategory"]:
            df[col] = pd.to_numeric(df[col], errors="ignore")

    final_df = format_data(df)
    merged_csv_gcs_path = gcs_path(f"{base_folder}/eod_us_{date_str}_merged.csv")
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmpf:
        final_df.to_csv(tmpf.name, index=False)
        upload_to_gcs(BUCKET_NAME, merged_csv_gcs_path, tmpf.name)
    os.remove(tmpf.name)

    if log_lines:
        log_gcs_path = gcs_path(f"{base_folder}/log.txt")
        upload_string_to_gcs(BUCKET_NAME, log_gcs_path, "\n".join(log_lines))

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
            # Divide MarketCapitalization by 1e6 if present

            df = df.merge(stock9k,on='Symbol', how='right')

            df = df.drop(columns=['Company Name_x'], errors='ignore')
            if "MarketCapitalization" in df.columns:
                df["MarketCapitalization"] = pd.to_numeric(df["MarketCapitalization"], errors="coerce") / 1e6
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
            df.to_csv(output_file, index=False)
            
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

            
            # Save merged file
            merged_file = hist_folder / f"{start_date.strftime('%m-%d-%y')} to {end_date.strftime('%m-%d-%y')}.csv"
            merged_df = format_data(merged_df)  # Apply final formatting
            merged_df.to_csv(merged_file, index=False)
            logger.info(f"Saved merged data to {merged_file}")

            # Also save/overwrite for_tableau_db.csv in historical/
            tableau_path = DATA_DIR / "historical" / "for_tableau_db.csv"
            merged_df.to_csv(tableau_path, index=False)
            logger.info(f"Saved Tableau-ready data to {tableau_path}")
        except Exception as e:
            error_msg = f"[ERROR] Failed to create merged file: {e}"
            logger.error(error_msg)
            log_lines.append(error_msg)

    if log_lines:
        (hist_folder / "log.txt").write_text("\n".join(log_lines))
        logger.warning(f"Errors logged at: {hist_folder}/log.txt")
    else:
        logger.info("Historical data downloaded with no errors.")
        return f"Historical data from {start_date} to {end_date} downloaded successfully."

# ──────────────────────────────────────────────────────────────────────────────
# TICKER WISE EPS/REVENUE LOADER
# ──────────────────────────────────────────────────────────────────────────────
def get_latest_transformed_folder(base_dir=DATA_DIR / "daily"):
    all_dates = sorted([d.name for d in base_dir.iterdir() if d.is_dir()], reverse=True)
    for date_str in all_dates:
        tx_dir = base_dir / date_str / "FINNHUB" / "transformed"
        if tx_dir.exists():
            return tx_dir
    raise FileNotFoundError("No valid transformed EPS/Revenue folder found.")

def load_eps_and_revenue_data():
    tx_dir = get_latest_transformed_folder()
    eps_file = next(tx_dir.glob("eps_transformed_*.csv"), None)
    rev_file = next(tx_dir.glob("revenue_transformed_*.csv"), None)
    if not eps_file or not rev_file:
        raise FileNotFoundError("EPS or Revenue file not found in latest folder.")

    eps_df = pd.read_csv(eps_file)
    rev_df = pd.read_csv(rev_file)
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

# Helper to upload a file to GCP bucket
def upload_to_gcs(bucket_name: str, destination_blob_name: str, source_file: str):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file)

# Helper to upload string data to GCP bucket
def upload_string_to_gcs(bucket_name: str, destination_blob_name: str, data: str):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_string(data)

BUCKET_NAME = os.getenv("BUCKET_NAME", "historical_data_evoke")

# Update this helper to prefix all GCS paths with 'market/' only
# so that the structure is: bucket/market/daily/... and bucket/market/historical/...
def gcs_path(path: str) -> str:
    return f"market/{path}" if not path.startswith("market/") else path

if __name__ == "__main__":
    tickers = load_tickers(limit=None)
    run_pipelines_concurrently(tickers)
    detect_eps_revenue_changes()
    #update_cron_stats(True)
    #run_daily_bulk_download(tickers)
    #run_historical_bulk_download(date(2024,3,1),date(2024,3,10),pd.read_csv("master_tickers_with_flags_types.csv",keep_default_na=False))
    #run_finnhub_data_pipeline(tickers)
