import argparse
from pathlib import Path
import pandas as pd
import logging
from datetime import datetime, timedelta
import re
from google.cloud import storage
import io
import tempfile


logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent / "market_data"
DATE_FMT = "%Y-%m-%d"
bucket_name="historical_data_evoke"

def upload_to_gcs(bucket_name, destination_blob_path, local_file_path):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_path)
    blob.upload_from_filename(local_file_path)
    print(f"Uploaded {local_file_path} to gs://{bucket_name}/{destination_blob_path}")

def read_csv_from_gcs(bucket_name, blob_path)-> pd.DataFrame:
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    data = blob.download_as_text()
    return pd.read_csv(io.StringIO(data), keep_default_na=False, na_values=["", " "])

    
def gcs_folder_exists(bucket_name, folder_prefix):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix=folder_prefix, max_results=1))
    return len(blobs) > 0

def get_date_folders(bucket_name="historical_data_evoke", prefix="market_data/daily/"):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    
    blobs = client.list_blobs(bucket, prefix=prefix, delimiter="/")
    date_folders = []
    for page in blobs.pages:
        for folder in page.prefixes:
            match = re.match(rf"{prefix}(\d{{4}}-\d{{2}}-\d{{2}})/", folder)
            if match:
                date_folders.append(match.group(1))
    
    return sorted(date_folders)


def get_quarter_for_date(date):
    y = date.year % 100
    q = (date.month - 1) // 3 + 1
    return f"Q{q}-{y:02d}"


def get_latest_and_prior_dates(date_folders, weeks_apart=4):
    if len(date_folders) < 2:
        return None, None
    latest = date_folders[-1]
    latest_date = datetime.strptime(latest, DATE_FMT)
    target_date = latest_date - timedelta(weeks=weeks_apart)
    prior = min(date_folders[:-1], key=lambda d: abs(datetime.strptime(d.name, DATE_FMT) - target_date))
    return prior, latest


def summarize_estimates(df, period):
    cols = [c for c in df.columns if c not in ["ticker", "api_run_date"]]
    if period not in cols:
        return pd.DataFrame({"ticker": [], "estimate": [], "api_run_date": [], "num_analysts": []})
    return pd.DataFrame({
        "ticker": df["ticker"],
        "estimate": df[period],
        "api_run_date": df["api_run_date"],
        "num_analysts": df[period].notna().astype(int)
    })


def get_period_label(quarter, year):
    return f"Q{int(quarter)}-{str(year)[-2:]}"


def get_annual_label(year):
    return str(year)


def avg_num_analysts_from_raw(raw_df, period_label_func, periods):
    result = {}
    for period in periods:
        if '-' in period:
            q, y = period.split('-')
            q = int(q[1:])
            y = int('20' + y) if len(y) == 2 else int(y)
            mask = (raw_df['quarter'] == q) & (raw_df['year'] == y)
        else:
            y = int(period)
            mask = (raw_df['year'] == y)
        avg_analysts = raw_df[mask].groupby('ticker')['numberAnalysts'].mean()
        for ticker, avg in avg_analysts.items():
            result[(ticker, period)] = avg
    return result


def compare_eps_revenue(from_date=None, to_date=None, quarters=None, output_file=None, annual=False):
    date_folders = get_date_folders()
    if len(date_folders) < 2:
        logger.warning("Not enough daily folders to compare changes.")
        return None

    # Robust date folder selection
    latest = None
    prior = None
    if to_date:
        to_path = f"market_data/daily/{to_date}/"
        if gcs_folder_exists(bucket_name, to_path):
            latest = to_date
        else:
            latest = date_folders[-1]
            logger.warning(f"to_date folder {to_date} not found, using latest available: {latest}")
    else:
        latest = date_folders[-1]

    if from_date:
        from_path = f"market_data/daily/{from_date}/"
        if gcs_folder_exists(bucket_name, from_path):
            prior = from_date
        else:
            # Pick the closest available folder before latest
            prior_candidates = [d for d in date_folders if d != latest]
            if prior_candidates:
                prior = prior_candidates[-1]
                logger.warning(f"from_date folder {from_date} not found, using prior available: {prior}")
            else:
                logger.warning("No prior folder available.")
                return None
    else:
        # Auto pick prior date ~4 weeks before latest
        prior, _ = get_latest_and_prior_dates(date_folders)
        if prior is None:
            logger.warning("Could not determine prior date folder.")
            return None

    latest_date = datetime.strptime(latest, DATE_FMT)
    periods = quarters if quarters else [
        get_annual_label(latest_date.year) if annual else get_quarter_for_date(latest_date)
    ]

    suffix = "annual" if annual else "quarterly"

    prev_rev_raw = read_csv_from_gcs("historical_data_evoke", f"market_data/daily/{prior}/FINNHUB/raw_data/revenue_estimates_quarterly.csv")
    curr_rev_raw = read_csv_from_gcs("historical_data_evoke", f"market_data/daily/{latest}/FINNHUB/raw_data/revenue_estimates_quarterly.csv")
    prev_eps_raw = read_csv_from_gcs("historical_data_evoke", f"market_data/daily/{prior}/FINNHUB/raw_data/eps_estimates_quarterly.csv")
    curr_eps_raw = read_csv_from_gcs("historical_data_evoke", f"market_data/daily/{latest}/FINNHUB/raw_data/eps_estimates_quarterly.csv")

    prev_rev = read_csv_from_gcs("historical_data_evoke", f"market_data/daily/{prior}/FINNHUB/transformed/revenue_transformed_{prior}.csv")
    curr_rev = read_csv_from_gcs("historical_data_evoke", f"market_data/daily/{latest}/FINNHUB/transformed/revenue_transformed_{latest}.csv")
    prev_eps = read_csv_from_gcs("historical_data_evoke", f"market_data/daily/{prior}/FINNHUB/transformed/eps_transformed_{prior}.csv")
    curr_eps = read_csv_from_gcs("historical_data_evoke", f"market_data/daily/{latest}/FINNHUB/transformed/eps_transformed_{latest}.csv")

    rows = []
    for period in periods:
        prev_rev_sum = summarize_estimates(prev_rev, period).set_index("ticker")
        curr_rev_sum = summarize_estimates(curr_rev, period).set_index("ticker")
        prev_eps_sum = summarize_estimates(prev_eps, period).set_index("ticker")
        curr_eps_sum = summarize_estimates(curr_eps, period).set_index("ticker")

        all_tickers = set(prev_rev_sum.index) | set(curr_rev_sum.index) | set(prev_eps_sum.index) | set(curr_eps_sum.index)

        for ticker in sorted(all_tickers):
            prev_rev_val = round(prev_rev_sum.at[ticker, "estimate"],2) if ticker in prev_rev_sum.index else float('nan')
            new_rev_val = round(curr_rev_sum.at[ticker, "estimate"],2) if ticker in curr_rev_sum.index else float('nan')
            prev_eps_val = round(prev_eps_sum.at[ticker, "estimate"], 2) if ticker in prev_eps_sum.index else float('nan')
            new_eps_val = round(curr_eps_sum.at[ticker, "estimate"], 2) if ticker in curr_eps_sum.index else float('nan')

            def pct_change(prev, new):
                if pd.isna(prev) or prev == 0 or pd.isna(new):
                    return float('nan')
                return round(((new - prev) / abs(prev)) * 100, 2)

            row = {
                "ticker": ticker,
                "prev_revenue_millions": prev_rev_val,
                "new_revenue_millions": new_rev_val,
                "revenue_pct_change": pct_change(prev_rev_val, new_rev_val),
                "prev_eps": prev_eps_val,
                "new_eps": new_eps_val,
                "eps_pct_change": pct_change(prev_eps_val, new_eps_val),
            }
            rows.append(row)

    df = pd.DataFrame(rows)

    # Merge metadata
    meta_df = read_csv_from_gcs("historical_data_evoke", f"market_data/daily/{latest}/EODHD/eod_us_{latest}_merged.csv")

    df = pd.merge(
        df,
        meta_df[['Symbol', 'Company_Name', 'Type', 'Sector', 'Industry', 'MarketCapitalization', 'Shares_Float', 'Earnings_Date']],
        left_on='ticker',
        right_on='Symbol',
        how='left'
    ).drop(columns='Symbol')

    # Reorder and sort
    first_cols = ['ticker', 'Company_Name', 'Type', 'Sector', 'Industry', 'MarketCapitalization', 'Shares_Float', 'Earnings_Date']
    other_cols = [col for col in df.columns if col not in first_cols]
    df = df[first_cols + other_cols]
    df = df.sort_values(by='MarketCapitalization', ascending=False)

    # Keep only required columns
    final_cols = [
        'ticker', 'Company_Name', 'Type', 'Sector', 'Industry',
        'MarketCapitalization', 'Shares_Float', 'Earnings_Date',
        'prev_revenue_millions', 'prev_eps',
        'new_revenue_millions', 'new_eps',
        'revenue_pct_change', 'eps_pct_change'
    ]
    df = df[final_cols]


    if output_file:
        # Create a temporary local file
        with tempfile.NamedTemporaryFile(mode="w+", delete=False, suffix=".csv") as tmp_file:
            df.to_csv(tmp_file.name, index=False)
            tmp_file.flush()
            upload_to_gcs(
                bucket_name="historical_data_evoke",
                destination_blob_path=f"market_data/revisions/{output_file}",
                local_file_path=tmp_file.name
            )
    #gcs_path = f"market_data/revisions/{output_file}"
    #df = read_csv_from_gcs("historical_data_evoke", gcs_path)
    #return df


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--from_date")
    parser.add_argument("--to_date")
    parser.add_argument("--quarters", nargs="*")
    parser.add_argument("--output", default="eps_revenue_comparison.csv")
    parser.add_argument("--annual", action="store_true")
    args = parser.parse_args()

    compare_eps_revenue(
        from_date=args.from_date,
        to_date=args.to_date,
        quarters=args.quarters,
        output_file=args.output,
        annual=args.annual
    )


def list_available_dates(bucket_name="historical_data_evoke") -> list:
    """Return a sorted list of available date folder names (YYYY-MM-DD) from GCS under market_data/daily/."""
    client = storage.Client()
    blobs = client.list_blobs(bucket_name, prefix="market_data/daily/", delimiter=None)
    folder_names = set()
    pattern = re.compile(r"market_data/daily/(\d{4}-\d{2}-\d{2})/")
    for blob in blobs:
        match = pattern.match(blob.name)
        if match:
            folder_names.add(match.group(1))
    return sorted(folder_names)


def list_available_periods(date_folder, bucket_name="historical_data_evoke"):
    """
    Return a list of available forecast periods (quarters or years) for a given date folder,
    by reading the transformed EPS file from GCS.
    """
    client = storage.Client()
    blob_path = f"market_data/daily/{date_folder}/FINNHUB/transformed/eps_transformed_{date_folder}.csv"
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    if not blob.exists():
        return []
    content = blob.download_as_text()
    df = pd.read_csv(io.StringIO(content))
    period_cols = [c for c in df.columns if c not in ["ticker", "api_run_date"]]
    return period_cols

if __name__ == "__main__":
    main()
