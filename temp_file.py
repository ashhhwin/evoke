import polars as pl
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
import gcsfs 
import re

import functools
GCS_BUCKET = "historical_data_evoke" 

PROGRESS_LOG = Path("market_data/progress.log")

from google.cloud import storage

import re
from google.cloud import storage

def get_latest_daily_date() -> str:
    client = storage.Client()
    blobs = client.list_blobs(GCS_BUCKET, prefix="market_data/daily/")

    latest_date = None
    date_pattern = re.compile(r"\d{4}-\d{2}-\d{2}")

    for blob in blobs:
        parts = blob.name.split("/")
        if len(parts) > 2:
            candidate = parts[2]
            if date_pattern.fullmatch(candidate):
                if (latest_date is None) or (candidate > latest_date):
                    latest_date = candidate  # update latest date

    if latest_date is None:
        return "No data available"

    return latest_date

run_date = get_latest_daily_date()
print(run_date)
