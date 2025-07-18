import pandas as pd
import numpy as np
import gcsfs
from sqlalchemy import create_engine
import urllib

# --- GCS Config ---
bucket = "historical_data_evoke"
gcs_path = f"{bucket}/Final_data_52w_cleaned"
project = "tonal-nucleus-464617-n2"

fs = gcsfs.GCSFileSystem(project=project)
files = fs.ls(gcs_path)
latest_file = sorted([f for f in files if f.endswith('.csv')])[-1]
print(f"📂 Latest file: {latest_file}")

with fs.open(latest_file, 'r') as f:
    df = pd.read_csv(f)
df = df.head(100)
print(f"✅ Loaded {len(df)} rows")

# --- Column Cleanup ---
float_cols = [
    'Beta', 'P_Open', 'P_High', 'P_Low', 'P_Close',
    'Prev_Close (Price)', 'P_50D_MA', 'P_200D_MA',
    'V_14D_MA', 'V_50D_MA', 'F52W_High', 'F52W_Low',
    'Close_to_Open (% from Prev Day Close)', 'Open_Close (%)',
    'High_Close(%)', 'Low_Close(%)', 'Close_to_Close (%)',
    'Short_Ratio', 'Short_Percent_Float',
    'Shares_Insiders', 'Shares_Institutions'
]

int_cols = ['Volume', 'Market_Cap', 'Shares_Out', 'Shares_Float']
date_cols = ['Trade_Date', 'F52W_H_DATE', 'F52W_L_DATE', 'Earnings_Date']

for col in float_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce').round(2)

for col in int_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype('Int64')

for col in date_cols:
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], errors='coerce').dt.date

# --- SQL Server Connection ---
username = "sqlserver"
password = "EvokeIntern@2025"
server = "34.58.50.83"
port = 1433
database = "eodhd_data"

params = urllib.parse.quote_plus(
    f"DRIVER=ODBC Driver 17 for SQL Server;"
    f"SERVER={server},{port};"
    f"DATABASE={database};"
    f"UID={username};PWD={password};"
    f"Encrypt=no;"
    f"TrustServerCertificate=yes;"
)

engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

# --- Upload row by row to catch bad inserts ---
for i, row in df.iterrows():
    try:
        row_df = pd.DataFrame([row])
        row_df.to_sql("Daily_Data", engine, if_exists="append", index=False)
    except Exception as e:
        print(f"❌ Row {i} failed: {e}")
        print(row)
        break

print("🚀 Finished uploading (or stopped on error)")
