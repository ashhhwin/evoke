import pandas as pd
import gcsfs
from sqlalchemy import create_engine
import urllib

# --- Config ---
bucket = "historical_data_evoke"
gcs_path = f"{bucket}/Final_data_52w_cleaned"
project = "tonal-nucleus-464617-n2"

# --- Get latest CSV from GCS ---
fs = gcsfs.GCSFileSystem(project=project)
files = fs.ls(gcs_path)
latest_file = sorted([f for f in files if f.endswith('.csv')])[0]
print(f"📂 Latest file: {latest_file}")

with fs.open(latest_file, 'r') as f:
    df = pd.read_csv(f)
df = df.head(100)
print(f"✅ Loaded {len(df)} rows")

# --- SQL Server connection ---
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

# --- Upload to SQL Server ---
#df.to_sql("test1", engine, if_exists="append", index=False)
df.to_sql("Daily_Data", engine, if_exists="append", index=False)
print("🚀 Upload complete!")
