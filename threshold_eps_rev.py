# ------------------ IMPORTS ------------------
import pandas as pd
import numpy as np
from datetime import datetime
from jinja2 import Template
import os
import gcsfs
import json
import re
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

from google.cloud import secretmanager

# ------------------ CONFIG ------------------
GCS_BUCKET = 'historical_data_evoke'
PROJECT = 'tonal-nucleus-464617-n2'

GCS_DATA_PATH = f'gs://{GCS_BUCKET}/market_data/daily'
CATALYST_FILE_PATH = f'gs://{GCS_BUCKET}/catalyst_events.json'
HTML_OUTPUT_PATH = f'gs://{GCS_BUCKET}/eps_rev_changes'

# ------------------ THRESHOLDS ------------------
THRESHOLDS = {
    "Mega Cap": 3.0,
    "Large Cap": 5.0,
    "Mid Cap": 8.0,
    "Small Cap": 12.0,
    "Micro Cap": 20.0,
    "Nano Cap": 30.0,
}

# ------------------ MAIN FUNCTION ------------------
def generate_daily_revisions_report():
    try:
        fs = gcsfs.GCSFileSystem(project=PROJECT)
        print("[INFO] Connected to GCS.")
    except Exception as e:
        print(f"[FATAL] Could not connect to GCS. Error: {e}")
        return

    # ------------------ HELPERS ------------------
    def extract_date_from_path(path):
        m = re.search(r'(\d{4}-\d{2}-\d{2})', path)
        return m.group(1) if m else "unknown"

    def get_latest_data_paths(base_gcs_path):
        all_items = fs.ls(base_gcs_path)
        date_folders = []
        for item in all_items:
            if fs.isdir(item):
                folder = os.path.basename(item.rstrip('/'))
                try:
                    datetime.strptime(folder, '%Y-%m-%d')
                    date_folders.append(item)
                except: pass
        if len(date_folders) < 2: return None, None, None
        sorted_folders = sorted(date_folders, key=lambda p: datetime.strptime(os.path.basename(p.rstrip('/')), '%Y-%m-%d'), reverse=True)
        latest, prev = sorted_folders[0], sorted_folders[1]
        latest_str, prev_str = os.path.basename(latest), os.path.basename(prev)
        paths = {
            'latest_eps': f'{latest}/FINNHUB/transformed/eps_transformed_{latest_str}.csv',
            'prev_eps':   f'{prev}/FINNHUB/transformed/eps_transformed_{prev_str}.csv',
            'latest_rev': f'{latest}/FINNHUB/transformed/revenue_transformed_{latest_str}.csv',
            'prev_rev':   f'{prev}/FINNHUB/transformed/revenue_transformed_{prev_str}.csv',
            'market_cap': f'{latest}/EODHD/eod_us_{latest_str}_merged.csv',
        }
        return paths, latest_str, prev_str

    QUARTER_PATTERNS = [re.compile(r'^Q[1-4][-_/]\d{4}$'), re.compile(r'^\d{4}Q[1-4]$')]
    YEAR_PATTERN = re.compile(r'^\d{4}$')
    def _is_quarter(c): return any(p.match(c) for p in QUARTER_PATTERNS)
    def _is_year(c): return YEAR_PATTERN.match(c) is not None

    def process_file(filepath, valname):
        if not filepath: return pd.DataFrame()
        try:
            with fs.open(filepath, 'rb') as f: df = pd.read_csv(f)
        except: return pd.DataFrame()
        if 'ticker' not in df.columns: return pd.DataFrame()
        value_vars = [c for c in df.columns if _is_quarter(c) or _is_year(c)]
        if not value_vars: return pd.DataFrame()
        long = df.melt(id_vars=['ticker'], value_vars=value_vars, var_name='period', value_name=valname)
        long[valname] = pd.to_numeric(long[valname], errors='coerce')
        long.dropna(subset=[valname], inplace=True)
        long['period_type'] = long['period'].apply(lambda x: 'Year' if _is_year(x) else 'Quarter')
        return long

    def load_market_cap(filepath):
        if not filepath: return pd.DataFrame()
        try:
            with fs.open(filepath, 'rb') as f:
                df = pd.read_csv(f, usecols=['Symbol','Company_Name','MarketCapitalization'])
            df.dropna(subset=['MarketCapitalization'], inplace=True)
            df['Symbol'] = df['Symbol'].str.split('.').str[0]
            df.rename(columns={'Symbol':'ticker','Company_Name':'companyName','MarketCapitalization':'marketCapitalization'}, inplace=True)
            return df
        except: return pd.DataFrame()

    def get_cap_bin(mc):
        if pd.isna(mc): return "N/A"
        if mc >= 200e9: return "Mega Cap"
        if mc >= 10e9:  return "Large Cap"
        if mc >= 2e9:   return "Mid Cap"
        if mc >= 300e6: return "Small Cap"
        if mc >= 50e6:  return "Micro Cap"
        return "Nano Cap"

    def merge_and_diff(df_old, df_new, prefix):
        if df_old.empty or df_new.empty: return pd.DataFrame()
        m = pd.merge(df_old, df_new, on=['ticker','period'], how='inner', suffixes=('_old','_new'))
        old, new = f"{prefix}_old", f"{prefix}_new"
        m[old] = pd.to_numeric(m[old], errors='coerce')
        m[new] = pd.to_numeric(m[new], errors='coerce')
        m.dropna(subset=[old,new], inplace=True)
        unchanged = np.isclose(m[old], m[new], rtol=1e-08, atol=1e-10)
        m = m[~unchanged].copy()
        if m.empty: return pd.DataFrame()
        m['abs_change'] = m[new]-m[old]
        m['pct_change'] = (m['abs_change']/m[old].replace(0,np.nan))*100
        return m.rename(columns={'period_type_x':'period_type'})

    def send_email(subject, body, attach_path, attach_name):
        sender = "anuashwork@gmail.com"
        client = secretmanager.SecretManagerServiceClient()
        name = "projects/555005178535/secrets/email_app_password/versions/latest"
        pw = client.access_secret_version(request={"name": name}).payload.data.decode("UTF-8")
        to_addrs = ["anuashwork@gmail.com"]
        bcc_addrs = ["ashwinram232@gmail.com","anubuthi.kottapalli@gmail.com"]
        msg = MIMEMultipart()
        msg['From'], msg['To'], msg['Subject'] = sender, ", ".join(to_addrs), subject
        msg.attach(MIMEText(body,'html'))
        with open(attach_path,"rb") as f:
            part = MIMEBase("application","octet-stream"); part.set_payload(f.read())
        encoders.encode_base64(part); part.add_header("Content-Disposition", f"attachment; filename={attach_name}")
        msg.attach(part)
        try:
            s = smtplib.SMTP('smtp.gmail.com',587); s.starttls(); s.login(sender,pw)
            s.sendmail(sender,to_addrs+bcc_addrs,msg.as_string()); s.quit()
            print("[INFO] Email sent.")
        except Exception as e: print(f"[ERROR] Email failed: {e}")

    # ------------------ MAIN ------------------
    paths, latest, prev = get_latest_data_paths(GCS_DATA_PATH)
    if not paths: return

    eps_new, eps_old = process_file(paths['latest_eps'],'eps_new'), process_file(paths['prev_eps'],'eps_old')
    rev_new, rev_old = process_file(paths['latest_rev'],'rev_new'), process_file(paths['prev_rev'],'rev_old')
    mcap = load_market_cap(paths['market_cap'])

    eps_chg, rev_chg = merge_and_diff(eps_old, eps_new,'eps'), merge_and_diff(rev_old, rev_new,'rev')
    eps_chg['type'], rev_chg['type'] = 'EPS','Revenue'
    eps_chg.rename(columns={'eps_old':'old_value','eps_new':'new_value'}, inplace=True)
    rev_chg.rename(columns={'rev_old':'old_value','rev_new':'new_value'}, inplace=True)

    df = pd.concat([eps_chg, rev_chg], ignore_index=True)
    if df.empty: 
        print("[INFO] No revisions."); return
    df = pd.merge(df, mcap, on='ticker', how='inner')
    df['marketCapBin'] = df['marketCapitalization'].apply(get_cap_bin)

    # --- Threshold filtering ---
    triggers = df[df.apply(lambda r: abs(r['pct_change']) >= THRESHOLDS.get(r['marketCapBin'],9999), axis=1)]
    if triggers.empty:
        print("[INFO] No rows crossed thresholds. Skipping email."); return

    # --- Generate HTML report (full df, not just triggers) ---
    output_filename = f"financial_revisions_report_{prev}_vs_{latest}.html"
    local_tmp = f"/tmp/{output_filename}"
    # (Rendering logic omitted for brevity — reuse your existing Template rendering block)
    # write html_output -> local_tmp, then fs.put(local_tmp, gcs_output_path)

    # --- Build email body highlighting triggers ---
    rows = []
    for _,r in triggers.iterrows():
        rows.append(f"<li><b>{r['ticker']}</b> ({r['marketCapBin']}): {r['type']} {r['pct_change']:+.2f}%</li>")
    body = f"""
    <html><body>
    <h2>Threshold-triggered Revisions on {latest}</h2>
    <ul>{''.join(rows)}</ul>
    <p>Full interactive report attached.</p>
    </body></html>
    """
    send_email(f"Daily Revisions Alert: {latest}", body, local_tmp, output_filename)
    os.remove(local_tmp)

# ------------------ RUN ------------------
if __name__=="__main__":
    generate_daily_revisions_report()
