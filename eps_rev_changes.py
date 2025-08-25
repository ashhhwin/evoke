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

def generate_daily_revisions_report():
    """
    A single, self-contained function to perform the entire daily revision analysis pipeline.
    """

    # ------------------ GCS SETUP ------------------
    GCS_DATA_PATH = f'{GCS_BUCKET}/market_data/daily'
    CATALYST_FILE_PATH = f'{GCS_BUCKET}/catalyst_events.json'
    HTML_OUTPUT_PATH = f'{GCS_BUCKET}/eps_rev_changes'

    try:
        fs = gcsfs.GCSFileSystem(project=PROJECT)
        print("[INFO] Successfully connected to GCS.")
    except Exception as e:
        print(f"[FATAL] Could not connect to GCS. Error: {e}")
        return

    # ------------------ HELPER FUNCTIONS ------------------

    def extract_date_from_path(path):
        """
        Extracts a YYYY-MM-DD date string from a GCS file path.
        """
        match = re.search(r'(\d{4}-\d{2}-\d{2})', path)
        if match:
            return match.group(1)
        else:
            return "unknown"
    def get_latest_data_paths(base_gcs_path):
        try:
            all_items = fs.ls(base_gcs_path)
            date_folders = []
            for item in all_items:
                if fs.isdir(item):
                    folder_name = os.path.basename(item.rstrip('/'))
                    try:
                        datetime.strptime(folder_name, '%Y-%m-%d')
                        date_folders.append(item)
                    except ValueError:
                        continue

            if len(date_folders) < 2:
                print(f"[ERROR] Found fewer than 2 date folders in '{base_gcs_path}'.")
                return None, None, None

            sorted_folders = sorted(date_folders, key=lambda p: datetime.strptime(os.path.basename(p.rstrip('/')), '%Y-%m-%d'), reverse=True)
            latest_folder_path, prev_folder_path = sorted_folders[0], sorted_folders[1]
            latest_date_str, prev_date_str = os.path.basename(latest_folder_path.rstrip('/')), os.path.basename(prev_folder_path.rstrip('/'))

            print(f"[INFO] Comparing data from: {prev_date_str} -> {latest_date_str}")

            paths = {
                'latest_eps': f'gs://{latest_folder_path}/FINNHUB/transformed/eps_transformed_{latest_date_str}.csv',
                'prev_eps': f'gs://{prev_folder_path}/FINNHUB/transformed/eps_transformed_{prev_date_str}.csv',
                'latest_rev': f'gs://{latest_folder_path}/FINNHUB/transformed/revenue_transformed_{latest_date_str}.csv',
                'prev_rev': f'gs://{prev_folder_path}/FINNHUB/transformed/revenue_transformed_{prev_date_str}.csv',
                'market_cap': f'gs://{latest_folder_path}/EODHD/eod_us_{latest_date_str}_merged.csv'
            }

            for key, path in paths.items():
                if not fs.exists(path):
                    print(f"[ERROR] Required file does not exist: {path}")
                    return None, None, None

            return paths, latest_date_str, prev_date_str

        except Exception as e:
            print(f"[ERROR] Failed during GCS file discovery. Error: {e}")
            return None, None, None

    def process_file_to_long(filepath, value_name):
        if not filepath: return pd.DataFrame()
        try:
            with fs.open(filepath, 'rb') as f:
                df = pd.read_csv(f)
        except Exception as e:
            print(f"[ERROR] Failed to read file from GCS: {filepath}. Error: {e}")
            return pd.DataFrame()

        if 'ticker' not in df.columns:
            print(f"[ERROR] 'ticker' column not found in {filepath}. Skipping file.")
            return pd.DataFrame()

        value_vars = [col for col in df.columns if ('Q' in col and '-' in col) or (col.isdigit() and len(col) == 4)]
        if not value_vars: return pd.DataFrame()

        long_df = df.melt(id_vars=['ticker'], value_vars=value_vars, var_name='period', value_name=value_name)
        long_df.dropna(subset=[value_name], inplace=True)
        long_df['period_type'] = long_df['period'].apply(lambda x: 'Year' if x.isdigit() else 'Quarter')
        return long_df

    def load_market_cap_data(filepath):
        # [MODIFIED] Reads Company_Name and standardizes ticker format
        if not filepath: return pd.DataFrame()
        try:
            with fs.open(filepath, 'rb') as f:
                df = pd.read_csv(f, usecols=['Symbol', 'Company_Name', 'MarketCapitalization'])
            df.dropna(subset=['MarketCapitalization'], inplace=True)
            
            # [FIX] Standardize ticker by removing exchange suffix (e.g., .US) to ensure merge works
            df['Symbol'] = df['Symbol'].str.split('.').str[0]

            df.rename(columns={'Symbol': 'ticker', 'Company_Name': 'companyName', 'MarketCapitalization': 'marketCapitalization'}, inplace=True)
            return df
        except Exception as e:
            print(f"[ERROR] Failed to read market cap file from GCS: {filepath}. Error: {e}")
            return pd.DataFrame()

    def get_market_cap_bin(mc_in_millions):
        if pd.isna(mc_in_millions): return "N/A"
        if mc_in_millions >= 200e3: return "Mega Cap"
        if mc_in_millions >= 10e3: return "Large Cap"
        if mc_in_millions >= 2e3: return "Mid Cap"
        if mc_in_millions >= 300: return "Small Cap"
        if mc_in_millions >= 0: return "Micro Cap"
        return "N/A"

    def merge_and_calculate_diff(df_old, df_new, value_prefix):
        if df_old.empty or df_new.empty: return pd.DataFrame()
        merged = pd.merge(df_old, df_new, on=['ticker', 'period'], how='inner')
        old_col, new_col = f"{value_prefix}_old", f"{value_prefix}_new"
        merged = merged[~np.isclose(merged[old_col], merged[new_col])].copy()
        if merged.empty: return pd.DataFrame()
        merged['abs_change'] = merged[new_col] - merged[old_col]
        merged['pct_change'] = (merged['abs_change'] / merged[old_col].replace(0, np.nan)) * 100
        return merged.rename(columns={'period_type_x': 'period_type'})

    def get_revision_direction(series):
        if series.empty: return "N/A"
        all_positive = (series > 0).all()
        all_negative = (series < 0).all()
        if all_positive: return "Up"
        elif all_negative: return "Down"
        else: return "Mixed"

    def update_catalyst_events(report_df, latest_date_str, gcs_path):
        if report_df.empty or 'ticker' not in report_df.columns:
            print("[INFO] No changes detected or 'ticker' column missing, catalyst log not updated.")
            return

        try:
            with fs.open(gcs_path, 'r') as f:
                catalyst_data = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            catalyst_data = {}

        for ticker, group in report_df.groupby('ticker'):
            eps_direction = get_revision_direction(group[group['type'] == 'EPS']['abs_change'])
            rev_direction = get_revision_direction(group[group['type'] == 'Revenue']['abs_change'])

            if eps_direction == "N/A" and rev_direction == "N/A": continue

            event_parts = []
            if eps_direction != "N/A": event_parts.append(f"EPS {eps_direction}")
            if rev_direction != "N/A": event_parts.append(f"Revenue {rev_direction}")

            new_event = {"date_of_change": latest_date_str, "event": ", ".join(event_parts)}

            if ticker in catalyst_data:
                if not any(event['date_of_change'] == latest_date_str for event in catalyst_data[ticker]):
                    catalyst_data[ticker].insert(0, new_event)
            else:
                catalyst_data[ticker] = [new_event]

        with fs.open(gcs_path, 'w') as f:
            f.write(json.dumps(catalyst_data, indent=2))

        print(f"[INFO] Catalyst event log 'gs://{gcs_path}' updated successfully.")

    def send_email_notification(subject, body, attachment_path, attachment_filename):
        sender_email = "anuashwork@gmail.com"
        client = secretmanager.SecretManagerServiceClient()
        name = "projects/555005178535/secrets/email_app_password/versions/latest"
        response = client.access_secret_version(request={"name": name})
        password = response.payload.data.decode("UTF-8")
        #password = "vapw nkcy jlup rsrs"

        to_addrs = ["anuashwork@gmail.com"]
        cc_addrs = [] # Add any CC emails here
        bcc_addrs = ["ashwinram232@gmail.com", "anubuthi.kottapalli@gmail.com"]

        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = ", ".join(to_addrs)
        msg['Cc'] = ", ".join(cc_addrs)
        msg['Subject'] = subject

        msg.attach(MIMEText(body, 'html'))

        with open(attachment_path, "rb") as attachment:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(attachment.read())
        encoders.encode_base64(part)
        part.add_header("Content-Disposition", f"attachment; filename= {attachment_filename}")
        msg.attach(part)

        try:
            server = smtplib.SMTP('smtp.gmail.com', 587)
            server.starttls()
            server.login(sender_email, password)
            all_recipients = to_addrs + cc_addrs + bcc_addrs
            server.sendmail(sender_email, all_recipients, msg.as_string())
            server.quit()
            print(f"[INFO] Email notification sent successfully to {len(all_recipients)} recipients.")
        except Exception as e:
            print(f"[ERROR] Failed to send email. Error: {e}")

    # ------------------ MAIN EXECUTION LOGIC ------------------
    file_paths, latest_date, prev_date = get_latest_data_paths(GCS_DATA_PATH)
    if not file_paths:
        print("[FATAL] Could not find all required files. Aborting.")
        return

    eps_new = process_file_to_long(file_paths['latest_eps'], 'eps_new')
    eps_old = process_file_to_long(file_paths['prev_eps'], 'eps_old')
    rev_new = process_file_to_long(file_paths['latest_rev'], 'revenue_new')
    rev_old = process_file_to_long(file_paths['prev_rev'], 'revenue_old')
    market_cap_df = load_market_cap_data(file_paths['market_cap'])

    eps_changes = merge_and_calculate_diff(eps_old, eps_new, 'eps')
    rev_changes = merge_and_calculate_diff(rev_old, rev_new, 'revenue')

    eps_changes['type'], rev_changes['type'] = 'EPS', 'Revenue'
    eps_changes.rename(columns={'eps_old': 'old_value', 'eps_new': 'new_value'}, inplace=True)
    rev_changes.rename(columns={'revenue_old': 'old_value', 'revenue_new': 'new_value'}, inplace=True)

    report_df = pd.concat([eps_changes, rev_changes], ignore_index=True)

    if not report_df.empty:
        if not market_cap_df.empty:
            # [MODIFIED] Changed to 'inner' merge to drop rows without market cap data
            report_df = pd.merge(report_df, market_cap_df, on='ticker', how='inner')
            report_df['marketCapBin'] = report_df['marketCapitalization'].apply(get_market_cap_bin)
        else:
            # If no market cap data exists at all, the report will be empty
            report_df = pd.DataFrame()
        
        if not report_df.empty:
            report_df = report_df.sort_values(by=['period_type', 'period', 'ticker'])

    if not report_df.empty and 'ticker' in report_df.columns:
        print(f"[INFO] Found {report_df['ticker'].nunique()} unique tickers with revisions.")
    else:
        print("[INFO] No revisions found or data is malformed.")

    summary = {}
    if not report_df.empty and 'ticker' in report_df.columns:
        revisions_by_mcap = report_df.drop_duplicates(subset=['ticker']).groupby('marketCapBin')['ticker'].count().to_dict()
        summary = {'revisions_by_mcap': revisions_by_mcap}
    else:
        summary = {'revisions_by_mcap': {}}

    # [MODIFIED] HTML template updated to include Company Name
    html_template = Template("""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Daily Financial Revisions Report</title>
        <style>
            @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
            :root {
                --primary-color: #4f46e5; --success-color: #16a34a; --danger-color: #dc2626;
                --light-gray: #f3f4f6; --medium-gray: #e5e7eb; --dark-gray: #4b5563;
                --bg-color: #f9fafb; --card-bg: #ffffff; --text-color: #1f2937;
            }
            body {
                font-family: 'Inter', sans-serif; background-color: var(--bg-color); color: var(--text-color);
                margin: 0; padding: 2rem; font-size: 14px;
            }
            .container { max-width: 1600px; margin: auto; background: var(--card-bg); padding: 2rem; border-radius: 12px; box-shadow: 0 4px 6px -1px rgb(0 0 0 / 0.1), 0 2px 4px -2px rgb(0 0 0 / 0.1); }
            header { display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid var(--medium-gray); padding-bottom: 1rem; margin-bottom: 1.5rem; }
            h1 { font-size: 1.75rem; font-weight: 700; color: #111827; margin: 0; }
            .subtitle { font-size: 0.9rem; color: #6b7280; margin: 0; text-align: right; }
            .summary-container { display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 1rem; margin-bottom: 2rem; }
            .metric-card { background-color: var(--card-bg); padding: 1rem; border-radius: 8px; border: 1px solid var(--medium-gray); }
            .metric-card h3 { margin: 0 0 0.5rem 0; font-size: 0.875rem; color: var(--dark-gray); font-weight: 600; }
            .metric-card .value { font-size: 1.75rem; font-weight: 700; color: var(--text-color); }
            .metric-card .context { font-size: 0.8rem; color: #6b7280; margin-top: 0.25rem; }
            .mcap-summary { list-style: none; padding: 0; margin: 0; font-size: 0.8rem; }
            .mcap-summary li { display: flex; justify-content: space-between; margin-bottom: 0.25rem; }
            .controls { display: flex; justify-content: space-between; align-items: center; margin-bottom: 1rem; flex-wrap: wrap; gap: 1rem;}
            .search-filter-group { display: flex; gap: 0.75rem; align-items: center; flex-wrap: wrap; }
            .search-box input { padding: 0.5rem 0.75rem; border: 1px solid var(--medium-gray); border-radius: 6px; font-size: 0.875rem; background-color: #fff; }
            .filters { display: flex; border-bottom: 2px solid var(--medium-gray); }
            .filters button { padding: 0.6rem 1.2rem; border: none; background-color: transparent; color: var(--dark-gray); cursor: pointer; font-size: 0.9rem; font-weight: 600; transition: all 0.2s ease; border-bottom: 2px solid transparent; margin-bottom: -2px; }
            .filters button.active { color: var(--primary-color); border-bottom-color: var(--primary-color); }
            table { width: 100%; border-collapse: collapse; margin-top: 1rem; }
            th, td { padding: 0.75rem 1rem; text-align: left; border-bottom: 1px solid var(--medium-gray); }
            th { background-color: var(--light-gray); font-weight: 600; font-size: 0.75rem; text-transform: uppercase; color: var(--dark-gray); cursor: pointer; user-select: none; position: sticky; top: 0; }
            th .sort-indicator { opacity: 0.3; display: inline-block; width: 1em; }
            tr:hover { background-color: #f9fafb; }
            td { font-size: 0.875rem; }
            td.monospace { font-family: 'SFMono-Regular', Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; }
            .change-pos { color: var(--success-color); font-weight: 500; }
            .change-neg { color: var(--danger-color); font-weight: 500; }
            .ticker { font-weight: 600; color: var(--primary-color); }
            .filter-button { padding: 0.5rem 1rem; border: 1px solid var(--medium-gray); border-radius: 6px; font-size: 0.875rem; background-color: #fff; cursor: pointer; display: flex; align-items: center; gap: 0.5rem; }
            .filter-button .badge { background-color: var(--primary-color); color: white; font-size: 0.75rem; padding: 0.1rem 0.4rem; border-radius: 10px; }
            .filter-panel { display: none; position: fixed; top: 0; right: 0; width: 350px; height: 100%; background-color: #fff; box-shadow: -5px 0 15px rgba(0,0,0,0.1); z-index: 1000; display: flex; flex-direction: column; transform: translateX(100%); transition: transform 0.3s ease-in-out; }
            .filter-panel.open { transform: translateX(0); }
            .filter-panel-header { padding: 1rem 1.5rem; border-bottom: 1px solid var(--medium-gray); }
            .filter-panel-header h3 { margin: 0; }
            .filter-panel-body { padding: 1.5rem; overflow-y: auto; flex-grow: 1; }
            .filter-group { margin-bottom: 1.5rem; }
            .filter-options { max-height: 200px; overflow-y: auto; border: 1px solid var(--medium-gray); border-radius: 6px; padding: 0.5rem; }
            .filter-options label { display: flex; align-items: center; margin-bottom: 0.5rem; cursor: pointer; }
            .filter-panel-footer { padding: 1rem 1.5rem; border-top: 1px solid var(--medium-gray); display: flex; gap: 0.5rem; }
            .btn-primary { background-color: var(--primary-color); color: white; border: none; padding: 0.6rem 1rem; border-radius: 6px; cursor: pointer; }
            .btn-secondary { background-color: var(--light-gray); color: var(--dark-gray); border: 1px solid var(--medium-gray); padding: 0.6rem 1rem; border-radius: 6px; cursor: pointer; }
        </style>
    </head>
    <body>
        <div class="container">
             <header>
                <h1>Daily Financial Revisions</h1>
                <div class="subtitle">Comparison: <b>{{ prev_date }}</b> ➝ <b>{{ latest_date }}</b></div>
            </header>
            {% if report_df_empty %}
                <h2>No Revisions Detected</h2>
            {% else %}
                <section class="summary-container" id="summarySection">
                    <div class="metric-card" id="tickersChangedCard"><div class="value"></div><div class="context"></div></div>
                    <div class="metric-card"><h3>Revisions by Market Cap</h3><ul class="mcap-summary">
                        {% for bin, count in summary.revisions_by_mcap.items() %}
                        <li><span>{{ bin }}</span> <strong>{{ count }}</strong></li>
                        {% endfor %}
                    </ul></div>
                    <div class="metric-card" id="upwardMoverCard"><h3>Top Upward Mover</h3><div class="value change-pos"></div><div class="context"></div></div>
                    <div class="metric-card" id="downwardMoverCard"><h3>Top Downward Mover</h3><div class="value change-neg"></div><div class="context"></div></div>
                </section>
                <section class="data-section">
                    <div class="controls">
                        <div class="search-filter-group">
                            <input type="text" id="searchInput" oninput="debouncedFilter()" placeholder="Search ticker or name...">
                            <button class="filter-button" onclick="toggleFilterPanel()"><svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M22 3H2l8 9.46V19l4 2v-8.54L22 3z"/></svg><span>Filters</span><span class="badge" id="filterBadge" style="display: none;"></span></button>
                        </div>
                        <div class="filters" id="view-toggle-filters">
                            {% if has_quarter_data %}<button class="active" onclick="setView('Revisions', this, 'Quarter')">Quarters</button>{% endif %}
                            {% if has_year_data %}<button class="{{ 'active' if not has_quarter_data else '' }}" onclick="setView('Revisions', this, 'Year')">Years</button>{% endif %}
                            <button onclick="setView('Tickers', this)">Ticker Summary</button>
                        </div>
                    </div>
                    <table id="revisionsTable">
                        <thead id="revisionsHeader"><tr><th onclick="sortTable(0)">Ticker</th><th onclick="sortTable(1)">Company Name</th><th onclick="sortTable(2)">Period</th><th onclick="sortTable(3)">Type</th><th onclick="sortTable(4)">M. Cap Bin</th><th onclick="sortTable(5)">M. Cap</th><th onclick="sortTable(6)">Old Value</th><th onclick="sortTable(7)">New Value</th><th onclick="sortTable(8)">Abs. Change</th><th onclick="sortTable(9)">% Change</th></tr></thead>
                        <thead id="tickerSummaryHeader" style="display: none;"><tr><th onclick="sortTable(0)">Ticker</th><th onclick="sortTable(1)">Company Name</th><th onclick="sortTable(2)">M. Cap Bin</th><th onclick="sortTable(3)">M. Cap</th><th onclick="sortTable(4)">EPS Revisions</th><th onclick="sortTable(5)">Revenue Revisions</th><th onclick="sortTable(6)">Avg EPS % Change</th><th onclick="sortTable(7)">Avg Rev % Change</th></tr></thead>
                        <tbody id="report-table-body"></tbody>
                    </table>
                </section>
            {% endif %}
        </div>
        <div class="filter-panel" id="filterPanel">
            <div class="filter-panel-header"><h3>Filter Options</h3></div>
            <div class="filter-panel-body">
                <div class="filter-group"><h4>Periods</h4><div class="filter-options" id="periodFilterOptions">
                    {% for period in all_periods %}<label><input type="checkbox" value="{{ period }}"> {{ period }}</label>{% endfor %}
                </div></div>
                <div class="filter-group"><h4>Market Cap</h4><div class="filter-options" id="mcapFilterOptions">
                    {% for bin in mcap_bins %}<label><input type="checkbox" value="{{ bin }}"> {{ bin }}</label>{% endfor %}
                </div></div>
            </div>
            <div class="filter-panel-footer"><button class="btn-secondary" onclick="clearFilters()">Clear</button><button class="btn-primary" onclick="applyFilters()">Apply</button></div>
        </div>
        <script>
            const reportData = {{ data_json | safe }};
            let currentView = 'Revisions';
            let currentPeriodType = '{{ "Quarter" if has_quarter_data else "Year" }}';
            const searchInput = document.getElementById('searchInput');
            const tableBody = document.getElementById('report-table-body');
            const filterPanel = document.getElementById('filterPanel');
            const quarterData = reportData.filter(r => r.period_type === 'Quarter');
            const yearData = reportData.filter(r => r.period_type === 'Year');

            function renderTable(data) {
                const fragment = document.createDocumentFragment();
                for (const row of data) {
                    const tr = document.createElement('tr');
                    const absChangeClass = row.abs_change > 0 ? 'change-pos' : 'change-neg';
                    const pctChange = row.pct_change === Infinity ? '+&infin;%' : row.pct_change != null ? `${row.pct_change > 0 ? '+' : ''}${row.pct_change.toFixed(2)}%` : 'N/A';
                    tr.innerHTML = `<td class="ticker">${row.ticker}</td><td>${row.companyName || 'N/A'}</td><td>${row.period}</td><td>${row.type}</td><td>${row.marketCapBin}</td><td>${formatMarketCap(row.marketCapitalization)}</td><td class="monospace">${row.old_value.toFixed(3)}</td><td class="monospace">${row.new_value.toFixed(3)}</td><td class="monospace ${absChangeClass}">${row.abs_change > 0 ? '+' : ''}${row.abs_change.toFixed(3)}</td><td class="monospace ${absChangeClass}">${pctChange}</td>`;
                    fragment.appendChild(tr);
                }
                tableBody.innerHTML = ''; tableBody.appendChild(fragment);
            }
            function renderTickerSummary(data) {
                const summary = {};
                for (const row of data) {
                    if (!summary[row.ticker]) {
                        summary[row.ticker] = { epsRevisions: 0, revRevisions: 0, epsChanges: [], revChanges: [], companyName: row.companyName, marketCapBin: row.marketCapBin, marketCapitalization: row.marketCapitalization };
                    }
                    const s = summary[row.ticker];
                    if (row.type === 'EPS') { s.epsRevisions++; if (row.pct_change !== Infinity && row.pct_change != null) s.epsChanges.push(row.pct_change); }
                    else { s.revRevisions++; if (row.pct_change !== Infinity && row.pct_change != null) s.revChanges.push(row.pct_change); }
                }
                const fragment = document.createDocumentFragment();
                for (const ticker in summary) {
                    const s = summary[ticker];
                    const avgEps = s.epsChanges.length ? s.epsChanges.reduce((a, b) => a + b, 0) / s.epsChanges.length : 0;
                    const avgRev = s.revChanges.length ? s.revChanges.reduce((a, b) => a + b, 0) / s.revChanges.length : 0;
                    const tr = document.createElement('tr');
                    tr.innerHTML = `<td class="ticker">${ticker}</td><td>${s.companyName || 'N/A'}</td><td>${s.marketCapBin}</td><td>${formatMarketCap(s.marketCapitalization)}</td><td>${s.epsRevisions}</td><td>${s.revRevisions}</td><td class="monospace ${avgEps > 0 ? 'change-pos' : 'change-neg'}">${avgEps > 0 ? '+' : ''}${avgEps.toFixed(2)}%</td><td class="monospace ${avgRev > 0 ? 'change-pos' : 'change-neg'}">${avgRev > 0 ? '+' : ''}${avgRev.toFixed(2)}%</td>`;
                    fragment.appendChild(tr);
                }
                tableBody.innerHTML = ''; tableBody.appendChild(fragment);
            }
            function filterAndRender() {
                const sQuery = searchInput.value.toLowerCase();
                const pQuery = [...document.querySelectorAll('#periodFilterOptions input:checked')].map(el => el.value);
                const mQuery = [...document.querySelectorAll('#mcapFilterOptions input:checked')].map(el => el.value);
                let sourceData = (currentView === 'Tickers') ? reportData : (currentPeriodType === 'Quarter' ? quarterData : yearData);
                const filteredData = sourceData.filter(row => (row.ticker.toLowerCase().includes(sQuery) || (row.companyName && row.companyName.toLowerCase().includes(sQuery))) && (currentView === 'Tickers' || pQuery.length === 0 || pQuery.includes(row.period)) && (mQuery.length === 0 || mQuery.includes(row.marketCapBin)));
                if (currentView === 'Revisions') { renderTable(filteredData); } else { renderTickerSummary(filteredData); }
                updateSummary(filteredData);
            }
            function updateSummary(visibleData) {
                const tickers = [...new Set(visibleData.map(r => r.ticker))];
                const tickersChangedCard = document.querySelector('#tickersChangedCard');
                const upwardMoverCard = document.querySelector('#upwardMoverCard');
                const downwardMoverCard = document.querySelector('#downwardMoverCard');
                tickersChangedCard.querySelector('.value').textContent = tickers.length;
                tickersChangedCard.querySelector('.context').textContent = 'Visible Tickers';
                if (visibleData.length > 0) {
                    let topUp = { ticker: 'N/A', change: -Infinity }, topDown = { ticker: 'N/A', change: Infinity };
                    const avgChanges = {};
                    for (const row of visibleData) {
                        if (!avgChanges[row.ticker]) avgChanges[row.ticker] = [];
                        if (row.pct_change !== Infinity && row.pct_change != null) { avgChanges[row.ticker].push(row.pct_change); }
                    }
                    for (const ticker in avgChanges) {
                        if (avgChanges[ticker].length === 0) continue;
                        const avg = avgChanges[ticker].reduce((a,b) => a+b, 0) / avgChanges[ticker].length;
                        if (avg > topUp.change) topUp = { ticker, change: avg };
                        if (avg < topDown.change) topDown = { ticker, change: avg };
                    }
                    upwardMoverCard.querySelector('.value').textContent = topUp.ticker !== 'N/A' ? `${topUp.change > 0 ? '+' : ''}${topUp.change.toFixed(2)}%` : 'N/A';
                    upwardMoverCard.querySelector('.context').textContent = topUp.ticker;
                    downwardMoverCard.querySelector('.value').textContent = topDown.ticker !== 'N/A' ? `${topDown.change > 0 ? '+' : ''}${topDown.change.toFixed(2)}%`: 'N/A';
                    downwardMoverCard.querySelector('.context').textContent = topDown.ticker;
                } else {
                    upwardMoverCard.querySelector('.value').textContent = 'N/A'; upwardMoverCard.querySelector('.context').textContent = '';
                    downwardMoverCard.querySelector('.value').textContent = 'N/A'; downwardMoverCard.querySelector('.context').textContent = '';
                }
            }
            function setView(view, btnElement, periodType = null) {
                currentView = view; if (periodType) currentPeriodType = periodType;
                document.querySelectorAll('#view-toggle-filters button').forEach(btn => btn.classList.remove('active'));
                btnElement.classList.add('active');
                document.getElementById('revisionsHeader').style.display = view === 'Revisions' ? '' : 'none';
                document.getElementById('tickerSummaryHeader').style.display = view === 'Tickers' ? '' : 'none';
                filterAndRender();
            }
            function formatMarketCap(value) {
                if (value == null || isNaN(value)) return "N/A";
                const actualValue = value * 1e6;
                if (actualValue >= 1e12) return `$${(actualValue/1e12).toFixed(2)}T`;
                if (actualValue >= 1e9) return `$${(actualValue/1e9).toFixed(2)}B`;
                if (actualValue >= 1e6) return `$${(actualValue/1e6).toFixed(2)}M`;
                return `$${(actualValue/1e3).toFixed(2)}K`;
            }
            let debounceTimer;
            function debouncedFilter() { clearTimeout(debounceTimer); debounceTimer = setTimeout(filterAndRender, 300); }
            function toggleFilterPanel() { filterPanel.classList.toggle('open'); }
            function applyFilters() { filterAndRender(); toggleFilterPanel(); updateFilterBadge(); }
            function clearFilters() { document.querySelectorAll('#periodFilterOptions input:checked, #mcapFilterOptions input:checked').forEach(el => el.checked = false); applyFilters(); }
            function updateFilterBadge() {
                const count = document.querySelectorAll('#periodFilterOptions input:checked, #mcapFilterOptions input:checked').length;
                const badge = document.getElementById('filterBadge');
                if (count > 0) { badge.textContent = count; badge.style.display = 'inline-block'; }
                else { badge.style.display = 'none'; }
            }
            document.addEventListener('DOMContentLoaded', () => filterAndRender());
        </script>
    </body>
    </html>
    """)

    if not report_df.empty:
        latest_date_str = extract_date_from_path(file_paths['latest_eps'])
        prev_date_str = extract_date_from_path(file_paths['prev_eps'])
        all_periods = sorted(report_df['period'].unique())
        mcap_bins = sorted(report_df['marketCapBin'].dropna().unique())
        has_quarter_data = 'Quarter' in report_df['period_type'].unique()
        has_year_data = 'Year' in report_df['period_type'].unique()
        data_json = report_df.to_json(orient='records')

        html_output = html_template.render(
            report_df_empty=report_df.empty,
            latest_date=latest_date_str, prev_date=prev_date_str, summary=summary,
            data_json=data_json, all_periods=all_periods, mcap_bins=mcap_bins,
            has_quarter_data=has_quarter_data, has_year_data=has_year_data
        )

        output_filename = f"financial_revisions_report_{prev_date_str}_vs_{latest_date_str}.html"
        local_temp_path = f"/tmp/{output_filename}"

        with open(local_temp_path, "w", encoding="utf-8") as f:
            f.write(html_output)

        gcs_output_path = f"{HTML_OUTPUT_PATH}/{output_filename}"
        fs.put(local_temp_path, gcs_output_path)
        print(f"\n[✅ DONE] Professional report uploaded to: gs://{gcs_output_path}")

        email_subject = f"Daily Financial Revisions Report: {latest_date_str}"
        email_body_html = f"""
        <html><body>
        <h2>Daily Revisions Summary for {latest_date_str}</h2>
        <p>This report details the financial estimate revisions detected between {prev_date} and {latest_date}.</p>
        <h3>Tickers with Revisions by Market Cap:</h3>
        <ul>
            {''.join([f'<li><b>{b}:</b> {c}</li>' for b, c in summary.get('revisions_by_mcap', {}).items()])}
        </ul>
        <p>The full interactive report is attached.</p>
        </body></html>
        """
        send_email_notification(email_subject, email_body_html, local_temp_path, output_filename)
        os.remove(local_temp_path)

        update_catalyst_events(report_df, latest_date_str, CATALYST_FILE_PATH)

    else:
        print("\n[INFO] No changes detected. Report not generated.")
if __name__ == "__main__":
    generate_daily_revisions_report()
