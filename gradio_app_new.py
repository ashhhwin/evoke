import time
import gradio as gr
from run_full_pipeline import (
    run_finnhub_pipeline,
    run_eodhd_pipeline,
    run_historical_pipeline,
    load_eps_and_revenue_data, 
    get_latest_daily_date,
    get_ticker_data,
    load_latest_eodhd_merged,
    load_historical_close_prices,
    load_eps_revenue_changes,
    run_pipelines_concurrently,
    load_tickers
)
import datetime
from gradio_calendar import Calendar
import plotly.graph_objects as go
import pandas as pd
from compare_eps_revenue import list_available_dates, list_available_periods, compare_eps_revenue
import os
import tempfile
import plotly.express as px
import io
import time
from google.cloud import storage
def read_csv_from_gcs(bucket_name, blob_path, max_retries=5, delay=2):
    """
    Read CSV from GCS with retry logic.
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)

    print(f"Reading from GCS path: {blob_path}")

    for attempt in range(max_retries):
        if blob.exists():
            content = blob.download_as_text()
            return pd.read_csv(io.StringIO(content))
        else:
            print(f"[Attempt {attempt+1}] File not found: {blob_path}. Retrying in {delay}s...")
            time.sleep(delay)

    raise FileNotFoundError(f"File not found in GCS after {max_retries} attempts: {bucket_name}/{blob_path}")

def get_ticker_list():
    try:
        eps_df, _ = load_eps_and_revenue_data()
        tickers = sorted(eps_df["ticker"].dropna().unique().tolist())
        return tickers
    except Exception as e:
        print(f"Error loading tickers: {e}")
        return [f"Error: {e}"]

def run_finnhub():
    try:
        msg = run_finnhub_pipeline()
        return msg if msg else "Finnhub data download completed."
    except Exception as e:
        return f"Failed: {e}"

def run_eodhd():
    try:
        msg = run_eodhd_pipeline()
        return msg if msg else "OHLCV (EODHD) download completed."
    except Exception as e:
        return f"Failed: {e}"

def run_historical(start, end):
    try:
        start_date = start.date()
        end_date = end.date()
        msg = run_historical_pipeline(start_date, end_date)
        return msg if msg else f"Historical data downloaded from {start_date} to {end_date}"
    except Exception as e:
        return f"Failed: {e}"
    
def get_eps_revenue_changes(ticker_filter=None):
    try:
        df = load_eps_revenue_changes()
        if ticker_filter:
            tickers = [t.strip().upper() for t in ticker_filter.split(",")]
            df = df[df["ticker"].isin(tickers)]
        return df
    except Exception as e:
        return pd.DataFrame({"Error": [f"Error loading EPS/Revenue changes: {e}"]})

def plot_eps_revenue(ticker: str, data_type: str):
    try:
        eps_df, rev_df = load_eps_and_revenue_data()
        eps, rev = get_ticker_data(ticker, eps_df, rev_df)
        # Choose only quarterly or annual columns
        if data_type == "Quarterly":
            cols = [c for c in eps.index if c.startswith("Q")]
        else:
            cols = [c for c in eps.index if not c.startswith("Q") and not c == "api_run_date"]
        # EPS Plot
        eps_plot = go.Figure()
        eps_plot.add_trace(go.Scatter(x=cols, y=eps.loc[cols].values.flatten(), mode='lines+markers', name="EPS"))
        eps_plot.update_layout(title=f"{ticker} EPS ({data_type})", xaxis_title="Period", yaxis_title="EPS")
        # Revenue Plot
        rev_plot = go.Figure()
        rev_plot.add_trace(go.Scatter(x=cols, y=rev.loc[cols].values.flatten(), mode='lines+markers', name="Revenue"))
        rev_plot.update_layout(title=f"{ticker} Revenue ({data_type})", xaxis_title="Period", yaxis_title="Revenue (in millions)")
        return eps_plot, rev_plot
    except Exception as e:
        return go.Figure(layout_title_text=f"Error loading EPS: {e}"), go.Figure(layout_title_text=f"Error loading Revenue: {e}")

def display_latest_ticker_snapshot(ticker: str):
    try:
        row = load_latest_eodhd_merged(ticker)

        display_keys = [
        "Trade_Date", "Symbol", "Company_Name", "Sector", "Industry", "MarketCapitalization", "Beta",
        "P_Open", "P_High", "P_Low", "P_Close", "volume", "Prev_Close (Price)",
        "P_50D_MA", "P_200D_MA", "V_14D_MA", "V_50D_MA", "Options", "hi_250d", "lo_250d",
        "Close_to_Open (% from Prev Day Close)", "Open_Close (%)", "High_Close(%)", "Low_Close(%)",
        "Close_to_Close (%)", "Shares_Out", "Shares_Float", "Short_Ratio", "Short_Percent_Float",
        "Earnings_Date", "Shares_Insiders", "Shares_Institutions"

        ]

        row_data = {k: row[k] for k in display_keys if k in row}

        # Format as row-wise table
        formatted = "\n".join([f"{k} : {v}" for k, v in row_data.items()])
        return formatted

    except Exception as e:
        return f"Error loading latest EODHD data: {e}"
    
def run_both_pipelines():
    try:
        try:
            tickers = load_tickers()
        except Exception as e:
            return f"Failed: {e}"
        run_pipelines_concurrently(tickers)
        return "Both Finnhub and EODHD pipelines completed."
    except Exception as e:
        return f"Failed: {e}"
       
def plot_close_price_history(ticker: str):
    try:
        df = load_historical_close_prices(ticker)
        # Sort by date
        df = df.sort_values("Trade_Date")
        colors = ["green" if val >= 0 else "red" for val in df["Close_to_Close (%)"]]
        # ----------- First Plot: Close Price -----------
        fig_close = go.Figure()
        fig_close.add_trace(go.Scatter(
            x=df["Trade_Date"],
            y=df["P_Close"],
            mode="lines+markers",
            name="Close Price"
        ))
        fig_close.update_layout(
            title=f"{ticker.upper()} - Close Price",
            xaxis=dict(
                title="Date",
                rangeselector=dict(
                    buttons=list([
                        dict(count=7, label="1W", step="day", stepmode="backward"),
                        dict(count=30, label="1M", step="day", stepmode="backward"),
                        dict(count=6, label="6M", step="month", stepmode="backward"),
                        dict(count=12, label="1Y", step="month", stepmode="backward"),
                        dict(step="all", label="All")
                    ])
                ),
                rangeslider=dict(visible=True),  # Optional: adds a zoom slider
                type="date"
            ),
            yaxis_title="Price",
            height=400
        )
        # ----------- Second Plot: Volume + MA Lines -----------
        fig_vol = go.Figure()
        fig_vol.add_trace(go.Bar(
            x=df["Trade_Date"],
            y=df["volume"],
            name="Volume",
            marker_color=colors
        ))
        if "V_14D_MA" in df.columns:
            fig_vol.add_trace(go.Scatter(
                x=df["Trade_Date"],
                y=df["V_14D_MA"],
                name="14D MA Volume",
                mode="lines",
                line=dict(color="orange", dash="dash")
            ))
        if "V_50D_MA" in df.columns:
            fig_vol.add_trace(go.Scatter(
                x=df["Trade_Date"],
                y=df["V_50D_MA"],
                name="50D MA Volume",
                mode="lines",
                line=dict(color="blue", dash="dot")
            ))
        fig_vol.update_layout(
            title=f"{ticker.upper()} - Volume + 14D/50D MAs",
            xaxis=dict(
                title="Date",
                rangeselector=dict(
                    buttons=list([
                        dict(count=7, label="1W", step="day", stepmode="backward"),
                        dict(count=30, label="1M", step="day", stepmode="backward"),
                        dict(count=6, label="6M", step="month", stepmode="backward"),
                        dict(count=12, label="1Y", step="month", stepmode="backward"),
                        dict(step="all", label="All")
                    ])
                ),
                rangeslider=dict(visible=True),  # Optional: adds a zoom slider
                type="date"
            ),
            yaxis_title="Volume",
            height=400
        )
        return fig_close, fig_vol
    except Exception as e:
        return (
            go.Figure(layout_title_text=f"Error: {e}"),
            go.Figure(layout_title_text=f"No volume data available: {e}")
        )


def run_comparison(from_date, to_date, period):
    import plotly.express as px

    safe_period = str(period).replace(' ', '').replace('/', '').replace('\\', '').replace(':', '')
    output_file = f"eps_revenue_comparison_{from_date}_to_{to_date}_for_{safe_period}.csv"
    compare_eps_revenue(from_date=from_date, to_date=to_date, quarters=[period] if period else None, output_file=output_file, annual=False)
    #print("I AM HERE")
    gcs_path = f'market_data/revisions/{output_file}'
    #print(f"Reading from GCS path: {gcs_path}")
    #time.sleep(20)
    df = read_csv_from_gcs('historical_data_evoke', gcs_path)
    #df = read_csv_from_gcs('historical_data_evoke',f'market_data/revisions/{output_file}')
    if df is None or df.empty:
        return None, "No data found for the selected options.", "", "", "", "", ""

    df['eps_pct_change'] = pd.to_numeric(df['eps_pct_change'], errors='coerce')
    df['revenue_pct_change'] = pd.to_numeric(df['revenue_pct_change'], errors='coerce')
    df['MarketCapitalization'] = pd.to_numeric(df['MarketCapitalization'], errors='coerce')

    def categorize_market_cap(cap):
        if pd.isna(cap): return "Unknown"
        if cap >= 200000: return "Mega Cap"
        elif cap >= 10000: return "Large Cap"
        elif cap >= 2000: return "Mid Cap"
        elif cap >= 250: return "Small Cap"
        elif cap >= 50: return "Micro Cap"
        else: return "Nano Cap"

    df['MarketCapCategory'] = df['MarketCapitalization'].apply(categorize_market_cap)
    df = df[df['Sector'].notna() & df['Industry'].notna() & df['eps_pct_change'].notna() & df['revenue_pct_change'].notna()]
    df['count'] = 1

    color_scale = [
        [0.0, "yellow"], [0.25, "red"],
        [0.5, "white"], [0.75, "green"], [1.0, "blue"]
    ]

    eps_grouped = df.groupby(['Sector', 'Industry']).agg({'eps_pct_change': 'mean', 'count': 'sum'}).reset_index()
    fig_eps = px.treemap(
        eps_grouped,
        path=['Sector', 'Industry'],
        values='count',
        color='eps_pct_change',
        color_continuous_scale=color_scale,
        color_continuous_midpoint=0,
        custom_data=['eps_pct_change'],
        title='EPS % Change: Sector → Industry'
    )
    fig_eps.update_traces(
        hovertemplate='<b>%{label}</b><br>EPS % Change: %{customdata[0]:.2f}%',
        texttemplate='%{label}<br>%{customdata[0]:.1f}%',
        textposition='middle center'
    )
    eps_plot = fig_eps

    rev_grouped = df.groupby(['Sector', 'Industry']).agg({'revenue_pct_change': 'mean', 'count': 'sum'}).reset_index()
    fig_rev = px.treemap(
        rev_grouped,
        path=['Sector', 'Industry'],
        values='count',
        color='revenue_pct_change',
        color_continuous_scale=color_scale,
        color_continuous_midpoint=0,
        custom_data=['revenue_pct_change'],
        title='Revenue % Change: Sector → Industry'
    )
    fig_rev.update_traces(
        hovertemplate='<b>%{label}</b><br>Revenue % Change: %{customdata[0]:.2f}%',
        texttemplate='%{label}<br>%{customdata[0]:.1f}%',
        textposition='middle center'
    )
    rev_plot = fig_rev
    top_eps_up = df.nlargest(10, 'eps_pct_change')[['ticker', 'Company_Name', 'eps_pct_change']]
    top_eps_down = df.nsmallest(10, 'eps_pct_change')[['ticker', 'Company_Name', 'eps_pct_change']]
    top_rev_up = df.nlargest(10, 'revenue_pct_change')[['ticker', 'Company_Name', 'revenue_pct_change']]
    top_rev_down = df.nsmallest(10, 'revenue_pct_change')[['ticker', 'Company_Name', 'revenue_pct_change']]

    eps_movers_table = (
        "<h4>Top EPS Up</h4>" + top_eps_up.to_html(index=False, escape=False) +
        "<h4>Top EPS Down</h4>" + top_eps_down.to_html(index=False, escape=False)
    )
    rev_movers_table = (
        "<h4>Top Revenue Up</h4>" + top_rev_up.to_html(index=False, escape=False) +
        "<h4>Top Revenue Down</h4>" + top_rev_down.to_html(index=False, escape=False)
    )

    summary = {
        'EPS Up': int((df['eps_pct_change'] > 0).sum()),
        'EPS Down': int((df['eps_pct_change'] < 0).sum()),
        'Revenue Up': int((df['revenue_pct_change'] > 0).sum()),
        'Revenue Down': int((df['revenue_pct_change'] < 0).sum()),
    }
    summary_text = " \
    ".join(f"{k}: {v}" for k, v in summary.items())

    return "Comparison and insights complete.", eps_plot, rev_plot, eps_movers_table, rev_movers_table, summary_text


# Dates & Periods

dates = list_available_dates()
latest = dates[-1] if dates else None
prior = dates[-2] if len(dates) > 1 else None

def get_periods_for_date(date):
    return list_available_periods(date)

def update_periods(to_date):
    periods = get_periods_for_date(to_date)
    return gr.update(choices=periods, value=periods[0] if periods else None)

def download_csv(file_path):
    return file_path


#ashwin changes start here

def load_news_from_gcs(date_str, ticker, bucket_name="historical_data_evoke"):
    """
    Load news JSON from GCS for a given date and ticker.
    """
    blob_path = f"news/{date_str}/{ticker.upper()}.json"
    print(blob_path)
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_path)

        if not blob.exists():
            return f"No news found for {ticker} on {date_str}."

        content = blob.download_as_text()
        data = json.loads(content)

        # Pretty print the JSON (you can format this better if needed)
        pretty_output = json.dumps(data, indent=4)
        return pretty_output

    except Exception as e:
        return f"Error reading news for {ticker} on {date_str}: {e}"


## ashwin changes end here

with gr.Blocks(theme=gr.themes.Soft()) as app:
    gr.Markdown("<h1 style='text-align:center; color:#00ff9d;'>📈 Market Data Dashboard</h1>")

    with gr.Tab("Download Daily Data"):
        gr.Markdown("## Select Data to Download")
        with gr.Row():
            finnhub_btn = gr.Button("Run EPS & Revenue (Finnhub)")
            eodhd_btn = gr.Button("Run OHLCV (EODHD)")
            both_btn = gr.Button("Run Both")
        status_output = gr.Textbox(label="Status", lines=2)
        with gr.Row():
            last_run_label = gr.Textbox(
                value=f"Latest Run Date: {get_latest_daily_date()}",
                label="Last Run (Daily Folder)",
                interactive=False
            )
        finnhub_btn.click(fn=run_finnhub, outputs=status_output)
        eodhd_btn.click(fn=run_eodhd, outputs=status_output)
        both_btn.click(fn=run_both_pipelines, outputs=status_output)

    with gr.Tab("Download Historical Data"):
        gr.Markdown("## Historical OHLCV Download")
        start_date = Calendar(type="date", label="Start Date")
        end_date = Calendar(type="date", label="End Date")
        run_btn = gr.Button("Run Historical Download")
        hist_status = gr.Textbox(label="Status", lines=2)

        run_btn.click(fn=run_historical, inputs=[start_date, end_date], outputs=hist_status)


    with gr.Tab("Ticker Insights"):
        gr.Markdown("## View EPS & Revenue by Ticker")

        with gr.Row():
            ticker_dropdown = gr.Dropdown(
                label="Select Ticker",
                choices=get_ticker_list(),
                interactive=True,
                filterable=True,
                allow_custom_value=False,
                show_label=True,
                scale=2
            )
            data_type = gr.Radio(
                choices=["Quarterly", "Annual"],
                value="Quarterly",
                label="Data Type"
            )

        # EPS and Revenue side-by-side
        with gr.Row():
            eps_plot = gr.Plot(label="EPS Plot")
            rev_plot = gr.Plot(label="Revenue Plot")

        gr.Markdown("## View Close Price and Volume by Ticker")
        # Close price chart below
        close_plot = gr.Plot(label="Close Price History")
        volume_plot = gr.Plot(label="Volume ")
        # Ticker snapshot info at the bottom
        ticker_info = gr.Textbox(label="Latest Ticker Snapshot", lines=30, interactive=False)

        def update_all(ticker, mode):
            try:
                eps_fig, rev_fig = plot_eps_revenue(ticker, mode)
            except Exception as e:
                eps_fig = go.Figure(layout_title_text=f"Error loading EPS: {e}")
                rev_fig = go.Figure(layout_title_text=f"Error loading Revenue: {e}")
            try:
                price_fig, vol_fig = plot_close_price_history(ticker)
            except Exception as e:
                price_fig = go.Figure(layout_title_text=f"Error loading price: {e}")
                vol_fig = go.Figure(layout_title_text=f"Error loading volume: {e}")
            try:
                snapshot = display_latest_ticker_snapshot(ticker)
            except Exception as e:
                snapshot = f"Error loading ticker snapshot: {e}"
            return eps_fig, rev_fig, price_fig, vol_fig, snapshot
    
        # Update all elements when ticker or type changes
        ticker_dropdown.change(
            fn=update_all,
            inputs=[ticker_dropdown, data_type],
            outputs=[eps_plot, rev_plot, close_plot, volume_plot, ticker_info]
        )

        data_type.change(
            fn=update_all,
            inputs=[ticker_dropdown, data_type],
            outputs=[eps_plot, rev_plot, close_plot, volume_plot, ticker_info]
        )

    with gr.Tab("EPS & Revenue Revisions"):
        gr.Markdown("### Compare EPS & Revenue Estimates")
        with gr.Row():
            from_date = gr.Dropdown(label="From Date", choices=dates, value=prior)
            to_date = gr.Dropdown(label="To Date", choices=dates, value=latest)
        period = gr.Dropdown(label="Period", choices=get_periods_for_date(latest), value=get_periods_for_date(latest))
        run_comparison_btn = gr.Button("Run Comparison")
        #output_file = gr.File(label="Download CSV", visible=False)
        status = gr.Textbox(label="Status", interactive=False)

        gr.Markdown("### EPS % Change Treemap")
        eps_treemap_plot = gr.Plot(label="EPS Treemap")
        gr.Markdown("### Revenue % Change Treemap")
        rev_treemap_plot = gr.Plot(label="Revenue Treemap")

        gr.Markdown("### Top EPS Movers")
        eps_movers_table = gr.HTML()

        gr.Markdown("### Top Revenue Movers")
        rev_movers_table = gr.HTML()

        gr.Markdown("### Summary of Revisions")
        summary_box = gr.Textbox(lines=8, interactive=False)

        to_date.change(fn=update_periods, inputs=[to_date], outputs=period)

        run_comparison_btn.click(
            fn=run_comparison,
            inputs=[from_date, to_date, period],
            outputs=[ status, eps_treemap_plot, rev_treemap_plot, eps_movers_table, rev_movers_table, summary_box]
        )

## ashwin changes start here

    with gr.Tab("Market News by Ticker"):
        gr.Markdown("## View Market News by Ticker and Date")
    
        with gr.Row():
            news_date = gr.Dropdown(label="Select Date", choices=dates, value=latest)
            news_ticker = gr.Dropdown(label="Select Ticker", choices=get_ticker_list(), value=None)
    
        load_news_btn = gr.Button("Load News")
        news_output = gr.Code(label="News JSON", language="json", lines=20)
    
        load_news_btn.click(
            fn=load_news_from_gcs,
            inputs=[news_date, news_ticker],
            outputs=news_output
        )

##ashwin changes end here

app.launch(server_name="0.0.0.0", server_port=7861)
