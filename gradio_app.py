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
from script import compute_eps_revenue_change_csv

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

with gr.Blocks() as app:
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
        start_date = Calendar(type="string", label="Start Date")
        end_date = Calendar(type="string", label="End Date")
        run_btn = gr.Button("Run Historical Download")
        hist_status = gr.Textbox(label="Status", lines=2)

        run_btn.click(fn=run_historical, inputs=[start_date, end_date], outputs=hist_status)

    with gr.Tab("EPS & Revenue Changes"):
        gr.Markdown("### View EPS/Revenue Estimate Revisions")

        ticker_input = gr.Textbox(label="Filter by Ticker(s) (comma-separated)", placeholder="e.g. AAPL, MSFT, NVDA")
        show_all_btn = gr.Button("Show All Changes")
        filter_btn = gr.Button("Show Filtered")

        changes_table = gr.Dataframe(label="Changes Table", wrap=True)

        show_all_btn.click(
            fn=lambda: get_eps_revenue_changes(),
            inputs=[],
            outputs=changes_table
        )

        filter_btn.click(
            fn=get_eps_revenue_changes,
            inputs=[ticker_input],
            outputs=changes_table
        )

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

    with gr.Tab("EPS/Revenue Change Export"):
        gr.Markdown("### Export EPS & Revenue Change Comparison CSV")
        with gr.Row():
            start_date_picker = Calendar(type="string", label="Start Date (default: 4 weeks ago)")
            end_date_picker = Calendar(type="string", label="End Date (default: latest)")
        export_btn = gr.Button("Generate CSV")
        file_output = gr.File(label="Download CSV")

        def run_eps_rev_change_export(start, end):
            # start and end are string (YYYY-MM-DD) or None
            s = start if start else None
            e = end if end else None
            out_path = "market_data/eps_revenue_change_comparison.csv"
            compute_eps_revenue_change_csv(start_date=s, end_date=e, output_path=out_path)
            return out_path

        export_btn.click(
            fn=run_eps_rev_change_export,
            inputs=[start_date_picker, end_date_picker],
            outputs=file_output
        )

app.launch(server_name="0.0.0.0", server_port=7860)
