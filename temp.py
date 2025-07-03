from datetime import time
import pandas as pd
import pandas_market_calendars as mcal

nyse = mcal.get_calendar('NYSE')

holidays = nyse.holidays()
holidays.holidays[-5:]

print(nyse.valid_days(start_date='2025-06-01', end_date='2025-08-10'))