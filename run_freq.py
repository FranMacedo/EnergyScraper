from run import run_mgmt
from datetime import datetime as dt
import calendar
from dateutil.relativedelta import relativedelta

today_date = dt.now().date()


if today_date.day < 10:
    date_start = today_date - relativedelta(months=1, day=1)
else:
    date_start = today_date - relativedelta(day=1)

run_mgmt('EGEAC', date_start=date_start, replace=True, headless=False)
run_mgmt('SCML', date_start=date_start, replace=True, headless=False)
run_mgmt('CML', date_start=date_start, replace=True, headless=False)
