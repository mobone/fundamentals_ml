import pandas as pd
import couchdb
import pandas_datareader.data as web
from datetime import datetime, timedelta
import sqlite3
import queue
import threading
from nyse_holidays import *
from time import sleep
from dateutil.relativedelta import relativedelta, FR
import calendar
from get_data import company
couchserver = couchdb.Server('http://mobone:C00kie32!@68.63.209.203:5984/')
db = couchserver['finviz_data']
print('connected')
for item in db.view('date/date-view', key="02-13-2018"):
    company(item.id, item.value)
