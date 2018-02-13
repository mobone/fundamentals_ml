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

def last_friday_of_month(month=None, year=None):
    month = month or date.today().month
    year = year or date.today().year
    return date(year, month, 1) + relativedelta(
        day=calendar.monthrange(year, month)[1],
        weekday=FR(-1))

class company(threading.Thread):
    def __init__(self, q, hold_time):
        threading.Thread.__init__(self)
        self.hold_time = hold_time
        self.q = q


    def run(self):
        sleep(3)
        print('thread starting')
        self.conn = sqlite3.connect("data.db")
        self.couchserver = couchdb.Server('http://mobone:C00kie32!@192.168.1.24:5984/')
        self.db = self.couchserver['finviz_data']

        exclude = ['Sector', 'Industry', 'Index', 'Date', 'Earnings',
                   '52W Range', 'Volatility', 'Optionable', 'Shortable',
                   'Ticker', '_id', '_rev']
        while not workQueue.empty():
            self.doc_id = self.q.get()
            doc = self.db[self.doc_id]
            print(self.doc_id)
            df = pd.DataFrame(doc, index = [self.doc_id]).T
            for i in df.iterrows():
                key = i[0]
                if key in exclude:
                    continue

                value = self.convert_to_num(i)
                df[self.doc_id][key] = value
            try:
                stock_perc_change = self.get_price_change(self.hold_time)
                index_perc_change = self.get_price_change(self.hold_time, spy_index=True)
                df.loc['stock_perc_change_'+str(hold_time)] = stock_perc_change
                df.loc['index_perc_change_'+str(hold_time)] = index_perc_change
                df.loc['abnormal_perc_change_'+str(hold_time)] = stock_perc_change - index_perc_change
                """
                stock_perc_change = self.get_price_change(10)
                index_perc_change = self.get_price_change(10, spy_index=True)
                df.loc['stock_perc_change_10'] = stock_perc_change
                df.loc['index_perc_change_10'] = index_perc_change
                df.loc['abnormal_perc_change_10'] = stock_perc_change - index_perc_change
                """
            except Exception as e:
                continue


            df = df.T
            df.to_sql('data', self.conn, index=False, if_exists='append')



    def convert_to_num(self, row):
        key = row[0]
        value = row[1].values[0]
        value = value.replace('%','')
        if value[-1:] == 'B':
            value = float(value.replace('B',''))*1000000000
        elif value[-1:] == 'M':
            value = float(value.replace('M',''))*1000000
        elif value[-1:] == 'K':
            value = float(value.replace('K',''))*1000
        #print(key, value)
        if value == '-':
            value = None
        return pd.to_numeric(value)

    def get_price_change(self, hold_time, spy_index=False):

        (symbol, date) = self.doc_id.split('_')

        start_date = datetime.strptime(date, '%m-%d-%Y')
        start_date = start_date + timedelta(days=1)
        end_date = start_date + timedelta(days=hold_time-1)

        if spy_index:
            history = web.DataReader('SPY', 'iex', start_date, end_date)
        else:
            history = web.DataReader(symbol, 'iex', start_date, end_date)

        start_date = start_date.strftime('%Y-%m-%d')
        end_date = end_date.strftime('%Y-%m-%d')
        history = history.reset_index()

        #open_price = history[history['date']==start_date]['open'].values[0]
        #close_price = history[history['date']==end_date]['close'].values[0]
        open_price = history['open'].head(0).values[0]
        close_price = history['close'].tail(0).values[0]
        percent_change = (close_price-open_price) / open_price

        return percent_change

"""
#get fridays
fridays = []
for month,year in [(10,2017),(11,2017),(12,2017),(1,2018),(2,2018)]:
    fridays.append(last_friday_of_month(month,year).strftime('%m-%d-%Y'))
print(fridays)
"""
pull_dates = ['10-31-17', '11-30-2017', '12-22-17']

workQueue = queue.Queue()

conn = sqlite3.connect("data.db")
cur = conn.cursor()
cur.executescript('drop table if exists data')

couchserver = couchdb.Server('http://mobone:C00kie32!@192.168.1.24:5984/')
db = couchserver['finviz_data']
for docid in db.view('_all_docs'):
    i = docid['id']
    date = i.split('_')[1]
    date = datetime.strptime(date, '%m-%d-%Y')
    """
    if date.weekday() == 4:
        workQueue.put(i)
    """
    if date in pull_dates:
        workQueue.put(i)

for i in range(10):
    print('starting thread')
    thread = company(workQueue,20)
    thread.start()
    sleep(.5)


while not workQueue.empty():
    sleep(1)
