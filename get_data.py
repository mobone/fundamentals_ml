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
import requests_cache

def last_friday_of_month(month=None, year=None):
    month = month or date.today().month
    year = year or date.today().year
    return date(year, month, 1) + relativedelta(
        day=calendar.monthrange(year, month)[1],
        weekday=FR(-1))


class worker(threading.Thread):
    def __init__(self, thread_id, q, hold_time):
        threading.Thread.__init__(self)
        self.thread_id = thread_id
        self.hold_time = hold_time
        self.q = q

    def run(self):
        expire_after = timedelta(days=3)
        session = requests_cache.CachedSession(cache_name='cache', backend='sqlite', expire_after=expire_after)

        sleep(3)
        print('thread starting')
        self.conn = sqlite3.connect("data.db")
        self.couchserver = couchdb.Server('http://mobone:C00kie32!@68.63.209.203:5984/')
        self.db = self.couchserver['finviz_data']
        letter = ''
        while not workQueue.empty():
            doc_id = self.q.get()
            doc = self.db[doc_id]
            if doc_id[1]!=letter and self.thread_id == 0:
                print(doc_id)
                letter = doc_id[1]
            company(doc_id, doc, self.conn, self.hold_time, session)


class company(object):
    def __init__(self, doc_id, doc, conn, hold_time = None, session = None):
        self.conn = conn
        self.session = session

        self.doc = doc
        self.doc_id = doc_id
        self.hold_time = hold_time
        self.create_company()

    def create_company(self):
        exclude = ['Sector', 'Industry', 'Index', 'Date', 'Earnings',
                   '52W Range', 'Volatility', 'Optionable', 'Shortable',
                   'Ticker', '_id', '_rev']
        df = pd.DataFrame(self.doc, index = [self.doc_id]).T

        for i in df.iterrows():
            key = i[0]
            if key in exclude:
                continue
            value = self.convert_to_num(i)
            df[self.doc_id][key] = value

        try:
            if self.hold_time:
                stock_perc_change = self.get_price_change(self.hold_time)
                index_perc_change = self.get_price_change(self.hold_time, spy_index=True)
                df.loc['stock_perc_change_'+str(self.hold_time)] = stock_perc_change
                df.loc['index_perc_change_'+str(self.hold_time)] = index_perc_change
                df.loc['abnormal_perc_change_'+str(self.hold_time)] = stock_perc_change - index_perc_change
        except Exception as e:
            return

        df = df.T
        if self.hold_time:
            df.to_sql('data_'+str(self.hold_time), self.conn, index=False, if_exists='append')
        else:
            df.to_sql('alerts', self.conn, index=False, if_exists='append')


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
        start_date = start_date + timedelta(days=3)

        end_date = start_date + timedelta(days=hold_time-1)

        if spy_index:
            history = web.DataReader('SPY', 'iex', start_date, end_date, session=self.session)
        else:
            history = web.DataReader(symbol, 'iex', start_date, end_date, session=self.session)

        start_date = start_date.strftime('%Y-%m-%d')
        end_date = end_date.strftime('%Y-%m-%d')
        history = history.reset_index()

        open_price = history[history['date']==start_date]['open'].values[0]
        close_price = history[history['date']==end_date]['close'].values[0]
        #open_price = history['open'].head(1).values[0]
        #close_price = history['close'].tail(1).values[0]
        percent_change = (close_price-open_price) / open_price

        return percent_change


if __name__ == "__main__":
    conn = sqlite3.connect("data.db")
    cur = conn.cursor()
    cur.executescript('drop table if exists data_10')
    cur.executescript('drop table if exists data_20')

    workQueue = queue.Queue()

    couchserver = couchdb.Server('http://mobone:C00kie32!@68.63.209.203:5984/')
    db = couchserver['finviz_data']
    for docid in db.view('_all_docs'):
        i = docid['id']
        date = i.split('_')[1]


        try:
            if datetime.strptime(date, '%m-%d-%Y').weekday() == 4:
                workQueue.put(i)
        except:
            pass

    for i in range(10):
        print('starting thread')
        thread = worker(i, workQueue,10)
        thread.start()
        sleep(.5)


    while not workQueue.empty():
        sleep(1)
