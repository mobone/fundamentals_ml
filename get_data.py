import pandas as pd
import couchdb
import pandas_datareader.data as web
from datetime import datetime, timedelta
import sqlite3
import queue
import threading
from nyse_holidays import *
from time import sleep

class company(threading.Thread):
    def __init__(self, q):
        threading.Thread.__init__(self)

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
                stock_perc_change = self.get_price_change(5)
                index_perc_change = self.get_price_change(5, spy_index=True)
                df.loc['stock_perc_change_5'] = stock_perc_change
                df.loc['index_perc_change_5'] = index_perc_change
                df.loc['abnormal_perc_change_5'] = stock_perc_change - index_perc_change
                stock_perc_change = self.get_price_change(10)
                index_perc_change = self.get_price_change(10, spy_index=True)
                df.loc['stock_perc_change_10'] = stock_perc_change
                df.loc['index_perc_change_10'] = index_perc_change
                df.loc['abnormal_perc_change_10'] = stock_perc_change - index_perc_change
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
        start_date = start_date + timedelta(days=3)
        end_date = start_date + timedelta(days=hold_time-1)
        """
        if start_date.strftime('%y%m%d') in NYSE_holidays():
            print('holiday')
            raise ValueError('Holiday')
        """

        if spy_index:
            history = web.DataReader('SPY', 'iex', start_date, end_date)
        else:
            history = web.DataReader(symbol, 'iex', start_date, end_date)

        start_date = start_date.strftime('%Y-%m-%d')
        end_date = end_date.strftime('%Y-%m-%d')
        history = history.reset_index()

        open_price = history[history['date']==start_date]['open'].values[0]
        close_price = history[history['date']==end_date]['close'].values[0]
        percent_change = (close_price-open_price) / open_price

        return percent_change

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
    if date.weekday() == 4:
        workQueue.put(i)

for i in range(10):
    print('starting thread')
    thread = company(workQueue)
    thread.start()
    sleep(.5)


while not workQueue.empty():
    sleep(1)
