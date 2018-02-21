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
from os import listdir
from os.path import isfile, join
from sklearn.externals import joblib
import numpy as np

conn = sqlite3.connect("data.db")

def convert_to_num(value):
    if value[-1:] == 'B':
        value = float(value.replace('B',''))*1000000000
    elif value[-1:] == 'M':
        value = float(value.replace('M',''))*1000000
    elif value[-1:] == 'K':
        value = float(value.replace('K',''))*1000
    return value


def get_data_from_couchdb(date):

    couchserver = couchdb.Server('http://mobone:C00kie32!@68.63.209.203:5984/')
    db = couchserver['finviz_data']
    print('connected')

    df = pd.read_sql('select * from alert_data where Date=="%s"' % date, conn)

    if df.empty:
        for item in db.view('date/date-view', key=date):

            marketcap = convert_to_num(item.value['Market Cap'])
            if marketcap<2000000000:
                print(item.id)
                company(item.id, item.value, conn)
        df = pd.read_sql('select * from alert_data where Date=="%s"' % date, conn)
        return df
    else:
        return df

def get_machines():
    files = [f for f in listdir('machines') if isfile(join('machines', f))]
    machines = []
    for f in files:
        machine = {}
        machine['model'] = joblib.load('machines/'+f)
        machine['name'] = f
        df = pd.read_sql('select * from results where features = "%s"' % f, conn)
        machine['pos_cutoff'] = df.ix[:,'Predicted-mean_pos'].values[0]
        machine['neg_cutoff'] = df.ix[:,'Predicted-mean_neg'].values[0]

        machines.append(machine)
    return machines

def get_open_close_dates(today):
    today = today + timedelta(days=1)
    while today.strftime('%y%m%d') == NYSE_holidays()[0].strftime('%y%m%d') or today.weekday()>=5:
        today = today + timedelta(days=1)
    start_date = today.strftime('%m-%d-%Y')
    for i in range(9):
        today = today + timedelta(days=1)
        while today.strftime('%y%m%d') == NYSE_holidays()[0].strftime('%y%m%d') or today.weekday()>=5:
            today = today + timedelta(days=1)
    end_date = today.strftime('%m-%d-%Y')

    return (start_date, end_date)

def get_previous_alerts(model_name):
    df = pd.read_sql('select * from "%s" where `Close Price` == null', conn)
    df = df.ix[:.['index', 'Ticker']]
    return df

today = datetime.now()
if today.strftime('%y%m%d') == NYSE_holidays()[0].strftime('%y%m%d'):
    exit()
df = get_data_from_couchdb(today.strftime('%m-%d-%Y'))
print(df)

machines = get_machines()
for machine in machines:
    features = machine['name'].split('__')[0].replace("'",'')

    features = ['Ticker']+features.split(', ')
    this_df = df[features].dropna()


    predictions = pd.Series(machine['model'].predict(this_df.ix[:,1:])).round(8)
    this_df['Predicted'] = predictions

    pos_alerts = this_df[this_df['Predicted']>machine['pos_cutoff']]
    pos_alerts['Alert Type'] = 'positive'
    if len(pos_alerts)/len(this_df)>.9:
        continue

    neg_alerts = this_df[this_df['Predicted']<machine['neg_cutoff']]
    neg_alerts['Alert Type'] = 'negative'
    if len(neg_alerts)/len(this_df)>.9:
        continue
    alerts = pd.concat([pos_alerts, neg_alerts])
    alerts['Date'] = today.strftime('%m-%d-%Y')
    alerts['Open Price'] = None
    alerts['Current Price'] = None
    alerts['Close Price'] = None
    alerts['Start Date'], alerts['End Date'] = get_open_close_dates(today)
    print(alerts)
    df = get_previous_alerts(machine['name'])
    print(df)

    #alerts.to_sql(machine['name'], conn, if_exists='append')
