import pandas as pd
import sqlite3
from sklearn.feature_selection import SelectKBest
from sklearn.feature_selection import f_regression, mutual_info_regression
import itertools
from sklearn.svm import SVR, SVC
from sklearn.model_selection import train_test_split
from random import shuffle
import time
import multiprocessing
from time import sleep
import signal
from redis import Redis
from rq import Queue
from RedisQueue import RedisQueue

class machine(multiprocessing.Process):
    def __init__(self, q, df):
        multiprocessing.Process.__init__(self)
        self.q = q
        self.df = df

    def run(self):
        print('start')
        self.conn = sqlite3.connect("data.db")
        while not self.q.empty():
            features = str(self.q.get())[3:-2].replace("'","").split(', ')
            self.features = list(features)
            for self.hold_time in ['_10']:
                df = self.df[self.features+['stock_perc_change'+self.hold_time, 'abnormal_perc_change'+self.hold_time]]
                targets = [self.df['stock_perc_change'+self.hold_time], self.df['abnormal_perc_change'+self.hold_time]]
                positive_dfs = []
                negative_dfs = []
                for i in range(6):

                    a_train, a_test, b_train, b_test = train_test_split(df.ix[:,:-2], df.ix[:,-2:], test_size=.4)



                    self.train(a_train, b_train)
                    test_result, negative_df, positive_df = self.test(a_test, b_test)
                    if test_result:

                        positive_dfs.append(positive_df)

                        negative_dfs.append(negative_df)
                    else:
                        break

                if test_result:
                    self.get_result(pd.concat(positive_dfs), pd.concat(negative_dfs))



    def train(self, a_train, b_train):
        self.clf = SVR(C=1.0, epsilon=0.2)

        self.clf.fit(a_train, b_train['abnormal_perc_change'+self.hold_time])


    def test(self, a_test, b_test):

        a_test['Predicted'] = self.clf.predict(a_test)
        a_test['Actual_stock_perc_change'+self.hold_time] = b_test['stock_perc_change'+self.hold_time]
        a_test['Actual_abnormal_perc_change'+self.hold_time] = b_test['abnormal_perc_change'+self.hold_time]

        if len(a_test['Predicted'].unique())<40:
            return False, None, None

        a_test = a_test.sort_values(by='Predicted')

        return True, a_test.ix[:,-3:].head(20), a_test.ix[:,-3:].tail(20)

    def get_result(self, df_p, df_n):

        p_result = df_p.describe()
        n_result = df_n.describe()

        if p_result.ix['mean','Actual_abnormal_perc_change_10']<0 or n_result.ix['mean','Actual_abnormal_perc_change_10']>0:
            return
        if p_result.ix['50%','Actual_abnormal_perc_change_10']<0 or n_result.ix['50%','Actual_abnormal_perc_change_10']>0:
            return



        p_result.index = p_result.index+'_pos'
        n_result.index = n_result.index+'_neg'

        p_result = p_result.stack().reset_index()
        p_result.index = p_result['level_1'] +'-'+ p_result['level_0']
        p_result = p_result[0]

        n_result = n_result.stack().reset_index()
        n_result.index = n_result['level_1'] +'-'+ n_result['level_0']
        n_result = n_result[0]


        result = p_result.append(n_result)
        result = pd.DataFrame(result).T
        result['features'] = str(self.features)[1:-1]+' '+self.hold_time[1:]

        result.to_sql('results', self.conn, index = False, if_exists='append')



if __name__ == '__main__':

    conn = sqlite3.connect("data.db")
    df = pd.read_sql('select * from data_10', conn)

    corr = df.corr()['abnormal_perc_change_10'].sort_values()

    corr = corr[:-2]

    columns = list(corr.index)+['stock_perc_change_10', 'abnormal_perc_change_10']
    #if 'index_perc_change_10' in columns:
    #    columns.remove('index_perc_change_10')

    df = df[columns]


    df = df[df['Market Cap']<=2000000000]

    # remove columns with too many nulls
    null_counts = df.isnull().sum()
    too_many = float(len(df))*.1
    null_counts = null_counts[null_counts<too_many]
    df = df[list(null_counts.index)]
    df = df.dropna()

    # get k features
    X = df.ix[:,:-2]
    y = df['abnormal_perc_change_10']

    k = SelectKBest(f_regression, k=12)
    k = k.fit(X,y)
    k_best_features = list(X.columns[k.get_support()])
    if 'index_perc_change_10' in k_best_features:
        k_best_features.remove('index_perc_change_10')
    print(k_best_features)



    permutations = []

    #for permute_length in range(3,7):
    for permute_length in range(3,8):
        for feature in list(itertools.permutations(k_best_features, r=permute_length)):
            permutations.append(feature)

    q = RedisQueue('test')

    shuffle(permutations)

    print('starting', len(permutations))
    # clear the queue
    while not q.empty():
        q.get()
    for feature in permutations:
        
        q.put(feature)


    for i in range(8):
        print('um')
        x = machine(q, df)
        print('starting...')
        x.start()


    while not q.empty():
        try:
            sleep(1)
        except:
            break

    del q
