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

class machine(multiprocessing.Process):
    def __init__(self, q, df):
        multiprocessing.Process.__init__(self)
        self.q = q
        self.df = df

    def run(self):
        print('start')
        self.conn = sqlite3.connect("data.db")
        while not self.q.empty():
            features = self.q.get()
            self.features = list(features)
            for self.hold_time in ['_20']:
                df = self.df[self.features+['abnormal_perc_change'+self.hold_time]]
                target = self.df['abnormal_perc_change'+self.hold_time]
                total_positives = []
                total_negatives = []
                for i in range(5):
                    a_train, a_test, b_train, b_test = train_test_split(df.ix[:,:-1], target, test_size=.4)
                    self.train(a_train, b_train)
                    test_result, positives, negatives = self.test(a_test, b_test)
                    if test_result:
                        total_positives.extend(positives)
                        total_negatives.extend(negatives)
                    else:
                        break

                if test_result:
                    self.get_result(total_positives, total_negatives)


    def train(self, a_train, b_train):
        self.clf = SVR(C=1.0, epsilon=0.2)

        self.clf.fit(a_train, b_train)


    def test(self, a_test, b_test):

        a_test['Predicted'] = self.clf.predict(a_test)
        a_test['Actual'] = b_test
        if len(a_test['Predicted'].unique())<20:
            return False, None, None

        a_test = a_test.sort_values(by='Predicted')

        positive = list(a_test.tail(20)['Actual'].values)
        negative = list(a_test.head(20)['Actual'].values)

        return True, positive, negative

    def get_result(self, positives, negatives):
        df_p = pd.DataFrame(positives)
        df_n = pd.DataFrame(negatives)
        p_result = df_p.describe()[0]
        n_result = df_n.describe()[0]

        if p_result['mean']<0 or n_result['mean']>0:
            return
        if p_result['50%']<0 or n_result['50%']>0:
            return

        p_result.index = p_result.index+'_pos'
        n_result.index = n_result.index+'_neg'
        result = p_result.append(n_result)
        result = pd.DataFrame(result).T
        result['features'] = str(self.features)[1:-1]+' '+self.hold_time[1:]
        print(result)
        result.to_sql('results', self.conn, index = False, if_exists='append')


if __name__ == '__main__':

    conn = sqlite3.connect("data.db")
    df = pd.read_sql('select * from data', conn)

    corr = df.corr()['abnormal_perc_change_20'].sort_values()

    corr = corr[:-2]

    columns = list(corr.index)+['abnormal_perc_change_20']
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
    y = df['abnormal_perc_change_20']

    k = SelectKBest(f_regression, k=12)
    k = k.fit(X,y)
    k_best_features = list(X.columns[k.get_support()])
    print(k_best_features)

    input()

    permutations = []

    for permute_length in range(3,8):

        for feature in list(itertools.permutations(k_best_features, r=permute_length)):
            permutations.append(feature)

    q = multiprocessing.Queue()

    shuffle(permutations)
    print('starting', len(permutations))
    for feature in permutations:
        q.put(feature)


    for i in range(7):
        x = machine(q, df)
        x.start()


    while not q.empty():
        try:
            sleep(1)
        except:
            break

    del q
