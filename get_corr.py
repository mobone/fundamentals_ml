import pandas as pd
import sqlite3

conn = sqlite3.connect("data.db")
df = pd.read_sql('select * from data_10', conn)


corr = df.corr()['abnormal_perc_change_10'].sort_values()
corr = corr[:-2]

print('\nAll', len(df))
print(corr.head(10))
print(corr.tail(10))


df = df[df['Market Cap']<=10000000000]
corr = df.corr()['abnormal_perc_change_10'].sort_values()
corr = corr[:-2]
print('\nMed', len(df))
print(corr.head(10))
print(corr.tail(10))


df = df[df['Market Cap']<=2000000000]
corr = df.corr()['abnormal_perc_change_10'].sort_values()
corr = corr[:-2]
print('\nSmall',len(df))
print(corr.head(10))
print(corr.tail(10))

"""
conn = sqlite3.connect("data.db")
df = pd.read_sql('select * from data', conn)


corr = df.corr()['abnormal_perc_change_10'].sort_values()
corr = corr[:-4]

print('\nAll', len(df))
print(corr.head(10))
print(corr.tail(10))


df = df[df['Market Cap']<=10000000000]
corr = df.corr()['abnormal_perc_change_10'].sort_values()
corr = corr[:-4]
print('\nMed', len(df))
print(corr.head(10))
print(corr.tail(10))


df = df[df['Market Cap']<=2000000000]
corr = df.corr()['abnormal_perc_change_10'].sort_values()
corr = corr[:-4]
print('\nSmall',len(df))
print(corr.head(10))
print(corr.tail(10))
"""
