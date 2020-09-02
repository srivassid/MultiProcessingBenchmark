import time
import pandas as pd
import numpy as np
import os

class MonoSimpleStatistics():

    def __init__(self):
        self.df_mono = pd.DataFrame(columns=['mean','sum','count','std','roll'])
        # self.df_mono = pd.DataFrame()

    def mean_stat(self, df):
        '''Mean time'''
        # print('Mean')
        self.start = time.time()
        self.val = df.mean()
        self.elapsed_time = time.time() - self.start
        # print(self.elapsed_time)
        # self.df_mean['mean'] = self.elapsed_time
        self.df_mono = self.df_mono.append({'mean':self.elapsed_time}, ignore_index=True)
        # self.df_mono['mean'] = self.elapsed_time
        return df.mean()

    def sum_stat(self, df):
        '''Sum time'''
        # print('Sum')
        self.start = time.time()
        self.val = df.sum()
        self.elapsed_time = time.time() - self.start
        # print(self.elapsed_time)
        self.df_mono = self.df_mono.append({'sum': self.elapsed_time}, ignore_index=True)
        # self.df_mono['sum'] = self.elapsed_time
        return df.sum()

    def count_stat(self, df):
        '''Count time'''
        # print('Count')
        self.start = time.time()
        self.val = df.count()
        self.elapsed_time = time.time() - self.start
        # print(self.elapsed_time)
        self.df_mono = self.df_mono.append({'count': self.elapsed_time}, ignore_index=True)
        # self.df_mono['count'] = self.elapsed_time
        return df.count()

    def stdev_stat(self, df):
        '''Stdev time'''
        # print('StdDev')
        self.start = time.time()
        self.val = df.std()
        self.elapsed_time = time.time() - self.start
        # print(self.elapsed_time)
        self.df_mono = self.df_mono.append({'std': self.elapsed_time}, ignore_index=True)
        # self.df_mono['std'] = self.elapsed_time
        return df.std()

    def roll_mean_stat(self, df):
        '''rolling mean'''
        # print('Rolling Mean')
        self.start = time.time()
        self.val = df.rolling(3).mean()
        # print(df.rolling(3).mean().head(20))
        # print(df.rolling(3).mean().tail(20))
        self.elapsed_time = time.time() - self.start
        # print(self.elapsed_time)
        self.df_mono = self.df_mono.append({'roll': self.elapsed_time}, ignore_index=True)
        # self.df_mono['roll'] = self.elapsed_time
        return df.rolling(3).mean()

    def to_csv(self):
        # print("self mono")
        self.df_mono['mean'] = self.df_mono['mean'].iloc[0]
        self.df_mono['sum'] = self.df_mono['sum'].iloc[1]
        self.df_mono['count'] = self.df_mono['count'].iloc[2]
        self.df_mono['std'] = self.df_mono['std'].iloc[3]
        self.df_mono['roll'] = self.df_mono['roll'][4]
        # self.df_mono = self.df_mono.drop_duplicates(keep='first')
        # df = pd.DataFrame(np.repeat(self.df_mono.values, 5, axis=0))
        # df.columns = newdf.columns
        self.df_mono = self.df_mono.transpose()
        self.df_mono = self.df_mono.rename(columns={0: 'value_s', 1: 'b', 2: 'c', 3: 'd', 4: '5'})
        # print(list(df))
        self.df_mono = self.df_mono[['value_s']]
        self.df_mono = self.df_mono.reset_index()
        # print(self.df_mono)
        # df.to_csv('multi_pivot.csv', sep=',', index=False)

        # print(self.df_mono)
        if not os.path.exists('csv/simpleStatistics/'):
            try:
                os.mkdir('csv/')
            except Exception as e:
                print("CSV dir exists")
            finally:
                os.mkdir('csv/simpleStatistics/')
        self.df_mono.to_csv('csv/simpleStatistics/mono.csv',index=False, sep=',',float_format='%f')

class MonoUtilityFunctions():

    def __init__(self):
        self.df_mono_util = pd.DataFrame(columns=['sort','search','merge','merge_asof','concat', 'join'])

    def sorting(self,df):
        '''Sorting time'''
        self.start = time.time()
        df = df.sort_index()
        # print("Mono time taken sorting")
        self.elapsed_time = time.time() - self.start
        # print(self.elapsed_time)
        self.df_mono_util = self.df_mono_util.append({'sort': self.elapsed_time}, ignore_index=True)
        # print(df)
        return df

    def searching(self, df, val):
        '''Searching time'''
        # print(df.head())
        self.start = time.time()
        df = df.loc[df['A'] == val]
        # print("Time taken to search")
        self.elapsed_time = time.time() - self.start
        # print(self.elapsed_time)
        self.df_mono_util = self.df_mono_util.append({'search': self.elapsed_time}, ignore_index=True)
        # print(df)
        return df

    def merging(self, df_left, df_right):
        '''Merging Time with couple of common dates as index'''
        self.start = time.time()
        self.df = pd.merge(df_left.reset_index(),df_right.reset_index(),how='outer', on='index')
        # print("Time taken to merge")
        self.elapsed_time = time.time() - self.start
        # print(self.elapsed_time)
        self.df_mono_util = self.df_mono_util.append({'merge': self.elapsed_time}, ignore_index=True)
        # print(self.df)
        # print(self.df.shape)
        return self.df

    def merge_asof(self, df_left, df_right):
        '''MergeAsof Time'''
        self.start = time.time()
        self.df = pd.merge_asof(df_left.reset_index(), df_right.reset_index(), on='index')
        # print("Time taken to merge asof")
        self.elapsed_time = time.time() - self.start
        # print(self.elapsed_time)
        self.df_mono_util = self.df_mono_util.append({'merge_asof': self.elapsed_time}, ignore_index=True)
        # print(self.df)
        # print(self.df.shape)
        return self.df

    def join(self, df_left, df_right):
        '''Join Time'''
        self.start = time.time()
        self.df = df_left.join(df_right, how='outer',lsuffix='_left', rsuffix='_right')
        # print("Time taken to join")
        self.elapsed_time = time.time() - self.start
        # print(self.elapsed_time)
        self.df_mono_util = self.df_mono_util.append({'join': self.elapsed_time}, ignore_index=True)
        # print(self.df)
        # print(self.df.shape)
        return self.df

    def concat(self, df_left, df_right):
        '''Concatentation along axis 0'''
        self.start = time.time()
        self.df = pd.concat([df_left, df_right], ignore_index=True)
        # print("Time taken to concat")
        self.elapsed_time = time.time() - self.start
        # print(self.elapsed_time)
        self.df_mono_util = self.df_mono_util.append({'concat': self.elapsed_time}, ignore_index=True)
        # print(self.df)
        return self.df

    def to_csv(self):
        "Save csv"
        # print("self mono")
        # print(self.df_mono_util)
        self.df_mono_util['sort'] = self.df_mono_util['sort'].iloc[0]
        self.df_mono_util['search'] = self.df_mono_util['search'].iloc[1]
        self.df_mono_util['merge'] = self.df_mono_util['merge'].iloc[2]
        self.df_mono_util['merge_asof'] = self.df_mono_util['merge_asof'].iloc[3]
        self.df_mono_util['concat'] = self.df_mono_util['concat'][5]
        self.df_mono_util['join'] = self.df_mono_util['join'][4]
        # self.df_mono = self.df_mono.drop_duplicates(keep='first')
        # df = pd.DataFrame(np.repeat(self.df_mono.values, 5, axis=0))
        # df.columns = newdf.columns
        self.df_mono_util = self.df_mono_util.transpose()
        self.df_mono_util = self.df_mono_util.rename(columns={0: 'value_s', 1: 'b', 2: 'c', 3: 'd', 4: '5','e':6})
        # print(list(df))
        self.df_mono_util = self.df_mono_util[['value_s']]
        self.df_mono_util = self.df_mono_util.reset_index()
        # print(self.df_mono_util)
        # df.to_csv('multi_pivot.csv', sep=',', index=False)

        # print(self.df_mono)
        if not os.path.exists('csv/utilFunctions/'):
            try:
                os.mkdir('csv/')
            except Exception as e:
                print("CSV dir exists")
            finally:
                os.mkdir('csv/utilFunctions/')
        self.df_mono_util.to_csv('csv/utilFunctions/mono.csv', index=False, sep=',', float_format='%f')

class GroupByAggregateNoLoop():

    def __init__(self):
        self.df_agg = pd.DataFrame(columns={'agg_s'})

    def groupByAggregateNoLoop(self, df):
        self.start = time.time()
        # print(df.reset_index())
        self.group_df = df.reset_index().groupby(['index']).aggregate({'A':'mean','B':'count','C':'sum','D':'prod'}).reset_index()
        self.elapsed_time = time.time() - self.start
        # print("Time taken to aggregate without loop")
        # print(self.elapsed_time)
        self.df_agg = self.df_agg.append({'agg_s':self.elapsed_time}, ignore_index=True)
        # print(self.group_df)
        if not os.path.exists('csv/groupbyAgg/'):
            try:
                os.mkdir('csv/')
            except Exception as e:
                print("CSV dir exists")
            finally:
                os.mkdir('csv/groupbyAgg/')
        self.df_agg.to_csv('csv/groupbyAgg/mono_no_loop.csv', index=False, sep=',')
        return self.group_df

class GroupByAggregateLoop():

    def __init__(self):
        self.df_agg = pd.DataFrame(columns={'agg_s_l'})

    def groupByAggregateLoop(self, df):
        self.start = time.time()
        # print(df.reset_index())
        # data = pd.DataFrame(np.random.rand(10, 3))
        self.group_df = pd.DataFrame()
        for chunk in np.array_split(df, 4):
            # print("Chunk size")
            # print(chunk.shape)
            self.group_df = self.group_df.append(chunk.reset_index().groupby(['index']).aggregate(
                {'A': 'mean', 'B': 'count', 'C': 'sum', 'D': 'prod'}).reset_index(),ignore_index=True)
        self.elapsed_time = time.time() - self.start
        # print("Time taken to aggregate with loop")
        # print(self.elapsed_time)
        self.df_agg = self.df_agg.append({'agg_s_l':self.elapsed_time}, ignore_index=True)
        # print(self.group_df)

        if not os.path.exists('csv/groupbyAggLoop/'):
            try:
                os.mkdir('csv/')
            except Exception as e:
                print("CSV dir exists")
            finally:
                os.mkdir('csv/groupbyAggLoop/')
        self.df_agg.to_csv('csv/groupbyAggLoop/mono_loop.csv', index=False, sep=',')
        return self.group_df






