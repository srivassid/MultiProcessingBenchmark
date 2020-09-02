import pandas as pd
import time
from multiprocessing import Pool
import numpy as np
import os
from itertools import repeat

'''Data Logic Layer'''
class MultiSimpleStatistics():

    def __init__(self):
        self.df_multi = pd.DataFrame(columns=['mean','sum','count','std','roll'])

    def mean_parallelize_dataframe(self, df, func, n_cores):
        '''Calculate mean using all cores'''
        self.start = time.time()
        self.df_split = np.array_split(df, n_cores)
        self.pool = Pool(n_cores)
        self.df_res = pd.concat(self.pool.map(func, self.df_split))
        self.pool.close()
        self.pool.join()
        # print("Concat df mean")
        # print(self.df_res.mean(level=0))
        self.elapsed_time = time.time() - self.start
        # print(self.elapsed_time)
        self.df_multi = self.df_multi.append({'mean': self.elapsed_time}, ignore_index=True)
        # return df

    def count_parallelize_dataframe(self, df, func, n_cores):
        '''Calculate count using all cores'''
        self.start = time.time()
        self.df_split = np.array_split(df, n_cores)
        self.pool = Pool(n_cores)
        self.df_res = pd.concat(self.pool.map(func, self.df_split))
        self.pool.close()
        self.pool.join()
        # print("Concat df count")
        # print(self.df_res.sum(level=0))
        self.elapsed_time = time.time() - self.start
        # print(self.elapsed_time)
        self.df_multi = self.df_multi.append({'count': self.elapsed_time}, ignore_index=True)
        # return df

    def sum_parallelize_dataframe(self, df, func, n_cores):
        '''Calculate sum using all cores'''
        self.start = time.time()
        self.df_split = np.array_split(df, n_cores)
        self.pool = Pool(n_cores)
        self.df_res = pd.concat(self.pool.map(func, self.df_split))
        self.pool.close()
        self.pool.join()
        # print("Concat df sum")
        # print(self.df_res.sum(level=0))
        self.elapsed_time = time.time() - self.start
        # print(self.elapsed_time)
        self.df_multi = self.df_multi.append({'sum': self.elapsed_time}, ignore_index=True)
        # return df

    def std_parallelize_dataframe(self, df, func, n_cores):
        '''Calculate std using all cores'''
        self.start = time.time()
        self.df_split = np.array_split(df, n_cores)
        self.pool = Pool(n_cores)
        self.df_res = pd.concat(self.pool.map(func, self.df_split))
        self.pool.close()
        self.pool.join()
        # print("Concat df std")
        # print(self.df_res.mean(level=0))
        self.elapsed_time = time.time() - self.start
        # print(self.elapsed_time)
        self.df_multi = self.df_multi.append({'std': self.elapsed_time}, ignore_index=True)
        # return df

    def roll_parallelize_dataframe(self, df, func, n_cores):
        '''Calculate rolling mean using all cores'''
        self.start = time.time()
        self.df_split = np.array_split(df, n_cores)
        self.pool = Pool(n_cores)
        self.df_res = pd.concat(self.pool.map(func, self.df_split))
        self.pool.close()
        self.pool.join()
        # print("Concat df roll")
        # print(self.df_res.rolling(3).mean().head(20))
        # print(self.df_res.rolling(3).mean().tail(20))
        self.elapsed_time = time.time() - self.start
        # print(self.elapsed_time)
        self.df_multi = self.df_multi.append({'roll': self.elapsed_time}, ignore_index=True)
        # return df

    def to_csv(self):
        '''Save data as csv'''
        # print('self.df_multi')
        self.df_multi['mean'] = self.df_multi['mean'].iloc[0]
        self.df_multi['sum'] = self.df_multi['sum'].iloc[1]
        self.df_multi['count'] = self.df_multi['count'].iloc[2]
        self.df_multi['std'] = self.df_multi['std'].iloc[3]
        self.df_multi['roll'] = self.df_multi['roll'][4]
        self.df_multi = self.df_multi.transpose()
        self.df_multi = self.df_multi.rename(columns={0: 'value_m', 1: 'b', 2: 'c', 3: 'd', 4: '5'})
        # print(list(df))
        self.df_multi = self.df_multi[['value_m']]
        self.df_multi = self.df_multi.reset_index()
        # print(self.df_multi)

        if not os.path.exists('csv/simpleStatistics/'):
            try:
                os.mkdir('csv/')
            except Exception as e:
                print("CSV dir exists")
            finally:
                os.mkdir('csv/simpleStatistics/')
        self.df_multi.to_csv('csv/simpleStatistics/multi.csv',index=False,sep=',')

class MultiUtilityFunctions():

    def __init__(self):
        self.df_multi_util = pd.DataFrame(columns=['sort','search','merge','merge_asof','concat', 'join'])

    def sorting(self, df, func, n_cores):
        '''Sort using all cores'''
        self.start = time.time()
        self.df_split = np.array_split(df, n_cores)
        self.pool = Pool(n_cores)
        self.df_res = pd.concat(self.pool.map(func, self.df_split))
        self.pool.close()
        self.pool.join()
        self.df_res = self.df_res.sort_index(ascending=True)
        # print("Multi time taken sorting")
        self.elapsed_time = time.time() - self.start
        # print(self.elapsed_time)
        self.df_multi_util = self.df_multi_util.append({'sort': self.elapsed_time}, ignore_index=True)
        # print(self.df_res)

    def searching(self, df, func, val, n_cores):
        '''Search using all cores'''
        self.start = time.time()
        self.df_split = np.array_split(df, n_cores)
        self.pool = Pool(n_cores)
        self.df_res = pd.concat(self.pool.starmap(func, [(i, val) for i in self.df_split]))
        # self.df_res = pd.concat(self.pool.starmap(func, zip(self.df_split, repeat(val))))
        self.pool.close()
        self.pool.join()
        # print("Multi time taken searching")
        self.elapsed_time = time.time() - self.start
        # print(self.elapsed_time)
        self.df_multi_util = self.df_multi_util.append({'search': self.elapsed_time}, ignore_index=True)
        # print(self.df_res)

    def merging(self, df_left, df_right, func, n_cores):
        '''Merge using all cores'''
        self.start = time.time()
        self.df_left_split = np.array_split(df_left, n_cores)
        self.df_right_split = np.array_split(df_right, n_cores)
        self.pool = Pool(n_cores)
        # self.df_res = pd.concat(self.pool.map(func, self.df_split, val))
        self.df_res = pd.concat(self.pool.starmap(func, [(i, j) for i,j in zip(self.df_left_split,self.df_right_split)]))
        # self.df_res = pd.concat(self.pool.starmap(func, zip(self.df_split, repeat(val))))
        self.pool.close()
        self.pool.join()
        # self.df_res = self.df_res.sort_index(ascending=True)
        # print("Multi time taken merge")
        self.elapsed_time = time.time() - self.start
        # print(self.elapsed_time)
        self.df_multi_util = self.df_multi_util.append({'merge': self.elapsed_time}, ignore_index=True)
        # print(self.df_res)

    def merging_asof(self, df_left, df_right, func, n_cores):
        '''MergeASOF using all cores'''
        self.start = time.time()
        self.df_left_split = np.array_split(df_left, n_cores)
        self.df_right_split = np.array_split(df_right, n_cores)
        self.pool = Pool(n_cores)
        # self.df_res = pd.concat(self.pool.map(func, self.df_split, val))
        self.df_res = pd.concat(self.pool.starmap(func, [(i, j) for i,j in zip(self.df_left_split,self.df_right_split)]))
        # self.df_res = pd.concat(self.pool.starmap(func, zip(self.df_split, repeat(val))))
        self.pool.close()
        self.pool.join()
        self.elapsed_time = time.time() - self.start
        # print(self.elapsed_time)
        self.df_multi_util = self.df_multi_util.append({'merge_asof': self.elapsed_time}, ignore_index=True)
        # print(self.df_res)

    def join(self, df_left, df_right, func, n_cores):
        '''Join using all cores'''
        self.start = time.time()
        self.df_left_split = np.array_split(df_left, n_cores)
        self.df_right_split = np.array_split(df_right, n_cores)
        self.pool = Pool(n_cores)
        self.df_res = pd.concat(self.pool.starmap(func, [(i, j) for i,j in zip(self.df_left_split,self.df_right_split)]))
        # self.df_res = pd.concat(self.pool.starmap(func, zip(self.df_split, repeat(val))))
        self.pool.close()
        self.pool.join()
        self.elapsed_time = time.time() - self.start
        # print(self.elapsed_time)
        self.df_multi_util = self.df_multi_util.append({'join': self.elapsed_time}, ignore_index=True)
        # print(self.df_res)

    def concat(self, df_left, df_right, func, n_cores):
        '''Concat using all cores'''
        self.start = time.time()
        self.df_left_split = np.array_split(df_left, n_cores)
        self.df_right_split = np.array_split(df_right, n_cores)
        self.pool = Pool(n_cores)
        self.df_res = pd.concat(self.pool.starmap(func, [(i, j) for i,j in zip(self.df_left_split,self.df_right_split)]))
        # self.df_res = pd.concat(self.pool.starmap(func, zip(self.df_split, repeat(val))))
        self.pool.close()
        self.pool.join()
        # print("Multi time taken Concat")
        self.elapsed_time = time.time() - self.start
        # print(self.elapsed_time)
        self.df_multi_util = self.df_multi_util.append({'concat': self.elapsed_time}, ignore_index=True)
        # print(self.df_res)

    def to_csv(self):
        '''Save CSV'''
        # print('self.df_multi')
        # print(self.df_multi_util)
        self.df_multi_util['sort'] = self.df_multi_util['sort'].iloc[0]
        self.df_multi_util['search'] = self.df_multi_util['search'].iloc[1]
        self.df_multi_util['merge'] = self.df_multi_util['merge'].iloc[2]
        self.df_multi_util['merge_asof'] = self.df_multi_util['merge_asof'].iloc[3]
        self.df_multi_util['concat'] = self.df_multi_util['concat'][5]
        self.df_multi_util['join'] = self.df_multi_util['join'][4]
        self.df_multi_util = self.df_multi_util.transpose()
        self.df_multi_util = self.df_multi_util.rename(columns={0: 'value_m', 1: 'b', 2: 'c', 3: 'd', 4: 'e', 5:'f'})
        # print(list(df))
        self.df_multi_util = self.df_multi_util[['value_m']]
        self.df_multi_util = self.df_multi_util.reset_index()
        # print(self.df_multi_util)
        # df.to_csv('multi_pivot.csv', sep=',', index=False)

        # print(self.df_multi)
        if not os.path.exists('csv/utilFunctions/'):
            try:
                os.mkdir('csv/')
            except Exception as e:
                print("CSV dir exists")
            finally:
                os.mkdir('csv/utilFunctions/')
        self.df_multi_util.to_csv('csv/utilFunctions/multi.csv',index=False,sep=',')

class MultiGroupByAggregateNoLoop():

    def __init__(self):
        self.df_agg = pd.DataFrame(columns={'agg_m'})

    def groupByAggregateNoLoop(self, df, func, n_cores):
        self.start = time.time()
        self.start = time.time()
        self.df_split = np.array_split(df, n_cores)
        self.pool = Pool(n_cores)
        self.df_res = pd.concat(self.pool.map(func, self.df_split))
        self.pool.close()
        self.pool.join()
        # self.group_df = df.reset_index().groupby(['index']).aggregate({'A':'mean','B':'count','C':'sum','D':'prod'}).reset_index()
        self.elapsed_time = time.time() - self.start
        # print("Time taken to aggregate without loop with cores")
        # print(self.elapsed_time)
        self.df_agg = self.df_agg.append({'agg_m':self.elapsed_time}, ignore_index=True)
        # print(self.df_split)
        if not os.path.exists('csv/groupbyAgg/'):
            try:
                os.mkdir('csv/')
            except Exception as e:
                print("CSV dir exists")
            finally:
                os.mkdir('csv/groupbyAgg/')
        self.df_agg.to_csv('csv/groupbyAgg/multi_no_loop.csv', index=False, sep=',')

class MultiGroupByAggregateLoop():

    def __init__(self):
        self.df_agg = pd.DataFrame(columns={'agg_m_l'})

    def groupByAggregateLoop(self, df, func, n_cores):
        self.start = time.time()
        self.start = time.time()
        self.df_split = np.array_split(df, n_cores)
        self.pool = Pool(n_cores)
        self.df_res = pd.concat(self.pool.map(func, self.df_split))
        self.pool.close()
        self.pool.join()
        self.elapsed_time = time.time() - self.start
        # print("Time taken to aggregate with loop with multi cores")
        # print(self.elapsed_time)
        self.df_agg = self.df_agg.append({'agg_m_l':self.elapsed_time}, ignore_index=True)
        # print(self.df_split)
        if not os.path.exists('csv/groupbyAggLoop/'):
            try:
                os.mkdir('csv/')
            except Exception as e:
                print("CSV dir exists")
            finally:
                os.mkdir('csv/groupbyAggLoop/')
        self.df_agg.to_csv('csv/groupbyAggLoop/multi_loop.csv', index=False, sep=',')
