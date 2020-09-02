import pandas as pd
import numpy as np
import time
from .CreateDataframeClass import CreateDataFrame
from .SimpleStatisticsClass import MonoSimpleStatistics, MonoUtilityFunctions, GroupByAggregateNoLoop, GroupByAggregateLoop
from .CreatePlottingClass import PlotTimes

pd.options.display.width = 0
pd.set_option('display.float_format', lambda x: '%.3f' % x)

class MonoProcessingClass():

    def __init__(self):
        pass

    def MonoSimpleStatistics(self,df):
        '''Call mono simple statistical functions'''
        self.simple_stats = MonoSimpleStatistics()
        self.simple_stats.mean_stat(df)
        self.simple_stats.sum_stat(df)
        self.simple_stats.count_stat(df)
        self.simple_stats.stdev_stat(df)
        self.simple_stats.roll_mean_stat(df)
        self.simple_stats.to_csv()

    def MonoUtilityFunctions(self,df, df_other, val):
        '''Call mono utility functions'''
        self.util_func = MonoUtilityFunctions()
        self.util_func.sorting(df.sample(frac=1))
        self.util_func.searching(df.apply(lambda x:round(x,2)), val)
        self.util_func.merging(df, df_other)
        self.util_func.merge_asof(df, df_other)
        self.util_func.join(df, df_other)
        self.util_func.concat(df, df_other)
        self.util_func.to_csv()

    def groupByAggregateNoLoop(self, df):
        self.group_agg_obj = GroupByAggregateNoLoop()
        self.group_agg_obj.groupByAggregateNoLoop(df)

    def groupByAggregateLoop(self, df):
        self.group_agg_obj = GroupByAggregateLoop()
        self.group_agg_obj.groupByAggregateLoop(df)
