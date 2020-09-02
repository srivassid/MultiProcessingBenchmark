import pandas as pd
import numpy as np
import time
from multiprocessing import Pool
from .CreateDataframeClass import CreateDataFrame
from .SimpleStatisticsClass import MonoSimpleStatistics, MonoUtilityFunctions, GroupByAggregateLoop, GroupByAggregateNoLoop
from .MultiProcClass import MultiSimpleStatistics, MultiUtilityFunctions, MultiGroupByAggregateNoLoop, MultiGroupByAggregateLoop
from .CreatePlottingClass import PlotTimes

pd.options.display.width = 0
pd.set_option('display.float_format', lambda x: '%.3f' % x)

class MultiProcessingClass():

    def __init__(self):
        pass

    def MultiSimpleStatistics(self, df, n_cores):
        '''Call Simple Statistics functions'''
        self.multi_proc = MultiSimpleStatistics()

        self.simple_stats = MonoSimpleStatistics()

        # mean
        self.multi_proc.mean_parallelize_dataframe(df, self.simple_stats.mean_stat, n_cores)

        # sum
        self.multi_proc.sum_parallelize_dataframe(df, self.simple_stats.sum_stat, n_cores)

        # count
        self.multi_proc.count_parallelize_dataframe(df, self.simple_stats.count_stat, n_cores)

        # stdev time
        self.multi_proc.std_parallelize_dataframe(df, self.simple_stats.stdev_stat, n_cores)

        # rolling mean time
        self.multi_proc.roll_parallelize_dataframe(df, self.simple_stats.roll_mean_stat, n_cores)

        self.multi_proc.to_csv()

    def multiUtilityFunctions(self, df, df_other, val, n_cores):
        '''Call multi utility functions'''
        self.multi_util = MultiUtilityFunctions()
        self.mono_util = MonoUtilityFunctions()

        self.multi_util.sorting(df.sample(frac=1), self.mono_util.sorting, n_cores)
        self.multi_util.searching(df.apply(lambda x:round(x,2)), self.mono_util.searching, val, n_cores)
        self.multi_util.merging(df,df_other,self.mono_util.merging, n_cores)
        self.multi_util.merging_asof(df,df_other,self.mono_util.merge_asof, n_cores)
        self.multi_util.join(df,df_other,self.mono_util.join, n_cores)
        self.multi_util.concat(df,df_other,self.mono_util.concat, n_cores)
        self.multi_util.to_csv()

    def multiAggregateNoLoop(self, df, n_cores):
        """Call aggregate functions with multi cores no loop"""
        self.multiAgg = MultiGroupByAggregateNoLoop()
        self.monoAgg = GroupByAggregateNoLoop()
        self.multiAgg.groupByAggregateNoLoop(df, self.monoAgg.groupByAggregateNoLoop, n_cores)

    def multiAggregateLoop(self, df, n_cores):
        """Call aggregate functions with multi cores no loop"""
        self.multiAgg = MultiGroupByAggregateLoop()
        self.monoAgg = GroupByAggregateLoop()
        self.multiAgg.groupByAggregateLoop(df, self.monoAgg.groupByAggregateLoop, n_cores)
