from .SimpleStatistics import MonoProcessingClass
from .CreateDataframeClass import CreateDataFrame
from .MultiProc import MultiProcessingClass
from .CreatePlottingClass import PlotTimes
import multiprocessing

class Benchmark():

    def __init__(self):
        pass

    def SimpleStatistics(self, n_cores, rows, start):
        '''Call mono and multi simple statistical functions'''
        self.create_df = CreateDataFrame()
        self.df = self.create_df.create_df(rows, start)

        self.mono = MonoProcessingClass()
        self.mono.MonoSimpleStatistics(self.df)

        self.multi = MultiProcessingClass()
        self.multi.MultiSimpleStatistics(self.df, n_cores)

        self.plot = PlotTimes()
        self.plot.plot_simpleStatistics()

    def utilFunctions(self, val, n_cores, rows, other_df_rows, first_df_start, sec_df_start):
        '''Call mono and multi utility functions'''
        self.create_df = CreateDataFrame()
        self.df = self.create_df.create_df(rows, first_df_start)
        self.df_other = self.create_df.create_another_df(other_df_rows,sec_df_start)

        self.mono = MonoProcessingClass()
        self.mono.MonoUtilityFunctions(self.df, self.df_other, val)

        self.multi = MultiProcessingClass()
        self.multi.multiUtilityFunctions(self.df, self.df_other, val, n_cores)

        self.plot = PlotTimes()
        self.plot.plot_utilFunctions()

    def agg_without_loop(self, n_cores, rows, first_df_start):
        # """Call aggregation functions without loops"""
        self.create_df = CreateDataFrame()
        self.df = self.create_df.create_df(rows,first_df_start)

        self.mono = MonoProcessingClass()
        self.mono.groupByAggregateNoLoop(self.df)

        self.multi = MultiProcessingClass()
        self.multi.multiAggregateNoLoop(self.df, n_cores)

        self.plot = PlotTimes()
        self.plot.plot_agg_no_loop()

    def agg_with_loops(self, n_cores, rows, first_df_start):
        """Call aggregation functions with loops"""
        self.create_df = CreateDataFrame()
        self.df = self.create_df.create_df(rows, first_df_start)

        self.mono = MonoProcessingClass()
        self.mono.groupByAggregateLoop(self.df)

        self.multi = MultiProcessingClass()
        self.multi.multiAggregateLoop(self.df, n_cores)

        self.plot = PlotTimes()
        self.plot.plot_agg_loop()

if __name__ == '__main__':
    n_cores = multiprocessing.cpu_count()
    val = 96.50
    rows = 375000
    other_df_rows = 375000
    first_df_start = '01-02-2020'
    second_df_start = '02-15-2020'
    bench = Benchmark()
    bench.SimpleStatistics(n_cores, rows, first_df_start)
    bench.utilFunctions(val, n_cores, rows, other_df_rows, first_df_start, second_df_start)
    bench.agg_without_loop(n_cores, rows, first_df_start)
    bench.agg_with_loops(n_cores, rows, first_df_start)