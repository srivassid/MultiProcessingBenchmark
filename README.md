# MultiProcessingBenchmark
A benchmarking library to check how does your system fares with all the cores for simple statistical functions, utility
functions and aggregation functions. 

Usage:

from MultiProcessingBenchmark import EntryPoint
import multiprocessing

bench = EntryPoint.Benchmark()
n_cores = multiprocessing.cpu_count() 
# or specify any number of cores you want to use

val = 96.50 
# to be used to search for a particular value, enter a value between 1 - 100, in decimal format

rows = 375000 
# number of rows for the dataset

other_df_rows = 375000 
# number of rows for the second dataset, to be used in merge and join

first_df_start = '01-02-2020' 
# start of time series data of first dataset

second_df_start = '02-15-2020' 
# start of time series data of second dataset


# Simple statistical functions used are count, sum, mean, standard deviation, rolling mean
bench.SimpleStatistics(n_cores, rows, first_df_start)

# utility functions are merge, merge_asof, join, concat, sort, search
bench.utilFunctions(val, n_cores, rows, other_df_rows, second_df_start, second_df_start)

# groupby aggregation function used are sum, count, mean, prod, without loops
bench.agg_without_loop(n_cores, rows, first_df_start)

# groupby aggregation function used are sum, count, mean, prod, with loops
bench.agg_with_loops(n_cores, rows, first_df_start)
