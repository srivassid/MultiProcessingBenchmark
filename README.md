# MultiProcessingBenchmark
A benchmarking library to check how does your system fares with all the cores for simple statistical functions, utility
functions and aggregation functions. 

<h4>Full Code</h4>

```
from MultiProcessingBenchmark import EntryPoint
import multiprocessing

bench = EntryPoint.Benchmark()
n_cores = multiprocessing.cpu_count()
val = 96.50
rows = 375000
other_df_rows = 375000
first_df_start = '01-02-2020'
second_df_start = '02-15-2020'
bench.SimpleStatistics(n_cores, rows, first_df_start)
bench.utilFunctions(val, n_cores, rows, other_df_rows, first_df_start, second_df_start)
bench.agg_without_loop(n_cores, rows, first_df_start)
bench.agg_with_loops(n_cores, rows, first_df_start)
```
<br><br>
Usage:

```
from MultiProcessingBenchmark import EntryPoint
import multiprocessing

bench = EntryPoint.Benchmark()
n_cores = multiprocessing.cpu_count()
```

<h5>or specify any number of cores you want to use, n_cores=4</h5>

```
val = 96.50
``` 

<h5>to be used to search for a particular value, enter a value between 1 - 100, in decimal format</h5>

```
rows = 375000
```

<h5>number of rows for the dataset</h5>

```
other_df_rows = 375000
``` 

<h5>number of rows for the second dataset, to be used in merge and join</h5>

```
first_df_start = '01-02-2020' 
```

<h5>start of time series data of first dataset</h5>

```
second_df_start = '02-15-2020'
```

<h5>start of time series data of second dataset</h5>

<br><br><br>

<h5>Simple statistical functions used are count, sum, mean, standard deviation, rolling mean</h5>

```
bench.SimpleStatistics(n_cores, rows, first_df_start)
```

<h5>utility functions are merge, merge_asof, join, concat, sort, search</h5>

```
bench.utilFunctions(val, n_cores, rows, other_df_rows, first_df_start, second_df_start)
```

<h5>groupby aggregation function used are sum, count, mean, prod, without loops</h5>

```
bench.agg_without_loop(n_cores, rows, first_df_start)
```

<h5>groupby aggregation function used are sum, count, mean, prod, with loops</h5>

```
bench.agg_with_loops(n_cores, rows, first_df_start)
```
