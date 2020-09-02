import numpy as np
import pandas as pd

class CreateDataFrame():

    def __init__(self):
        pass

    def create_df(self, rows, start):
        np.random.seed(0)
        self.df = pd.DataFrame(np.random.uniform(0, 100, size=(rows, 4)), columns=list('ABCD'),
                          index=pd.date_range(start=start, freq='s', periods=rows)) #15000000 01-02-2020
        # print(self.df.head())
        # print(self.df.tail())
        print(self.df.info(memory_usage='deep'))
        return self.df

    def create_another_df(self, rows, start):
        np.random.seed(0)
        self.df = pd.DataFrame(np.random.uniform(0, 100, size=(rows, 4)), columns=list('ABCD'),
                               index=pd.date_range(start=start, freq='s', periods=rows)) #02-15-2020
        # print(self.df.head())
        # print(self.df.tail())
        print(self.df.info(memory_usage='deep'))
        return self.df