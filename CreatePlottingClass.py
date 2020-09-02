import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import seaborn as sns
from matplotlib.colors import ListedColormap

class PlotTimes():

    def __init__(self):
        pass

    def plot_graphs(self):
        self.df_mono = pd.read_csv('others/mono.csv')
        self.df_multi = pd.read_csv('others/multi.csv')

        self.df = pd.concat([self.df_mono,self.df_multi],axis=1)
        # print(self.df)

        data = [list(self.df_mono.values.tolist())[0],
                list(self.df_multi.values.tolist())[0]]
        # print(data)
        X = np.arange(5)
        fig = plt.figure()
        ax = fig.add_axes([0.03, 0.05, 0.93, 0.93])
        rects1 = ax.bar(X + 0.00, data[0], color='b', width=0.25)
        rects2 = ax.bar(X + 0.25, data[1], color='g', width=0.25)

        def autolabel(rects):
            """Attach a text label above each bar in *rects*, displaying its height."""
            for rect in rects:
                height = round(rect.get_height(),2)
                ax.annotate('{}'.format(height),
                            xy=(rect.get_x() + rect.get_width() / 2, height),
                            xytext=(0, 3),  # 3 points vertical offset
                            textcoords="offset points",
                            ha='center', va='bottom')

        autolabel(rects1)
        autolabel(rects2)
        ax.set_ylim(0,15)
        ax.set_xticks(np.arange(4), ('Mean', 'Sum', 'Count', 'StDev', 'Roll'))
        ax.set_xticklabels(['Mean','Mean', 'Sum', 'Count', 'StdDev', 'Roll'])
        ax.legend(labels=['Mono', 'Multi'],loc='center left')
        fig.suptitle("\nRun Time of Simple Statistical functions, mono and multi processor modes")

        plt.show()

    def plot_simpleStatistics(self):
        '''Plot Simple statistics graph, multi vs mono'''
        self.df_s = pd.read_csv('csv/simpleStatistics/mono.csv')
        self.df_m = pd.read_csv('csv/simpleStatistics/multi.csv')
        self.df_s['value_m'] = pd.Series(self.df_m['value_m'])
        self.df_s = self.df_s.set_index('index')
        # print(self.df_s)
        # print(df_m)
        ax =self.df_s.plot(kind='bar', colormap=ListedColormap(sns.color_palette("Accent", 5)))
        for p in ax.patches:
            ax.annotate(str(round(p.get_height(), 2)), (p.get_x() * 1.015, p.get_height() * 1.005))
        plt.title('Time in seconds taken for simple statistical functions, single vs multi core execution')
        ax.legend(["Single Core", "Multi Core"]);
        plt.show()

    def plot_utilFunctions(self):
        '''Plot UtilFunctions graph, multi vs mono'''
        self.df_s = pd.read_csv('csv/utilFunctions/mono.csv')
        self.df_m = pd.read_csv('csv/utilFunctions/multi.csv')
        self.df_s['value_m'] = pd.Series(self.df_m['value_m'])
        self.df_s = self.df_s.set_index('index')
        # print(self.df_s)
        # print(df_m)
        ax =self.df_s.plot(kind='bar', colormap=ListedColormap(sns.color_palette("Accent", 5)))
        for p in ax.patches:
            ax.annotate(str(round(p.get_height(), 2)), (p.get_x() * 1.015, p.get_height() * 1.005))
        plt.title('Time in seconds taken for utility functions, single vs multi core execution')
        ax.legend(["Single Core", "Multi Core"]);
        plt.show()

    def plot_agg_no_loop(self):
        """Plot agg functions no loop"""
        self.df_s = pd.read_csv('csv/groupbyAgg/mono_no_loop.csv')
        self.df_m = pd.read_csv('csv/groupbyAgg/multi_no_loop.csv')
        # print(self.df_s)
        # print(self.df_m)
        self.df = pd.concat([self.df_s, self.df_m], axis=1)
        # print(self.df.stack().reset_index())
        self.df = self.df.rename({'level_1':'axis',0:'value'})
        # print(self.df)
        ax = self.df.plot(kind='bar', colormap=ListedColormap(sns.color_palette("Accent", 5)))
        for p in ax.patches:
            ax.annotate(str(round(p.get_height(), 2)), (p.get_x() * 0.6, p.get_height() * 1.005))
        plt.title('Time in seconds taken for aggregation without loop, single vs multi core execution')
        ax.legend(["Single Core", "Multi Core"]);
        ax.set_xlabel('')
        plt.show()

    def plot_agg_loop(self):
        """Plot agg functions no loop"""
        self.df_s = pd.read_csv('csv/groupbyAggLoop/mono_loop.csv')
        self.df_m = pd.read_csv('csv/groupbyAggLoop/multi_loop.csv')
        # print(self.df_s)
        # print(self.df_m)
        self.df = pd.concat([self.df_s, self.df_m], axis=1)
        self.df = self.df.rename({'level_1': 'axis', 0: 'value'})
        # print(self.df)
        ax = self.df.plot(kind='bar', colormap=ListedColormap(sns.color_palette("Accent", 5)))
        for p in ax.patches:
            ax.annotate(str(round(p.get_height(), 2)), (p.get_x() * 0.6 , p.get_height() * 1.005))
        plt.title('Time in seconds taken for aggregation with loop, single vs multi core execution')
        ax.legend(["Single Core", "Multi Core"]);
        plt.show()