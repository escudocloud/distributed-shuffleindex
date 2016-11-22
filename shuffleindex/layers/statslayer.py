from collections import Counter
from datalayer import DataLayer

class StatsLayer(DataLayer):

    def __init__(self, datalayer):
        self._datalayer = datalayer
        self.getcount =  Counter()
        self.putcount =  Counter()

    def get(self, key):
        self.getcount[key] += 1
        return self._datalayer.get(key)

    def put(self, key, value):
        self.putcount[key] += 1
        return self._datalayer.put(key, value)

    def reset(self):
        self.getcount.clear()
        self.putcount.clear()

    def plot_get(self, show=True):
        self._plot_counter(self.getcount, 'GET', show)

    def plot_put(self, show=True):
        self._plot_counter(self.putcount, 'PUT', show)

    def _plot_counter(self, dictionary, title, show):
        import matplotlib.pyplot as plt       # import here for loose dependency
        data = sorted(dictionary.items())   # extract the data sorted by node ID
        xs, ys = zip(*data) if data else ([], [])      # zip to create xs and ys
        plt.xlim(0, max(xs) + 1)                     # set plot limits on x axis
        xs = [x - 0.4 for x in xs]                # put bars centered with ticks
        plt.xlabel('node ID')                        # set other plot attributes
        plt.ylabel('count')
        plt.title(title)
        plt.bar(xs, ys)                                     # plot the bar chart
        if show: plt.show()
