from collections import Counter
from datalayer import DataLayer
from time import time

class StatsLayer(DataLayer):

    def __init__(self, datalayer):
        self._datalayer = datalayer
        self.getcount =  Counter()
        self.putcount =  Counter()
        self.gettimes = []
        self.puttimes = []

    def get(self, key):
        self.getcount[key] += 1
        start = time()
        result = self._datalayer.get(key)
        self.gettimes.append(time() - start)
        return result

    def put(self, key, value):
        self.putcount[key] += 1
        start = time()
        result = self._datalayer.put(key, value)
        self.puttimes.append(time() - start)
        return result

    def reset(self):
        self.getcount.clear()
        self.putcount.clear()
        self.gettimes = []
        self.puttimes = []

    def plot_get(self, title='GET', show=True):
        self._plot_counter(self.getcount, title, show)

    def plot_put(self, title='PUT', show=True):
        self._plot_counter(self.putcount, title, show)

    def _plot_counter(self, dictionary, title, show):
        import matplotlib.pyplot as plt       # import here for loose dependency
        data = sorted(dictionary.items())   # extract the data sorted by node ID
        xs, ys = zip(*data) if data else ([], [])      # zip to create xs and ys
        plt.xlim(0, int((max(xs) + 1) * 1.02))       # set plot limits on x axis
        xs = [x - 0.4 for x in xs]                # put bars centered with ticks
        plt.xlabel('node_ID')                        # set other plot attributes
        plt.ylabel('# accesses')
        plt.title(title)
        plt.bar(xs, ys, color='b', edgecolor='b')           # plot the bar chart
        plt.tight_layout()
        if show: plt.show()
