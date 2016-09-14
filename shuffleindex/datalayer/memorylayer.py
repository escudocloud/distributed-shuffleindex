from six.moves import cPickle as pickle
from datalayer import DataLayer

class MemoryDataLayer(DataLayer):

    def __init__(self, loadfile=None):
        if loadfile is not None:
            with open(loadfile) as lf:
                self._dict = pickle.load(lf)
        else:
            self._dict = dict()

    def get(self, key):
        return self._dict.__getitem__(key)

    def put(self, key, value):
        self._dict.__setitem__(key, value)
        return key

    def save(self, savefile):
        with open(savefile, 'w') as sf:
            pickle.dump(self._dict, sf)
