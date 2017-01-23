from datalayer import DataLayer

class PredicateException(Exception):
    pass

class PredicateLayer(DataLayer):

    def __init__(self, datalayer, get_predicate=None, put_predicate=None):
        self.get_predicate = get_predicate
        self.put_predicate = put_predicate
        self._datalayer = datalayer

    def get(self, key):
        value = self._datalayer.get(key)
        if self.get_predicate and not self.get_predicate(key, value):
            raise PredicateException('GET key=%s, value=%s' % (key, value))
        return value

    def put(self, key, value):
        result = self._datalayer.put(key, value)
        if self.put_predicate and not self.put_predicate(key, value):
            raise PredicateException('PUT key=%s, value=%s' % (key, value))
        return result
