from datalayer import DataLayer
from multiprocessing import Pipe
from threading import Thread

class AsyncLayer(DataLayer):

    def __init__(self, datalayer=None):

        # create the communication pipes
        self._get_parent, get_child = Pipe(duplex=True)
        self._put_parent, put_child = Pipe(duplex=True)

        def get_worker():
            while True:
                key = get_child.recv()
                value = datalayer.get(key)
                get_child.send(value)

        self._get_thread = Thread(target=get_worker)
        self._get_thread.daemon = True
        self._get_thread.start()

        def put_worker():
            while True:
                key, value = put_child.recv()
                result = datalayer.put(key, value)
                put_child.send(result)

        self._put_thread = Thread(target=put_worker)
        self._put_thread.daemon = True
        self._put_thread.start()

    def get_send(self, key):
        self._get_parent.send(key)

    def get_recv(self):
        return self._get_parent.recv()

    def get(self, key):
        self.get_send(key)
        return self.get_recv()

    def put_send(self, key, value):
        self._put_parent.send((key, value))

    def put_recv(self):
        return self._put_parent.recv()

    def put(self, key, value):
        self.put_send(key, value)
        return self.put_recv()
