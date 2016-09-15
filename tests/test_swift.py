import pytest
from shuffleindex.datalayer.swift import SwiftDataLayer
from config_swift import *
from swiftclient.exceptions import ClientException

data = [(1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'd')]


class TestSwift:

    def setup(self):
        self.swift = SwiftDataLayer(authurl, user, password,
                                    tenant_name, container_name)

    def test_put(self):
        for i in range(len(data)):
            self.swift.put(data[i][0], data[i][1])

    def test_get_existing_keys(self):
        for i in range(len(data)):
            self.swift.get(data[i][0])

    def test_get_missing_keys(self):
        keys = [10, 15]
        for k in keys:
            with pytest.raises(ClientException):
                self.swift.get(k)

    def clean(self):
        obj_head, objs = self.swift.swift.get_container(container_name)
        for o in objs:
            self.swift.swift.delete_object(container=container_name, obj=o['name'])
