from swiftclient import client
from ..datalayer import DataLayer


class SwiftDataLayer(DataLayer):

    def __init__(self, authurl, username, key, tenant_name,
                 container_name, auth_version='2.0'):
        self._swift = client.Connection(authurl=authurl,
                                        user=username,
                                        key=key,
                                        tenant_name=tenant_name,
                                        auth_version=auth_version)
        self._container_name = container_name

    def get(self, key):
        header, obj = self._swift.get_object(self._container_name, key)
        return obj

    def put(self, key, value):
        self._swift.put_object(self._container_name, key, value)
        return key

    def clean(self):
        header, objs = self._swift.get_container(self._container_name)
        for obj in objs:
            self._swift.delete_object(self._container_name, obj['name'])
