from datalayer import DataLayer
from swiftclient import client


class SwiftDataLayer(DataLayer):

    def __init__(self, authurl, username, key, tenant_name,
                 container_name, auth_version='2.0'):
        self._swift = client.Connection(authurl=authurl,
                                        user=username,
                                        key=key,
                                        tenant_name=tenant_name,
                                        auth_version=auth_version)
        self._container = container_name

    def get(self, key):
        return self._swift.get_object(container=self._container, obj=key)

    def put(self, key, value):
        self._swift.put_object(container=self._container, obj=key, contents=value)

    def clean(self):
        _, objs = self._swift.get_container(self._container)
        for o in objs:
            self._swift.delete_object(container=self._container, obj=o['name'])
