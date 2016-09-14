from datalayer import DataLayer
from swiftclient import client


class SwiftDataLayer(DataLayer):

    def __init__(self, authurl, username, key, tenant_name,
                 container_name, auth_version='2.0'):
        self.swift = client.Connection(authurl=authurl,
                                       user=username,
                                       key=key,
                                       tenant_name=tenant_name,
                                       auth_version=auth_version)
        self.container = container_name

    def get(self, key):
        return self.swift.get_object(container=self.container, obj=key)

    def put(self, key, value):
        self.swift.put_object(container=self.container, obj=key, contents=value)
