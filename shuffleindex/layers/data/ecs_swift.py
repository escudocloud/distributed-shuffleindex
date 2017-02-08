from swiftclient import client
from ..datalayer import DataLayer


class ECSSwiftDataLayer(DataLayer):

    def __init__(self, authurl, username, key, namespace,
                 bucket_name, auth_version='1.0'):
        self._ecs_swift = client.Connection(authurl=authurl,
                                            user=username,
                                            key=key,
                                            tenant_name=namespace,
                                            auth_version=auth_version,
                                            insecure=True)
        self._container_name = bucket_name

    def get(self, key):
        header, obj = self._ecs_swift.get_object(self._container_name, key)
        return obj

    def put(self, key, value):
        self._ecs_swift.put_object(container=self._container_name,
                                   obj=key,
                                   contents=value,
                                   content_type='text/plain')
        return key

    def clean(self):
        header, objs = self._ecs_swift.get_container(self._container_name)
        for obj in objs:
            self._ecs_swift.delete_object(self._container_name, obj['name'])
