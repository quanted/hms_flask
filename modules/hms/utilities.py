import requests
import urllib3


# class CustomHttpAdapter (requests.adapters.HTTPAdapter):
#     '''Transport adapter" that allows us to use custom ssl_context.'''
#
#     def __init__(self, ssl_context=None, **kwargs):
#         self.ssl_context = ssl_context
#         super().__init__(**kwargs)
#
#     def init_poolmanager(self, connections, maxsize, block=False):
#         self.poolmanager = urllib3.poolmanager.PoolManager(
#             num_pools=connections, maxsize=maxsize,
#             block=block, ssl_context=self.ssl_context)
