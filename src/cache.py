'''
Created on Oct 24, 2010

@author: escarpellini
'''

import logging
import redis


class CacheManager(object):
    def __init__(self, host="localhost", port=6379, db=0, password=""):
        self._logger = logging.getLogger(self.__class__.__name__)

        self.host = host
        self.port = port
        self.db = db
        self.password = password
        self.cache_ttl = 172800
        self.redis = None

        self._connect()

    def _connect(self):
        try:
            self.redis = redis.Redis(host=self.host, port=self.port, db=self.db,
                                     password=self.password)
        except Exception, e:
            self.logger.error("Error on trying to connect to redis server")
            self.logger.exception(e)
            raise

    def add(self, url):
        try:
            self.redis.set(url, "ok")
            self.redis.expire(url, self.cache_ttl)
        except Exception, e:
            self._logger.error("Error on trying to create a new cache entry")
            self._logger.exception(e)

    def exists(self, url):
        try:
            return self.redis.exists(url)
        except Exception, e:
            self._logger.error("Error on trying to retrieve cache data")
            self._logger.exception(e)
