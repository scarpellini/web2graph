'''
Created on Oct 22, 2010

@author: escarpellini
'''

from datetime import datetime
import signal
import logging
import simplejson
import neo4j
from qmanager import QueueManager


class Persistence(object):
    _graphdb = None

    def __init__(self, dbpath="/tmp"):
        self._logger = logging.getLogger(self.__class__.__name__)

        self.dbpath = dbpath

        self._connect()
        self._create_index()

        self.queue = QueueManager(mqhost="localhost", mqport=61613, userid="",
                                  passwd="", qin="to_persist", qout="to_fetch",
                                  recv_callback=self.worker, encode_parms=["data"],
                                  encoding="base64")

    def _connect(self):
        if not Persistence._graphdb:
            try:
                Persistence._graphdb = neo4j.GraphDatabase(self.dbpath)
            except Exception, e:
                self._logger.error("Error on trying to connect to database")
                self._logger.exception(e)
                raise

    def _create_index(self):
        with Persistence._graphdb.transaction:
            self.index = Persistence._graphdb.index("url", create=True)

    def _insert(self, data={}):
        parent = None
        site = None

        with Persistence._graphdb.transaction:
            if self.index[data["parent"]]:
                self._logger.info("Using index for %s" % data["parent"])
                parent = self.index[data["parent"]]

            else:
                self._logger.info("Creating node+index for %s" % data["parent"])
                parent = Persistence._graphdb.node(name=data["parent"],
                                                   url=data["parent"],
                                                   timestamp=str(datetime.now().isoformat()))
                self.index[data["parent"]] = parent

            if self.index[data["url"]]:
                self._logger.info("Using index for %s" % data["url"])
                site = self.index[data["url"]]
            else:
                self._logger.info("Creating node+index for %s" % data["url"])
                site = Persistence._graphdb.node(name=data["url"],
                                                 url=data["url"],
                                                 timestamp=str(datetime.now().isoformat()))
                self.index[data["url"]] = site

            parent.href(site)

    def start(self):
        self.queue.subscribe()

    def stop(self):
        Persistence._graphdb.shutdown()

    def worker(self, msg={}):
        try:
            self.queue.enqueue(msg=msg)
            self._insert(data=msg)
        except Exception, e:
            self._logger.error("Error on trying to store node data")
            self._logger.exception(e)


if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal.SIG_DFL)

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    gdb = Persistence(dbpath="/Users/escarpellini/workspace/neo-crawler/src/data/")
    gdb.start()
