'''
Created on Oct 22, 2010

@author: escarpellini
'''

import re
import signal
import logging
from urllib2 import urlopen
from qmanager import QueueManager
from cache import CacheManager


class Fetcher(object):
    def __init__(self, url="http://www.python.org/", timeout=10):
        self._logger = logging.getLogger(self.__class__.__name__)

        self.url = url
        self.extensions_blacklist_r = r'\.(?:jpg|jpeg|png|gif|zip|gz|rar|tar|pdf|doc|docx|ppt|pptx|xls|xlsx|iso|mp3|wav|mid|wmv|wma|txt|scr|exe|com|bat|eml)$'
        self.timeout = timeout

        self.cache = CacheManager()

        self.queue = QueueManager(mqhost="localhost", mqport=61613, userid="",
                                  passwd="", qin="to_fetch", qout="to_parse",
                                  recv_callback=self.worker, encode_parms=["data"],
                                  encoding="base64")

    def start(self):
        self.queue.subscribe()

        # REMOVE
        import simplejson
        QueueManager._qinst.send(simplejson.dumps({"url": self.url, "parent": "start"}),
                                    destination="/queue/to_fetch")
    def worker(self, msg={}):
        url = msg["url"]

        if re.search(self.extensions_blacklist_r, url, re.IGNORECASE):
            return

        try:
            self._logger.info("Fetching %s" % msg["url"])
            req = urlopen(url, timeout=self.timeout)
        except Exception, e:
            self._logger.error("Error on trying to fetch %s" % url)
            self._logger.exception(e)

        else:
            self.cache.add(url)

            if req.code == 200:
                try:
                    res = req.read()
                    self.queue.enqueue(msg={"url": url,
                                            "data": res,
                                            "headers": dict(req.headers.items())})
                except:
                    pass


if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal.SIG_DFL)

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    wc = Fetcher()
    wc.start()
