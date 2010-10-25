'''
Created on Oct 22, 2010

@author: escarpellini
'''

import re
import signal
import logging
from HTMLParser import HTMLParser
from qmanager import QueueManager


class DOCParser(HTMLParser):
    def __init__(self, *args, **kwargs):
        HTMLParser.__init__(self, *args, **kwargs)

        self._logger = logging.getLogger(self.__class__.__name__)

        self.msg = {}
        # local (per-document) href cache
        self.hrefs = []

        self.protocols_r = r'^http://'
        self.mimetype_r = r'^text\/html'

        self.queue = QueueManager(mqhost="localhost", mqport=61613, userid="",
                                  passwd="", qin="to_parse", qout="to_persist",
                                  recv_callback=self.worker, encode_parms=["data"],
                                  encoding="base64")

    def start(self):
        self.queue.subscribe()

    def handle_starttag(self, tag, attrs):
        attrs_h = dict(attrs)

        if tag == "a" and \
         attrs_h.has_key("href") and \
         attrs_h["href"] is not self.msg["url"] and \
         attrs_h["href"] not in self.hrefs and \
         re.search(self.protocols_r, attrs_h["href"], re.IGNORECASE):

            self._logger.info("Found %s. Enqueuing..." % attrs_h["href"])

            try:
                self.queue.enqueue(msg={"parent": self.msg["url"],
                                        "url": attrs_h["href"]})
            except:
                pass
            else:
                self.hrefs.append(attrs_h["href"])

    def worker(self, msg={}):
        self.reset()
        self.msg = msg
        self.hrefs = []

        if re.search(self.mimetype_r, self.msg["headers"]["content-type"], re.IGNORECASE):
            self._logger.info("Received %s" % msg["url"])
            self.feed(msg["data"])


if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal.SIG_DFL)

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    dp = DOCParser()
    dp.start()
