'''
Created on Oct 22, 2010

@author: escarpellini
'''

import base64
import simplejson
import logging
import stomp

class QueueManager(object):
    _qinst = None

    def __init__(self, mqhost="localhost", mqport=61613, userid="", passwd="",
                 qin="", qout="", recv_callback=None, encode_parms=[], encoding="base64"):
        self._logger = logging.getLogger(self.__class__.__name__)

        self.mqhost = mqhost
        self.mqport = mqport
        self.userid = userid
        self.passwd = passwd
        self.qin = qin
        self.qout = qout
        self.recv_callback = recv_callback
        self.encode_parms = encode_parms
        self.encoding = encoding

        self._connect()

    def _connect(self):
        if not QueueManager._qinst:
            try:
                QueueManager._qinst = stomp.Connection(
                                            host_and_ports=[(self.mqhost, self.mqport)],
                                            user=self.userid,
                                            passcode=self.passwd)

                QueueManager._qinst.set_listener('QueueManagerListener',
                                                 QueueManagerListener(
                                                        callback=self.recv_callback,
                                                        encode_parms=self.encode_parms,
                                                        encoding=self.encoding)
                                                 )

                QueueManager._qinst.start()
                QueueManager._qinst.connect()

            except Exception, e:
                QueueManager._qinst = None

                self._logger.error("Error on trying to connecto to stomp server")
                self._logger.exception(e)
                raise

    def subscribe(self):
        QueueManager._qinst.subscribe(destination="/queue/%s" % self.qin, ack="auto")

    def enqueue(self, msg={}):
        try:
            if self.encode_parms:
                for k in self.encode_parms:
                    if msg.has_key(k):
                        msg[k] = base64.b64encode(msg[k])

            QueueManager._qinst.send(simplejson.dumps(msg), destination="/queue/%s" % self.qout)
        except Exception, e:
            self._logger.error("Error on trying to enqueue a new message")
            self._logger.exception(e)
            raise


class QueueManagerListener(object):
    def __init__(self, callback=None, encode_parms=[], encoding="base64"):
        self._logger = logging.getLogger(__name__)

        self.callback = callback
        self.encode_parms = encode_parms
        self.encoding = encoding

    def on_error(self, headers, message):
        self._logger.error("Stomp server error: %s" % message)

    def on_message(self, headers, message):
        try:
            msg = simplejson.loads(message)

            for k in self.encode_parms:
                if msg.has_key(k):
                    msg[k] = base64.b64decode(msg[k])

        except Exception, e:
            self._logger.error("Error on trying to encode. Aborting...")
            self._logger.exception(e)

        else:
            try:
                self.callback(msg=msg)
            except Exception, e:
                self._logger.error("Error on callingback")
                self._logger.exception(e)
