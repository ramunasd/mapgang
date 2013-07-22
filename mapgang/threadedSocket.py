#!/usr/bin/python

import os
import socket
import errno
import logging
import threading
import SocketServer

from mapgang.protocol import protocol, ProtocolPacketV2

class ThreadedUnixStreamHandler(SocketServer.BaseRequestHandler):
    def rx_request(self, request):
        logging.debug(request.commandStatus)
        if (request.commandStatus != protocol.Render) \
           and (request.commandStatus != protocol.Dirty) \
           and request.commandStatus != protocol.RenderPrio \
           and request.commandStatus != protocol.RenderBulk:
            return

        if request.bad_request():
            if (request.commandStatus == protocol.Render):
                request.send(protocol.NotDone)
            return

        cur_thread = threading.currentThread()
        logging.debug("%s: xml(%s) z(%d) x(%d) y(%d)", cur_thread.getName(), request.xmlname, request.z, request.x, request.y)

        status = self.server.queue_handler.add(request)
        if status in ("rendering", "requested"):
            # Request queued, response will be sent on completion
            return
        if status == protocol.Ignore:
            return

        # The tile won't be rendered soon, tell the requestor straight away
        if (request.commandStatus == protocol.Render):
            request.send(protocol.NotDone)

    def handle(self):
        cur_thread = threading.currentThread()
        max_len = ProtocolPacketV2().len()

        while True:
            try:
                data = self.request.recv(max_len)
            except socket.error, e:
                if e[0] == errno.ECONNRESET:
                    logging.info("Connection reset by peer")
                    break
                else:
                    raise
            #print "Got data: %s" % data
            if len(data) == max_len:
                req_v2 = ProtocolPacketV2()
                req_v2.receive(data, self.request)
                self.rx_request(req_v2)
            elif len(data) == 0:
                logging.info("%s: Connection closed", cur_thread.getName())
                break
            else:
                logging.warn("Invalid request length %d", len(data))
                break

class ThreadedUnixStreamServer(SocketServer.ThreadingMixIn, SocketServer.UnixStreamServer):
    def __init__(self, address, queue_handler, handler):
        if(os.path.exists(address)):
            os.unlink(address)
        self.address = address
        self.queue_handler = queue_handler
        SocketServer.UnixStreamServer.__init__(self, address, handler)
        self.daemon_threads = True
        os.chmod(address, 0666)
