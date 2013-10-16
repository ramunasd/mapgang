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
        if (not protocol.isRender(request.command) and not protocol.isDirty(request.command)):
            return

        if request.bad_request():
            if (protocol.isRender(request.command)):
                request.send(protocol.NotDone)
            return

        status = self.server.queue_handler.add(request)

        if status in ("rendering", "dropped"):
            request.send(protocol.Ignore)

        if status in ("rendering", "requested"):
            # Request queued, response will be sent on completion
            return
        if status == "dirty":
            request.send
            return

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
        
    def server_close(self):
        self.socket.close()
        os.unlink(self.address)
