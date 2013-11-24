#!/usr/bin/python

import os
import struct
import socket
import errno
import logging
import threading
import SocketServer

from mapgang.protocol import protocol, VER_LENGTH

class ThreadedUnixStreamHandler(SocketServer.BaseRequestHandler):
    def rx_request(self, request):
        if (not protocol.isRender(request.command) and not protocol.isDirty(request.command)):
            return

        if request.bad_request():
            if (protocol.isRender(request.command)):
                request.send(protocol.NotDone)
            return

        status = self.server.queue.add(request)

        if status in ("rendering", "dropped"):
            request.send(protocol.Ignore)

        if status in ("requested"):
            # Request queued, response will be sent on completion
            return

    def handle(self):
        cur_thread = threading.currentThread()

        while True:
            try:
                # receive first integer - protocol version
                data = self.request.recv(VER_LENGTH)
            except socket.error, e:
                if e[0] == errno.ECONNRESET:
                    logging.info("%s: Connection reset by peer", cur_thread.getName())
                    break
                else:
                    raise

            l = len(data)
            if l == 0:
                logging.info("%s: Connection closed", cur_thread.getName())
                break;

            t = struct.unpack("1i", data)
            ver = t[0]
            req = protocol.getProtocolByVersion(ver)
            if not req:
                logging.warn("%s: Invalid request version %s", cur_thread.getName(), ver)
                break
            
            try:
                # receive remaining data
                data = self.request.recv(req.len())
            except socket.error, e:
                if e[0] == errno.ECONNRESET:
                    logging.info("%s: Connection reset by peer", cur_thread.getName())
                    break
                else:
                    raise
            
            req.receive(data, self.request)
            self.rx_request(req)

class ThreadedUnixStreamServer(SocketServer.ThreadingMixIn, SocketServer.UnixStreamServer):
    def __init__(self, address, queue, handler):
        if(os.path.exists(address)):
            os.unlink(address)
        self.address = address
        self.queue = queue
        SocketServer.UnixStreamServer.__init__(self, address, handler)
        self.daemon_threads = True
        os.chmod(address, 0666)
        
    def server_close(self):
        self.socket.close()
        os.unlink(self.address)
