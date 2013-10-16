#!/usr/bin/python

import logging
import Queue
import threading
from mapgang.protocol import protocol

class Requests:
    def __init__(self, request_limit = 32, dirty_limit = 1000):
        # We store requests in several lists
        # - Incoming render requests are initially put into the request queue
        # If the request queue is full then the new request is assigned to the dirty queue
        # - Incoming 'dirty' requests are put into the dirty queue, or dropped if this is full
        # - The render queue holds the requests which are in progress by the render threads
        self.render = Queue.Queue(request_limit)
        self.dirty = Queue.Queue(dirty_limit)
        self.requests = {}
        self.rendering = {}
        self.not_empty = threading.Condition()

    def add(self, request):
        t = request.meta_tuple()
        self.not_empty.acquire()
        try:
            if t in self.rendering:
                self.rendering[t].append(request)
                return "rendering"
            if t in self.requests:
                self.requests[t].append(request)
                return "requested"
            # If we've reached here then there are no existing requests for this tile
            if protocol.isRender(request.command):
                try:
                    self.render.put_nowait(t)
                    self.not_empty.notify()
                    self.requests[t] = [request]
                    return "requested"
                except Queue.Full:
                    # when primary queue is full then move to dirty
                    request.command = protocol.Dirty
                    pass
            if protocol.isDirty(request.command):
                try:
                    self.dirty.put_nowait(t)
                    self.not_empty.notify()
                    self.requests[t] = [request]
                    return "dirty"
                except Queue.Full:
                    logging.debug("Dirty queue is full, dropping...")
                    return "dropped"
                
        finally:
            self.not_empty.release()


    def fetch(self):
        # Fetches a request tuple from the request or dirty queue
        # The requests are moved to the rendering queue while they are being rendered
        self.not_empty.acquire()
        try:
            while self.render.empty() and self.dirty.empty():
                self.not_empty.wait()
            # Pull request from one of the incoming queues
            try:
                t = self.render.get(False)
            except Queue.Empty:
                try:
                    t = self.dirty.get(False)
                except Queue.Empty:
                    logging.debug("Odd, queues empty")
                    return

            self.rendering[t] = self.requests[t]
            return (t, self.requests[t])
        finally:
            self.not_empty.release()

    def pop_requests(self, t):
        # Removes this tuple from the rendering queue
        # and returns the list of request for the tuple
        self.not_empty.acquire()
        try:
            return self.rendering.pop(t)
        except KeyError:
            # Should never happen. It implies the requests queues are broken
            logging.warning("Failed to locate request in rendering list!")
        finally:
            self.not_empty.release()
