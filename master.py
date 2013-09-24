#!/usr/bin/python

import sys, os
import logging
import threading
import json

try:
    from gearman import GearmanClient
    from gearman.constants import PRIORITY_NONE, PRIORITY_LOW, PRIORITY_HIGH, JOB_UNKNOWN
except ImportError:
    print "Missing Gearman client"
    sys.exit()

from mapgang.protocol import protocol
from mapgang.session import Issuer
from mapgang.config import Config
from mapgang.threadedSocket import ThreadedUnixStreamServer, ThreadedUnixStreamHandler
from mapgang.storage.disk import Disk

class RequestThread(GearmanClient):
    priorities = {
        protocol.RenderPrio: PRIORITY_HIGH,
        protocol.RenderBulk: PRIORITY_LOW,
        protocol.RenderLow:  PRIORITY_LOW,
        protocol.Dirty:      PRIORITY_LOW
    }
    
    def __init__(self, styles, queue_handler, host_list):
        self.queue_handler = queue_handler
        self.styles = styles
        GearmanClient.__init__(self, host_list)
        os.umask(0)

    def render_request(self, t, request):
        (style, x, y, z) = t
        
        if not style in self.styles:
            logging.error("No map for: '%s'", style)
            return False
        
        priority = self.get_priotity(request)
        logging.debug("Sending render request, priority: %s, tile: %s", priority, t)
        try:
            response = self.submit_job("render_" + style,
                                   json.dumps(t),
                                   priority=priority, background=False,
                                   max_retries=3)
        except Exception, e:
            logging.critical(e)
            return False

        if response.state == JOB_UNKNOWN:
            logging.warning("Job %s connection failed!", response.unique)
            return False
        if response.timed_out:
            logging.warning("Job %s timed out", response.unique)
            return False

        if response.complete:
            if response.result == "" or response.result is None:
                return False
            return self.save_tiles(style, x, y, z, response.result)

        return False;

    def save_tiles(self, style, x, y, z, tiles):
        return storage.write(style, x, y, z, tiles)
    
    def get_priotity(self, request):
        if request.command in self.priorities:
            return self.priorities[request.command]
        
        return PRIORITY_NONE

    def loop(self):
        while True:
            #Fetch a meta-tile to render
            item = self.queue_handler.fetch()
            tile = item[0]
            request = (item[1][:1] or [None])[0]
            rendered = self.render_request(tile, request)
            # Retrieve all requests for this meta-tile
            requests = self.queue_handler.pop_requests(tile)
            for request in requests:
                if protocol.isRender(request.command):
                    if rendered == True:
                        request.send(protocol.Done)
                    else:
                        request.send(protocol.NotDone)


class RequestQueues:
    def __init__(self, request_limit = 32, dirty_limit = 1000):
        # We store requests in several lists
        # - Incoming render requests are initially put into the request queue
        # If the request queue is full then the new request is assigned to the dirty queue
        # - Incoming 'dirty' requests are put into the dirty queue, or dropped if this is full
        # - The render queue holds the requests which are in progress by the render threads
        self.requests = {}
        self.dirties = {}
        self.rendering = {}

        self.request_limit = request_limit
        self.dirty_limit = dirty_limit
        self.not_empty = threading.Condition()

    def add(self, request):
        self.not_empty.acquire()
        try:
            t = request.meta_tuple()
            if t in self.rendering:
                self.rendering[t].append(request)
                return "rendering"
            if t in self.requests:
                self.requests[t].append(request)
                return "requested"
            if t in self.dirties:
                self.dirties[t].append(request)
                return "dirty"
            # If we've reached here then there are no existing requests for this tile
            if protocol.isRender(request.command) and len(self.requests) < self.request_limit:
                self.requests[t] = [request]
                self.not_empty.notify()
                return "requested"
            if protocol.isDirty(request.command) and len(self.dirties) < self.dirty_limit:
                self.dirties[t] = [request]
                self.not_empty.notify()
                return "dirty"
            return "dropped"
        finally:
            self.not_empty.release()


    def fetch(self):
        # Fetches a request tuple from the request or dirty queue
        # The requests are moved to the rendering queue while they are being rendered
        self.not_empty.acquire()
        try:
            while (len(self.requests) == 0) and (len(self.dirties) == 0):
                self.not_empty.wait()
            # Pull request from one of the incoming queues
            try:
                item = self.requests.popitem()
            except KeyError:
                try:
                    item = self.dirties.popitem()
                except KeyError:
                    logging.debug("Odd, queues empty")
                    return

            t = item[0]
            self.rendering[t] = item[1]
            return item
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

def create_session(password, styles, host_list):
    import base64
    session = base64.b64encode(os.urandom(16))
    issuer = Issuer(session, password, styles, host_list)
    session_thread = threading.Thread(target=issuer.work)
    session_thread.setDaemon(True)
    session_thread.start()
    logging.info("Started session thread")
    return session

if __name__ == "__main__":
    try:
        cfg_file = os.environ['RENDERD_CFG']
    except KeyError:
        cfg_file = "/etc/mapgang.conf"

    config = Config(cfg_file)
    
    logging.basicConfig(filename=config.get("master", "log_file"), level=config.getint("master", "log_level"), format='%(asctime)s %(levelname)s: %(message)s')

    num_threads    = config.getint("master", "threads")
    socket         = config.get("master", "socket")
    tile_dir       = config.get("master", "tile_dir")
    job_server     = config.get("master", "job_server")
    password       = config.get("master", "job_password")

    storage = Disk()
    storage.set_path(tile_dir)
    
    styles = config.getStyles()
    #sessionId = create_session(password, styles, [job_server])

    try:
        queue_handler = RequestQueues(config.getint("master", "request_limit"), config.getint("master", "dirty_limit"))
        for i in range(num_threads):
            renderer = RequestThread(styles, queue_handler, [job_server])
            render_thread = threading.Thread(target=renderer.loop)
            render_thread.setDaemon(True)
            render_thread.start()
            logging.info("Started request thread %s", render_thread.getName())
            
        # Create the server
        server = ThreadedUnixStreamServer(socket, queue_handler, ThreadedUnixStreamHandler)
        # Loop forever servicing requests
        server.serve_forever()
    except (KeyboardInterrupt, SystemExit):
        logging.warning("terminating...")
        sys.exit()
