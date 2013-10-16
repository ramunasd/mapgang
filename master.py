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
from mapgang.config import Config
from mapgang.threadedSocket import ThreadedUnixStreamServer, ThreadedUnixStreamHandler
from mapgang.storage.disk import Disk
from mapgang.queue import Requests

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

    try:
        queue = Requests(config.getint("master", "request_limit"), config.getint("master", "dirty_limit"))
        for i in range(num_threads):
            renderer = RequestThread(styles, queue, [job_server])
            render_thread = threading.Thread(target=renderer.loop)
            render_thread.setDaemon(True)
            render_thread.start()
            logging.info("Started request thread %s", render_thread.getName())
            
        # Create the server
        server = ThreadedUnixStreamServer(socket, queue, ThreadedUnixStreamHandler)
        # Loop forever servicing requests
        server.serve_forever()
    except (KeyboardInterrupt, SystemExit):
        logging.warning("terminating...")
        sys.exit()
