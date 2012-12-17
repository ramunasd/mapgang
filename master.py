#!/usr/bin/python

import sys, os, struct, time, errno
import thread, threading
import socket
import json
import ConfigParser
from cStringIO import StringIO

try:
    from gearman import GearmanClient
    from gearman.constants import PRIORITY_NONE, PRIORITY_LOW, PRIORITY_HIGH, JOB_UNKNOWN, JOB_PENDING
except ImportError:
    print "Missing Gearman client"
    sys.exit()

from mapgang.constants import *
from mapgang.protocol import protocol
from mapgang.session import Issuer

class MetaTile():
    path = '/var/lib/mod_tile'

    @staticmethod
    def get_path(style, x, y, z):
        mask = METATILE - 1
        x &= ~mask
        y &= ~mask
        hashes = {}
        for i in range(5):
            hashes[i] = ((x & 0x0f) << 4) | (y & 0x0f)
            x >>= 4
            y >>= 4

        path = "%s/%s/%d/%u/%u/%u/%u/%u.meta" % (MetaTile.path, style, z, hashes[4], hashes[3], hashes[2], hashes[1], hashes[0])
        d = os.path.dirname(path)
        if not os.path.exists(d):
            try:
                os.makedirs(d, 0777)
            except OSError:
                # Multiple threads can race when creating directories,
                # ignore exception if the directory now exists
                if not os.path.exists(d):
                    raise
        return path

    @staticmethod
    def get_header(self, x, y, z):
        return struct.pack("4s4i", META_MAGIC, METATILE * METATILE, x, y, z)


class RequestThread(GearmanClient):
    def __init__(self, tile_path, styles, queue_handler, host_list):
        self.tile_path = tile_path
        self.queue_handler = queue_handler
        self.styles = styles
        GearmanClient.__init__(self, host_list)
        os.umask(0)

    def render_request(self, t):
        (style, x, y, z) = t
        try:
            m = self.styles[style]
        except KeyError:
            print "No map for: '%s'" % style
            return False
        
        response = self.submit_job("render", json.dumps(t), priority=PRIORITY_HIGH, background=False)
        if response.complete:
            if response.result == "":
                return False
            return self.save_tiles(style, x, y, z, response.result)
        elif response.timed_out:
            print "Job %s timed out" % rsponse.unique
        elif response.state == JOB_UNKNOWN:
            print "Job %s connection failed!" % response.unique
            
        return False;

    def save_tiles(self, style, x, y, z, tiles):
        # Calculate the meta tile size to use for this zoom level
        size = min(METATILE, 1 << z)
        tile_path = MetaTile.get_path(style, x, y, z)
        tmp = "%s.tmp.%d" % (tile_path, thread.get_ident())
        f = open(tmp, "w")
        f.write(tiles)
        f.close()
        os.rename(tmp, tile_path)
        os.chmod(tile_path, 0666)
        print "Wrote: %s" % tile_path
        return True

    def loop(self):
        while True:
            #Fetch a meta-tile to render
            r = self.queue_handler.fetch()
            rendered = self.render_request(r)
            # Retrieve all requests for this meta-tile
            requests = self.queue_handler.pop_requests(r)
            for request in requests:
                if request.commandStatus in (protocol.Render, protocol.RenderPrio, protocol.RenderBulk):
                    if rendered == True:
                        request.send(protocol.Done)
                    else:
                        request.send(protocol.NotDone)



class RequestQueues:
    def __init__(self, request_limit = 32, dirty_limit = 1000):
        # We store requests in several lists
        # - Incoming render requests are initally put into the request queue
        # If the request queue is full then the new request is demoted to the dirty queue
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
            if (request.commandStatus in (protocol.Render, protocol.RenderPrio, protocol.RenderBulk)) and (len(self.requests) < self.request_limit):
                self.requests[t] = [request]
                self.not_empty.notify()
                return "requested"
            if len(self.dirties) < self.dirty_limit:
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
                    print "Odd, queues empty"
                    return

            t = item[0]
            self.rendering[t] = item[1]
            return t
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
            print "WARNING: Failed to locate request in rendering list!"
        finally:
            self.not_empty.release()


def start_renderers(num_threads, tile_path, styles, queue_handler, host_list):
    for i in range(num_threads):
        renderer = RequestThread(tile_path, styles, queue_handler, host_list)
        render_thread = threading.Thread(target=renderer.loop)
        render_thread.setDaemon(True)
        render_thread.start()
        print "Started render thread %s" % render_thread.getName()

def listener(address, queue_handler):
    from mapgang.threadedSocket import ThreadedUnixStreamServer, ThreadedUnixStreamHandler
    # Create the server
    server = ThreadedUnixStreamServer(address, queue_handler, ThreadedUnixStreamHandler)
    # Loop forever servicing requests
    server.serve_forever()

def display_config(config):
    for xmlname in config.sections():
        if xmlname != "renderd" and xmlname != "mapnik":
            print "Layer name: %s" % xmlname
            uri = config.get(xmlname, "uri")
            xml = config.get(xmlname, "xml")
            print "    URI(%s) = XML(%s)" % (uri, xml)

def read_styles(config):
    styles = {}
    for xmlname in config.sections():
        if xmlname != "renderd" and xmlname != "mapnik":
            styles[xmlname] = config.get(xmlname, "xml")
    return styles

def create_session(password, styles, host_list):
    import base64
    session = base64.b64encode(os.urandom(16))
    issuer = Issuer(session, password, styles, host_list)
    session_thread = threading.Thread(target=issuer.work)
    session_thread.setDaemon(True)
    session_thread.start()
    print "Started session thread"
    return session

if __name__ == "__main__":
    try:
        cfg_file = os.environ['RENDERD_CFG']
    except KeyError:
        cfg_file = "/etc/mapgang.conf"

    default_cfg = StringIO("""
[renderd]
socketname=/tmp/mod_tile.sock
num_threads=1
tile_dir=/var/lib/mod_tile
""")

    config = ConfigParser.ConfigParser()
    config.readfp(default_cfg)
    config.read(cfg_file)
    display_config(config)
    styles = read_styles(config)

    num_threads    = config.getint("renderd", "num_threads")
    renderd_socket = config.get("renderd", "socketname")
    tile_dir       = config.get("renderd", "tile_dir")
    job_server     = config.get("renderd", "job_server")
    password       = config.get("renderd", "job_password")

    MetaTile.path = tile_dir
    sessionId = create_session(password, styles, [job_server])
    queue_handler = RequestQueues()
    start_renderers(num_threads, tile_dir, styles, queue_handler, [job_server])
    try:
        listener(renderd_socket, queue_handler)
    except (KeyboardInterrupt, SystemExit):
        print "terminating..."
        sys.exit()