#!/usr/bin/python

import sys, os, time
import logging
import threading

from cStringIO import StringIO
from ConfigParser import ConfigParser

try:
    from mapgang.session import Client
    from mapgang.renderer import Renderer
except ImportError as e:
    print e
    sys.exit()

def read_styles(config):
    styles = {}
    for xmlname in config.sections():
        if xmlname != "renderd" and xmlname != "mapnik":
            styles[xmlname] = config.get(xmlname, "xml")
    return styles

def start_renderers(num_threads, job_server, styles, stop):
    for i in range(num_threads):
        worker = Renderer([job_server], styles, stop)
        worker_thread = threading.Thread(target=worker.work, args=(3.0,))
        worker_thread.setDaemon(True)
        worker_thread.start()
        logging.info("Started worker thread %s", worker_thread.getName())

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
worker_threads=1
log_file=
log_level=20
""")

    config = ConfigParser()
    config.readfp(default_cfg)
    config.read(cfg_file)

    num_threads = config.getint("renderd", "worker_threads")
    job_server  = config.get("renderd", "job_server")
    password    = config.get("renderd", "job_password")
    
    logging.basicConfig(filename=config.get("renderd","log_file"),level=config.getint("renderd","log_level"),format='%(asctime)s %(levelname)s:%(message)s')
    
    try:
        #session_client = Client([job_server])
        #session  = session_client.getSession(password)
        #if session == "":
        #    print "Cannot start session"
        #    sys.exit()
        #print "got session %s" % session
        #styles = session_client.getStyles(session)
        #print "got styles %s" % styles

        styles = read_styles(config)
        stop = threading.Event()
        start_renderers(num_threads, job_server, styles, stop)
        while(True):
            time.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        logging.warning("terminating...")
        stop.set()
        main_thread = threading.currentThread()
        for t in threading.enumerate():
            if t is main_thread:
                continue
            t.join(10)
        sys.exit()
