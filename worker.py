#!/usr/bin/python

import sys, os, time
import logging
import threading

try:
    from mapgang.session import Client
    from mapgang.renderer import Renderer
    from mapgang.config import Config
except ImportError as e:
    print e
    sys.exit()

if __name__ == "__main__":
    try:
        cfg_file = os.environ['RENDERD_CFG']
    except KeyError:
        cfg_file = "/etc/mapgang.conf"

    config = Config(cfg_file)

    num_threads = config.getint("worker", "threads")
    job_server  = config.get("master", "job_server")
    password    = config.get("master", "job_password")
    
    logging.basicConfig(filename=config.get("master", "log_file"), level=config.getint("master", "log_level"), format='%(asctime)s %(levelname)s: %(message)s')
    
    try:
        styles = config.getStyles()
        stop = threading.Event()

        for i in range(num_threads):
            worker = Renderer([job_server], styles, stop)
            worker_thread = threading.Thread(target=worker.work, args=(3.0,))
            worker_thread.setDaemon(True)
            worker_thread.start()
            logging.info("Started worker thread %s", worker_thread.getName())
        
        while(True):
            time.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        logging.warning("terminating...")
        stop.set()
        main_thread = threading.currentThread()
        # shutdown gracefully
        for t in threading.enumerate():
            if t is main_thread:
                continue
            t.join(10)
        sys.exit()

