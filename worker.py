#!/usr/bin/python

import sys, os, time
import logging

try:
    from mapgang.config import Config
    from mapgang.pool import WorkerPool
    from mapgang.renderer import Renderer
except ImportError as e:
    print e
    sys.exit()

if __name__ == "__main__":
    try:
        cfg_file = os.environ['RENDERD_CFG']
    except KeyError:
        cfg_file = "/etc/mapgang.conf"

    config = Config(cfg_file)

    num_workers = config.getint("worker", "threads")
    job_server  = config.get("master", "job_server")
    password    = config.get("master", "job_password")
    
    logging.basicConfig(filename=config.get("master", "log_file"), level=config.getint("master", "log_level"), format='%(asctime)s %(levelname)s: %(message)s')
    
    try:
        styles = config.getStyles()
        def worker_factory(stop):
            return Renderer([job_server], styles, stop)
        pool = WorkerPool(worker_factory, num_workers)
        pool.start()
                
        while(True):
            time.sleep(2)
            pool.check()
    except (KeyboardInterrupt, SystemExit):
        logging.warning("terminating...")
        pool.join()
