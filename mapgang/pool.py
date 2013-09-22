#!/usr/bin/python

from multiprocessing import Process, Event

class WorkerPool(object):
    """
    Starts and manages a pool of worker processes.
    
    :param size: number of parallel worker processes
    
    """
    
    def __init__(self, factory, size=2):
        self.processes = {}
        self.factory = factory
        self.size = size
        self.stop = Event()
        
    def start(self):
        for i in xrange(self.size - len(self.processes)):
            p = Worker()
            p.factory = self.factory
            p.stop = self.stop
            p.start()
            self.processes[p.pid] = p
            
    def check(self):
        for proc in self.processes.values():
            if not proc.is_alive():
                self.processes.pop(proc.pid)

        self.start()
        
    def join(self, timeout = 5):
        self.stop.set()
        for proc in self.processes.values():
            proc.join(timeout)
        self.processes.clear()
        self.stop.clear()
        
    def terminate(self):
        self.stop.set()
        for proc in self.processes.values():
            proc.terminate()
        self.processes.clear()
        self.stop.clear()

class Worker(Process):
    def run(self):
        try:
            self.worker = self.factory(self.stop)
            self.worker.work()
        except (Exception, KeyboardInterrupt, SystemExit):
            return
    