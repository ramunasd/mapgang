#!/usr/bin/python

try:
    from gearman import GearmanClient, GearmanWorker
    from gearman.constants import PRIORITY_NONE, PRIORITY_LOW, PRIORITY_HIGH, JOB_UNKNOWN, JOB_PENDING
    
    import json
except ImportError:
    print "Missing Gearman client"
    sys.exit()

class Client(GearmanClient):
    def __init__(self, host_list=None):
        GearmanClient.__init__(self, host_list=host_list)

    def getSession(self, password):
        data = self.submit_job("get_mapworker_session", password, priority=PRIORITY_HIGH)
        return data.result

    def getStyles(self, session):
        data = self.submit_job("get_mapworker_styles", session, priority=PRIORITY_HIGH)
        return data.result

class Issuer(GearmanWorker):
    def __init__(self, session, password, styles, host_list = None):
        GearmanWorker.__init__(self, host_list = host_list)
        self.session = session
        self.password = password
        self.styles = styles
        self.register_task("get_mapworker_session", self.getSession)
        self.register_task("get_mapworker_styles", self.getStyles)

    def getSession(self, worker, job):
        if job.data != self.password:
            return ""
        return self.session

    def getStyles(self, worker, job):
        if job.data != self.session:
            return ""
        return json.dumps(self.styles)
    
