#!/usr/bin/python

from cStringIO import StringIO
from ConfigParser import ConfigParser

class Config(ConfigParser):
    def __init__(self, filename):
        default_cfg = StringIO("""
[master]
socketname=/tmp/mod_tile.sock
tile_dir=/var/lib/mod_tile
log_file=
log_level=5
threads=32
[worker]
threads=2
""")
        self.readfp(default_cfg)
        self.read(filename)
        
    def getStyles(self):
        styles = {}
        for name in self.sections():
            if name != "master" and name != "worker":
                styles[name] = self.get(name, "xml")
        return styles
