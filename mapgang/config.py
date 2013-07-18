#!/usr/bin/python

from cStringIO import StringIO
from ConfigParser import ConfigParser

class Config(ConfigParser):
    def __init__(self, filename):
	ConfigParser.__init__(self)
	# write defaults
	self._sections["master"] = {
		"socketname": "/tmp/mod_tile.sock",
		"tile_dir": "/var/lib/mod_tile",
		"log_file": "",
		"log_level": "5",
		"threads": "32",
		"request_limit": "32",
		"dirty_limit": 1000
	}
	# default worker options
	self._sections["worker"] = {
		"threads": "2"
	}
	# default runtime options for mapnik
	self._sections["mapnik"] = {
	}
        self.read(filename)
        
    def getStyles(self):
        styles = {}
        for name in self.sections():
            if name not in ("master", "worker", "mapnik"):
                styles[name] = self.get(name, "xml")
        return styles

    def printout(config):
        for xmlname in config.sections():
            if xmlname != "renderd" and xmlname != "mapnik":
               print "Layer name: %s" % xmlname
               uri = config.get(xmlname, "uri")
               xml = config.get(xmlname, "xml")
               print " - URI(%s) = XML(%s)" % (uri, xml)
