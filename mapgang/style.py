#!/usr/bin/python

class MapnikStyle:
    def __init__(self, name, filename):
        self.name = name
        self.filename = filename
        self.load()

    def load(self):
        f = open(self.filename, 'r')
        self.content = f.read()
        print "style %s loaded" % self.name

    def getCompacted(self):
        return self.content
