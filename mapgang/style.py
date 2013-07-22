#!/usr/bin/python

import logging

class MapnikStyle:
    def __init__(self, name, filename):
        self.name = name
        self.filename = filename
        self.load()

    def load(self):
        f = open(self.filename, 'r')
        self.content = f.read()
        logging.info("style %s loaded", self.name)

    def getCompacted(self):
        return self.content
