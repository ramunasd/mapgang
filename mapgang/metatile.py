#!/usr/bin/python

import os
import struct
from cStringIO import StringIO
from mapgang.constants import METATILE, META_MAGIC

class MetaTile():
    def __init__(self, style, x, y, z):
        self.style = style
        self.x = x
        self.y = y
        self.z = z
        self.content = StringIO()
        m2 = METATILE * METATILE
        # space for header
        self.content.write(struct.pack("4s4i", META_MAGIC, m2, 0, 0, 0))
        # space for offset/size table
        self.content.write(struct.pack("2i", 0, 0) * m2)
        self.sizes = {}
        self.offsets = {}
    
    def get_header(self):
        return struct.pack("4s4i", META_MAGIC, METATILE * METATILE, self.x, self.y, self.z)
    
    def write_header(self):
        self.content.seek(0)
        # write header
        self.content.write(self.get_header())
        # Write out the offset/size table
        for n in range(0, METATILE * METATILE):
            if n in self.sizes:
                self.content.write(struct.pack("2i", self.offsets[n], self.sizes[n]))
            else:
                self.content.write(struct.pack("2i", 0, 0))

    def write_tile(self, x, y, tile):
        mask = METATILE - 1
        n = (x & mask) * METATILE + (y & mask)
        # seek to end
        self.content.seek(0, os.SEEK_END)
        # mark offset
        self.offsets[n] = self.content.tell()
        # write content
        self.content.write(tile)
        # mark size
        self.sizes[n] = len(tile)
    
    def getvalue(self):
        self.write_header()
        return self.content.getvalue()
    
    def to_string(self):
        return "%s/%d/%d/%d" % (self.style, self.z, self.x, self.y)
