#!/usr/bin/python

import struct
from cStringIO import StringIO
from mapgang.constants import METATILE

META_MAGIC = "meta"

class MetaTile():
    def __init__(self, style, x, y, z):
        self.style = style
        self.x = x
        self.y = y
        self.z = z
        self.content = StringIO()
        # fill header with zeros
        offset = len(META_MAGIC) + 4 * 4
        # Need to pre-compensate the offsets for the size of the offset/size table we are about to write
        offset += (2 * 4) * (METATILE * METATILE)
        self.content.seek(offset)
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
        
    def write_tile(self, n, tile):
        # seek to end
        self.content.seek(0, 2)
        self.offsets[n] = self.content.tell()
        self.content.write(tile)
        self.sizes[n] = len(tile)
    
    def getvalue(self):
        self.write_header()
        return self.content.getvalue()
    
    def to_string(self):
        return "%s/%d/%d/%d" % (self.style, self.z, self.x, self.y)
