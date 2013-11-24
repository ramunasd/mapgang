#!/usr/bin/python

import struct
import socket
import errno
import logging
from mapgang.constants import MAX_ZOOM, METATILE

VER_LENGTH = 4 #struct.calcsize("1i")

class protocol:
    # ENUM values for commandStatus field in protocol packet
    Ignore     = 0
    Render     = 1
    Dirty      = 2
    Done       = 3
    NotDone    = 4
    RenderPrio = 5
    RenderBulk = 6
    RenderLow  = 7
    
    titles = {
        0: "Ignore",
        1: "Render",
        2: "Dirty",
        3: "Done",
        4: "Not done",
        5: "Render priority",
        6: "Render bulk",
        7: "Render low"
    }
    
    @staticmethod
    def isRender(status):
        return status in (protocol.Render, protocol.RenderPrio, protocol.RenderBulk, protocol.RenderLow)
    
    @staticmethod
    def isDirty(status):
        return (status == protocol.Dirty)
    
    @staticmethod
    def getProtocolByVersion(ver):
        if ver == 3:
            return ProtocolPacketV3()
        elif ver == 2:
            return ProtocolPacketV2()
        elif ver == 1:
            return ProtocolPacket(1, "4i")
        else:
            return False

class ProtocolPacket:
    command = protocol.Ignore
    x = 0
    y = 0
    z = 0
    mx = 0
    my = 0
    
    def __init__(self, version, fields = ""):
        self.version = version
        self.xmlname = ""
        self.fields = fields
    
    def send(self, status):
        try:
            self.dest.send(self.get_data(status))
        except socket.error, e:
            if e[0] != errno.EBADF:
                raise
    def set_coords(self, x, y, z):
        self.x = x
        self.y = y
        self.z = z
        # Calculate Meta-tile value for this x/y
        self.mx = x & ~(METATILE - 1)
        self.my = y & ~(METATILE - 1)

    def len(self):
        return struct.calcsize(self.fields)

    def bad_request(self):
        # Check that the requested (x,y,z) is invalid
        x = self.x
        y = self.y
        z = self.z

        if (z < 0) or (z > MAX_ZOOM):
            return True
        limit = (1 << z) -1
        if (x < 0) or (x > limit):
            return True
        if (y < 0) or (y > limit):
            return True
        return False

    def meta_tuple(self):
        # This metatile tuple is used to identify duplicate request in the rendering queue
        return (self.xmlname, self.mx, self.my, self.z)
    
    def to_string(self):
        return "%s %s/%u/%u/%u" % (protocol.titles[self.command], self.xmlname, self.z, self.x, self.y)

class ProtocolPacketV2(ProtocolPacket):
    def __init__(self):
        ProtocolPacket.__init__(self, 2, "4i41sxxx")

    def receive(self, data, dest):
        request, x, y, z, xmlname = struct.unpack(self.fields, data)
        self.dest = dest
        self.command = request
        self.set_coords(x, y, z)
        self.xmlname = xmlname.rstrip('\000') # Remove trailing NULL
        logging.debug("Got request: %s", self.to_string())

    def get_data(self, status):
        return struct.pack("1i", self.version) + struct.pack(self.fields, status, self.x, self.y, self.z, self.xmlname)


class ProtocolPacketV3(ProtocolPacket):
    def __init__(self):
        ProtocolPacket.__init__(self, 3, "4i41s41s41sxxx")

    def receive(self, data, dest):
        request, x, y, z, xmlname, mimetype, options = struct.unpack(self.fields, data)
        self.dest = dest
        self.command = request
        self.set_coords(x, y, z)
        self.xmlname = xmlname
        self.mimetype = mimetype
        self.options = options.rstrip("\000")
        logging.debug("Got request: %s", self.to_string())
    
    def get_data(self, status):
        return struct.pack("1i", self.version) + struct.pack(self.fields, status, self.x, self.y, self.z, self.xmlname, self.mimetype, self.options)
