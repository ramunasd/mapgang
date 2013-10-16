#!/usr/bin/python

import struct
import socket
import errno
import logging
from mapgang.constants import MAX_ZOOM, METATILE

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
        ProtocolPacket.__init__(self, 2, "5i41sxxx")

    def receive(self, data, dest):
        version, request, x, y, z, xmlname = struct.unpack(self.fields, data)

        if version != 2:
            logging.warn("Received V2 packet with incorrect version %d", version)
        else:
            self.command = request
            self.x = x
            self.y = y
            self.z = z
            self.xmlname = xmlname.rstrip('\000') # Remove trailing NULs
            logging.debug("Got request: %s", self.to_string())
            # Calculate Meta-tile value for this x/y
            self.mx = x & ~(METATILE-1)
            self.my = y & ~(METATILE-1)
            self.dest = dest

    def send(self, status):
        x = self.x
        y = self.y
        z = self.z
        xmlname = self.xmlname
        data = struct.pack(self.fields, 2, status, x, y, z, xmlname)
        try:
            self.dest.send(data)
        except socket.error, e:
            if e[0] != errno.EBADF:
                raise

