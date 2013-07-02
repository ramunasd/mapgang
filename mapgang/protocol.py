#!/usr/bin/python

import struct
import socket
import errno
from mapgang.constants import *

class protocol:
    # ENUM values for commandStatus field in protocol packet
    Ignore = 0
    Render = 1
    Dirty = 2
    Done = 3
    NotDone = 4
    RenderPrio = 5
    RenderBulk = 6

class ProtocolPacket:
    def __init__(self, version, fields = ""):
        self.version = version
        self.xmlname = ""
        self.x = 0
        self.y = 0
        self.z = 0
        self.mx = 0
        self.my = 0
        self.commandStatus = protocol.Ignore
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

class ProtocolPacketV2(ProtocolPacket):
    def __init__(self):
        ProtocolPacket(2)
        self.fields = "5i41sxxx"

    def receive(self, data, dest):
        version, request, x, y, z, xmlname = struct.unpack(self.fields, data)

        if version != 2:
            print "Received V2 packet with incorect version %d" % version
        else:
            #print "Got V2 request, command(%d), xmlname(%s), x(%d), y(%d), z(%d)" % (request, xmlname, x, y, z)
            self.commandStatus = request
            self.x = x
            self.y = y
            self.z = z
            self.xmlname = xmlname.rstrip('\000') # Remove trailing NULs
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
