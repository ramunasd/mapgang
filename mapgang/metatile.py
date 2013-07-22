#!/usr/bin/python

import os, struct
from mapgang.constants import METATILE, META_MAGIC

class MetaTile():
    path = '/var/lib/mod_tile'

    @staticmethod
    def get_path(style, x, y, z):
        mask = METATILE - 1
        x &= ~mask
        y &= ~mask
        hashes = {}
        for i in range(5):
            hashes[i] = ((x & 0x0f) << 4) | (y & 0x0f)
            x >>= 4
            y >>= 4

        path = "%s/%s/%d/%u/%u/%u/%u/%u.meta" % (MetaTile.path, style, z, hashes[4], hashes[3], hashes[2], hashes[1], hashes[0])
        d = os.path.dirname(path)
        if not os.path.exists(d):
            try:
                os.makedirs(d, 0777)
            except OSError:
                # Multiple threads can race when creating directories,
                # ignore exception if the directory now exists
                if not os.path.exists(d):
                    raise
        return path

    @staticmethod
    def get_header(self, x, y, z):
        return struct.pack("4s4i", META_MAGIC, METATILE * METATILE, x, y, z)
