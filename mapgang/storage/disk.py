#!/usr/bin/python

import os
import logging

from mapgang.constants import METATILE

class Disk():
    path = '/var/lib/mod_tile'
    
    def set_path(self, path):
        self.path = path
    
    def get_meta_path(self, style, x, y, z):
        mask = METATILE - 1
        x &= ~mask
        y &= ~mask
        hashes = {}
        for i in range(5):
            hashes[i] = ((x & 0x0f) << 4) | (y & 0x0f)
            x >>= 4
            y >>= 4

        path = "%s/%s/%d/%u/%u/%u/%u/%u.meta" % (self.path, style, z, hashes[4], hashes[3], hashes[2], hashes[1], hashes[0])
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
    
    def write(self, style, x, y, z, tiles):
        path = self.get_meta_path(style, x, y, z)
        f = os.open(path, os.O_WRONLY | os.O_CREAT)
        os.write(f, tiles)
        os.close(f)
        logging.debug("Wrote meta tile %s", path)
        return True
        