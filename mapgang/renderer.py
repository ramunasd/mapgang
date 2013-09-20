#!/usr/bin/python

import sys, struct
import logging
import json
from cStringIO import StringIO

try:
    import mapnik

    from mapgang.constants import *
    from mapgang.projection import SphericalProjection

    from gearman import GearmanWorker
except ImportError as e:
    print e
    sys.exit()

def render_job(worker, job):
    return worker.render_job(job)

class Renderer(GearmanWorker):
    def __init__(self, host_list=None, styles = {}, stop = None):
        GearmanWorker.__init__(self, host_list)

        self.maps = {}
        self.prj = {}
        self.stop = stop
        
        # Projects between tile pixel co-ordinates and LatLong (EPSG:4326)
        self.tileproj = SphericalProjection(MAX_ZOOM)

        for style in styles:
            logging.debug("Creating map object for %s using %s" % (style, styles[style]))
            m = mapnik.Map(TILE_SIZE, TILE_SIZE)
            self.maps[style] = m
            # Load XML style
            mapnik.load_map(m, styles[style], True)
            # Obtain <Map> projection
            self.prj[style] = mapnik.Projection(m.srs)
            self.register_task("render_" + style, render_job);

    def after_poll(self, any_activity):
        if self.stop.is_set():
            return False
        return True

    def render_image(self, style, m, x, y, z):
        # Calculate the meta tile size to use for this zoom level
        size = min(METATILE, 1 << z)
        # Calculate pixel positions of bottom-left & top-right
        p0 = (x * TILE_SIZE, (y + size) * TILE_SIZE)
        p1 = ((x + size) * TILE_SIZE, y * TILE_SIZE)
        # Convert to LatLong (EPSG:4326)
        l0 = self.tileproj.fromPixelToLL(p0, z);
        l1 = self.tileproj.fromPixelToLL(p1, z);
        # Convert to map projection (e.g. mercator co-ords EPSG:900913)
        c0 = self.prj[style].forward(mapnik.Coord(l0[0], l0[1]))
        c1 = self.prj[style].forward(mapnik.Coord(l1[0], l1[1]))
        # Bounding box for the meta-tile
        bbox = mapnik.Envelope(c0.x,c0.y, c1.x,c1.y)
        meta_size = TILE_SIZE * size
        m.resize(meta_size, meta_size)
        m.zoom_to_box(bbox)
        if(m.buffer_size == 0):
            m.buffer_size = 128
        im = mapnik.Image(meta_size, meta_size);
        mapnik.render(m, im)
        return im

    def render_job(self, job):
        (style, x, y, z) = json.loads(job.data)
        logging.info("Got job: %s %d/%d/%d", style, z, x, y)
        
        try:
            m = self.maps[style]
        except KeyError:
            logging.error("No map for '%s'", style)
            self.send_job_failure(job)
            return
        
        try:
            im = self.render_image(style, m, x, y, z)
            size = min(METATILE, 1 << z)
            offset = len(META_MAGIC) + 4 * 4
            # Need to pre-compensate the offsets for the size of the offset/size table we are about to write
            offset += (2 * 4) * (METATILE * METATILE)
            # Collect all the tile sizes
            sizes = {}
            offsets = {}
            
            # tiles buffer
            tiles = StringIO()
            mask = METATILE - 1
            for xx in range(size):
                for yy in range(size):
                    mt = ((x+xx) & mask) * METATILE + ((y+yy) & mask)
                    view = im.view(xx * TILE_SIZE , yy * TILE_SIZE, TILE_SIZE, TILE_SIZE)
                    tile = view.tostring('png256:z=3')
                    tiles.write(tile)
                    sizes[mt] = len(tile)
                    offsets[mt] = offset
                    offset += len(tile)
        except Exception, e:
            logging.critical(e)
            self.send_job_failure(job)
            return

        # meta tile file
        meta = StringIO()
        # write header
        meta.write(struct.pack("4s4i", META_MAGIC, METATILE * METATILE, x, y, z))
        # Write out the offset/size table
        for mt in range(0, METATILE * METATILE):
            if mt in sizes:
                meta.write(struct.pack("2i", offsets[mt], sizes[mt]))
            else:
                meta.write(struct.pack("2i", 0, 0))
        # write tiles data
        tiles.seek(0)
        meta.write(tiles.read())
        
        logging.info("Completed: %s %d/%d/%d", style, z, x, y)
        return meta.getvalue()
