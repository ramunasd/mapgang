#!/usr/bin/python

import sys
import logging
import json
import time

try:
    import mapnik

    from mapgang.constants import *
    from mapgang.projection import SphericalProjection
    from mapgang.metatile import MetaTile
    from gearman import GearmanWorker
except ImportError as e:
    print e
    sys.exit()

def render_job(worker, job):
    return worker.render_job(job)

class Renderer(GearmanWorker):
    def __init__(self, host_list=None, styles = {}, stop = None, max_jobs=1000, lifetime=3600):
        GearmanWorker.__init__(self, host_list)

        self.maps = {}
        self.prj = {}
        self.stop = stop
        self.max_jobs = max_jobs
        self.done = 0
        self.lifetime = lifetime
        self.started = time.time()
        
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
            logging.debug("Caught stop signal")
            return False
        if self.done >= self.max_jobs:
            logging.debug("Max jobs limit exceeded, stopping")
            return False
        if time.time() - self.started > self.lifetime:
            logging.debug("Max worker lifetime exceeded, stopping")
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
        t = MetaTile(style, x, y, z)
        logging.info("Got job: %s", t.to_string())
        
        try:
            m = self.maps[style]
        except KeyError:
            logging.error("No map for '%s'", style)
            self.send_job_failure(job)
            return
        
        try:
            im = self.render_image(style, m, x, y, z)
            size = min(METATILE, 1 << z)
            mask = METATILE - 1
            for xx in range(size):
                for yy in range(size):
                    mt = ((x+xx) & mask) * METATILE + ((y+yy) & mask)
                    view = im.view(xx * TILE_SIZE , yy * TILE_SIZE, TILE_SIZE, TILE_SIZE)
                    tile = view.tostring('png256:z=3')
                    t.write_tile(mt, tile)
        except Exception, e:
            logging.critical(e)
            self.send_job_failure(job)
            return

        logging.info("Completed: %s", t.to_string())
        self.done += 1
        return t.getvalue()
