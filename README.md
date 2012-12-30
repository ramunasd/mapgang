
# What is MapGang

MapGang is open source distributed map rendering platform for OSM community. It's written in Python, uses Mapnik high level Python bindings and is based on Gearman.

# Overview

MapGang consists of tow parts: a master process and a workers. Master part is single process which listens for render requests on mod_tile socket and creates rendering jobs. On opposite, worker is multi-threaded process which takes rendering jobs from Gearman server and returns complete meta tile.

# Install
See [Install.md].

# Limitations

* only one Mapnik datasource is supported - database

# TODO

* rendering queue pool with variable thread count for master
* password secured session handling
* complete mapnik style transfer on sesson start and update for workers
* stats collector
* more mapnik datasource options

# License

MapGang software is free and is released under LGPL ([GNU Lesser General Public License](http://www.gnu.org/licenses/lgpl.html_). Please see [COPYING](https://github.com/ramunasd/mapgang/blob/master/COPYING) for more information.
