===
jax
===


Jax is basically a file format converter. It is the backend for the online monitor. 

You can provide jax with either a raw or processed XENON1T data file. In the case 
of raw file, jax will process each event (or you can prescale to get a subset). 
Either way jax will output a (optionally reduced) form of each event to the interface 
designated in the output class. Right now only a flat, simple BSON/pymongo output 
is designed for use with the online monitor. It is easy to extend this to any 
other interface and format if you want to try something creative.

Jax is not an acronym. The name is inspired by a faux-cheesy snack food. The base 
was coded mostly before lunch one day.

Installation
============

Run the following::

  pip install -r requirements.txt
  # You also need pax. See xenon1t.github.io/pax
  python setup.py install

Note if you use ROOT file input you need rootpy, so install this when you install pax.


Description
===========

A config file must be specified on the command line. Run jax like this::

  jaxer --config=<path_to_config> -j8

And jax will start processing with 8 cores. 

In addition to the configuration options you need to set 4 environment variables 
for the MongoDB connectivity. The runs database is accessed using the normal 
MONGO_USER and MONGO_PASSWORD variables that are also used for pax. This 
requires only a read-only connection and authenticates to run. The monitor 
requires read/write permission and its user/password are set with MONITOR_USER 
and MONITOR_PASSWORD. You specify in any case the authentication database in 
your connection string in the ini file. Note! If you change to use a different
output format you can probably omit MONITOR_USER and MONITOR_PASSWORD. An update
to do this will be forthcoming as soon as someone needs it.
