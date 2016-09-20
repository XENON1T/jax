===
jax
===


Jax is the backend for the online monitor. It's also a cheesy snack food.

Installation
============

Run the following::

  pip install -r requirements.txt
  # You also need pax. See xenon1t.github.io/pax
  python setup.py install



Description
===========

Currently jax supports raw data input only. A config file must be specified on 
the command line. Run jax like this::

  jaxer --config=<path_to_config> -j8

And jax will start processing with 8 cores. 

In addition to the configuration options you need to set 4 environment variables 
for the MongoDB connectivity. The runs database is accessed using the normal 
MONGO_USER and MONGO_PASSWORD variables that are also used for pax. This 
requires only a read-only connection and authenticates to run. The monitor 
requires read/write permission and its user/password are set with MONITOR_USER 
and MONITOR_PASSWORD. You specify in any case the authentication database in 
your connection string in the ini file. 

Support is being added for processed data input. This will allow overwriting of 
collections previously written with a subset of raw data with the full processed 
data. This feature will come 'soon'. Probably.
