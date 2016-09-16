===
jax
===


Jax is the backend for the online monitor. It's also a cheesy snack food.


Description
===========

This is a work in progress. Will update with better docs once running. 
In principle you should be able to do:

jax -c path_to_config -j8

And jax will start processing with 8 cores. 

Support is being added for both raw data (jax processes a subset itself)
and processed data (input every event) inputs. Output is a small subset
of processed variables to a non relational DB. Online monitoring is done
via aggregations on this database. You could actually even do basic
analyses on this database. It's pretty spiffy.
