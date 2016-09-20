#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This is a skeleton file that can serve as a starting point for a Python
console script. To run this script uncomment the following line in the
entry_points section in setup.cfg:

    console_scripts =
     fibonacci = jax.skeleton:run

Then run `python setup.py install` which will install the command `fibonacci`
inside your current environment.
Besides console scripts, the header (i.e. until _logger...) of this file can
also be used as template for Python modules.

Note: This skeleton file can be safely removed if not needed!
"""

import argparse
import sys
import logging

from jax import __version__
from jax.runs_generator import RunsGenerator
from jax.output import MonitorOutput
from jax.processor import Processor
from configparser import ConfigParser
from multiprocessing import Process

import time

__author__ = "Daniel Coderre"
__copyright__ = "Daniel Coderre"
__license__ = "gpl3"

_logger = logging.getLogger(__name__)


def parse_args(args):
    """
    Parse command line parameters

    :param args: command line parameters as list of strings
    :return: command line parameters as :obj:`argparse.Namespace`
    """
    parser = argparse.ArgumentParser(
        description="Online monitor backend")
    parser.add_argument(
        '--version',
        action='version',
        version='jax {ver}'.format(ver=__version__))
    parser.add_argument(
        '--config',
        dest="config",
        help="path to configuration file",
        type=str,
        default="config/default.ini",
        metavar="config")
    parser.add_argument(
        '-j', 
        type=int,
        dest="cores",
        help="Concurrent processing threads",
        default=1
    )
    parser.add_argument(
        '-v',
        '--verbose',
        dest="loglevel",
        help="set loglevel to INFO",
        action='store_const',
        const=logging.INFO)
    parser.add_argument(
        '-vv',
        '--very-verbose',
        dest="loglevel",
        help="set loglevel to DEBUG",
        action='store_const',
        const=logging.DEBUG)
    return parser.parse_args(args)


def main(args):
    args = parse_args(args)
    logging.basicConfig(level=args.loglevel, stream=sys.stdout)
    _logger.debug("Starting monitoring")

    # Get configuration
    configp = ConfigParser(inline_comment_prefixes='#',
                           strict=True,
                           default_section='powdered_cheddar')
    configp.read(args.config)

    # Initialize runs list generator, output plugin, processor
    runs = RunsGenerator(configp)
    output = MonitorOutput(configp)
    processor = Processor(configp)

    # Loop runs list, insert data
    autorun=False
    if(configp.getboolean("jax", "autoprocess")):
        autorun = configp.getboolean("jax", "autoprocess")

    threads = []
    while(True):
        
        # Start up to n processes
        for i in range(0, args.cores):
        
            # Check if we have a lingering thread open
            if i < len(threads):
                continue
            
            # For each process, loop through runs
            for run in runs.get():                
                
                if output.register_processor(run):
                    rundoc = runs.get_run_doc(run)
                    print("Processing run " + rundoc['name'])
                    t = (Process(target = thread_process, 
                                args=(configp, rundoc)))
                    threads.append(t)
                    print("Appended new thread. ("+str(len(threads))+")")
                    t.start()
                    break
        
        # Join threads with 1 second timeout. 
        # If we're autorunning, return only living threads to list,
        # allowing new threads to be spawned if one finishes
        for t in threads:
            t.join(1)
        ntb = len(threads)
        if autorun:
            threads = [t for t in threads if t.is_alive()]
        nta = len(threads)
        if len(threads)==0 and not autorun:
            break
        if ntb != nta:
            print("Killed " + str(nta-ntb) + " threads")
        print("Sleeping")
        time.sleep(5)

    # Loop through threads and join all
    print("End of program. Joining threads")
    for t in threads:
        t.join()

    _logger.info("Monitor stopped")

def thread_process(config, rundoc):
    output = MonitorOutput(config)
    processor = Processor(config)
    return processor.process_run(output, rundoc)


def run():
    main(sys.argv[1:])


if __name__ == "__main__":
    run()
