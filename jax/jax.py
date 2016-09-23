#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This is a converter from either XENON1T raw data (BSON) or XENON1T
processed data (ROOT) to a reduced output suitable for serving on the
web. For raw data this program facilitates processing via pax.
For processed data it simply unpacks and loops the ROOT file. 
The initial use is for the online monitor, for which we have a flat
BSON output. You could very easily repurpose the output class to store 
the data in any format you want using any technology for which there is 
a python interface.
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
            for run in runs.get(processor):                
                
                if output.register_processor(run, processor.get_mode(),
                                             processor.get_prescale()):
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
