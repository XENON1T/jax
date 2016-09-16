"""
Mediates communication with the runs database. 
Allows list input as well if wanted.
"""

import pymongo
from ConfigParser import ConfigParser

import logging
_logger = logging.getLogger(__name__)

class RunsGenerator(object):
    """
    The whole point of this thing is to tell the processing nodes which
    run to look at next.
    """

    def __init__(self, config):

        # Declare runs DB
        self.db = None
        if ( config.getstring("runs_input", "runs_uri") and
             config.getstring("runs_input", "runs_db") and
             config.getstring("runs_input", "runs_collection") ):
            try:
                client = pymongo.MongoClient(config.getstring(
                    "runs_input", "runs_uri"))              
                self.db = client[config.getstring(
                    "runs_input", "runs_db")][config.getstring(
                        "runs_input", "runs_collection")]
            except:
                _logger.debug("Failed to connect to runs DB. Standalone mode?")

        # In case we get a list of runs
        self.runs_to_process = []
        if config.getlist("runs_input", "process_runs"):
            self.runs_to_process = config.getlist("runs_input","process_runs")

        # Get some query options
        self.detector = None
        self.first_run = None
        self.last_run = None
        if config.getstring("runs_input", "detector"):
            self.detector = config.getstring("runs_input", "detector")
        if config.getstring("runs_input", "first_run"):
            self.first_run = config.getstring("runs_input", "first_run")
        if config.getstring("runs_input", "last_run"):
            self.last_run = config.getstring("runs_input", "last_run")

    def get(self):

        # If user defined runs_to_process this is easy
        if len(self.runs_to_process) > 0:
            for item in self.runs_to_process:
                yield item
        
        if self.db == None:
            return

        # Make the query
        query = {}
        if self.detector is not None:
            query["detector"] = self.detector
        if self.first_run is not None:
            query["number"] = {"$gte":self.first_run}
            if self.last_run is not None:
                query["number"]["$lte"] = self.last_run
        elif self.last_run isnot None:
            query["number"] = {"$lte": self.last_run}

        try:
            cursor = self.db.find(query).sort("_id", -1)
        except:
            _logger.error("Tried but failed to query runs DB")
            return

        for doc in cursor:
            yield doc['name']
