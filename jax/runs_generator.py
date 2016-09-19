"""
Mediates communication with the runs database. 
Allows list input as well if wanted.
"""


from configparser import ConfigParser
import logging
_logger = logging.getLogger(__name__)
import pymongo
import os

class RunsGenerator(object):
    """
    The whole point of this thing is to tell the processing nodes which
    run to look at next.
    """

    def __init__(self, config):

        # Declare runs DB
        self.db = None
        if ( config.has_option("runs_input", "runs_uri") and
             config.has_option("runs_input", "runs_db") and
             config.has_option("runs_input", "runs_collection") ):
            try:
                # Get environment variables for mongo
                user = os.getenv("MONGO_USER")
                password = os.getenv("MONGO_PASSWORD")
                upstr = ""
                if user is not None and password is not None:
                    upstr = user + ":" + password + "@"
                client = pymongo.MongoClient("mongodb://" + upstr + config.get(
                    "runs_input", "runs_uri"))        
                database = config.get("runs_input", "runs_db")
                collection = config.get("runs_input", "runs_collection")
                self.db = client[database][collection]

            except pymongo.errors.ConnectionFailure as e:
                print("Error! Didn't connect to runs DB! " + e)
                _logger.debug("Failed to connect to runs DB. Standalone mode?")

        # In case we get a list of runs
        self.runs_to_process = []
        if config.has_option("runs_input", "process_runs"):
            self.runs_to_process = config.get("runs_input","process_runs")

        # Get some query options
        self.detector = None
        self.first_run = None
        self.last_run = None
        if config.has_option("runs_input", "detector"):
            self.detector = config.get("runs_input", "detector")
        if config.has_option("runs_input", "first_run"):
            self.first_run = config.get("runs_input", "first_run")
        if config.has_option("runs_input", "last_run"):
            self.last_run = config.get("runs_input", "last_run")

    def get(self):

        # If user defined runs_to_process this is easy
        if len(self.runs_to_process) > 0:
            for item in self.runs_to_process:
                yield item
        
        if self.db == None:
            return None

        # Make the query
        query = {}
        if self.detector is not None:
            query["detector"] = self.detector
        if self.first_run is not None:
            query["number"] = {"$gte":self.first_run}
            if self.last_run is not None:
                query["number"]["$lte"] = self.last_run
        elif self.last_run is not None:
            query["number"] = {"$lte": self.last_run}

        print(query)
        try:
            cursor = self.db.find(query).sort("_id", -1)
        except:
            _logger.error("Tried but failed to query runs DB")
            return None

        for doc in cursor:
            yield doc['name']
            
    def get_run_doc(self, name):
        
        if self.db == None:
            return

        # Make the query. Support for muon veto "just in case"
        query = {"detector": "tpc", "name": name}
        if self.detector is not None:
            query["detector"] = self.detector

        try:
            cursor = self.db.find(query)
        except:
            _logger.error("Tried but failed to get run doc for run " + name)
            return None
        
        if cursor.count() != 1:
            _logger.error("Tried to get doc for run " + name +
                          "but cursor returned a count of " + 
                          str(cursor.count()))
            return None

        return cursor[0]
