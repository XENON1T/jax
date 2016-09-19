"""
Mediates communication with the runs database. 
Allows list input as well if wanted.
"""


from configparser import ConfigParser
import logging
_logger = logging.getLogger(__name__)
import pymongo
import os

class MonitorOutput(object):
    """
    Connect to output database and save reduced events and waveforms
    """

    def __init__(self, config):

        # Declare monitor DB
        self.mdb = None
        if ( config.has_option("mongo_output", "monitor_uri") and
             config.has_option("mongo_output", "monitor_db") ):
            try:
                # Get environment variables for mongo
                user = os.getenv("MONGO_USER")
                password = os.getenv("MONGO_PASSWORD")
                upstr = ""
                if user is not None and password is not None:
                    upstr = user + ":" + password + "@"
                client = pymongo.MongoClient("mongodb://" + upstr + config.get(
                    "mongo_output", "monitor_uri"))        
                database = config.get("mongo_output", "monitor_db")
                self.mdb = client[database]

            except pymongo.errors.ConnectionFailure as e:
                print("Error! Can't connect to monitor db. " + e)
                _logger.debug("Failed to connect to monitor DB.")

        # Declare waveform db
        self.wdb = None
        if ( config.has_option("mongo_output", "waveform_uri") and
             config.has_option("mongo_output", "waveform_db") ):
            try:
                # Get environment variables for mongo
                user = os.getenv("MONGO_USER")
                password = os.getenv("MONGO_PASSWORD")
                upstr = ""
                if user is not None and password is not None:
                    upstr = user + ":" + password + "@"
                client = pymongo.MongoClient("mongodb://" + upstr + config.get(
                    "mongo_output", "waveform_uri"))
                database = config.get("mongo_output", "waveform_db")
                self.mdb = client[database]

            except pymongo.errors.ConnectionFailure as e:
                print("Error! Can't connect to waveform db. " + e)
                _logger.debug("Failed to connect to waveform DB.")
                
        self.instance_id = 0
        if config.has_option("mongo_output", "instance_id"):
            self.instance_id = config.getint("mongo_output", "instance_id")
        self.reprocess = False
        if config.has_option("mongo_output", "reprocess"):
            self.instance_id = config.getboolean("mongo_output", "reprocess")

    def register_processor(self, collection):
        """
        Looks for a status document. The status document looks like:
        {
           "type": "status",
           "instance_id": int,
        }
        We have two possible rules:
                1) Do not process runs who have been processed
                2) Process runs who have been processed but only if they
                   have a different instance ID as the current run
        """
        
        if self.mdb == None:
            print("No mongo")
            return False
            
        no_collection = True
        if collection in self.mdb.collection_names():
            no_collection = False
        
            try:
                stat = self.mdb[collection].find({"type": "status"})
            except:
                print("Can't connect to output DB")
                return False
        
        # Register it.
        if ( no_collection or stat.count() == 0 or 
             ( self.reprocess and "instance_id" in stat[0]
               and stat[0]["instance_id"] != self.instance_id) ):
            self.mdb[collection].update_one(
                {'type': 'status'},
                {'$set': {'instance_id': self.instance_id,
                          'type': 'status'}
             }, upsert=True)
            print("Returning true")
            return True
        print("Found everything but returning false")
        return False

    def save_doc(self, event, collection):

        if self.mdb == None:
            return

        # Put it into mongo. Simple stuff for now                                  
        insert_doc = {
            "type": "data",
            "s1": None,
            "s2": None,
            "largest_other_s1": None,
            "largest_other_s2": None,
            "ns1": None,
            "ns2": None,
            "dt": None,
            "x": None,
            "y": None,
            "time": None,
            "interactions": None,
            "prescale": prescale
        }
        insert_doc['ns1'] = len(event.s1s())
        insert_doc['ns2'] = len(event.s2s())
        insert_doc['time'] = event.start_time
        if len(event.s1s())>0:
            insert_doc['s1'] = event.s1s()[0].area
        if len(event.s1s())>1:
            insert_doc['largest_other_s1'] = event.s1s()[1].area
        if len(event.s2s())>0:
            insert_doc['s2'] = event.s2s()[0].area
        if len(event.s2s())>1:
            insert_doc['largest_other_s2'] = event.s2s()[1].area
        if len(event.interactions)>0:
            insert_doc['interactions'] = len(event.interactions)
            insert_doc['dt'] = event.interactions[0].drift_time
            insert_doc['x'] = event.interactions[0].x
            insert_doc['y'] = event.interactions[0].y
        #print(insert_doc)                                   

        try:
            self.mdb[collection].insert_one(insert_doc)
        except:
            print("Failed to insert document!")
        return

    def save_waveform(self, event, collection):
        
        if self.wdb == None:
            return

        # Compress the event to make larger events fit in BSON
        smaller = self.CompressEvent(loads(event.to_json()))
        try:
            self.wdb['waveforms'].insert_one(smaller)
        except Exception as e:
            print("Error inserting waveform. Maybe it's too large. " + e)
        return

    def CompressEvent(self, event):

        """ 
        Compresses an event by suppressing zeros in 
        waveform in a way the frontend will understand 
        Format is the char 'zn' where 'z' is a char and 
        'n' is the number of following zero bins                                                                                 
        Also removes fields
        """
        
        for x in range(0, len(event['sum_waveforms'])):
            waveform = event['sum_waveforms'][x]['samples']
            zeros = 0
            ret = []

            for i in range(0, len(waveform)):
                if waveform[i] == 0:
                    zeros += 1
                    continue
                else:
                    if zeros != 0:
                        ret.append('z')
                        ret.append(str(zeros))
                        zeros = 0
                    ret.append(str(waveform[i]))
        if zeros != 0:
            ret.append('z')
            ret.append(str(zeros))
        event['sum_waveforms'][x]['samples'] = ret

        # Unfortunately we also have to remove the pulses 
        # or some events are huuuuuuuuuge-uh
        del event['pulses']
        return event
    
