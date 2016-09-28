"""
Mediates communication with the runs database. 
Allows list input as well if wanted.
"""


from configparser import ConfigParser
import logging
_logger = logging.getLogger(__name__)
import pymongo
import os
import json

import sys

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
                user = os.getenv("MONITOR_USER")
                password = os.getenv("MONITOR_PASSWORD")
                upstr = ""
                if user is not None and password is not None:
                    upstr = user + ":" + password + "@"
                client = pymongo.MongoClient("mongodb://" + upstr + config.get(
                    "mongo_output", "monitor_uri"))        
                database = config.get("mongo_output", "monitor_db")
                self.mdb = client[database]

            except pymongo.errors.ConnectionFailure as e:
                _logger.debug("Failed to connect to monitor DB.")

        # Declare waveform db
        self.wdb = None
        if ( config.has_option("mongo_output", "waveform_uri") and
             config.has_option("mongo_output", "waveform_db") ):
            try:
                # Get environment variables for mongo
                user = os.getenv("MONITOR_USER")
                password = os.getenv("MONITOR_PASSWORD")
                upstr = ""
                if user is not None and password is not None:
                    upstr = user + ":" + password + "@"
                client = pymongo.MongoClient("mongodb://" + upstr + config.get(
                    "mongo_output", "waveform_uri"))
                database = config.get("mongo_output", "waveform_db")
                self.wdb = client[database]

            except pymongo.errors.ConnectionFailure as e:
                _logger.debug("Failed to connect to waveform DB.")
                
        self.instance_id = 0
        if config.has_option("mongo_output", "instance_id"):
            self.instance_id = config.getint("mongo_output", "instance_id")
        self.reprocess = False
        if config.has_option("mongo_output", "reprocess"):
            self.reprocess = config.getboolean("mongo_output", "reprocess")
        self.finish = False
        if config.has_option("mongo_output", "finish"):
            self.finish = config.getboolean("mongo_output", "finish")

    def register_processor(self, collection, mode, prescale):
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

        # If this is a raw thread and we find a processed file skip
        if ( not no_collection and stat.count() > 0 and mode == "raw" and 
             "mode" in stat[0] and stat[0]["mode"]=="processed" ):
            return False

        # Register it.
        if ( 
                # New collection, no status. Definitely process
                no_collection or stat.count() == 0 or 
                
                # This is processed data. We only had raw. Eat it up.
                (mode == "processed" and ( "mode" not in stat[0] or 
                                           ("mode" in stat[0] and 
                                            stat[0]["mode"]=="raw"))) or

                
                # Reprocess runs from other instances. 
             ( self.reprocess and "instance_id" in stat[0]
               and stat[0]["instance_id"] != self.instance_id) or

                # Finish unfinished runs, but don't reprocess finished runs
             ( self.finish and "instance_id" in stat[0] 
               and stat[0]['instance_id'] != self.instance_id and
               ( 'finished' not in stat[0] or stat[0]['finished']==False))):

            # "Finish" doesn't actually mean finish. It means start again.
            self.mdb[collection].drop()
            status_doc = {
                'type': 'status',
                'instance_id': self.instance_id,
                'finished': False,
                'mode': mode,
                'prescale': 1
            }
            if mode == "raw":
                status_doc['prescale'] = prescale

            self.mdb[collection].insert_one(status_doc)
            return True            
        return False
        
    def close(self, collection, nevents):
        """
        Close the run. Tell DB you're done and how many events were processed
        """
        if self.mdb == None:
            _logger.error("output.close: no mongo")
            return False

        if collection not in self.mdb.collection_names():
            _logger.error("output.close: collection not found")
            
        try:
            self.mdb[collection].update_one(
                {'type': 'status'},
                {'$set': {'finished': True,
                          'events': nevents}
             })
        except:
            _logger.error("output.close: error updating status doc")
            return 
        return


    def save_doc(self, event, collection):
        
        if self.mdb == None:
            _logger.error("No monitor db")
            return

        # Put it into mongo. Simple stuff for now                                  
        insert_doc = {
            "type": "data",
            "event_number": None,
            "cs1": None,
            "cs2": None,
            "s1": None,
            "s2": None,
            "s1_range_50p_area": None,
            "s2_range_50p_area": None,
            "s1_area_fraction_top": None,
            "s2_area_fraction_top": None,
            "s1_n_hits": None,
            "s2_n_hits": None,
            "s1_hit_time_mean": None,
            "s2_hit_time_mean": None,
            "event_time": None,
            "z": None,            
            "largest_other_s1": None,
            "largest_other_s2": None,
            "largest_other_s1_time": None,
            "largest_other_s2_time": None,
            "s1_n_contributing_channels": None,
            "s2_n_contributing_channels": None,
            "s1_n_saturated_channels": None,
            "s2_n_saturated_channels": None, 
            "event_length": None,
            "largest_other_s1_hit_time_mean": None,
            "largest_other_s2_hit_time_mean": None,
            "ns1": None,
            "ns2": None,
            "dt": None,
            "x": None,
            "y": None,
            "interactions": None,
        }

        # This is a bit nasty. Here's the thing. The pax event class 
        # is different if using ROOT output or if using the python
        # native version. 
        try:
            self.FillDocPaxOutput(insert_doc, event)
        except Exception as e:
            _logger.error("Couldn't read event class in either pax native "
                          "or ROOT form. Failing. " + str(e))
            return                
        try:
            self.mdb[collection].insert_one(insert_doc)
        except Exception as e:
            _logger.error("Failed to insert document! " +str(e))
        return

    def FillDocPaxOutput(self, insert_doc, event):

        # Nasty things to maintain ROOT and python-native compatibility
        # Don't ask, just DO.
        try:
            s1s = event.s1s()
            s2s = event.s2s()
            peaks = event.peaks
            interactions = event.interactions
        except Exception as e:            
            s1s = event.s1s
            s2s = event.s2s
            peaks = event.peaks
            interactions = event.interactions

        insert_doc['ns1'] = len(s1s)
        insert_doc['ns2'] = len(s2s)
        insert_doc['time'] = event.start_time
        insert_doc['event_number'] = event.event_number
        insert_doc['event_length'] = event.stop_time - event.start_time
        insert_doc['interactions'] = len(interactions)
        if len(interactions) > 0:
            interaction = interactions[0]
            s1 = peaks[interaction.s1]
            s2 = peaks[interaction.s2]
            
            insert_doc['s1'] = s1.area
            insert_doc['s2'] = s2.area
            insert_doc['cs1'] = insert_doc['s1']*interaction.s1_area_correction
            insert_doc['cs2'] = insert_doc['s2']*interaction.s2_area_correction
            insert_doc['x'] = interaction.x
            insert_doc['y'] = interaction.y
            insert_doc['z'] = interaction.z
            insert_doc['dt'] = interaction.drift_time
            insert_doc['s1_range_50p_area'] = s1.range_area_decile[5]
            insert_doc['s2_range_50p_area'] = s2.range_area_decile[5]
            insert_doc['s1_hit_time_mean'] = s1.hit_time_mean
            insert_doc['s2_hit_time_mean'] = s2.hit_time_mean
            insert_doc['s1_area_fraction_top'] = s1.area_fraction_top
            insert_doc['s2_area_fraction_top'] = s2.area_fraction_top
            insert_doc['s1_n_hits'] = s1.n_hits
            insert_doc['s2_n_hits'] = s2.n_hits
            insert_doc['s2_n_contributing_channels'] = s1.n_contributing_channels
            insert_doc['s2_n_contributing_channels'] = s2.n_contributing_channels
            insert_doc['s1_n_saturated_channels'] = s1.n_saturated_channels
            insert_doc['s2_n_saturated_channels'] = s2.n_saturated_channels
            
        # Now we want the largest other s1 and largest other s1
        los1 = None
        alos1 = 0
        for s1 in s1s:
            try:
                if peaks[s1].area > alos1:
                    alos1 = peaks[s1].area
                    los1 = peaks[s1]
            except:
                if s1.area > alos1:
                    alos1 = s1.area
                    los1 = s1
        los2 = None
        alos2 = 0
        for s2 in s2s:
            try:
                if peaks[s2].area > alos2:
                    alos2 = peaks[s2].area
                    los2 = peaks[s2]
            except:
                if s2.area > alos2:
                    alos2 = s2.area
                    los2 = s2

        if los1 is not None:
            insert_doc['largest_other_s1'] = los1.area
            insert_doc['largest_other_s1_range_50p_area'] = los1.range_area_decile[5]
            insert_doc['largest_other_s1_hit_time_mean'] = los1.hit_time_mean
            insert_doc['largest_other_s1_n_contributing_channels'] = los1.n_contributing_channels
        if los2 is not None:
            insert_doc['largest_other_s2'] = los2.area
            insert_doc['largest_other_s2_range_50p_area'] = los2.range_area_decile[5]
            insert_doc['largest_other_s2_hit_time_mean'] = los2.hit_time_mean
            insert_doc['largest_other_s2_n_contributing_channels'] = los2.n_contributing_channels


    def save_waveform(self, event, collection):
        
        if self.wdb == None:
            return False

        # Compress the event to make larger events fit in BSON
        smaller = self.CompressEvent(json.loads(event.to_json()))
        try:
            self.wdb[collection].insert_one(smaller)
        except Exception as e:
            print("Error inserting waveform. Maybe it's too large. ")
            return False
        return True
    
    def CompressEvent(self, event):

        """ 
        Compresses an event by suppressing zeros in 
        waveform in a way the frontend will understand 
        Format is the char 'zn' where 'z' is a char and 
        'n' is the number of following zero bins                                                                                 
        Also removes fields
        """
        
        print("Size before compression = " + str(sys.getsizeof(json.dumps(event))))

        # First compress the waveform
        ret_event = {}
        detectors = ['tpc']
        names = ['tpc']
        for x in range(0, len(event['sum_waveforms'])):
            if event['sum_waveforms'][x]['detector'] == 'tpc':
                print("Name is " + event['sum_waveforms'][x]['name'])
                print("Waveform for TPC is " + str(len(event['sum_waveforms'][x]['samples'])))
            if ( event['sum_waveforms'][x]['detector'] not in detectors or
                 event['sum_waveforms'][x]['name'] not in names ):
                continue
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
        print("Size after compression = " + str(sys.getsizeof(json.dumps(event))))

        ret_event['sum_waveforms'] = []
        for waveform in event['sum_waveforms']:
            if ( waveform['detector'] not in detectors or
                 waveform['name'] not in names ):
                continue
            ret_event['sum_waveforms'].append(waveform)


        # Now compress each peak
        ret_event['peaks'] = []
        peak_vars = ['area', 'area_fraction_top', 'area_per_channel',
                     'center_time', 'index_of_maximum', 'left', 
                     'n_contributing_channels', 'right', 'type']
        for peak in event['peaks']:
            new_peak = {}
            for var in peak_vars:
                new_peak[var] = peak[var]
            ret_event['peaks'].append(new_peak)
        
        # Now hits
        ret_event['all_hits'] = event['all_hits']

        # Metadata
        for value in ['dataset_name', 'event_number', 'start_time', 'stop_time']:
            ret_event[value] = event[value]
        print("Size of ret event " + str(sys.getsizeof(json.dumps(ret_event))))
        print("Breakdown. Waveforms: "+str(sys.getsizeof(json.dumps(ret_event['sum_waveforms']))) + " Hits: " + str(sys.getsizeof(json.dumps(ret_event['all_hits']))) + " Peaks: " + str(sys.getsizeof(json.dumps(ret_event['peaks']))))
        return ret_event
    
