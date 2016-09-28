"""
Processes data or just reads ROOT files into data class.
"""


from configparser import ConfigParser
import logging
_logger = logging.getLogger(__name__)
import os
import glob
from pax import core
from jax.output import MonitorOutput


class Processor(object):
    """
    Processes raw data (with some prescale) or directly inputs
    processed data into reduced format. 
    """

    def __init__(self, config):

        # Understands "raw" and "processed"
        self.input_type = "raw"
        if config.has_option("jax", "input_type"):
            self.input_type = config.get("jax", "input_type")

        self.search_path = ""
        if config.has_option("jax", "data_path"):
            self.search_path = config.get("jax", "data_path")

        self.raw_prescale = 10
        if config.has_option("jax", "raw_prescale"):
            self.raw_prescale = config.getint("jax", "raw_prescale")
        
        self.waveform_prescale = 1000
        if config.has_option("jax", "waveform_prescale"):
            self.waveform_prescale = config.getint("jax", "waveform_prescale")

        self.file_timeout_counter = 10000 # Queries
        if config.has_option("jax", "file_timeout_counter"):
            self.file_timeout_counter = config.getint("jax", "file_timeout_counter")

    def get_mode(self):
        return self.input_type
    def get_prescale(self):
        return self.raw_prescale
    
    def check_available(self, run_doc):
        """
        We want to know if the raw/processed data for this run is
        even there. Otherwise no need to start. This should be easy.
        """
        search_path = os.path.join(self.search_path, run_doc['name'])
        search_path += '*' #because of .root
        if not glob.glob(search_path):
            return False
        return True

    def process_run(self, output, run_doc):
        """
        Processes a run. If it's raw data this means processing a prescaled
        subset of the run's events and putting them into mongo. If it's processed,
        the run gets offloaded to another funciton.

        Return is number of events processed. If error return -1.
        """
        if self.input_type == "processed":
            return self.process_processed(output, run_doc)

        run_name = run_doc['name']
        run_number = run_doc['number']
        saved_events = 0

        # First we have to find the file. If we can't find the file we quit
        path_base = ( self.search_path + run_name + "/" + "XENON1T-"+
                      str(run_number)+"-")

        if not glob.glob(path_base+"*"):
            return -1

        counter = 0
        current_event = 0
        while not self.check_finished(run_doc, counter, current_event):

            counter += 1
            
            # Check if the current file even exists
            if not glob.glob(path_base+str(current_event).zfill(9)+"*"):
                time.sleep(5)
                continue

            current_file = os.path.basename(
                glob.glob(path_base+str(current_event).zfill(9)+"*")[0])
            
            # Check if the current file is a temp file
            if 'temp' in current_file:
                time.sleep(5)
                continue

            counter = 0
            
            # Get the list of events to process
            filenamelist = current_file.split("-")            
            last_event = int(filenamelist[3])
            print("Thread processing " + current_file + " with events " + 
                  str(current_event) + " to " + str(last_event))
            to_process = range(current_event, last_event, self.raw_prescale)
            
            # Do the processing using pax
            pax_config = {
                "pax":
                {                
                    'output': 'Dummy.DummyOutput',
                    'pre_output': [],
                    'encoder_plugin':     None,
                    'logging_level': 'ERROR',
                    'events_to_process': to_process,
                    'input_name': os.path.join(self.search_path,
                                               run_name)
                },
            "MongoDB":
                {
                    "user": os.getenv("MONGO_USER"),
                    "password": os.getenv("MONGO_PASSWORD"),
                    "host": "gw",
                    "port": 27017,
                    "database": "run"
                },
            }
            thispax = core.Processor(config_names="XENON1T", 
                                     config_dict=pax_config)
            
            # Loop through processed events
            saveNext = False
            for event in thispax.get_events():
                processed = thispax.process_event(event)
                output.save_doc(processed, run_name)
                
                if ( saveNext or self.waveform_prescale > 0 and 
                     saved_events % self.waveform_prescale == 0 ):
                    if not output.save_waveform(processed, run_name):
                        saveNext = True
                    else:
                        saveNext = False
            
                saved_events+=1
            
            # Set current event for start of next file
            current_event = last_event + 1
        
        output.close(run_name, saved_events)
        print("Processed " + saved_events + " events")
        return saved_events
        
    def check_finished(self, run_doc, counter, current_event):
        """
        How do we know if we're done with this run?
          (a) if it is not done processing then we aren't done!
          (b) if it is done processing and we just finished the final file,
              then we ARE done
          (c) if we hit a predefined timeout waiting for processing, then
              we ARE done
        """
        
        # Predefined timeout
        if counter > self.file_timeout_counter:
            print("COUNTER")
            return True

        # Is the run even finished?
        run_name = run_doc['name']
        path_log = ( self.search_path + run_name + "/" + "eventbuilder.log" )
        if not glob.glob(path_log):
            return False

        # It is finished, did we do the last event? Note the run doc might
        # be outdated if the run wasn't finished when we started processing
        run_number = run_doc['number']
        path_base = ( self.search_path + run_name + "/" + "XENON1T-"+
                      str(run_number)+"-")

        # If the run is finished but current_file does not exist, we're done
        if not glob.glob(path_base+str(current_event).zfill(9)+"*"):
            print("NOFILE" + current_file)
            return True
        return False

    def process_processed(self, output, run_doc):
        """
        If we already processed the data then let's not waste CPU cycles 
        Put each and every event into our reduced DB
        """
        
        filename = os.path.join( self.search_path, run_doc['name'])
        filename += ".root"
        if not os.path.exists(filename):
            _logger.error("That ROOT file doesn't exist."
                      "Actually you should not have gotten this far.")
            return -1

        # If you have trouble with this part of the code please contact
        # somebody who thought using ROOT was a good idea.
        import ROOT
        from pax.plugins.io.ROOTClass import load_pax_event_class_from_root
        from pax.exceptions import MaybeOldFormatException

        try:
            load_pax_event_class_from_root(filename)
        except MaybeOldFormatException:
            _logger.error("There was a problem loading the ROOT class from your file."
                          "I guess just give up. Or use a new ROOT file with the "
                          "class embedded. Sorry.")
        except Exception as e:
            _logger.error("Unconventional exception for "+filename+": "+str(e))
            return -1


        tfile = ROOT.TFile(filename)
        tree = tfile.Get("tree")
        n_events = tree.GetEntries()

        # Loop it
        saved=0
        for i in range(0, n_events):
            tree.GetEntry(i)
            event = tree.events
            try:
                output.save_doc(event, run_doc['name'])
            except:
                _logger.error("Couldn't save processed event to output. Quitting.")
                return -1
            saved +=1
        output.close(run_doc['name'], saved)
        return saved

    
    
