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
            pax_config = {"pax":
                          {
                              'output':'Dummy.DummyOutput',
                              'encoder_plugin':     None,
                              'pre_output': [],
                              'logging_level': 'ERROR',
                              'events_to_process': to_process,
                              'input_name': os.path.join(self.search_path,
                                                         run_name)
                          }
            }
            thispax = core.Processor(config_names="XENON1T", config_dict=pax_config)
            
            # Loop through processed events
            for event in thispax.get_events():
                output.save_doc(event, run_name)
                
                if saved_events % self.waveform_prescale == 0:
                    output.save_waveform(event, run_name)
            
                saved_events+=1
            
            # Set current event for start of next file
            current_event = last_event + 1
            
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
        #to do
        return -1

    
    
