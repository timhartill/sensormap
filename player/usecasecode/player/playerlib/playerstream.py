"""
Stream Processing module that consumes recorded json data from a file, writes to a kafka topic
"""
__version__ = '0.2'

import json
import logging
import time
from timeit import default_timer as timer
import iso8601
from datetime import datetime, timedelta
import copy

import pandas as pd
from kafka import KafkaProducer, errors
#from kafka import KafkaConsumer
#from cassandra.cluster import Cluster

from playerlib import constants, validation, trackerutils

class PlayerStream:
    """
    The main class for streaming Processor

    Instance variables stored:
    1. state_obj {StateTracker} -- The state tracker object - for tracking state across time ie over multiple kafka reads
    2. config {dict} -- Config dictionary 

    """

    def __init__(self, in_file, in_kafkaservers, in_kafkatopics, 
                 out_kafkaservers, out_kafkatopics,
                 db, keyspace,
                 config_file, time_prof_flag=False, verbose_log=False,
                 log_profile_file="processor_time_profile_log.csv"):
        """
        Initialize player
        Arguments:
            in_file {string} -- Input json file
            in_kafkaservers {string} -- Input kafka bootstrap servers. UNUSED
            in_kafkatopics {string} -- Input kafka topic. UNUSED 
            out_kafkaservers {string} -- Output kafka bootstrap servers
            out_kafkatopics {string} -- Output kafka topic
            config_file {string} -- The Processor config file
            time_prof_file {boolean} -- Flag (True/False) to enable/disable
                time profiling
            verbose_log {boolean}  -- Flag (True/False) to enable/disable verbose logging to screen and log file    
        """
        self.in_file = in_file
        self.in_kafkaservers = in_kafkaservers
        self.in_kafkatopics = in_kafkatopics
        self.out_kafkaservers = out_kafkaservers
        self.out_kafkatopics = out_kafkatopics
        self.db = db
        self.keyspace = keyspace
        self.time_prof_flag = time_prof_flag
        self.verbose_log = verbose_log
        self.log_profile_file = log_profile_file
        self.config = json.load(open(config_file))

        # Schema validation
        self.schema = None
        self.schema_validator = None
        self.schema_file_name = self.config.get("JSON_SCHEMA_FILE", None)  #Not used, left for future..
        print('Schema file name:', self.schema_file_name)
        if self.schema_file_name is not None:
            try:
                with open(self.schema_file_name) as schema_file:
                    self.schema = json.load(schema_file)
            except IOError:
                logging.error(
                    "ERROR: Schema file (%s) could not be opened. "
                    "No validation will be performed", self.schema_file_name)
            except ValueError:
                logging.error(
                    "ERROR: Schema file (%s) has invalid json. "
                    "No validation will be performed", self.schema_file_name)

        # time to sleep before outputting next batch of msgs:
        self.sleep_time_sec = self.config.get("resample_time_sec", constants.RESAMPLE_TIME_IN_SEC)
        # number of msgs to write out each time
        self.num_recs_to_write = self.config.get("num_recs_to_write", constants.NUM_RECS_TO_WRITE)
        # if true, ignore num_recs_to_write and instead write out all msgs since last time till now and save 'now'
        self.write_recs_since_last = self.config.get("write_recs_since_last", constants.WRITE_RECS_SINCE_LAST)
        # if true, pretend recorded events are happening "now" - ise in conjunction with isLive:True in node-apis config file
        self.is_live = self.config.get("is_live", constants.IS_LIVE)

        
        #self.state_obj = statetracker.StateTracker(self.config, verbose_log=self.verbose_log)

        # Debug related
        self.reid_timings = []
        
        # read entire input file
        print('Loading json file...')
        with open(self.in_file, "r") as f:
            self.all_json_list = f.read()
        self.all_json_list = self.all_json_list.split('\n')
        self.num_jsons = len(self.all_json_list)
        self.all_json_list = [json.loads(j) for j in self.all_json_list]
        print('JSON successfully loaded. Number of json messages in file is ', self.num_jsons)
        print('Will write JSON every ', self.sleep_time_sec, 'secs')
        if self.write_recs_since_last:
            print('MODE: Writing out all records since last write every ', self.sleep_time_sec, 'secs')
        else:
            print('MODE: Number of msgs to write each time: ', self.num_recs_to_write)

        self.live_calc_time_taken = 0.0    
        self.set_timestamps_to_live()    
       
        # Instantiate kafka producer/consumer
        self.consumer = None
        self.producer = None

        try:
            self.producer = KafkaProducer(bootstrap_servers=self.out_kafkaservers,
                                          value_serializer=lambda m:
                                          json.dumps(m).encode('utf-8'))
        except errors.NoBrokersAvailable:
            err_msg = "ERROR: Producer broker not available: {}".format(
                self.out_kafkaservers)
            logging.error(err_msg)
            print("Cannot start streaming Processor: {}".format(err_msg))
            exit()
        except Exception as exception:
            err_msg = "ERROR: Producer cannot be started. Unknown error: {}".format(
                exception)
            logging.error(err_msg)
            print("Cannot start streaming Processor: {}".format(err_msg))
            exit()

        if self.producer is None:
            err_msg = "ERROR: Producer cannot be instantiated. Unknown error"
            logging.error(err_msg)
            print("Cannot start streaming Processor: {}".format(err_msg))
            exit()
        print('Kafka Producer successfully started:', self.out_kafkaservers, self.out_kafkatopics)

        
        self.cluster = None
        self.session = None
        return
    

    def start_processor(self):
        """
        This method:
        1. Continuously writes json out to kafka topic (given by out_kafkaservers
        and out_kafkatopics)
        """
        self.set_timestamps_to_live()   #run twice -1st time in __init__ sets self.live_calc_time_taken    
        json_idx = 0
        json_idx_time = iso8601.parse_date(self.all_json_list[0].get("@timestamp", None))
        iters = 0
        num_msgs_received = 0
        # Debugging-related objects
        start_time = ptime_taken = ttime_taken = None
        num_iters_to_print = int(
            constants.APPROX_TIME_PERIOD_TO_PRINT_INFO_IN_SEC /
            float(self.sleep_time_sec))

        while True:
            start_time = time.time()
            curr_time_utc = datetime.now(tz=self.timezone)
            json_list = []
            if self.write_recs_since_last:
                #end_json_time = json_idx_time + timedelta(seconds=self.sleep_time_sec)
                #if end_json_time > curr_time_utc:
                end_json_time = curr_time_utc
                num_written = 0
                for i in range(json_idx, self.num_jsons):
                    curr_idx_time = iso8601.parse_date(self.all_json_list[i].get("@timestamp", None))
                    if curr_idx_time <= end_json_time:
                        json_list.append(self.all_json_list[i])
                        num_written += 1
                    else:
                        break
                json_idx = json_idx + num_written
                if json_idx >= self.num_jsons:
                    json_idx = 0
                    self.set_timestamps_to_live()
                #json_idx_time = iso8601.parse_date(self.all_json_list[json_idx].get("@timestamp", None))
                num_msgs_received += num_written    
            else:    
                end_json_idx = json_idx + self.num_recs_to_write
                if end_json_idx >= self.num_jsons:
                    json_list = self.all_json_list[json_idx : self.num_jsons]
                    end_json_idx = self.num_recs_to_write - (self.num_jsons - json_idx)
                    json_idx = 0
                    self.set_timestamps_to_live()
                json_list = json_list + self.all_json_list[json_idx : end_json_idx]  
                json_idx += end_json_idx           
                num_msgs_received += self.num_recs_to_write
            
            iters += 1
            if (iters % num_iters_to_print) == 0:
                logging.info(
                    "Stream: %s: TOTAL Num msgs received since start = %d", str(datetime.now()), num_msgs_received)
                if self.verbose_log:
                    logging.info("JSON IN: %s", json_list)
                print(f'Next batch start time: {self.all_json_list[json_idx].get("@timestamp", None)}  Json idx: {json_idx}  Curr UTC time: {curr_time_utc}')    

            if self.time_prof_flag:
                itime_taken = time.time() - start_time
                pstart_time = time.time()

            msgs_write, db_write = self.track_list(json_list)
            
            if self.time_prof_flag:
                ptime_taken = time.time() - pstart_time
                otime_start = time.time()
                    
            if msgs_write:
                self.write_to_kafka(msgs_write)
                if self.verbose_log:
                    logging.info(
                        "%s JSON OUT: %s", str(datetime.now()), msgs_write)
                    
            #self.write_to_cassandra(db_write)

            if self.time_prof_flag:
                ttime_taken = time.time() - start_time
                otime_taken = time.time() - otime_start

                res = {'currTime': start_time, 'count': len(json_list),
                       'inTimeTaken_secs': itime_taken,
                       'outTimeTaken_secs': otime_taken,
                       'processTimeTaken_secs': ptime_taken,
                       'totalTimeTaken_secs': ttime_taken}
                self.reid_timings.append(res)


            time_taken = time.time() - start_time
            tts = self.sleep_time_sec - time_taken
            if tts > 0:
                time.sleep(tts)
        return
    
    def set_timestamps_to_live(self):
        """ If playing live stream, set the json timestamps to imitate being sent 'now'.
        """
        self.json_start_time = iso8601.parse_date(self.all_json_list[0].get("@timestamp", None)) + timedelta(seconds=self.live_calc_time_taken)
        self.timezone = self.json_start_time.tzinfo
        curr_time_utc = datetime.now(tz=self.timezone)
        time_offset_secs = (curr_time_utc - self.json_start_time).total_seconds()
        if self.is_live:
            for i in range(self.num_jsons):
                msg_time = iso8601.parse_date(self.all_json_list[i].get("@timestamp", None))
                msg_time = msg_time + timedelta(seconds=time_offset_secs)
                self.all_json_list[i]["@timestamp"] = trackerutils.get_timestamp_str(msg_time)
            print('LIVE Mode so adjusted each timestamp in recorded json by ', time_offset_secs / (60*60*24), 'days')
            self.json_start_time = iso8601.parse_date(self.all_json_list[0].get("@timestamp", None))
        end_time_utc = datetime.now(tz=self.timezone)
        self.live_calc_time_taken = (end_time_utc - curr_time_utc).total_seconds()
        print('First timestamp: ', str(self.json_start_time), '  live_calc_time_taken (secs): ',  self.live_calc_time_taken)
        return
    

    def dump_stats(self):
        """
        Write all the tracking timings into the file specified by
        self.log_profile_file
        """
        if self.reid_timings:
            recs_pd = pd.DataFrame(self.reid_timings)
            recs_pd.to_csv(self.log_profile_file, index=False)
            logging.info("%s Dumping Stats:", str(datetime.now()))
            logging.info("%s", str(recs_pd.describe(percentiles=[
                0.05, 0.1, 0.25, 0.5, 0.75, 0.90, 0.95])))
        else:
            logging.info("dump_stats: No data received")
        return    


    def track_list(self, all_json_list):
        """
        This method performs the processing for a given set of detection records
        (all_json_list). 

        Arguments:

            all_json_list {list} -- List of all detections in day2 schema format
        Returns:
            list -- List of all detections to output in day2 schema format
        """
        msgs_write = []
        all_json_list = validation.ignore_bad_records(all_json_list)  #just checks for valid timestamp

        # Debugging-related objects
        start_time = end_time = None

        if all_json_list:
            if self.time_prof_flag:
                start_time = timer()

            all_json_list.sort(
                key=lambda json_ele: json_ele.get("@timestamp", None))
            #self.state_obj.process_batch(all_json_list)

            if self.time_prof_flag:
                end_time = timer()
                logging.info(
                    "Stream: Process_Batch Time taken: %f",
                    (end_time - start_time))

            #msgs_write = self.state_obj.state.msgs_write  #msgs_write is the output json to write to anomaly topic
            #self.state_obj.state.reset_msgs_write()
            msgs_write = copy.deepcopy(all_json_list)  
            
        #db_write = self.state_obj.state.db_write  #data to write to database
        #self.state_obj.state.reset_db_write()
        db_write = None

        return msgs_write, db_write
    

    def write_to_kafka(self, json_list):
        """
        Write the tracked detections to kafka topic

        Arguments:
            json_list {list} -- the list of detections in day2 schema
        """
        if self.producer is not None:
            for json_ele in json_list:
                self.producer.send(self.out_kafkatopics, json_ele)
        return


    def write_to_cassandra(self, db_write):
        """ Write data to Cassandra
        t = datetime.utcfromtimestamp(time.time())
        flow = db_write.get('flow', [])
        if flow != []:
            flow = flow[0]  #strip list from dict - only one entry in this list
            future = self.session.execute_async(self.query_u_flowrate_entry,
                                                [flow.get('entry_count', 0), flow.get('location', 'UNK'), flow.get('timestamp', t)])
            future.add_callbacks(self.handle_success, self.handle_error)
            future = self.session.execute_async(self.query_u_flowrate_exit,
                                                [flow.get('exit_count', 0), flow.get('location', 'UNK'), flow.get('timestamp', t)])
            future.add_callbacks(self.handle_success, self.handle_error)
        
        parking = flow = db_write.get('parking', [])
        if parking != []:
            for p in parking:
                timestamp = iso8601.parse_date(p['timestamp'])
                future = self.session.execute_async(self.query_i_ParkingSpotDelta, [json.dumps(p)])
                future.add_callbacks(self.handle_success, self.handle_error)
                future = self.session.execute_async(self.query_i_ParkingSpotPlayback, [json.dumps(p)])
                future.add_callbacks(self.handle_success, self.handle_error)
                future = self.session.execute_async(self.query_u_ParkingSpotState, 
                                                    [p['messageid'], p['mdsversion'], timestamp,
                                                     json.dumps(p['place']), json.dumps(p['sensor']),
                                                     json.dumps(p['analyticsModule']), json.dumps(p['object']),
                                                     json.dumps(p['event']), p['videoPath'], p['garageid'],
                                                     p['level'], p['spotid']])
                future.add_callbacks(self.handle_success, self.handle_error)

        aisle = flow = db_write.get('aisle', [])
        if aisle != []:
            for a in aisle:
                future = self.session.execute_async(self.query_i_Aisle, [json.dumps(a)])
                future.add_callbacks(self.handle_success, self.handle_error)
        """
        return

    #callback fns for asynchronous cassandra updates
    def handle_success(self, rows):
        if self.verbose_log:
            logging.info("Cassandra: Successful query execution %s", str(datetime.now()))
            # NB: don't re-raise errors in the callback
    
    def handle_error(self, exception):
        print(str(datetime.now()), 'ERROR: Cassandra: Failed to execute query: ', exception)
        logging.error("%s ERROR: Cassandra: Failed to execute query: %s", str(datetime.now()), exception)



        
