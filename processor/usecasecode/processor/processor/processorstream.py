"""
Stream Processing module that consumes the data from kafka, writes to Cassandra,
and outputs anomalies into another kafka topic
"""
__version__ = '0.2'

import json
import logging
import time
from timeit import default_timer as timer
import iso8601
from datetime import datetime

import pandas as pd
from kafka import KafkaConsumer, KafkaProducer, errors

from cassandra.cluster import Cluster

from processor import constants, statetracker, validation

class ProcessorStream:
    """
    The main class for streaming Processor

    Instance variables stored:
    1. state_obj {StateTracker} -- The state tracker object - for tracking state across time ie over multiple kafka reads
    2. in_kafkaservers {string} -- Input kafka bootstrap servers
    3. in_kafkatopics {string} -- Input kafka topic
    4. out_kafkaservers {string} -- Output kafka bootstrap servers
    5. out_kafkatopics {string} -- Output kafka topic
    6. config {dict} -- Config dictionary for StateTracker

    """

    def __init__(self, in_kafkaservers, in_kafkatopics, 
                 out_kafkaservers, out_kafkatopics,
                 db, keyspace,
                 config_file, time_prof_flag=False, verbose_log=False, add_timestamps=False,
                 log_profile_file="processor_time_profile_log.csv"):
        """
        Initialize processor tracker
        Arguments:
            in_kafkaservers {string} -- Input kafka bootstrap servers
            in_kafkatopics {string} -- Input kafka topic
            out_kafkaservers {string} -- Output kafka bootstrap servers
            out_kafkatopics {string} -- Output kafka topic
            config_file {string} -- The Processor config file
            time_prof_file {boolean} -- Flag (True/False) to enable/disable
                time profiling
            verbose_log {boolean}  -- Flag (True/False) to enable/disable verbose logging to screen and log file    
        """
        self.in_kafkaservers = in_kafkaservers
        self.in_kafkatopics = in_kafkatopics
        self.out_kafkaservers = out_kafkaservers
        self.out_kafkatopics = out_kafkatopics
        self.db = db
        self.keyspace = keyspace
        self.time_prof_flag = time_prof_flag
        self.verbose_log = verbose_log
        self.add_timestamps = add_timestamps
        self.log_profile_file = log_profile_file
        self.config = json.load(open(config_file))

        # Schema validation
        self.schema = None
        self.schema_validator = None
        self.schema_file_name = self.config.get("JSON_SCHEMA_FILE", None)
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

        # time to wait between reads of the input queue
        self.sleep_time_sec = self.config.get("resample_time_sec", constants.RESAMPLE_TIME_IN_SEC)
        # time to wait on kafka input queue
        self.input_queue_wait_sec = self.config.get("input_queue_wait_sec", constants.INPUT_QUEUE_WAIT_SEC)        
                
        
        self.state_obj = statetracker.StateTracker(self.config, verbose_log=self.verbose_log)

        # Debug related
        self.reid_timings = []

        # Instantiate kafka producer/consumer
        self.consumer = None
        self.producer = None
        try:
            self.consumer = KafkaConsumer(self.in_kafkatopics,
                                          bootstrap_servers=self.in_kafkaservers,
                                          value_deserializer=lambda m:
                                          validation.schema_validate(m, self.schema))
        except errors.NoBrokersAvailable:
            err_msg = "ERROR: Consumer broker not available: {}".format(
                self.in_kafkaservers)
            logging.error(err_msg)
            print("Cannot start streaming Processor: {}".format(err_msg))
            exit()
        except Exception as exception:
            err_msg = "ERROR: Consumer cannot be started. Unknown error: {}".format(
                exception)
            logging.error(err_msg)
            print("Cannot start streaming Processor: {}".format(err_msg))
            exit()

        if self.consumer is None:
            err_msg = "ERROR: Consumer cannot be instantiated. Unknown error"
            logging.error(err_msg)
            print("Cannot start streaming Processor: {}".format(err_msg))
            exit()
        
        print('Kafka Consumer successfully started:', self.in_kafkaservers, self.in_kafkatopics)

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
        try:
            self.cluster = Cluster([db])
        except Exception as exception:
            err_msg = "ERROR: Unable to connect to Cassandra. {}".format(
                exception)
            logging.error(err_msg)
            print("Cannot instantiate Cassandra connection: {}".format(err_msg))
            exit()
            
        try:
            self.session = self.cluster.connect(keyspace)
        except Exception as exception:
            err_msg = "ERROR: Unable to reach Cassandra keyspace. {}".format(
                exception)
            logging.error(err_msg)
            print("Cannot instantiate Cassandra keyspace connection: {}".format(err_msg))
            exit()
        print('Cassandra database connection successfully established: ', db, keyspace)

        #Table: objectmarker        #PRIMARY KEY (messageid, timestamp)
        #Table: flowrate     #not used here but retained for future use NB:cassandra updates create new rows if primary key doesnt exist, otherwise updates existing row. Appears to write out seprate entry and exit records...
            

        # flow rate related queries  ['id', 'timestamp', 'entry', 'exit']. Difft queries for entry and exit in d360 even through it might have been better to combine them.. 
        # UPDATE flowrate  SET exit = 5  WHERE id = 'test' and timestamp = '2018-02-27 21:33:17.098Z'
        self.query_u_flowrate_entry  = "UPDATE flowrate  SET entry=?  WHERE id = ? and timestamp = ?"
        self.query_u_flowrate_entry = self.session.prepare(self.query_u_flowrate_entry)  
        self.query_u_flowrate_exit  = "UPDATE flowrate  SET exit=?  WHERE id = ? and timestamp = ?"
        self.query_u_flowrate_exit = self.session.prepare(self.query_u_flowrate_exit)  

        # objectmarker related queries
        self.query_i_objectmarker = "INSERT INTO objectmarker JSON ?"
        self.query_i_objectmarker = self.session.prepare(self.query_i_objectmarker)
        

    def start_processor(self):
        """
        This method:
        1. Continuously listens to an input kafka (given by in_kafkaservers and
        in_kafkatopics)
        2. Performs processing
        3. Writes state info to Cassandra and potentially anomalies to another kafka topic (given by out_kafkaservers
        and out_kafkatopics)
        """

        iters = 0
        num_msgs_received = 0
        recs = []
        # Debugging-related objects
        start_time = ptime_taken = ttime_taken = None
        num_iters_to_print = int(
            constants.APPROX_TIME_PERIOD_TO_PRINT_INFO_IN_SEC /
            float(self.sleep_time_sec))

        while True:
            start_time = time.time()

            raw_messages = self.consumer.poll(
                timeout_ms=self.input_queue_wait_sec*1000.0, max_records=5000)

            json_list = []
            for _, msg_list in raw_messages.items():
                num_msgs_received += len(msg_list)
                for msg in msg_list:
                    #curr_time = int(round(time.time() * 1000))
                    #kafka_ts = msg.timestamp
                    #recs.append({'curTime': curr_time, 'kafkaTs': kafka_ts})
                    if self.add_timestamps:
                        msg.value['object']['signature'].append(time.time())  #5th signature item  = processor read from kafka
                    json_list.append(msg.value)

            iters += 1
            if (iters % num_iters_to_print) == 0:
                logging.info(
                    "Processor Stream: %s: TOTAL Num msgs received since start = %d", str(datetime.now()), num_msgs_received)
                if self.verbose_log > 1:
                    logging.info(
                        "JSON IN: %s", json_list)

            if self.time_prof_flag:
                itime_taken = time.time() - start_time
                pstart_time = time.time()

            msgs_write, db_write = self.track_list(json_list)
            
            if self.time_prof_flag:
                ptime_taken = time.time() - pstart_time
                otime_start = time.time()
                    
            if msgs_write:
                self.write_to_kafka(msgs_write)
                if self.verbose_log > 1:
                    logging.info(
                        "%s JSON OUT: %s", str(datetime.now()), msgs_write)
                    
            self.write_to_cassandra(db_write)

            if self.time_prof_flag:
                ttime_taken = time.time() - start_time
                otime_taken = time.time() - otime_start

                res = {'curTime': start_time, 'count': len(json_list),
                       'inTime_secs': itime_taken,
                       'outTime_secs': otime_taken,
                       'procTime_secs': ptime_taken,
                       'totalTime_secs': ttime_taken}
                self.reid_timings.append(res)


            time_taken = time.time() - start_time
            tts = self.sleep_time_sec - time_taken
            if tts > 0:
                time.sleep(tts)
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
            logging.info("%s", str(recs_pd.drop(columns='curTime').describe(percentiles=[
                0.05, 0.1, 0.25, 0.5, 0.75, 0.90, 0.95])))
        else:
            logging.info("dump_stats: No data received")


    def track_list(self, all_json_list):
        """
        This method performs the processing for a given set of detection records
        (all_json_list). It returns re-identified set of json objects.

        Arguments:

            all_json_list {list} -- List of all detections in day2 schema format
        Returns:
            list -- List of all tracked detections in day2 schema format
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
            self.state_obj.process_batch(all_json_list)

            if self.time_prof_flag:
                end_time = timer()
                logging.info(
                    "Processor Stream: Process_Batch Time taken: %f",
                    (end_time - start_time))

            msgs_write = self.state_obj.state.msgs_write  #msgs_write is the output json to write to anomaly topic
            self.state_obj.state.reset_msgs_write()
            
        db_write = self.state_obj.state.db_write  #data to write to database
        self.state_obj.state.reset_db_write()

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
        """
        t = datetime.utcfromtimestamp(time.time())
        flow = db_write.get('flow', [])
        if flow:
            flow = flow[0]  #strip list from dict - only one entry in this list
            future = self.session.execute_async(self.query_u_flowrate_entry,
                                                [flow.get('entry_count', 0), flow.get('location', 'UNK'), flow.get('timestamp', t)])
            future.add_callbacks(self.handle_success, self.handle_error)
            future = self.session.execute_async(self.query_u_flowrate_exit,
                                                [flow.get('exit_count', 0), flow.get('location', 'UNK'), flow.get('timestamp', t)])
            future.add_callbacks(self.handle_success, self.handle_error)
        
        detections = flow = db_write.get('detection', [])
        if detections:
            for d in detections:
                if self.add_timestamps:
                    d['object']['signature'].append(time.time())  #6th signature item = processor write to cassandra
                future = self.session.execute_async(self.query_i_objectmarker, [json.dumps(d)])
                future.add_callbacks(self.handle_success, self.handle_error)
        
        return

    #callback fns for asynchronous cassandra updates
    def handle_success(self, rows):
        if self.verbose_log:
            logging.info("Cassandra: Successful query execution %s", str(datetime.now()))
            # NB: don't re-raise errors in the callback
    
    def handle_error(self, exception):
        print(str(datetime.now()), 'ERROR: Cassandra: Failed to execute query: ', exception)
        logging.error("%s ERROR: Cassandra: Failed to execute query: %s", str(datetime.now()), exception)



        
