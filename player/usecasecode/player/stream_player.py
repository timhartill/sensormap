"""
Main file for streaming player to stream json into raw queue

Input is from a file of json messages of form:

{json 1}
{json 2}
...
{json n}

NB: Input is always from a file however stubs for an input kafka queue 
    and a Cassandra db connection have been left in the code in case needed in future..
    
"""
__version__ = '0.2'

import argparse
import json
import logging
import signal
import sys
import os
import time
#from sys import exit

from playerlib import playerstream

#logging.basicConfig(filename='processor.log', level=logging.INFO)
DEFAULT_CONSUMER_KAFKA_BOOTSTRAP_SERVER_URL = ""
DEFAULT_PRODUCER_KAFKA_BOOTSTRAP_SERVER_URL = "localhost"

DEFAULT_CONSUMER_KAFKA_TOPIC = ""
DEFAULT_PRODUCER_KAFKA_TOPIC = "metromind-raw"

DEFAULT_DB_SERVER_URL = ""
DEFAULT_DB_KEYSPACE = ""
DEFAULT_DB_STARTUP_WAIT = 0

DEFAULT_PROCESSOR_CONFIG_FILE = "../../config/config_player.json"
DEFAULT_STREAM_CONFIG_FILE = "../../config/config_player_stream.json"

DEFAULT_LOG_DIR = "./logs"
DEFAULT_LOG_FILE = "player.log"
DEFAULT_PROFILE_FILE = "player_time_profile_log.csv"

DEFAULT_INPUT_DIR = "./json_playback"
DEFAULT_INPUT_FILE = "playbackData.json"


stream_obj = None


def signal_handler(signum, _):
    """Signal handler. This function will dump all stream object stats and exit

    Arguments:
        signum {int} -- The signal number
        frame {list} -- Stack frame
    """

    logging.error("Stream got a signal: %d", signum)
    try:
        if stream_obj is not None:
            stream_obj.dump_stats()
    except Exception:
        pass
    exit()


def main():
    """Main function. Starts stream object and runs continuously
    until killed
    """
    global stream_obj
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", help="Config file for player",
                        default=DEFAULT_PROCESSOR_CONFIG_FILE)
    parser.add_argument("-s", "--sconfig", help="Config file for streaming setup",
                        default=DEFAULT_STREAM_CONFIG_FILE)
    args = parser.parse_args()

    stream_config = None
    try:
        stream_config = json.load(open(args.sconfig))
    except IOError as ioe:
        err_msg = "ERROR: Stream Config I/O Error({}): {}: {}. Quitting".format(
            ioe.errno, args.sconfig, ioe.strerror)
        logging.error(err_msg)
        print(err_msg)
        exit()

    except:
        err_msg = "ERROR: Stream Config Error: {}: {}. Quitting".format(
            args.sconfig, sys.exc_info()[0])
        logging.error(err_msg)
        print(err_msg)
        exit()

    print(stream_config)

    in_dir = (stream_config
              .get("inputFileConfig", {})
              .get("inputDir",
                   DEFAULT_INPUT_DIR))
    
    in_file = (stream_config
             .get("inputFileConfig", {})
             .get("inputFile",
                   DEFAULT_INPUT_FILE))           
    
    in_file = os.path.join(in_dir, in_file)
    
    
    # currently unused
    ckafka = (stream_config
              .get("msgBrokerConfig", {})
              .get("inputKafkaServerUrl",
                   DEFAULT_CONSUMER_KAFKA_BOOTSTRAP_SERVER_URL))

    pkafka = (stream_config
              .get("msgBrokerConfig", {})
              .get("outputKafkaServerUrl",
                   DEFAULT_PRODUCER_KAFKA_BOOTSTRAP_SERVER_URL))

    # currently unused
    itopic = (stream_config
              .get("msgBrokerConfig", {})
              .get("inputKafkaTopic", DEFAULT_CONSUMER_KAFKA_TOPIC))

    otopic = (stream_config
              .get("msgBrokerConfig", {})
              .get("outputKafkaTopic",
                   DEFAULT_CONSUMER_KAFKA_TOPIC))
    # currently unused
    db = (stream_config
              .get("dbConfig", {})
              .get("cassandraHost",
                   DEFAULT_DB_SERVER_URL))
    
    # currently unused
    keyspace = (stream_config
              .get("dbConfig", {})
              .get("cassandraKeyspace",
                   DEFAULT_DB_KEYSPACE))
    
    time_it_flag = stream_config.get("profileTime", False)
    verbose_flag = stream_config.get("verboseLog", False)
    add_timestamps = stream_config.get("msg_signature_add_timestamps", False)
    
    startup_sleep = (stream_config
              .get("dbConfig", {})
              .get("cassandraStartupWaitSec",
                   DEFAULT_DB_STARTUP_WAIT))
    
    log_dir = (stream_config
              .get("logConfig", {})
              .get("logDir",
                   DEFAULT_LOG_DIR))
    
    if not os.path.isdir(log_dir):
        os.mkdir(log_dir)

    log_file = (stream_config
             .get("logConfig", {})
             .get("logFile",
                   DEFAULT_LOG_FILE))           
    
    log_file = os.path.join(log_dir, log_file)
        
    logging.basicConfig(filename=log_file, level=logging.INFO)
    
    log_profile_file = (stream_config
             .get("logConfig", {})
             .get("logProfileFile",
                   DEFAULT_PROFILE_FILE))    
    log_profile_file = os.path.join(log_dir, log_profile_file)


    print("Starting Processor app with following args:\n"
          "input json file={}\n"
          "consumer kafka server={}\n"
          "consumer kafka topic={}\n"
          "producer kafka server={}\n"
          "producer kafka topic={}\n"
          "cassandra server={}\n"
          "cassandra keyspace={}\n"
          "cassandra startup wait secs={}\n"
          "Time profile={}\n"
          "Verbose log={}\n"
          "Add timing timestamps={}\n"
          "Log file={}\n"
          "Time Profile file={}\n"
          "Processor Config File={}\n".format(in_file, ckafka, itopic,
                                               pkafka, otopic,
                                               db, keyspace, startup_sleep,
                                               time_it_flag,
                                               verbose_flag,
                                               add_timestamps,
                                               log_file,
                                               log_profile_file,
                                               args.config))
    
    logging.info("%s Starting Processor with config %s", str(time.ctime()), str(stream_config))

    # Set the signal handler for ctrl-c. Since the program runs indefinitely,
    # we need to dump some stats when sigint is received
    # (when profiling is enabled)
    signal.signal(signal.SIGINT, signal_handler)
    
    if startup_sleep > 0:
        print('Waiting for DB startup: ', startup_sleep)
        time.sleep(startup_sleep)
    

    stream_obj = playerstream.PlayerStream(in_file, ckafka, itopic,
                                            pkafka, otopic, db, keyspace,
                                            args.config, time_it_flag, 
                                            verbose_log=verbose_flag,
                                            add_timestamps=add_timestamps,
                                            log_profile_file=log_profile_file)
    stream_obj.start_processor()


if __name__ == "__main__":
    main()
