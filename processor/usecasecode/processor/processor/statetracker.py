"""
This module has core classes for multicam stream processing
"""

__version__ = '0.2'

import copy
import logging
import time
import uuid
import iso8601
from datetime import datetime

from processor import constants, trackerutils



class StateTrackerState:
    """
    This class takes care of preserving state of processor tracker. This is
    needed for streaming systems which need to maintain state and then use it
    in consequent time-periods. Currently, the class preserves the state in
    an object. Few options for users to persist the state on disk:
    1. Store the object as a pickle
    2. Serialize and de-serialize this object as json: Convert this class
       variables to json, and persist the json
    """

    def __init__(self, config, verbose_log=False):
        """Init method
        """
        self.verbose_log = verbose_log
        
        # previous timestep input data - not currently used
        self.prev_list = []  # json msgs from t-1 
        self.prev_timestamp = None
        
        # output data (data in these items are read by processorstream, then reset to empty):
        self.msgs_write = []  # messages to write into output queue
        self.reset_msgs_write()
        self.db_write = {}  # data to write into db
                #'flow' = [{'location':'test1', 'entry_count':0, 'exit_count':0, 'start_time':time.time(), 'timestamp':datetime.datetime}]

        self.reset_db_write()
        
        # data internal to the StateTracker object typically maintained over time       
        self.possible_motionless = {}  #TJH added. key = tracker id. structure: {'trackerid', {'start_time':time, 'type':'veh' or 'per', 'json':json}}
        self.entry_exit_count = self.reset_entry_exit_count()  #not used
        self.stalled_veh_classids = config.get("stalled_veh_classids", constants.STALLED_VEH_CLASSIDS)
        self.stalled_veh_thresh_sec = config.get("stalled_veh_thresh_sec", constants.STALLED_VEH_THRESH_SEC)
        self.stalled_veh_thresh_mtr = config.get("stalled_veh_thresh_mtr", constants.STALLED_VEH_THRESH_MTR)
        self.stalled_veh_delete_sec = config.get("stalled_veh_delete_sec", constants.STALLED_VEH_DELETE_SEC)
        self.motionless_classids = config.get("motionless_person_classids", constants.MOTIONLESS_CLASSIDS)
        self.motionless_thresh_sec = config.get("motionless_person_thresh_sec", constants.MOTIONLESS_THRESH_SEC)
        self.motionless_thresh_mtr = config.get("motionless_person_thresh_mtr", constants.MOTIONLESS_THRESH_MTR)
        self.motionless_delete_sec = config.get("motionless_person_delete_sec", constants.MOTIONLESS_DELETE_SEC)
        return
    
    def reset_msgs_write(self):
        """ called from processorstream to reset once messages have been written to output topic
        """
        self.msgs_write = []
        return
    
    def reset_db_write(self):
        """ called from processorstream to reset once data has been written to database
        """
        self.db_write = {'flow':[], 'detection':[], 'others':[]}  # data to write into db  'flow' and 'others' not used
        return
    
    def reset_entry_exit_count(self):
        return {'location':'endeavor', 'entry_count':0, 'exit_count':0, 'start_time':time.time()}
        
    
    
class StateTracker:
    """
    Main state tracking algorithm. The algorithm inputs and outputs list
    of jsons in day2 schema. The output list contains the json to write to the anomalies topic.
    
    Object location info is read from the input list and written to Cassandra for display via the UI
    """

    def __init__(self, config, verbose_log=False):
        """
        Init method

        Arguments:
            config [dict] -- A dictionary 
        Returns: None
        """
        self.state = StateTrackerState(config, verbose_log=verbose_log)
        return

    def init_transforms(self, json_list):
        """
        This method is called as an init method before passing the json schema.
        Some of the work done by this method is:
        1. No initialisations needed at this time..

        Arguments:
            json_list {[list]} -- [Transformed dictionaries of detections in json schema]
        """
        # Make obj id as sensor_id + obj id
        #for json_ele in json_list:
        #    if ((json_ele.get("sensor", {}).get("id", None) is not None) and
        #            (json_ele.get("object", {}).get("id", None) is not None)):

        #        json_ele["object"]["id"] = trackerutils.get_obj_id_in_sensor(
        #            json_ele)
        #    if json_ele.get("object", {}).get("vehicle", None) is not None:
        #        veh = json_ele["object"]["vehicle"]
        #        if veh.get("license", None) is None:
        #            veh["license"] = ""
        #        if veh.get("licenseState", None) is None:
        #            veh["licenseState"] = ""

        return



    def get_objects_in_difft_states(self, json_list):
        """
        This method parses the json_list and categorizes them into types
        of records based on the event observed.
        a. detection, detection_adj records
        b. Other records: None of the above

        Arguments:
            json_list {[list]} -- List of json schema based dictionaries
            for detections

        Returns:
            [dict] -- Dictionary with the state keys
            The value is the list of dictionaries (in json schema) under
            each category
        """
        detections = []
        other_recs = []
        for json_ele in json_list:
            if json_ele.get("event", {}).get("type", None) == "detection":
                detections.append(json_ele)
            elif json_ele.get("event", {}).get("type", None) == "detection_adj":
                detections.append(json_ele)
            else:
                other_recs.append(json_ele)
        return {"detection": detections,
                "others": other_recs}


    def calc_movement(self, state_recs, params):
        """ Process and return stalled/motionless objects
        params structure =         params = { 'type':'per' or 'veh', 
                                              'classids': self.motionless_classids,
                                              'thresh_sec':self.motionless_thresh_sec,
                                              'thresh_mtr':self.motionless_thresh_mtr,
                                              'delete_sec':self.motionless_delete_sec,
                                              'curr_time':datetime of 'now'}   
        self.possible_motionless structure =  {'trackerid', {'start_time':time, 'type':'veh' or 'per', 'json':json}}
        NOTE: IN reality you would need to run a periodic process to delete orphaned possible_motionless records, say once an hour
        """
        if not params['classids']:
            return []
        motionless = []
        curr_time = params['curr_time']
        state_recs_should_move = state_recs['detection']
        for json_ele in state_recs_should_move:
            curr_class = trackerutils.get_classid_string(json_ele)
            if curr_class in params['classids']:
                curr_object = trackerutils.get_tracker_string(json_ele)
                if curr_object:
                    curr_possible_motionless_object = self.state.possible_motionless.get(curr_object, {})
                    if not curr_possible_motionless_object:
                        self.state.possible_motionless.update({curr_object:{'start_time': curr_time,
                                                                            'type': params['type'],
                                                                            'json': json_ele}})
                    else:
                        age = (curr_time - curr_possible_motionless_object['start_time']).total_seconds()
                        if age >= params['thresh_sec']:
                            prev_json_ele = curr_possible_motionless_object.get('json', {})
                            prev_xy = trackerutils.get_xy(prev_json_ele)
                            curr_xy = trackerutils.get_xy(json_ele)
                            if abs(curr_xy[0] - prev_xy[0]) < params['thresh_mtr'] and \
                               abs(curr_xy[1] - prev_xy[1]) < params['thresh_mtr']:
                                   json_ele.update({'endTimestamp': json_ele.get('@timestamp','')})
                                   json_ele.update({'startTimestamp': prev_json_ele.get('@timestamp','')})
                                   if params['type'] == 'veh':
                                       json_ele.update({'event':{'id': str(uuid.uuid4()), 'type': 'UnexpectedStopping'}})
                                       json_ele.update({'analyticsModule':{'id': '1', 
                                                                            'description': 'Unexpected Stopping ' + str(age) + ' seconds', 
                                                                            'source': 'ProcessorModule-UnexpectedStopping', 
                                                                            'version': '1.0'}})
                                   else:
                                       json_ele.update({'event':{'id': str(uuid.uuid4()), 'type': 'MotionlessPerson'}})
                                       json_ele.update({'analyticsModule':{'id': '1', 
                                                                            'description': 'Motionless for ' + str(age) + ' seconds', 
                                                                            'source': 'ProcessorModule-MotionlessPerson', 
                                                                            'version': '1.0'}})
                                       
                                   motionless.append(json_ele)
                            del self.state.possible_motionless[curr_object]
                            
        return motionless
    

    
    def replace_timestamp(self, json_list):
        """ replace '@timestamp' with 'timestamp'
        """
        for json_ele in json_list:
            json_ele['timestamp'] = json_ele.pop('@timestamp')
        return 

    
    def remove_unwanted_fields(self, json_list):
        """ remove json fields not present in cassandra table
        """
        for json_ele in json_list:
            if json_ele.get('analyticsModule', {}).get('confidence', None) is not None:
                del json_ele['analyticsModule']['confidence']
        return    


    def add_calculated_fields(self, json_list):
        """ add PK fields used in parkingspot tables
        """
        for json_ele in json_list:
            json_ele['garageid'] = json_ele.get('place',{}).get('name', 'UNKNOWN_GARAGE')
            json_ele['spotid'] = json_ele.get('place',{}).get('parkingSpot', {}).get('id', 'UNKNOWN_SPOT')
            json_ele['level'] = json_ele.get('place',{}).get('parkingSpot', {}).get('level', 'UNKNOWN_LEVEL')
            json_ele['sensortype'] = json_ele.get('sensor',{}).get('type', 'UNKNOWN_SENSOR_TYPE')
        return  


    def update_location_fields(self, json_list):
        """ Update fields going into objectmarker table which has primary key (messageid, timestamp)
            where messageid = placename-level
        """          
        for json_ele in json_list:
            level = json_ele.get('place', {}).get('subplace', {}).get('level', None)
            if level is None:
                level = 'UNKNOWN_LEVEL'
            name = json_ele.get('place',{}).get('name', 'UNKNOWN_NAME')    
            json_ele['messageid'] = name + '-' + level
        return
    
    
    def prune_motionless_list(self, timestamp):
        """
        Prune the possible motionless list if an element has been on the list more than a threshold.
        """
        keys = list(self.state.possible_motionless.keys())
        for key in keys:
            pm = self.state.possible_motionless[key]
            ele_ts = pm['start_time']
            ele_type = pm['type']
            delta_time = (timestamp - ele_ts).total_seconds()
            if ele_type == 'veh':
                thresh = self.state.stalled_veh_delete_sec
            else:
                thresh = self.state.motionless_delete_sec
            if delta_time > thresh:
                del self.state.possible_motionless[key]
        return
    
    
    def process_batch(self, all_json_list):
        """"
        This is the main method that will be called for Processing. The
        detections are passed in day2 schema in a list (all_json_list). This
        method returns nothing. The tracked objects are stored in the variable
        state.msgs_write and state.db_write

        Arguments:
            all_json_list {[list]} -- List of detections in day2 schema
        """

        if not all_json_list:
            return

        #self.init_transforms(all_json_list)  #no transforms. left in here for future use

        # All the records are within one batch (say, within 0.5 seconds).
        # Choose one representative timestamp. Make sure its fast (no O(n), etc)
        
        #logging.info("%s all_json_list=%s", str(datetime.now()), all_json_list)
        
        # test_ts = "2019-06-30T07:40:39.951Z"
        timestamp = iso8601.parse_date(all_json_list[0]['@timestamp'])  # returns a datetime object in utc timezone

        state_recs = self.get_objects_in_difft_states(all_json_list)

        # process database updates to be done
        state_recs_copy = copy.deepcopy(state_recs)
        detections = state_recs_copy['detection']
        self.replace_timestamp(detections)
        #self.remove_unwanted_fields(detections)  #not presently needed
        self.update_location_fields(detections)
        self.state.db_write['detection'] = detections
        
        if self.state.verbose_log:
            print(("{} number of state_recs: detections={} others={}").format(str(datetime.now()), len(state_recs['detection']), len(state_recs['others'])))
            logging.info("%s number of state_recs: detections=%d others=%d", str(datetime.now()), len(state_recs['detection']), len(state_recs['others']))
        
        # Calculate stalled vehicles based on tracker info & xy changes
        params = {'type':'veh', 
                  'classids': self.state.stalled_veh_classids,
                  'thresh_sec':self.state.stalled_veh_thresh_sec,
                  'thresh_mtr':self.state.stalled_veh_thresh_mtr,
                  'delete_sec':self.state.stalled_veh_delete_sec,
                  'curr_time': timestamp}        
        stalls = self.calc_movement(state_recs, params)
        
        # Calculate motionless people based on tracker info & xy changes
        params = {'type':'per', 
                  'classids': self.state.motionless_classids,
                  'thresh_sec':self.state.motionless_thresh_sec,
                  'thresh_mtr':self.state.motionless_thresh_mtr,
                  'delete_sec':self.state.motionless_delete_sec,
                  'curr_time': timestamp}        
        motionless = self.calc_movement(state_recs, params)
        
        if self.state.verbose_log:
            if len(stalls) + len(motionless) > 0:
                print(str(datetime.now()), 'Stalls:', len(stalls), 'Motionless People:', len(motionless))
            logging.info("%s stalls=%d", str(datetime.now()), len(stalls))
            logging.info("%s motionless people=%d", str(datetime.now()), len(motionless))
            logging.info("%s possible_motionless= %d", str(datetime.now()), len(self.state.possible_motionless))
            if self.state.verbose_log > 1:
                logging.info("%s possible_motionless= %d  %s", str(datetime.now()), len(self.state.possible_motionless), self.state.possible_motionless)
            
        
        self.state.msgs_write = stalls + motionless
        if self.state.verbose_log:
            logging.info("%s ProcessBatch: msgs_write=%d db_write=%d", str(datetime.now()), len(self.state.msgs_write), len(self.state.db_write['detection']))

        self.prune_motionless_list(timestamp)
        self.state.prev_list = all_json_list
        self.state.prev_timestamp = timestamp
        return


