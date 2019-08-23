"""
This module has core classes for multicam tracking
"""

__version__ = '0.2'

import copy
import logging
import math
import time
import uuid
import iso8601
import numpy as np
from datetime import datetime
#import scipy.spatial.distance as ssd
#from scipy.cluster.hierarchy import fcluster, linkage
#from scipy.optimize import linear_sum_assignment
#from scipy.spatial import distance_matrix
#from shapely.geometry import LineString, Point

from playerlib import constants, trackerutils
#from geo.core import spatial
#from euclidean import euchelper
#from network import networkhelper


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
        self.possible_stalled_cars = {}  #TJH added. key = vehicle license. will contain the license, bbox, start time
        self.possible_understays = {}  #TJH added. key:vehicle license, start_time:time, json:json
        self.entry_exit_count = self.reset_entry_exit_count()
        self.entry_exit_update_sec = config.get("entry_exit_update_sec", constants.ENTRY_EXIT_UPDATE_SEC)
        self.understay_thresh_sec = config.get("understay_thresh_sec", constants.UNDERSTAY_THRESH_SEC)
        self.stalled_car_thresh_sec = config.get("stalled_car_thresh_sec", constants.STALLED_CAR_THRESH_SEC)
        self.stalled_car_thresh_mtr = config.get("stalled_car_thresh_mtr", constants.STALLED_CAR_THRESH_MTR)
        self.stalled_car_delete_sec = config.get("stalled_car_delete_sec", constants.STALLED_CAR_DELETE_SEC)
        return
    
    def reset_msgs_write(self):
        """ called from processorstream to reset once messages have been written to output topic
        """
        self.msgs_write = []
        return
    
    def reset_db_write(self):
        """ called from processorstream to reset once data has been written to database
        """
        self.db_write = {'flow':[], 'parking':[], 'aisle':[]}  # data to write into db
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
        This method is called as an init method before passing the day2 schema.
        Some of the work done by this method is:
        1. Change the object id as a combination of sensor id and object id
        2. If the detected object is a "vehicle", and if the license and
           licenseState is "None", then we convert them to empty strings
        3. If SNAP_POINTS_TO_GRAPH is True, then it will also change the (x,y)
           of each vehicle. The (x,y) will be snapped to the road-network edge.
           Snapping is done by projecting the original (x,y) to the nearest
           point on the nearest edge

        Arguments:
            json_list {[list]} -- [Transformed dictionaries of detections in day2 schema]
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


    def smooth_x_y_in_list(self, rec_list, reason="No reason"):
        """
        This method replaces a vehicle detections in a list by a representative
        point (x,y). Currently, the logic is:
            computed (x,y) = (mean(x_i), mean(y_i)) for all (xi,yi) in rec_list

        All the records in the rec_list are updated with the new (x,y)
        computed. The original (xi, yi) are stored in
        rec['object']['coordinate']['origPoints']
        where rec is a dict in rec_list

        Arguments:
            rec_list {[list]} -- The list of day2 schema dictionaries of
            vehicle detection

        Keyword Arguments:
            reason {str} -- Used for logging so that we know why
            the points in the list were consolidated
            (default: {"No reason"})
        """
        rec_list = sorted(rec_list, key=lambda k: k['@timestamp'])


        # Different ways to pick the (x,y) from the list of points
        # pts = [(rec['object']['coordinate']['x'], rec['object']
        #        ['coordinate']['y']) for rec in rec_list]
        # 1. Mean,
        #x_rep, x_rep = trackutils.get_median_xy(pts)
        # 2. Mean
        #x_rep, x_rep = trackutils.get_mean_xy(pts)
        # 3. Find the point that is nearest to the camera. In the 360-d usecase,
        # this is the point with highest camera_y coordinate
        pts = [(rec['object']['coordinate']['x'],
                rec['object']['coordinate']['y'],
                (rec['object']['bbox']['topleftx'] +
                 rec['object']['bbox']['bottomrightx'])/2.0,
                max(rec['object']['bbox']['toplefty'],
                    rec['object']['bbox']['bottomrighty']),
                ) for rec in rec_list]
        x_rep, y_rep = trackerutils.get_max_camy_xy(pts)

        for rec in rec_list:
            if rec['object']['coordinate'].get('origPoints', None) is None:
                rec['object']['coordinate']['origPoints'] = []
            rec['object']['coordinate']['origPoints'].append({
                "x": rec['object']['coordinate']['x'],
                "y": rec['object']['coordinate']['y'],
                "reason": reason
            })
            # rec['object']['coordinate']['orig_y'] = rec['object']['coordinate']['y']
            rec['object']['coordinate']['x'] = x_rep
            rec['object']['coordinate']['y'] = y_rep
        return

    def select_rep_member_from_list(self, json_list):
        """
        This method selects one representative dict from the list
        of vehicle detections (in json_list)

        Returns:
            [dict] -- A representative day2 schema dictionary of
            selected representative vehicle detection
        """
        retval = None
        if json_list:
            retval = json_list[0]
            pref = 100
            min_obj_id = None
            for ele in json_list:
                # 1st pref = entry/exit
                # 2nd pref = one with videopath
                if ele["event"]["type"] in ["entry", "exit"]:
                    retval = ele
                    pref = 1
                elif(pref > 1) and (ele["videoPath"] != ""):
                    retval = ele
                    pref = 2
                elif(pref > 2) and (min_obj_id is None or
                                    min_obj_id > ele["object"]["id"]):
                    retval = ele
                    min_obj_id = ele["object"]["id"]
                    pref = 3
        return retval




    def get_vehicles_in_difft_states(self, json_list):
        """ TJH Updated to separate entry and exit types
        This method parses the json_list and categorizes them into four types
        of records based on the event observed.
        a. Parked records
        b. Empty records: When a parking spot goes empty
        c. Moving records: For moving cars
        d. Entry/Exit records: For cars in the entry or Exit areas respectively
        e. Other records: None of the above

        Arguments:
            json_list {[list]} -- List of day2 schema based dictionaries
            for detections

        Returns:
            [dict] -- Dictionary with the keys:
            "parked", "empty", "moving", "entry", "exit", "others"
            The value is the list of dictionaries (in day2 schema) under
            each category
        """
        parked_cars = []
        empty_spots = []
        entry_cars = []
        exit_cars = []
        moving_cars = []
        other_recs = []
        for json_ele in json_list:
            if trackerutils.is_spot_rec(json_ele):
                if json_ele.get("event", {}).get("type", None) == "parked":
                    parked_cars.append(json_ele)
                elif json_ele.get("event", {}).get("type", None) == "empty":
                    empty_spots.append(json_ele)
                else:
                    # -- logging.warning(
                    # --     "Parked car warning: The record is a parking spot"
                    # --  " record. However, it does not state if: (a) a car "
                    # --  "is parked or, (b) a spot is empty.\n Rec: {}"
                    # --  .format(json_ele))
                    pass
            elif trackerutils.is_aisle_rec(json_ele):
                if json_ele.get("event", {}).get("type", None) in ["moving","stopped"]:
                    moving_cars.append(json_ele)
                elif json_ele.get("event", {}).get("type", None) == "entry":
                    entry_cars.append(json_ele)
                elif json_ele.get("event", {}).get("type", None) == "exit":
                    exit_cars.append(json_ele)
                else:
                    pass
                
            else:
                other_recs.append(json_ele)
                # -- logging.warning(
                # --     "Unknown movement: The record is neither a car in "
                # --     "spot or aisle.\n Rec: {}".format(json_ele))
        return {"parked": parked_cars,
                "empty": empty_spots,
                "moving": moving_cars,
                "entry": entry_cars,
                "exit": exit_cars,
                "others": other_recs}
        
        


    def calc_stalls(self, state_recs):
        """ Process and return stalled vehicles
        self.possible_stalled_cars structure =  {'vehicle license', {'start_time':time, 'json':json}}
        NOTE: IN reality you would need to run a periodic process to delete orphaned possible_stalled_cars records, say once an hour
        """
        stalls = []
        curr_time = time.time()
        state_recs_should_move = state_recs['moving'] + state_recs['entry'] + state_recs['exit']
        for json_ele in state_recs_should_move:
            curr_vehicle = trackerutils.get_vehicle_string(json_ele)
            if curr_vehicle:   #if it's a vehicle
                curr_possible_stalled_car = self.state.possible_stalled_cars.get(curr_vehicle, {})
                if not curr_possible_stalled_car:
                    self.state.possible_stalled_cars.update({curr_vehicle:{'start_time':curr_time, 'json':json_ele}})
                else:
                    age = curr_time - curr_possible_stalled_car.get('start_time', 0.0)
                    if age >= self.state.stalled_car_thresh_sec:
                        prev_json_ele = curr_possible_stalled_car.get('json', {})
                        prev_xy = trackerutils.get_xy(prev_json_ele)
                        curr_xy = trackerutils.get_xy(json_ele)
                        if abs(curr_xy[0] - prev_xy[0]) < self.state.stalled_car_thresh_mtr and \
                           abs(curr_xy[1] - prev_xy[1]) < self.state.stalled_car_thresh_mtr:
                               json_ele.update({'endTimestamp': json_ele.get('@timestamp','')})
                               json_ele.update({'startTimestamp': prev_json_ele.get('@timestamp','')})
                               json_ele.update({'event':{'id': str(uuid.uuid4()), 'type': 'UnexpectedStopping'}})
                               json_ele.update({'analyticsModule':{'id': '1', 
                                                                    'description': 'Unexpected Stopping ' + str(age) + ' seconds', 
                                                                    'source': 'MetromindAM-UnexpectedStopping', 
                                                                    'version': '1.0', 
                                                                    'confidence': 0}})
                               stalls.append(json_ele)
                        del self.state.possible_stalled_cars[curr_vehicle]
                            
        for json_ele in state_recs['parked']:  # if vehicle reaches a parked state delete the rec
            curr_vehicle = trackerutils.get_vehicle_string(json_ele)
            if curr_vehicle:
                curr_possible_stalled_car = self.state.possible_stalled_cars.get(curr_vehicle, {})
                if curr_possible_stalled_car:
                    del self.state.possible_stalled_cars[curr_vehicle]        
        return stalls
    

    def calc_understays(self, state_recs):
        """ Process and return understays
        self.possible_understays structure =  {'vehicle license', {'start_time':time, 'json':json}}
        NOTE: IN reality you would need to run a periodic process to delete orphaned possible_understays records, say once an hour
        """
        understays = []
        curr_time = time.time()
        # process entries
        for json_ele in state_recs['entry']:
            curr_vehicle = trackerutils.get_vehicle_string(json_ele)
            if curr_vehicle:  #if this is a vehicle
                self.state.possible_understays.update({curr_vehicle:{'start_time':curr_time, 'json':json_ele}})

        #process exits                
        for json_ele in state_recs['exit']:   #IT APPEARS WE DONT SEE ANY 'EXIT' msgs so actually we wont see any understays!
            curr_vehicle = trackerutils.get_vehicle_string(json_ele)
            exitdict = self.state.possible_understays.get(curr_vehicle, {})
            if exitdict:  #if this is a vehicle with a potential understay record
                time_of_stay = curr_time - exitdict.get('start_time', 0.0)   #should decode json_ele's timestamp instead but ok for demo
                if time_of_stay <= self.state.understay_thresh_sec:
                    prev_json_ele = exitdict.get('json', {})
                    json_ele.update({'endTimestamp': json_ele.get('@timestamp','')})
                    json_ele.update({'startTimestamp': prev_json_ele.get('@timestamp','')})
                    json_ele.update({'event':{'id': str(uuid.uuid4()), 'type': 'Understay'}})
                    json_ele.update({'analyticsModule':{'id': '1', 
                                                        'description': 'Short Stay ' + str(time_of_stay/60) + ' minutes', 
                                                        'source': 'MetromindAM-Understay', 
                                                        'version': '1.0', 
                                                        'confidence': 0}})
                    understays.append(json_ele)
                    del self.state.possible_understays[curr_vehicle]  
        
        #delete from possible_understays for any parked vehicles that have been there longer than the understay threshold           
        if len(self.state.possible_understays) > 0:
            state_recs_check_understay = state_recs['moving'] + state_recs['parked'] + state_recs['empty']
            for json_ele in state_recs_check_understay:
                curr_vehicle = trackerutils.get_vehicle_string(json_ele)
                parkeddict = self.state.possible_understays.get(curr_vehicle, {})
                if parkeddict:
                    time_of_stay = curr_time - parkeddict.get('start_time', 0.0)
                    if time_of_stay >= self.state.understay_thresh_sec:
                        del self.state.possible_understays[curr_vehicle]  
        return understays

    
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

    def update_aisle_fields(self, json_list):
        """ Update fields going into aisle table as required by how the ui application is written
        """          
        for json_ele in json_list:
            level = json_ele.get('place', {}).get('entrance', {}).get('level', None)
            if level is None:
                level = json_ele.get('place',{}).get('aisle', {}).get('level', 'UNKNOWN_LEVEL')
            name = json_ele.get('place',{}).get('name', 'UNKNOWN_NAME')    
            json_ele['messageid'] = name + '-' + level
        return
    
    # Functions visible to outside
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

        state_recs = self.get_vehicles_in_difft_states(all_json_list)

        # process database updates to be done
        state_recs_copy = copy.deepcopy(state_recs)
        parkingspot = state_recs_copy['parked'] + state_recs_copy['empty']
        aisle = state_recs_copy['moving'] + state_recs_copy['entry'] + state_recs_copy['exit']

        self.replace_timestamp(parkingspot)
        self.remove_unwanted_fields(parkingspot)
        self.add_calculated_fields(parkingspot)
        self.state.db_write['parking'] = parkingspot

        self.replace_timestamp(aisle)
        self.remove_unwanted_fields(aisle)
        self.update_aisle_fields(aisle)
        self.state.db_write['aisle'] = aisle
        
        # Calc entry/exit counts (for vehicles) & extrapolate to hours. then write to Cassandra
        self.state.entry_exit_count['entry_count'] += len(state_recs['entry']) #{'location':0, 'count':0, 'start_time':time.time()}
        self.state.entry_exit_count['exit_count'] += len(state_recs['exit'])
        now_time = time.time()
        if now_time >= self.state.entry_exit_count['start_time'] + self.state.entry_exit_update_sec:
            if self.state.verbose_log:
                print(str(datetime.now()), 'Calculating Entry/Exit flow from ', self.state.entry_exit_count)
                logging.info("%s self.state.entry_exit_count=%s", str(datetime.now()), self.state.entry_exit_count)
            elapsed = (now_time - self.state.entry_exit_count['start_time']) / 3600.0  # convert elapsed seconds to hours    
            self.state.entry_exit_count['entry_count'] = round(self.state.entry_exit_count['entry_count'] / elapsed)
            self.state.entry_exit_count['exit_count'] = round(self.state.entry_exit_count['exit_count'] / elapsed)
            self.state.entry_exit_count['timestamp'] = timestamp
            self.state.db_write['flow'] = [self.state.entry_exit_count.copy()]
            self.state.entry_exit_count = self.state.reset_entry_exit_count()

        if self.state.verbose_log:
            print(("{} number of state_recs: entry={} exit={} parked={} moving={} empty={} others={}").format(str(datetime.now()), len(state_recs['entry']), len(state_recs['exit']), len(state_recs['parked']), len(state_recs['moving']), len(state_recs['empty']), len(state_recs['others'])))
            logging.info("%s number of state_recs: entry=%d exit=%d parked=%d moving=%d empty=%d others=%d", str(datetime.now()), len(state_recs['entry']), len(state_recs['exit']), len(state_recs['parked']), len(state_recs['moving']), len(state_recs['empty']), len(state_recs['others']))
        
        # Calculate stalled cars based on license info & xy changes
        stalls = self.calc_stalls(state_recs)
        # Calculate understay anomalies
        understays = self.calc_understays(state_recs) 
        
        if self.state.verbose_log:
            if len(stalls) > 0:
                print(str(datetime.now()), 'Stalls:', len(stalls))
                logging.info("%s stalls=%s", str(datetime.now()), stalls)
                logging.info("%s possible_stalled_cars= %d  %s", str(datetime.now()), len(self.state.possible_stalled_cars), self.state.possible_stalled_cars)
            
            if len(understays) > 0:
                print(str(datetime.now()), 'Understays:', len(understays))
                logging.info("%s understays=%s", str(datetime.now()), understays)
                logging.info("%s possible_understays= %d  %s", str(datetime.now()), len(self.state.possible_understays), self.state.possible_understays)
        
        prev_json_list = self.state.prev_list
        prev_timestamp = self.state.prev_timestamp

        self.state.msgs_write = stalls + understays
        if self.state.verbose_log:
            logging.info("%s ProcessBatch: msgs_write=%d aisle=%d parkingspot=%d", str(datetime.now()), len(self.state.msgs_write), len(aisle), len(parkingspot))

        self.state.prev_list = all_json_list
        self.state.prev_timestamp = timestamp
        return


