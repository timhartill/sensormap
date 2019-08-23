"""
This module has utility functions for multi-cam tracker. Most of the functions
are related to get/set json schema parameters
"""

__version__ = '0.2'

import random
import string
from collections import OrderedDict
import copy

import numpy as np
import pandas as pd

import json
import datetime
import iso8601


def load_json_for_test(json_input_file='/media/tim/dl3storage/Datasets/virat/json/virat_json_fps_5.0_Tracker_False_sorted_secs_900.json'):
    """ Used in testing - returns a list of dicts from a file of json of form {json1}\n{json2}\n...{json3}
    call as: total_all_json_list = trackerutils.load_json_for_test()
    """
    with open(json_input_file, "r") as f:
        json_input_list = f.read()
    json_input_list = json_input_list.split('\n')   #typically sorted by timestamp + messageid
    
    # takes about 33GB RAM:
    json_input_list = [json.loads(j) for j in json_input_list]
    return json_input_list


def get_json_timewindow_for_test(total_all_json_list, start_idx=0 , window_time_in_secs=0.5):
    """ Used in testing - returns a list of json for a time window of window_time_in_secs 
        starting at total_all_json_list[start_idx]
    call as: all_json_list = trackerutils.get_json_timewindow_for_test(total_all_json_list, start_idx=0 , window_time_in_secs=0.5)    
    """
    all_json_list = []
    start_timestamp = iso8601.parse_date(total_all_json_list[start_idx]['@timestamp'])
    end_timestamp = start_timestamp + datetime.timedelta(seconds=window_time_in_secs)
    for i in range(start_idx, len(total_all_json_list)):
        timestamp = iso8601.parse_date(total_all_json_list[i]['@timestamp'])
        if timestamp > end_timestamp:
            break
        all_json_list.append(total_all_json_list[i])
    all_json_list = copy.deepcopy(all_json_list)    
    num_selected = len(all_json_list)
    print('Number of json msgs selected:', num_selected)
    if num_selected > 0:
        print('starting timestamp: ', all_json_list[0]['@timestamp'])
        print('ending timestamp: ', all_json_list[num_selected-1]['@timestamp'])        
    return all_json_list


def get_timestamp_str(datetime_obj):
    """Return the string formatted timestamp for a given datetime object.
    We assume that the datetime object is in the UTC timezone
    Return format = '<YYYY>-<mm>-<dd>T<HH>:<MM>:<SS.SSS>Z'
    where YYYY = 4 digit year
        mm = 2 digit month
        dd = 2 digit date
        HH = 2 digit hour ( 0  to 23 )
        HH = 2 digit minute ( 0  to 59 )
        SS.SSS = Seconds (upto millisecond accuracy, 00.000 to 59.999)
        Z signifies the UTC time-zone
    Returns:
        [string] -- Timestamp string
    """
    (dtobj, micro) = datetime_obj.strftime('%Y-%m-%dT%H:%M:%S.%f').split('.')
    dtobj = "%s.%03dZ" % (dtobj, int(micro) / 1000)
    return dtobj


def create_time_windows(json_list, window_time_in_secs=0.5):
    """
    This method will input a json list and resample the json list by timestamp
    It will return a ordered-dictionary where key is a timestamp resampled
    at "window_time_in_sec", and value is a list of json records for that time
    window

    Returns:
        [dict] -- Ordered Dictionary of <time, list of jsonschema recs>
    """

    time_indexed_json_list = [
        {'timestamp': json_ele['@timestamp'], 'json': json_ele}
        for json_ele in json_list]
    time_indexed_json_pd = pd.DataFrame(time_indexed_json_list)
    time_indexed_json_pd['timestamp'] = pd.to_datetime(
        time_indexed_json_pd['timestamp'], utc=True)
    time_indexed_json_pd.set_index('timestamp', inplace=True)
    # print(time_indexed_json_pd)
    time_indexed_json_pd = time_indexed_json_pd['json'].sort_index(
    ).resample('{}S'.format(window_time_in_secs)).apply(list)

    time_indexed_json_dict = time_indexed_json_pd.to_dict(OrderedDict)
    return time_indexed_json_dict


def get_xy(json_ele):
    """
    Get the x and y values of a detected vehicle

    Arguments:
        json_ele {[dict]} -- Detection record in json schema

    Returns:
        [list] -- A list comprised of [x,y]
    """
    if json_ele.get("object", {}).get("centroid", None) is not None:
        (varx, vary) = (json_ele['object']['centroid']
                        ['x'], json_ele['object']['centroid']['y'])
        return [varx, vary]
    else:
        return None


def get_obj_id_in_sensor(json_ele):
    """
    Get the object id value of a detected vehicle. The object id is a
    combination of the sensor id and the object id

    Arguments:
        json_ele {[dict]} -- Detection record in json schema

    Returns:
        [string] -- Object ID
    """
    return "^S{}_^O{}".format(json_ele['sensor']['id'],
                              json_ele['object']['id'])


def get_mean_xy(pts):
    """
    Return the mean xy point (x_mean, y_mean) from the set of points

    Arguments:
        pts {list} -- List of (x,y) points

    Returns:
        [tuple] -- Mean (x,y) point
    """

    x_list = [p[0] for p in pts]
    y_list = [p[1] for p in pts]
    x_rep = np.mean(x_list)
    y_rep = np.mean(y_list)
    return (x_rep, y_rep)


def get_median_xy(pts):
    """
    Return the median xy point (x_median, y_median) from the set of points

    Arguments:
        pts {list} -- List of (x,y) points

    Returns:
        [tuple] -- Median (x,y) point
    """

    x_list = [p[0] for p in pts]
    y_list = [p[1] for p in pts]
    # Use mean. Its faster
    x_rep = np.median(x_list)
    y_rep = np.median(y_list)
    return (x_rep, y_rep)


def get_max_camy_xy(pts):
    """
    Return the xy point with maximum y from the set of points

    Arguments:
        pts {list} -- List of (global_x,global_y, cam_x, cam_y) points

    Returns:
        [tuple] -- (x,y) point
    """
    x_rep = None
    y_rep = None
    best_camy = None
    for point in pts:
        if best_camy is None or best_camy < point[3]:
            best_camy = point[3]
            x_rep = point[0]
            y_rep = point[1]
    return (x_rep, y_rep)


def get_obj_classid(json_ele):
    """
    Not used in favour of get_classid_string below that returns '' instead of None on lookup failure
    Get the class id value of a detected object. 
    
    Arguments:
        json_ele {[dict]} -- Detection record in json schema

    Returns:
        [string] -- Object ID
    """
    return json_ele.get('object', {}).get('classid', None)



def get_obj_id(json_ele):
    """
    Get the object id value of a detected object. The object id returned
    in this function is a string of the the object id

    Arguments:
        json_ele {[dict]} -- Detection record in json schema

    Returns:
        [string] -- Object ID
    """
    return json_ele.get('object', {}).get('id', None)


def get_obj_id_str(json_ele):
    """
    Get the object id value of a detected vehicle. The object id returned
    in this function is a string of the the object id (prefixed by some
    unique chars)

    Arguments:
        json_ele {[dict]} -- Detection record in json schema

    Returns:
        [string] -- Object ID
    """
    return "^^O{}".format(get_obj_id(json_ele))


def get_camera(json_ele):
    """
    Get the sensor/camera id of the detection

    Arguments:
        json_ele {[dict]} -- Detection record in json schema

    Returns:
        [string] -- Sensor ID
    """
    return json_ele['sensor']['id']


def is_spot_rec(json_ele):
    """
    Is the given detection a parking spot record? The record is a parking spot
    record if the "place" has a "parkingSpot" element, and if the event type
    is either ["parked", "pulled", "empty"]

    Arguments:
        json_ele {[dict]} -- Detection record in json schema

    Returns:
        [boolean] -- True if the record is a parking spot record
    """
    if ((json_ele.get("place", {}).get("parkingSpot", None) is not None) and
        (json_ele.get("event", {}).get("type", None) in
         ["parked", "pulled", "empty"])):
        return True
    return False


def is_parked_rec(json_ele):
    """
    Returns if the given detection is of a vehicle parked record. The record
    is a parking spot record if the "place" has a "parkingSpot" element, and
    if the event type is "parked"

    Arguments:
        json_ele {[dict]} -- Detection record in json schema

    Returns:
        [boolean] -- True if the record is of a parked car
    """
    if ((json_ele.get("place", {}).get("parkingSpot", None) is not None) and
            (json_ele.get("event", {}).get("type", None) == "parked")):
        return True
    return False


def is_empty_spot_rec(json_ele):
    """
    Returns if the given detection is of a parking spot that is empty. The
    record is a parking spot record if the "place" has a "parkingSpot"
    element, and if the event type is "empty"

    Arguments:
        json_ele {[dict]} -- Detection record in json schema

    Returns:
        [boolean] -- True if the record is of a empty spot
    """
    if ((json_ele.get("place", {}).get("parkingSpot", None) is not None) and
            (json_ele.get("event", {}).get("type", None) == "empty")):
        return True
    return False


def is_pulled_rec(json_ele):
    """
    Returns if the given detection is of a vehicle that has pulled
    from a parking spot. The record is a parking spot record if the
    "place" has a "parkingSpot" element, and if the event type is
    "pulled"

    Arguments:
        json_ele {[dict]} -- Detection record in json schema

    Returns:
        [boolean] -- True if the record is of a pulled ar
    """
    if ((json_ele.get("place", {}).get("parkingSpot", None) is not None) and
            (json_ele.get("event", {}).get("type", None) == "pulled")):
        return True
    return False


def is_aisle_rec(json_ele):
    """
    Returns if the given detection is of a vehicle is for a vehicle on aisle.
    The record is a aisle record if the "place" has a "aisle", "entrace" or
    "exit" element, and if the event type is one among
    ["entry",  "exit", "moving", "stopped"]

    Arguments:
        json_ele {[dict]} -- Detection record in json schema

    Returns:
        [boolean] -- True if the record is of a pulled ar
    """
    if(((json_ele.get("place", {}).get("entrance", None) is not None) or
        (json_ele.get("place", {}).get("exit", None) is not None) or
        (json_ele.get("place", {}).get("aisle", None) is not None)) and
        (json_ele.get("event", {}).get("type", None)
         in ["entry", "exit", "moving", "stopped"])):
        return True
    return False


def get_random_lp():
    """Generate a random license plate string (in standard California
    state style)

    Returns:
        [string] -- Random license plate string
    """
    retval = str(random.choice([5] * 1 + [6]*2 + [7] * 2 + [8] * 1))
    retval += ''.join(random.choice(string.ascii_uppercase)
                      for _ in range(3))
    retval += ''.join(random.choice(string.digits) for _ in range(3))
    return retval


def get_vehicle_string(json_ele):
    """
    Get a string that describes a vehicle (in terms of its attributes).
    Currently it just returns the license plate. It can be extended
    to return other attributes

    Arguments:
        json_ele {[dict]} -- Detection record in json schema

    Returns:
        [string] -- Vehicle description string
    """
    vehicle = json_ele['object'].get('vehicle',{})
    veh_string = vehicle.get('license', "")
    return veh_string


def get_tracker_string(json_ele):
    return json_ele.get('object', {}).get('trackerid', '')


def get_classid_string(json_ele):
    """ 
    """
    return json_ele.get('object', {}).get('classid', '')

