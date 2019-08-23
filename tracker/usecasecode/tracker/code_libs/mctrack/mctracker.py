"""
This module has core classes for multicam tracking
"""

__version__ = '0.2'

import copy
import logging
import math

import iso8601
import numpy as np
import scipy.spatial.distance as ssd
from scipy.cluster.hierarchy import fcluster, linkage
from scipy.optimize import linear_sum_assignment
from scipy.spatial import distance_matrix
from shapely.geometry import LineString, Point

from code_libs.mctrack import constants, trackerutils, tracklog
from code_libs.geo.core import spatial
from code_libs.euclidean import euchelper
from code_libs.network import networkhelper


class MulticamTrackerConfig:
    """
    This class has the configuration for multicam tracking
    """

    def __init__(self, config):
        self.cl_dist_thresh_m = (config
                                 .get("CLUSTER_DIST_THRESH_IN_M",
                                      constants.DEF_CLUS_DIST_THRESH_M))
        self.match_max_dist_m = (config
                                 .get("MATCH_MAX_DIST_IN_M",
                                      constants.DEF_MATCH_MAX_DIST_IN_M))
        self.carry_time_sec = (config
                               .get("CARRY_OVER_LIST_PRUNE_TIME_IN_SEC",
                                    constants.DEF_CARRY_PRUNE_TIME_SEC))


class MulticamTrackerState:
    """
    This class takes care of preserving state of multi-cam tracker. This is
    needed for streaming systems which need to maintain state and then use it
    in consequent time-periods. Currently, the class preserves the state in
    an object. Few options for users to persist the state on disk:
    1. Store the object as a pickle
    2. Serialize and de-serialize this object as json: Convert this class
       variables to json, and persist the json
    """

    def __init__(self, config, verbose_log=False, log_config={}):
        """Init method

        Arguments:
            config [dict] -- A dictionary that has the following keys
                a. "overlapping_camera_ids": This key specifies the
                   cameras which have overlapping coverages. If this
                   dictionary has non-zero number of keys, then the tracker
                   will only merge detections from the overlapping cameras.
                   It will not merge between the cameras that do not overlap;
                   it will always be kept separate
                b. "dont_match_cameras_adj_list": This key specifies the cameras
                   whose detections shoult NOT be merged together. For example,
                   there two objects detected from two neighboring cameras that
                   monitor entry and exit lanes are closeby in space. However,
                   since this lane is divided, we would not want the detections
                   from both cameras to be merged even though their detections
                   are closeby.
                c. "MAP_INFO": This key specifies the road-network information.
                   The road-network graph is represented as a set of lines. Each
                   line has a set of points [set of (lon, lat)]. This map will be
                   used to snap detected points to the road network if the option
                   if SNAP_POINTS_TO_GRAPH is True
                d. "IGNORE_DETECTION_DICT_MOVING": This dictionary value indicates
                   the polygons inside which detections has to be ignored (not
                   processed). For each camera, we can specify a list of polygons
                   where detections have to be ignored. Often there are regions
                   (ROIs) where the detections should be ignored. The reason for
                   ignoring may be since the user has defined only specific ROI to
                   ignore, or also may be because the detections in those regions
                   are prone to high false-detections (e.g., due to frequent
                   lighting changes)

        Returns: None
        """
        self.verbose_log = verbose_log
        self.log_config = log_config

        #self.unidentified_cars = []
        self.prev_list = []
        self.prev_timestamp = None
        self.carry_over_list = []   # Unassigned json to be carried  forward to next timestep
        self.retval = []
        self.match_stats = []
        self.possible_parked_cars = []
        # STATE STORE: The below variables store the states of the tracking module
        #self.parking_spot_state = {}

        self.curr_unknown_veh_id = 0
        self.match_id = 0    #indexer for match stats, incremented every process_batch iteration

        self.overlapping_camera_ids = config.get("overlapping_camera_ids", {})
        if self.overlapping_camera_ids == {}:  #TJH ADDED
            self.dont_match_cameras_adj_list = config.get(
                "dont_match_cameras_adj_list", {})
            if self.dont_match_cameras_adj_list != {}:
                self.match_type = 1  # matching everything except cam pairs appearing in this list
            else:
                self.match_type = 2  # no matching rules set except same cams never match
    
        else:
            self.dont_match_cameras_adj_list = {}
            self.match_type = 0       # matching based on specifying overlapping cameras
            
        # True if object ids are consistent across frames from a camera ie tracking is occuring    
        #self.object_ids_track_across_frames = config.get("object_ids_track_across_frames", constants.OBJECT_IDS_TRACK_ACROSS_FRAMES)    
        self.assume_objs_have_same_id_intra_frame_period = config.get("object_ids_track_across_frames", constants.ASSUME_OBJS_HAVE_SAME_ID_INTRA_FRAME_PERIOD)
            
        self.map_info = config.get("MAP_INFO", None)
        self.dense_map_info = None
        self.road_network = None

        self.clustered_oid_map = {}  #key: object_id : {"update_ts": timestamp, "id_set": set(object_ids in this cluster), "id": mctracker cluster id}
        self.curr_cl_obj_id = 0   # current mc tracker cluster id, incremented whenever a new cluster is added

        if self.map_info is not None:
            self.dense_map_info = euchelper.densify_graph(self.map_info)
            self.road_network = networkhelper.Network(
                self.dense_map_info, max_point_dist=0.1)


class MulticamTracker:
    """
    Main multicamera tracking algorithm. The algorithm inputs and outputs list
    of jsons in day2 schema. The output list contains the tracked objects of
    the input list.

    The algorithm has three main components:
    1. Per-Camra Clustering: Aggregate multiple frames from the same camera
       that arrive within 0.5 second.
    2. Inter-Camera Clustering: Aggregate across cameras and transfer
       attributes
    3. Inter-Period Matching: Match across consecutive time-periods and
       transfer attributes
    """

    def __init__(self, config, verbose_log=False, log_config={}):
        """
        Init method

        Arguments:
            config [dict] -- A dictionary that has the following keys
                a. "overlapping_camera_ids": This key specifies the
                   cameras which have overlapping coverages. If this
                   dictionary has non-zero number of keys, then the tracker
                   will only merge detections from the overlapping cameras.
                   It will not merge between the cameras that do not overlap;
                   it will always be kept separate
                b. "dont_match_cameras_adj_list": This key specifies the cameras
                   whose detections shoult NOT be merged together. For example,
                   there two objects detected from two neighboring cameras that
                   monitor entry and exit lanes are closeby in space. However,
                   since this lane is divided, we would not want the detections
                   from both cameras to be merged even though their detections
                   are closeby.
                c. "MAP_INFO": This key specifies the road-network information.
                   The road-network graph is represented as a set of lines. Each
                   line has a set of points [set of (lon, lat)]. This map will be
                   used to snap detected points to the road network if the option
                   if SNAP_POINTS_TO_GRAPH is True
                d. "IGNORE_DETECTION_DICT_MOVING": This dictionary value indicates
                   the polygons inside which detections has to be ignored (not
                   processed). For each camera, we can specify a list of polygons
                   where detections have to be ignored. Often there are regions
                   (ROIs) where the detections should be ignored. The reason for
                   ignoring may be since the user has defined only specific ROI to
                   ignore, or also may be because the detections in those regions
                   are prone to high false-detections (e.g., due to frequent
                   lighting changes)

        Returns: None
        """
        self.state = MulticamTrackerState(config, verbose_log=verbose_log, log_config=log_config)
        self.mclogger = tracklog.MulticamTrackLogger(config, log_config=log_config)
        self.config = MulticamTrackerConfig(config.get("trackerConfig", {}))

    def init_transforms(self, json_list):
        """
        This method is called as an init method before passing the json schema.
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
        for json_ele in json_list:
            if ((json_ele.get("sensor", {}).get("id", None) is not None) and
                    (json_ele.get("object", {}).get("id", None) is not None)):

                json_ele["object"]["id"] = trackerutils.get_obj_id_in_sensor(json_ele)  #make id into sensor id + object id
#            if json_ele.get("object", {}).get("vehicle", None) is not None:
#                veh = json_ele["object"]["vehicle"]
#                if veh.get("license", None) is None:
#                    veh["license"] = ""
#                if veh.get("licenseState", None) is None:
#                    veh["licenseState"] = ""

            # make sure "level" is in uppercase
            subplace_rec = json_ele.get("place", {}).get("subplace", None)
            if subplace_rec is not None:
                subplace_rec['level'] = subplace_rec['level'].upper()

        if constants.SNAP_POINTS_TO_GRAPH:
            self.match_moving_points_to_map(
                json_list, map_info=self.state.map_info)

    def match_point_to_map(self, json_ele, map_info):
        """
        This method matches the vehicle detection (argument "json_ele") to the
        road-network. The (x,y) will be snapped to the road-network edge.
        Snapping is done by projecting the original (x,y) to the nearest
        point on the nearest edge. Note that the json_ele is overwritten with
        the new (x,y) point on the road-network

        Arguments:
            json_ele {[dict]} -- [The day2 schema dict of the vehicle detection]
            map_info {[list]} -- [The road-network given as a list of line-strings]

        Returns: None. Note that the json_ele is overwritten with
        the new (x,y) point on the road-network.
        """
        varxy = trackerutils.get_xy(json_ele)
        if varxy is not None:
            min_line, min_dist, projected_pt = self.get_snap_pt(
                varxy, map_info)
            if min_line is not None and min_dist <= constants.MAX_DIST_SNAP_MAP:
                json_ele["object"]["centroid"]["x"] = projected_pt[0]
                json_ele["object"]["centroid"]["y"] = projected_pt[1]
                # -- logging.debug("\tJson ele: {}: Snapping pt  {} to {}"
                # .format(get_vehicle_string(json_ele), xy, projected_pt))

    def match_moving_points_to_map(self, json_list, map_info):
        """This method maps all points in the json_list to the nearest points
        on the map_info

        Arguments:
            json_list {[list]} -- The list of json schema dictionaries of
            vehicle detection
            map_info {[type]} -- road-network graph as a list of line-strings
        """
        map_info = np.array(map_info)
        if map_info is not None:
            for json_ele in json_list:
                self.match_point_to_map(json_ele, map_info)

    # Clustering functions
    def merge_cluster_id_sets(self, merge_obj_id_list):
        """
        Method to merge all ids for a single cluster given that two or
        more ids (in merge_obj_id_list)

        Arguments:
            merge_obj_id_list {list} -- The set of objects for which the ids
                need to be merged
        """

        min_id = None
        uniq_obj_ids = set()
        max_ts = None
        logging.info("ID list: Merging for objects %s", str(merge_obj_id_list))
        for obj_id in merge_obj_id_list:
            this_dict = self.state.clustered_oid_map[obj_id]
            logging.info("ID list:\tOld: %s: %s", obj_id, str(this_dict))
            if min_id is None or this_dict["id"] < min_id:
                min_id = this_dict["id"]
                uniq_obj_ids.update(this_dict["id_set"])
            if max_ts is None or this_dict["update_ts"] > max_ts:
                max_ts = this_dict["update_ts"]

        new_dict = {
            "update_ts": max_ts,
            "id_set": uniq_obj_ids,
            "id": min_id
        }
        for obj_id in uniq_obj_ids:
            self.state.clustered_oid_map[obj_id] = new_dict

    def prune_cluster_id_sets(self, timestamp):
        """Prune cluster object ids which were updated long back. The time
        threshold is given by self.config.cluster_obj_id_staytime_in_sec

        Arguments:
            timestamp {datetime} -- current timestamp
        """

        obj_keys = list(self.state.clustered_oid_map.keys())
        for obj_id in obj_keys:
            this_ts = self.state.clustered_oid_map[obj_id]["update_ts"]
            delta_time = (timestamp - this_ts).total_seconds()
            if delta_time > constants.CLUSTERED_OBJ_ID_PRUNETIME_SEC:
                del self.state.clustered_oid_map[obj_id]

    def maintain_matched_ids(self, clustered_json_list):
        """Maintain the object ids of the clustered objects
        
        self.state.clustered_oid_map = {"objectid": {"update_ts": timestamp, 
                                                     "id_set": {set of object ids},
                                                     "id": cluster_id=mctracker_id}}

        Arguments:
            clustered_json_list {list} -- list of detections in a single cluster
        """
        if clustered_json_list:

            # 1. Find a valid set across all elements
            first_obj_id = None
            same_id_list = None
            json_ele = None
            for json_ele in clustered_json_list:
                first_obj_id = trackerutils.get_obj_id(json_ele)
                same_id_list = self.state.clustered_oid_map.get(
                    first_obj_id, None)
                if same_id_list is not None:
                    break

            if same_id_list is None:
                # Create a new set with this obj id in it
                same_id_list = {
                    "update_ts": iso8601.parse_date(
                        json_ele.get("@timestamp")),
                    "id_set": set([first_obj_id]),
                    "id": self.state.curr_cl_obj_id
                }
                self.state.curr_cl_obj_id += 1  #increment mc tracker id
                self.state.clustered_oid_map[first_obj_id] = same_id_list

            set_id = same_id_list['id']
            curr_set = same_id_list['id_set']
            merge_sets = []
            for json_ele in clustered_json_list:
                obj_id = trackerutils.get_obj_id(json_ele)
                this_set_list = self.state.clustered_oid_map.get(
                    obj_id, None)
                this_ts = iso8601.parse_date(json_ele["@timestamp"])

                if this_set_list is None:

                    # This is a new object. Add it to the list
                    curr_set.add(obj_id)
                    self.state.clustered_oid_map[obj_id] = same_id_list
                else:

                    if ((this_set_list['id'] != set_id) or
                            (first_obj_id not in this_set_list['id_set'])):

                        # There is some problem. We see that two different
                        # ids have been issued for the same cluster elements
                        merge_sets.append(obj_id)

                if this_ts > same_id_list["update_ts"]:
                    same_id_list["update_ts"] = this_ts

            if merge_sets:
                self.merge_cluster_id_sets([first_obj_id] + merge_sets)


    def prune_nearby_points_in_list(self, timestamp, json_list, params):
        """
        This method clusters all points in the list (json_list). The method
        returns a list of points, where nearby points (in one cluster) have
        been collated into one representative point
        
        Collates objects of same trackerid in same timeframe into single object

        Arguments:
            timestamp {[type]} -- The timestep at which this clustering will
            be done
            json_list {[list]} -- The list of json schema dictionaries of
            vehicle detection
            params {dict} -- Dictionary parameters for clustering. It can have
            the following keys:
            a. "dist_thresh" key: indicates the distance threshold for
               clustering

            If "dist_thresh" key is not provided or if params is None, then
            "dist_thresh" for clustering defaults to CLUSTER_DIST_THRESH_IN_M
            in the config file or constants.DEF_CLUS_DIST_THRESH_M


        Returns:
            [list] -- list of points (in json schema dict) where points in each
            cluster has been collated into one representative point
        """
        json_list = self.collate_single_obj_attr(json_list)  #collate objects of same sensor/tracker id into single object
        retval = json_list
        dist_thresh = params.get("dist_thresh", self.config.cl_dist_thresh_m)
        if len(json_list) > 1:
            retval = []
            # cluster objects across cameras according to the overlapping cameras or non-matching cameras rules
            cluster_assocs = self.get_cluster(json_list, dist_thresh, params)  #cluster_assocs = [len(json_list)] with elements indicating cluster number
            clusters = set(cluster_assocs)  # unique cluster numbers

            final_cid = 0
            # Create hash for clusters
            for k in clusters:
                members = [i for i in range(
                    len(cluster_assocs)) if cluster_assocs[i] == k]  #get indices of members of cluster

                cluster_cameras = set()
                for mem in members:
                    cam_name = trackerutils.get_camera(json_list[mem])
                    cluster_cameras.add(cam_name)

                rec_list = [json_list[i] for i in members]  # get jsons for cluster members
                if self.state.assume_objs_have_same_id_intra_frame_period:  # TJH set by config.get("object_ids_track_across_frames", constants.ASSUME_OBJS_HAVE_SAME_ID_INTRA_FRAME_PERIOD)
                    self.maintain_matched_ids(rec_list)  #build/maintain a dict of clustered objects over timesteps
                # If points from more than one camera is in the same cluster, they are the same
                if len(cluster_cameras) > 1:  #merge jsons
                    self.mclogger.log_cluster_points(timestamp, rec_list,
                                                     "C_{}".format(final_cid))
                    self.smooth_x_y_in_list(rec_list, reason="Clustering points from multiple cameras")
                    #self.xfer_attrb_for_1valid_veh(rec_list)  #TJH No specific 'vehicle' processing needed
                    sel_rec = self.select_rep_member_from_list(rec_list)  # rec_list[len(rec_list) - 1]
                    
                    sel_rec["object"]["id_list"] = self.concatenate_member_ids(rec_list)  #used in match_points id_dist_matrix generation: returns/saves list of ids in this cluster that were merged into sel_rec
                    retval.append(sel_rec)
                    final_cid += 1
                else:
                    # Points are from single camera. Send them as individual records. 
                    retval += rec_list
                    for i in members:
                        rec_list = [json_list[i]]
                        self.mclogger.log_cluster_points(timestamp, rec_list,
                                                         "C_{}".format(
                                                             final_cid))
                        final_cid += 1

        elif len(json_list) == 1:
            self.mclogger.log_cluster_points(timestamp, json_list, "C_0")
        return retval


    def prune_nearby_points(self, timestamp, state_recs, params):
        """
        This method will cluster all nearby detected points. 

        Arguments:
            timestamp {[datetime]} -- The timestamp time-step at which the
            clustering is being done
            state_recs {[dict]} -- A list of json schema dictionary of
            detections. This dictionary will have detected (and adjusted detection) points under the
            state_recs['detections']
            params {dict} -- Dictionary parameters for clustering. It can have
            the following keys:
            a. "dist_thresh" key: indicates the distance threshold for
               clustering

        Keyword Arguments:
            match_id {int} -- The id of the timestep at which this clustering
            is done (default: {0})

        Returns:
            [dict] -- the state_recs dictionary itself. Note that duplicate points across overlapping cameras
             will be substituted with a single point for each cluster
        """
        # Merge points which detections or adjusted detections, ignore 'others' types
        detection_recs = state_recs['detection']
        state_recs['detection'] = self.prune_nearby_points_in_list(timestamp, detection_recs, params)
        return state_recs
    

    def smooth_x_y_in_list(self, rec_list, reason="No reason"):
        """
        This method replaces a vehicle detections in a list by a representative
        point (x,y). Currently, the logic is:
            computed (x,y) = (mean(x_i), mean(y_i)) for all (xi,yi) in rec_list

        All the records in the rec_list are updated with the new (x,y)
        computed. The original (xi, yi) are stored in
        rec['object']['centroid']['origPoints']
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
        pts = [(rec['object']['centroid']['x'], rec['object']['centroid']['y']) for rec in rec_list]
        # 1. Mean,
        #x_rep, x_rep = trackutils.get_median_xy(pts)
        # 2. Mean
        x_rep, y_rep = trackerutils.get_mean_xy(pts)
        # 3. Find the point that is nearest to the camera. In the 360-d usecase,
        # this is the point with highest camera_y centroid
        #pts = [(rec['object']['centroid']['x'],
        #        rec['object']['centroid']['y'],
        #        (rec['object']['bbox']['topleftx'] +
        #         rec['object']['bbox']['bottomrightx'])/2.0,
        #        max(rec['object']['bbox']['toplefty'],
        #            rec['object']['bbox']['bottomrighty']),
        #        ) for rec in rec_list]
        #x_rep, y_rep = trackerutils.get_max_camy_xy(pts)

        for rec in rec_list:
            if rec['object']['centroid'].get('origPoints', None) is None:
                rec['object']['centroid']['origPoints'] = []
            rec['object']['centroid']['origPoints'].append({
                "x": rec['object']['centroid']['x'],
                "y": rec['object']['centroid']['y'],
                "reason": reason
            })
            # rec['object']['centroid']['orig_y'] = rec['object']['centroid']['y']
            rec['object']['centroid']['x'] = x_rep
            rec['object']['centroid']['y'] = y_rep
            

    def select_rep_member_from_list(self, json_list):
        """
        This method selects one representative dict from the list
        of vehicle detections (in json_list)

        Returns:
            [dict] -- A representative json schema dictionary of
            selected representative vehicle detection
        """
        retval = None
        if json_list:
            retval = json_list[0]
            pref = 100
            min_obj_id = None
            for ele in json_list:
                # 1st pref = prioritise "detection" over "detection_adj"
                # 2nd pref = one with videopath
                if ele["event"]["type"] in ["detection"]:  #prioritise "detection" over "detection_adj"
                    retval = ele
                    pref = 1
                elif (pref > 1) and (ele["videoPath"] != ""):
                    retval = ele
                    pref = 2
                elif (pref > 2) and (min_obj_id is None or
                                    min_obj_id > ele["object"]["id"]):
                    retval = ele
                    min_obj_id = ele["object"]["id"]
                    pref = 3
        return retval


    def get_cluster(self, json_list, max_d, params):
        """
        This method clusters all vehicle detections in the json_list. Currently
        it:
        1. computes distance matrix between vehicle detections
        2. hierarchical aggregation
        3. Cuts the dendrogram at max_d

        Arguments:
            json_list {[list]} -- The list of json schema dictionaries of
            vehicle detection
            max_d {[double]} -- The cut-off distance. This variable is passed
            to fcluster with criterion="distance"
            params {[type]} -- Parameters required for clustering vehicle
            detections. Key parameters (none or one, not both will be non-empty) include:
            a. cam_overlap_adj_list: A adjacency list of which cameras
                have overlap with which other cameras. This is computed
                based on the "overlapping_camera_ids" key in the config dict
                passed to MulticamTracker. This key specifies the
                cameras which have overlapping coverages. If this
                dictionary has non-zero number of keys, then the tracker
                will only merge detections from the overlapping cameras.
                It will not merge between cameras that do not overlap;
                they always be kept separate
            b. dont_match_cameras_adj_list: This key specifies the cameras
                whose detections shoult NOT be merged together. For example,
                there two objects detected from two neighboring cameras that
                monitor entry and exit lanes are closeby in space. However,
                since this lane is divided, we would not want the detections
                from both cameras to be merged even though their detections
                are closeby.

        Returns:
            [list] -- Cluster number of each of points in json_list
        """
        dist_matrix = self.get_distance_matrix(json_list, json_list)
        rows = cols = dist_matrix.shape[0]
        match_type = params['match_type']
        for i in range(0, rows):
            for j in range(i + 1, cols):

                if self.state.assume_objs_have_same_id_intra_frame_period:  # TJH config.get("object_ids_track_across_frames", constants.ASSUME_OBJS_HAVE_SAME_ID_INTRA_FRAME_PERIOD)
                    # If two objects have been assigned same id in the past, then distance = 0
                    obj_1_id = trackerutils.get_obj_id(json_list[i])
                    obj_2_id = trackerutils.get_obj_id(json_list[j])
                    obj_1_set = (self.state.clustered_oid_map.get(obj_1_id, {}).get("id_set", set()))
                    obj_2_set = (self.state.clustered_oid_map.get(obj_2_id, {}).get("id_set", set()))
                    if obj_1_id in obj_2_set or obj_2_id in obj_1_set:
                        dist_matrix[i][j] = dist_matrix[j][i] = 0.0
                #cameras_overlap = False when no "overlap" entry covers this camera pair
                #dont_match_cameras = True when a "dont match" entry covers this camera pair 
                if trackerutils.get_camera(json_list[i]) == trackerutils.get_camera(json_list[j]):
                    # same camera, so set dist large to force no matching
                    dist_matrix[i][j] = dist_matrix[j][i] = max_d * \
                        constants.CLUSTER_DIFFT_CAMERAS_LARGE_SCALE_FACTOR
                elif json_list[i]["object"]["classid"] != json_list[j]["object"]["classid"]: #difft classes detected so can't be a match        
                    dist_matrix[i][j] = dist_matrix[j][i] = max_d * \
                        constants.CLUSTER_DIFFT_CAMERAS_LARGE_SCALE_FACTOR
                else:   #not same camera
                    if match_type == 0:  #   use overlapping cameras rule
                        if (self.cameras_overlap(json_list[i], json_list[j], params) == False):
                            # Overlapping cameras = False. Dist so set distance large  to force no matching
                            dist_matrix[i][j] = dist_matrix[j][i] = max_d * \
                                constants.CLUSTER_DIFFT_CAMERAS_LARGE_SCALE_FACTOR
                    elif match_type == 1:  # non-matching cameras rule
                        if self.dont_match_cameras(json_list[i], json_list[j], params):  #if true, overlapping cameras = {}
                            # dont match = true for these 2 cameras, so set dist large to force no matching
                            dist_matrix[i][j] = dist_matrix[j][i] = max_d * \
                                constants.CLUSTER_DIFFT_CAMERAS_LARGE_SCALE_FACTOR
                    else: #match_type == 2  No rules so set dist large to force no matching
                        dist_matrix[i][j] = dist_matrix[j][i] = max_d * \
                            constants.CLUSTER_DIFFT_CAMERAS_LARGE_SCALE_FACTOR                        

        dist_array = ssd.squareform(dist_matrix)
        z_val = linkage(dist_array, 'complete')
        clusters = fcluster(z_val, max_d, criterion='distance')
        return clusters


    def cluster_recs_from_same_cam(self, json_list):
        """
        Each camera may have multiple frames within a given resample period.
        In such cases, single object may be detected multiple times. If
        multiple detections of same object is present, it becomes harder to
        track. This method will keep a single detection of the object per
        resample period

        Objects detected in each frame should have the same timestamp and be non-duplicates
        hence no objects with identical timestamps should be considered duplicates.
        
        Assume Camera stream assigns objects within and across frames unique ids 
        unless it is tracking and detects the same object..
        
        TJH: changed second [i] to [j] in if stmt within i,j loop below...

        Returns:
            [list] -- List of json schema based dictionaries of
            non-replicated detections from each camera per resample
            period
        """

        if len(json_list) > 1:
            dist_matrix = self.get_distance_matrix(json_list, json_list)  #numpy [len(json), len(json)] values euclidean distance
            rows = cols = dist_matrix.shape[0]
            for i in range(0, rows):
                for j in range(i + 1, cols):
                    if json_list[i]["object"]["id"] == json_list[j]["object"]["id"]:  #TJH changed second [i]->[j] otherwise sets all dist_matrix elements to zero!
                        dist_matrix[i][j] = dist_matrix[j][i] = 0  # Same id assigned by camera detection stream (within same frame of same camera or across frames from same camera)
                    elif json_list[i]["@timestamp"] == json_list[j]["@timestamp"]:
                        # Assume these must be difft objects since camera stream only sends distinct objects for a single frame/timestamp
                        if not constants.MERGE_CLOSE_BBS_FROM_SAME_CAM: #MERGE_CLOSE_BBS_FROM_SAME_CAM was True, now False in constants so this always happens
                            # Dist is inf
                            dist_matrix[i][j] = dist_matrix[j][i] = (
                                constants.INTRA_FRAME_PERIOD_CLUST_DIST_IN_M *
                                constants.INTRA_FRAME_CLUSTER_LARGE_SCALE_FACTOR)
                    elif json_list[i]["object"]["classid"] != json_list[j]["object"]["classid"]:  #difft timestamp, difft classid should be difft objects
                            dist_matrix[i][j] = dist_matrix[j][i] = (
                                constants.INTRA_FRAME_PERIOD_CLUST_DIST_IN_M *
                                constants.INTRA_FRAME_CLUSTER_LARGE_SCALE_FACTOR)

            dist_array = ssd.squareform(dist_matrix)  #flattens distance matrix without reverse entries, seems to remove diagonal entries / 2 ie shape = (dist_matrix.size / 2) - (386/2)
            # z_val = The hierarchical clustering encoded as a linkage matrix.
            z_val = linkage(dist_array, 'complete') #Perform hierarchical/agglomerative clustering.
            #cluster_assocs = [len(json_list)] with each element= the cluster number ie assigns each json to a cluster
            cluster_assocs = fcluster(
                z_val, constants.INTRA_FRAME_PERIOD_CLUST_DIST_IN_M,
                criterion='distance')  #form flat clusters from the heirarchical clustering with threshold constants.INTRA_FRAME_PERIOD_CLUST_DIST_IN_M
            
            clusters = set(cluster_assocs)  #unique clusters ie set of cluster numbers
            retval = []
            for k in clusters:  #select a representative json from each cluster and add to retval
                members = [i for i in range(
                    len(cluster_assocs)) if cluster_assocs[i] == k]  #get members indices of cluster k
                member_recs = [json_list[i] for i in members]        #get json for each member
                if len(member_recs) > 1:
                    #diff_id_list = [rec["object"]["id"]
                    #                for rec in member_recs]
                    #diff_id_set = set(diff_id_list)  #unique ids
                    #if len(diff_id_set) > 1:
                    #    logging.warning("SameCamFrameCluster: Double detects?: %s", diff_id_list)
                    # Do not smooth xy yet. we need it for compiuting directions
                    # Do not transfer attributes. Just assign same object id
                    sel_rec = self.select_rep_member_from_list(member_recs)
                    for rec in member_recs:  #set all member_recs ids the same - but still returns all of them. Later only the latest one is retained
                        rec["object"]["id"] = sel_rec["object"]["id"]
                retval += member_recs
        else:
            retval = json_list
        return retval

    # Matching functions
    def normalize_dist(self, xy_list, dist_norm_parameters):
        """This method normalizes the distance between an array of
         (x,y) points based on the minimum x,y and range of x,y.
         Specifically,
         normalized x = (x - xmin) / xrange
         normalized y = (y - ymin) / yrange

        Arguments:
            xy_list {[list]} -- List of (x,y) points
            dist_norm_parameters {[dict]} -- Parameters for normalization. It
            contains the float values for the following keys:
            a. "minx": minimum of x
            b. "miny": minimum of y
            c. "xrange": range of x
            d. "yrange": range of y

        Note: values in constants.py:
                DEFAULT_DIST_NORM_XRANGE = 1
                DEFAULT_DIST_NORM_YRANGE = 1
                DEFAULT_DIST_NORM_MINX = 0
                DEFAULT_DIST_NORM_MINY = 0
                These values cause no normalisation to occur..

        Returns:
            [list] -- List of normalized (x,y) points
        """
        minx = (dist_norm_parameters
                .get("minx", constants.DEFAULT_DIST_NORM_MINX)
                if dist_norm_parameters is not None
                else constants.DEFAULT_DIST_NORM_MINX)
        miny = (dist_norm_parameters
                .get("miny", constants.DEFAULT_DIST_NORM_MINY)
                if dist_norm_parameters is not None
                else constants.DEFAULT_DIST_NORM_MINY)
        rangex = (dist_norm_parameters
                  .get("xrange", constants.DEFAULT_DIST_NORM_XRANGE)
                  if dist_norm_parameters is not None
                  else constants.DEFAULT_DIST_NORM_XRANGE)
        rangey = (dist_norm_parameters
                  .get("yrange", constants.DEFAULT_DIST_NORM_YRANGE)
                  if dist_norm_parameters is not None
                  else constants.DEFAULT_DIST_NORM_YRANGE)

        return [((xy[0] - minx) / float(rangex),
                 (xy[1] - miny) / float(rangey))
                for xy in xy_list]

    def merge_costs(self, dist_matrix, id_dist_matrix):
        """
        This method inputs two distance matrices:
        a. spatial distance matrix (dist_matrix): This is a matrix with
           distances between two detections (nxm matrix for n detections)
        b. ID distance matrix (id_dist_matrix): This is a nxm matrix of 0s
           and 1s. If two detections i and j have the same id (e.g., single
           camera tracker id for the same camera), then the ixj th cell is
           set to 1. Else it is set to 0

        The output is also an nxm matrix where the distance is the spatial
        distance only if the IDs are different. If IDs are the same, then
        the distance is set to zero

        Special case: If either dist_matrix or id_dist_matrix is None, then
        the method returns dist_matrix

        Arguments:
            dist_matrix {[np.array]} -- Spatial distance matrix
            id_dist_matrix {[np.array]} -- ID distance matrix

        Returns:
            [np.array] -- Merged distance matrix
        """

        if dist_matrix is not None and id_dist_matrix is not None:
            assert dist_matrix.shape == id_dist_matrix.shape
            cost_matrix = dist_matrix * id_dist_matrix  #TJH changed to this from loop below..
            #cost_matrix = dist_matrix.copy()
            #for i in range(dist_matrix.shape[0]):
            #    for j in range(dist_matrix.shape[1]):
            #        cost_matrix[i, j] = cost_matrix[i, j] * \
            #            id_dist_matrix[i, j]
                    # if(id_dist_matrix[i, j] == 0):
                    #    cost_matrix[i, j] = 0.0
        else:
            # -- logging.debug("Dist or id matrix is none: DM={}, IDM={}".format(
            # --     dist_matrix, id_dist_matrix))
            cost_matrix = dist_matrix
        return cost_matrix


    def get_distance_matrix(self, prev_json_list, json_list,
                            dist_norm_parameters=None):
        """
        This method computes the eucledian distance between the detections at
        timestep (t-1) (indicated by prev_json_list) [or could be t json_list repeated] and detections at
        timestep (t) (json_list). The x,y distances may be normalied by
        providing appropriate min and range values in dist_norm_parameters.

        Arguments:
            prev_json_list {[list]} -- List of json schema based dictionaries
            for detections at timestep (t-1).
            json_list {[list]} -- List of json schema based dictionaries
            for detections at timestep (t).

        Keyword Arguments:
            dist_norm_parameters {dict} -- Parameters for normalization. It
            contains the float values for the following keys:
            a. "minx": minimum of x
            b. "miny": minimum of y
            c. "xrange": range of x
            d. "yrange": range of y
            (default: None)
            
        Note: No calls to this fn in mctracker.py include dist_norm_parameters
              Hence the normalise_dist routine defaults to constants.py values which are set to cause no normalisation 

        Returns:
            [np.array] -- Spatial distance matrix
        """

        xy1 = [trackerutils.get_xy(json_ele) for json_ele in prev_json_list]   # [[centroid1], [centroid2],..[centroidn]]
        xy1 = self.normalize_dist(xy1, dist_norm_parameters)
        xy2 = [trackerutils.get_xy(json_ele) for json_ele in json_list]  # [[centroid1], [centroid2],..[centroidn]]
        xy2 = self.normalize_dist(xy2, dist_norm_parameters)
        dist_matrix = distance_matrix(xy1, xy2)  #computes euclidean dist, shape: [len(xy2), len(xy1)]
        return dist_matrix


    def get_obj_id_dist_matrix(self, prev_json_list, json_list):
        """
        This method computes the ID distance between the detections at
        timestep (t-1) (indicated by prev_json_list) and detections at
        timestep (t) (json_list). The x,y distances may be normalized by
        providing appropriate min and range values in dist_norm_parameters.

        The output is an nxm matrix where the distance is 1 if the IDs
        are different. If IDs are the same, then the distance is set to zero

        Note that an object might have multiple ids (e.g., two cameras
        might have detected same cars, and we might have clustered them).
        In such cases, even if one id in one list matches with any id in
        other list, we set the value to 0.

        Arguments:
            prev_json_list {[list]} -- List of json schema based dictionaries
            for detections at timestep (t-1).
            json_list {[list]} -- List of json schema based dictionaries
            for detections at timestep (t).

        Returns:
            [np.array] -- ID distance matrix
        """

        id1 = [self.get_id_list(json_ele) for json_ele in prev_json_list]
        id2 = [self.get_id_list(json_ele) for json_ele in json_list]

        dist_matrix = np.ones((len(id1), len(id2)), dtype=np.int)

        for i in range(len(id1)):
            iset = set(id1[i])
            for j in range(len(id2)):
                jset = set(id2[j])
                int_set = iset.intersection(jset)
                # If there is at-least one id common in both
                if int_set:
                    dist_matrix[i, j] = 0.0
        return dist_matrix

    def match_points(self, prev_json_list, json_list, prev_timestamp,
                     timestamp, params, match_id=0):
        """
        This is the main matching function that matches detections at
        timestep (t-1) (prev_json_list) with detections at timestep (t)
        (json_list).

        Arguments:
            prev_json_list {[list]} -- List of json schema based dictionaries
            for detections at timestep (t-1).
            json_list {[list]} -- List of json schema based dictionaries
            for detections at timestep (t).
            prev_timestamp {[datetime]} -- Timestamp  @ timestep (t-1)
            timestamp {[datetime]} -- Timestamp  @ timestep (t)
            params {dict} -- Parameters for matching

        Keyword Arguments:
            match_id {int} -- The id of the matching step (default: {0}), incremented on each process_batch loop


        Returns:
            [dict] -- Returns a dictionary with the following keys:
                "assignedListindices": An array of integers pointing to the
                    detections in json_list
                "assignedPrevListindices": An array of integers pointing to
                    the detections in prev_json_list
                "unassignedPrevListindices": The indices in prev_json_list
                    which are not matched to any point
                "unassignedListindices": The indices in json_list
                    which are not matched to any point
                "carryOver": List of indices from json_list which should be
                    carried over to the next timestep
                "stats": Statistics of matching
        """

        num_rows = len(prev_json_list)
        num_cols = len(json_list)
        unassigned_row_indices = set(range(num_rows))
        unassigned_col_indices = set(range(num_cols))
        carry_over_list = []
        match_stats = []
        assigned = []
        assigned_prev = []
        match_type = params['match_type']  #0=overlapping cameras, 1=non-overlapping cameras, 2=none specified
        # -- logging.debug("\tMatching={},{}".format(num_rows, num_cols))
        if num_rows > 0 and num_cols > 0:
            dist_matrix = self.get_distance_matrix(prev_json_list, json_list)
            id_dist_matrix = self.get_obj_id_dist_matrix(prev_json_list, json_list)  #set to 0 where object ids match, 1 otherwise
            cost_matrix = self.merge_costs(dist_matrix, id_dist_matrix)
            # Infeasible matchings (all distances more than 'x',
            # conflicting cameras) should be removed
            max_val = max(cost_matrix.max(), self.config.match_max_dist_m * 1.1)  # max_val larger than self.config.match_max_dist_m 
            for i in range(cost_matrix.shape[0]):
                for j in range(cost_matrix.shape[1]):
                    if cost_matrix[i][j] > self.config.match_max_dist_m:
                        cost_matrix[i][j] = max_val
                    elif trackerutils.get_classid_string(prev_json_list[i]) != trackerutils.get_classid_string(json_list[j]):
                        cost_matrix[i][j] = max_val
                    elif match_type == 0:  #overlapping cameras  TJH ADDED, used to just work on dont_match_cameras
                        if (self.cameras_overlap(prev_json_list[i], json_list[j], params) == False):                        
                            cost_matrix[i][j] = max_val
                    elif match_type == 1:  # dont match cameras                  
                        if self.dont_match_cameras(prev_json_list[i], json_list[j], params):
                            cost_matrix[i][j] = max_val
                    else:  #match_type = 2, no matching rules
                         cost_matrix[i][j] = max_val    
            final_dist_matrix = copy.deepcopy(cost_matrix)
            cost_matrix = np.square(cost_matrix)
            row_ind, col_ind = linear_sum_assignment(cost_matrix) 
            # linear_sum_assignment: cost_matrix[row_ind, col_ind] is the set of matches ie row_ind[n], col_ind[n] is a match

            # Take away all matchings which exceed certain distance
            i = 0
            while i < row_ind.shape[0]:
                if (final_dist_matrix[row_ind[i]][col_ind[i]] > self.config.match_max_dist_m):
                    row_ind = np.delete(row_ind, i)    #delete index
                    col_ind = np.delete(col_ind, i)
                else:
                    i += 1

            for i in range(len(row_ind)):
                #self.xfer_attrb_for_1valid_veh([prev_json_list[row_ind[i]], json_list[col_ind[i]]])
                json_list[col_ind[i]]['object']['trackerid'] = trackerutils.get_tracker_string(prev_json_list[row_ind[i]])
                json_list[col_ind[i]]['object']['id'] = prev_json_list[row_ind[i]]['object']['id']

                self.mclogger.log_match_points(
                    prev_timestamp, prev_json_list[row_ind[i]], timestamp,
                    json_list[col_ind[i]], match_id)
                direction = self.get_direction(
                    prev_json_list[row_ind[i]], json_list[col_ind[i]],
                    dist_thresh=0)
                self.update_direction(json_list[col_ind[i]], direction)  #update direction


            for i in range(len(row_ind)):
                object_t_1 = trackerutils.get_tracker_string(prev_json_list[row_ind[i]])
                object_t_0 = trackerutils.get_tracker_string(json_list[col_ind[i]])
                unassigned_row_indices.remove(row_ind[i])
                unassigned_col_indices.remove(col_ind[i])
                assigned.append(col_ind[i])
                assigned_prev.append(row_ind[i])
                error = 0 if (object_t_0 == object_t_1) else 1
                (varx1, vary1) = trackerutils.get_xy(prev_json_list[row_ind[i]])
                (varx2, vary2) = trackerutils.get_xy(json_list[col_ind[i]])
                match_stats.append(
                    {"matchId": match_id,
                     "index": i,
                     "isError": error,
                     "rowInd": row_ind[i], "colInd": col_ind[i],
                     "vehicle1": object_t_1,
                     "time1": prev_json_list[row_ind[i]]['@timestamp'],
                     "x1": varx1, "y1": vary1, "x2": varx2, "y2": vary2,
                     "cam1": trackerutils.get_camera(
                         prev_json_list[row_ind[i]]),
                     "cam2": trackerutils.get_camera(
                         json_list[col_ind[i]]),
                     "vehicle2": object_t_0,
                     "time2": json_list[col_ind[i]]['@timestamp']})

        else:
            if num_rows <= 0 and num_cols > 0:  # t json but no t-1 json
                pass
            elif num_cols <= 0 and num_rows > 0: # no t json but have t-1 json
                carry_over_list += prev_json_list.copy()

        for i in unassigned_row_indices:
            self.mclogger.log_match_points(
                prev_timestamp, prev_json_list[i], timestamp, None, match_id)
            carry_over_list.append(prev_json_list[i])

        for i in unassigned_col_indices:
            self.mclogger.log_match_points(prev_timestamp, None, timestamp,
                                           json_list[i], match_id)

        return {"assignedListindices": assigned,
                "assignedPrevListindices": assigned_prev,
                "unassignedPrevListindices": unassigned_row_indices,
                "unassignedListindices": unassigned_col_indices,
                "carryOver": carry_over_list,
                "stats": match_stats}

    # Util functions
    def get_id_list(self, json_ele):
        """Get the list of ids associated with a given detection

        Arguments:
            json_ele {[dict]} -- json schema dictionary

        Returns:
            [list] -- List of string IDs
        """
        return json_ele["object"].get("id_list", [json_ele["object"]["id"]])


    def xfer_attr_from_1vehicle(self, act_record, json_list):
        """
        Transfer vehicle attributes such as license plate, make, color
        from one record (act_record) to all the records in the list
        (json_list)


        Arguments:
            act_record {[dict]} -- Record from which the attribute need to be
            transfered
            json_list {[list]} -- List of records to which the records needs
            to be transferred to
        """
        for rec in json_list:
            if rec != act_record:
                rec['object']['vehicle'] = act_record['object']['vehicle'].copy()
                rec['object']['id'] = act_record['object']['id']

    def xfer_attrb_for_1valid_veh(self, json_list):
        """
        Transfer vehicle attributes such as license plate, make, color
        from one representative record to all the records in the list
        (json_list)

        Arguments:
            json_list {[type]} --  List of records where the attributes have
            to be the same
        """
        vindex = 0
        valid_vehicle = {}
        if json_list:
            for rec in json_list:
                key = trackerutils.get_vehicle_string(rec)
                if key != constants.UNK_VEH_KEY_STR_FORMAT:
                    if valid_vehicle.get(key, None) is None:
                        valid_vehicle[key] = [rec]
                    else:
                        valid_vehicle[key].append(rec)
                vindex += 1

            if len(valid_vehicle) > 1:
                pass
            else:
                if len(valid_vehicle) == 1:
                    act_record = None
                    for i in valid_vehicle:
                        act_record = valid_vehicle[i][0]
                else:
                    act_record = json_list[0]
                    # add_synthetic_attributes_if_necessary([act_record])

                self.xfer_attr_from_1vehicle(
                    act_record, json_list)


    def collate_single_obj_attr(self, json_list):
        """
        This method collates objects of same id
        and from the same sensor into a single object.
        
        It also assigns a direction/orientation to the single object

        Arguments:
            json_list {[list]} -- List of json schema based dictionaries
            for detections

        Returns:
            [list] -- List of json schema based dictionaries
            for detections with same objects having same attributes
        """
        # 1. First detect if two objects have been detected too close within the camera
        obj_in_cam_list = {}  #key = sensor id, value = [jsons]
        for rec in json_list:
            cam_id = rec['sensor']['id']
            key = cam_id
            curr_obj_rec = obj_in_cam_list.get(key, None)
            if curr_obj_rec is None:
                obj_in_cam_list[key] = [rec]  # This is the first
            else:
                obj_in_cam_list[key].append(rec)

        new_json_list = []
        for key in obj_in_cam_list:
            recs = obj_in_cam_list[key]
            # Cluster with very small threshold
            recs = self.cluster_recs_from_same_cam(recs)  # set object_id of duplicate objects to same - below only one json per camera/objectid will then be kept per time window
            new_json_list += recs
        json_list = new_json_list

        # 2. Now single-camera tracker and/or cluster_recs_from_same_cam() has tried to put a tracker for each object by assigning the same object id to same object. Use that to now only take a single instance of each object from across the period
        obj_in_cam_list = {}  #key: #sensor__ + sensor + id  value = [jsons]
        for rec in json_list:
            cam_id = rec['sensor']['id']
            #obj_id = ""
            #if self.state.assume_objs_have_same_id_intra_frame_period:  
            obj_id = rec['object']['id']   #sensor + id

            key = cam_id + "__" + obj_id  #sensor__ + sensor + id  or just sensor__
            curr_obj_rec = obj_in_cam_list.get(key, None)
            if curr_obj_rec is None:
                obj_in_cam_list[key] = [rec]  # This is the first
            else:
                obj_in_cam_list[key].append(rec)

        if constants.TAKE_ONE_FRAME_PER_PERIOD:  #was True in constants.py, now false to allow merging below vs just taking one record here
            new_json_list = []
            for key in obj_in_cam_list:
                recs = obj_in_cam_list[key]
                if (recs is not None) and recs:
                    new_json_list.append(recs[len(recs) - 1])  #takes only last json per camera + objectid
            json_list = new_json_list  #takes only last json per camera + object id

            # Now re-key the records after transferring the attributes
            obj_in_cam_list = {}
            for rec in json_list:
                cam_id = rec['sensor']['id']
                obj_id = rec['object']['id']
                key = cam_id + "__" + obj_id
                curr_obj_rec = obj_in_cam_list.get(key, None)
                if curr_obj_rec is None:
                    # This is the first
                    obj_in_cam_list[key] = [rec]
                else:
                    obj_in_cam_list[key].append(rec)
                    
        retval = []
        for key in obj_in_cam_list:
            rec_list = obj_in_cam_list[key]
            if rec_list:
                if len(rec_list) > 1:  # never true if constants.TAKE_ONE_FRAME_PER_PERIOD
                    # Get direction if more than one point
                    rec_list = sorted(rec_list, key=lambda k: k['@timestamp'])
                    rec = rec_list[0]
                    first_pt = (rec['object']['centroid']['x'],
                                rec['object']['centroid']['y'])
                    last_rec = rec_list[len(rec_list) - 1]
                    last_pt = (last_rec['object']['centroid']['x'], last_rec['object']['centroid']['y'])
                    self.smooth_x_y_in_list(rec_list, reason="Collating multiple points within frame time period")

                    # Compute direction only if dist beween two points is more than x
                    dist_in_m = spatial.get_euc_dist(first_pt, last_pt)

                    if dist_in_m > constants.MIN_THRESHOLD_DIST_IN_M_WITHIN_RESAMPLE_TIME:
                        orientation_rad = spatial.get_radangle_flat_earth(
                            first_pt, last_pt)
                        orientation = math.degrees(orientation_rad)
                        last_rec['object']['direction'] = orientation
                        last_rec['object']['orientation'] = orientation

                    #logging.debug(
                    #    "SAME TIME-CAM-OBJ: Transferring attributes: %d", len(rec_list))
                    #self.xfer_attrb_for_1valid_veh(rec_list)
                retval.append(rec_list[len(rec_list) - 1])  #add last instance of object
        return retval


    def cameras_overlap(self, ele1, ele2, params):
        """
        Returns if two detections are from overlapping cameras or not

        Arguments:
            ele1 {[dict]} -- first detection (in day2 schema)
            ele2 {[dict]} -- second detection (in day2 schema)
            params {[dict]} -- A dictionary that has the following keys
                a. cam_overlap_adj_list: A adjacency list of which cameras
                have overlap with which other cameras. This key specifies the
                cameras which have overlapping coverages. If this
                dictionary has non-zero number of keys, then the tracker
                will only merge detections from the overlapping cameras.
                It will not merge between the cameras that do not overlap;
                it will always be kept separate

        Returns:
            [boolean] -- True if two detections are from overlapping cameras.
            False otherwise
        """
        cam_overlap_adj_list = params['cam_overlap_adj_list']
        retval = False
        cam1 = trackerutils.get_camera(ele1)
        cam2 = trackerutils.get_camera(ele2)
        cam_list = cam_overlap_adj_list.get(cam1, None)
        cam_list2 = cam_overlap_adj_list.get(cam2, None)
        #if(cam_list is None) and (cam_list2 is None):
        #    return True   # TJH this "if" stmt was originally included, now commented out
        # We are assuming the graph is bidirectional and edge is listed in
        # adj list of either of the ends (not necessarily both)
        # TJH Changed "(not cam_list) to (cam_list)"
        if (cam_list is not None) and (cam2 in cam_list):
            retval = True
        else:
            if (cam_list2 is not None) and (cam1 in cam_list2):
                retval = True
        return retval


    def dont_match_cameras(self, ele1, ele2, params):
        """
        Returns if two detections are from "don't match" cameras or not

        Arguments:
            ele1 {[dict]} -- first detection (in day2 schema)
            ele2 {[dict]} -- first detection (in day2 schema)
            params {[dict]} -- A dictionary that has the following keys
                a. "dont_match_cameras_adj_list": This key specifies the cameras
                   whose detections should NOT be merged together. For example,
                   there two objects detected from two neighboring cameras that
                   monitor entry and exit lanes are closeby in space. However,
                   since this lane is divided, we would not want the detections
                   from both cameras to be merged even though their detections
                   are closeby.

        Returns:
            [boolean] -- True if two detections are from "don't match" cameras.
            False otherwise
        """
        cam_nomatch_adj_list = params.get('dont_match_cameras_adj_list', None)

        retval = False
        cam1 = trackerutils.get_camera(ele1)
        cam2 = trackerutils.get_camera(ele2)
        cam_list = cam_nomatch_adj_list.get(cam1, None)
        cam_list2 = cam_nomatch_adj_list.get(cam2, None)

        # We are assuming the graph is bidirectional and edge is listed in adj
        # list of either of the ends ie only need to list it one way.
        if (cam_list is not None) and (cam2 in cam_list):
            retval = True
        else:
            if (cam_list2 is not None) and (cam1 in cam_list2):
                retval = True
        return retval


    def get_snap_pt(self, point, map_info):
        """
        Given a point (argument "point") and road_network, return the nearest
        point to given "point" on road-network
        Arguments:
            point {[tuple]} -- (x,y) of the point
            map_info {[list]} --
                   The road-network graph is represented as a set of lines.
                   Each line has a set of points [set of (x, y)]. This map
                   will be used to snap detected points to the road network
                   if the option if SNAP_POINTS_TO_GRAPH is True

        Returns:
            [tuple] -- Returns a tuple of three elements
            (min_line, min_dist, min_projected_pt)
            where min_line = the line to which the point was nearest
            min_dist = minimum distance from point to the line
            min_projected_pt = Projected point on the line
        """
        point = Point(point[0], point[1])
        min_dist = np.inf
        min_line = None
        min_projected_pt = None
        for orig_line in map_info:
            line = LineString(orig_line)
            line_projection = line.project(point)
            projected_pt = line.interpolate(line_projection)
            dist = point.distance(line)
            # -- logging.debug("\t\tPt to line: {} to {}. Dist = {}; "
            # -- "projection={}; projected_pt={}".format(pt,
            # -- list(line.coords), d, line_projection, projected_pt))
            if dist < min_dist:
                min_dist = dist
                min_line = orig_line
                min_projected_pt = list(projected_pt.coords)[0]

        # -- logging.debug("\tMin line. {} to {}. Min Dist = {}; "
        # -- "Projected Pt = {}".format(pt, min_line, min_dist,
        # -- min_projected_pt))
        return min_line, min_dist, min_projected_pt

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


    def concatenate_member_ids(self, json_list):
        """
        Given all the objects in json_list, create a list of all ids
        for as a list and return the list of ids

        Arguments:
            json_list {[type]} -- List of day2 schema based dictionaries
            for detections

        Returns:
            [list] -- List of ids
        """
        id_list = []
        for ele in json_list:
            id_list.append(ele["object"]["id"])
        return id_list


    def get_direction(self, ele1, ele2, dist_thresh=0):
        """
        Get the direction between detections ele1 and ele2 detections.
        This is usually used if the same object moves between two locations
        and we need to find the direction of movement

        Arguments:
            ele1 {[dict]} -- first detection (in day2 schema)
            ele2 {[dict]} -- second detection (in day2 schema)

        Keyword Arguments:
            dist_thresh {int} -- Mininum distance to compute direction.
            If the distance is less than this distance then the
            current direction (As tagged in the ele1) is returned
            (default: {0})

        Returns:
            [type] -- The direction in degrees [0,360)
        """
        first_pt = (ele1['object']['centroid']['x'],
                    ele1['object']['centroid']['y'])
        last_pt = (ele2['object']['centroid']['x'],
                   ele2['object']['centroid']['y'])

        # Compute direction only if dist beween two points is more than x
        dist_in_m = spatial.get_euc_dist(first_pt, last_pt)
        orientation = None
        if dist_in_m > dist_thresh:
            orientation_rad = spatial.get_radangle_flat_earth(
                first_pt, last_pt)
            orientation = math.degrees(orientation_rad)
        else:
            orientation = ele1['object']['direction']
        return orientation


    def update_direction(self, ele, direction):
        """[summary]
        Update the direction of the detection in ele by the given direction
        Arguments:
            ele {[dict]} -- detection in day2 schema
            direction {[float]} -- Direction in degrees [0,360)
        """
        if direction is not None:
            ele['object']['direction'] = direction
            ele['object']['orientation'] = direction

 
    def add_synthetic_attr(self, json_list):
        """
        If the vehicle detected does not have any attributes
        (e.g., license plate, color), then add dummy attributes

        Arguments:
            json_list {[list]} -- List of detections in json schema
        """
        for json_ele in json_list:
            curr_obj = trackerutils.get_tracker_string(json_ele)
            if curr_obj == '':
                obj_id = ('UNK-' + trackerutils.get_classid_string(json_ele) + "-"
                          + str(self.state.curr_unknown_veh_id))                
                json_ele['object']['trackerid'] = obj_id
                self.state.curr_unknown_veh_id += 1
            else:
                obj_id = ('TRK-' + trackerutils.get_classid_string(json_ele) + "-"
                          + curr_obj)
                json_ele['object']['trackerid'] = obj_id


    def prune_carry_over_list(self, carry_over_list, timestamp):
        """
        Prune the carry over list. The cars that are not matched are carried over
        to the next timestep. However, if the car that was carried over is more
        than mcconfig.carry_over_time_in_sec seconds earlier, we prune them
        off the list

        Returns:
            [tuple] -- A tuple of (carry_over_list, removed_cars) where
                carry_over_list = The new carry over list
                removed_cars = The list of vehicles that were pruned
        """
        i = 0
        removed_cars = []
        while i < len(carry_over_list):
            ele = carry_over_list[i]
            ele_ts = iso8601.parse_date(ele["@timestamp"])
            delta_time = (timestamp - ele_ts).total_seconds()
            if delta_time > self.config.carry_time_sec:
                removed_cars.append(carry_over_list[i])
                del carry_over_list[i]
            else:
                i += 1
        return carry_over_list, removed_cars


    # Functions visible to outside
    def process_batch(self, all_json_list):
        """"
        This is the main method that will be called for multicam tracking. The
        detections are passed in json schema in a list (all_json_list). This
        method returns nothing. The tracked objects are stored in the variable
        state.retval

        Arguments:
            all_json_list {[list]} -- List of detections in json schema
        """

        # Removing the retval from previous iteration
        # retval=state_dict.get("retval",[])
        retval = []

        if not all_json_list:
            return

        self.init_transforms(all_json_list)
        match_id = self.state.match_id   #0

        # All the records are within one batch (say, within 0.5 seconds).
        # Choose one representative timestamp. Make sure its fast (no O(n), etc)
        timestamp = iso8601.parse_date(all_json_list[0]['@timestamp'])

        state_recs = self.get_objects_in_difft_states(all_json_list)

        params = {
            "cam_overlap_adj_list":
                self.state.overlapping_camera_ids,
            "dont_match_cameras_adj_list":
                self.state.dont_match_cameras_adj_list,
            "match_type": self.state.match_type,    
            "dist_thresh": self.config.cl_dist_thresh_m
        }

        # cluster points across cameras in current timestep:
        state_recs = self.prune_nearby_points(timestamp, state_recs, params)

        json_list = state_recs['detection']

        if constants.SNAP_POINTS_TO_GRAPH:
            self.match_moving_points_to_map(
                json_list, map_info=self.state.map_info)

        prev_json_list = self.state.prev_list  #contains t-1 json + t-1 carryover list
        prev_timestamp = self.state.prev_timestamp
        carry_over_list = self.state.carry_over_list  #not actually used as prev_json_list already contains carryovers

        # retval=[]
        if prev_json_list is not None:
            # match points between previous and current timestep:
            match_ret = self.match_points(
                prev_json_list, json_list, prev_timestamp, timestamp,
                params, match_id)

            carry_over_list = match_ret["carryOver"]
            # unassigned_prev = [prev_json_list[i]
            #                   for i in
            #                   match_ret["unassignedPrevListindices"]]
            unassigned_this = [json_list[i] for i in match_ret["unassignedListindices"]]

            tmp_match_stats = match_ret["stats"]

            if (unassigned_this is not None) and unassigned_this:
                # Something new appeared
                self.add_synthetic_attr(unassigned_this)

            retval += (json_list + state_recs['others'])

            match_stats = self.state.match_stats
            match_id += 1
            match_stats += tmp_match_stats
            self.state.match_id = match_id
        else:
            # This is the first record set. If there are unknown objects,
            # then add them syn ids
            self.add_synthetic_attr(json_list)

        carry_over_list, _ = self.prune_carry_over_list(carry_over_list, timestamp)

        # Prune the object ids that are mapped to same clusters (mc tracker ids)
        self.prune_cluster_id_sets(timestamp)

        self.state.retval = retval
        if self.state.verbose_log:
            logging.info("ProcessBatch: Retval=%d", len(self.state.retval))

        self.state.carry_over_list = carry_over_list

        prev_json_list = json_list + carry_over_list
        self.state.prev_list = prev_json_list
        prev_timestamp = timestamp
        self.state.prev_timestamp = prev_timestamp

        # Flush the files, if logging is enabled
        self.mclogger.flush_files()


    def remove_all_additional_fields(self, json_list):
        """
        The tracker adds additional fields to the detection dictionaries that
        are passed. This will violate the json schema. Use this method to
        remove all additional fields that might be added by the tracker

        Arguments:
            json_list {[list]} -- List of detections in json schema
        """

        added_fields_obj_coord = ["origPoints"]
        for json_ele in json_list:
            for field in added_fields_obj_coord:
                if json_ele.get('object', {}).get("id_list", None) is not None:
                    del json_ele['object']['id_list']
                if(json_ele.get('object', {}).get('centroid', {}).get(field, None) is not None):
                    del json_ele['object']['centroid'][field]
     

"""               
if __name__ == "__main__":
    testobj = mctrack_obj.mctracker_obj
    total_all_json_list = trackerutils.load_json_for_test()
    all_json_list = trackerutils.get_json_timewindow_for_test(total_all_json_list, start_idx=0 , window_time_in_secs=0.5)   
    all_json_list2 = trackerutils.get_json_timewindow_for_test(total_all_json_list, start_idx=386 , window_time_in_secs=0.5)    
    testobj.process_batch(all_json_list)
    testobj.process_batch(all_json_list2)
"""

