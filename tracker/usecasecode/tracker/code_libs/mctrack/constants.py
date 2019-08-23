"""
Constants etc
"""

__version__ = '0.2'

# Global configuration variables
# ================================
# Constants for options
# ----------------------------------
SNAP_POINTS_TO_GRAPH = False
#OBJECT_IDS_TRACK_ACROSS_FRAMES = False   #set to True (in config file) if tracking on ie json stream has tracked objects ie same object id for same object across frames
TAKE_ONE_FRAME_PER_PERIOD = False  #if this is true: restricts json_list to last json per camera/object id AFTER cluster_recs_from_same_cam() WITHOUT merging objects. If False, objects with same id are merged instead.
ASSUME_OBJS_HAVE_SAME_ID_INTRA_FRAME_PERIOD = True  #If true, assume same objects have same id across time windows (vs just being assumed to be the same object within a time window via single camera clustering)
MERGE_CLOSE_BBS_FROM_SAME_CAM = False    #if False, in cluster_recs_from_same_cam() objects in same timestamp(frame) are considered non-duplicates and never merged. If True, they can be merged if their distance is < INTRA_FRAME_PERIOD_CLUST_DIST_IN_M
APPROX_TIME_PERIOD_TO_PRINT_INFO_IN_SEC = 10.0


# Sensitive thresholds (Be careful while tuning)
# ---------------------------------

# Clustering thresholds
DEF_CLUS_DIST_THRESH_M = 25.0  #default distance threshold for multi-camera clustering

# Matching thresholds
DEF_MATCH_MAX_DIST_IN_M = 20.0
MATCH_MAX_DIST_FOR_PULLED_CAR = 10.0

# How long to hold points for matching
DEF_CARRY_PRUNE_TIME_SEC = 2.5


# Not so sensitive features to tune
# ---------------------------------
CLUSTERED_OBJ_ID_PRUNETIME_SEC = 20

MIN_THRESHOLD_DIST_IN_M_WITHIN_RESAMPLE_TIME = 1  #min distance apart for 2 detections of same object to calc direction/orientation
#Same camera, single period clustering:
INTRA_FRAME_PERIOD_CLUST_DIST_IN_M = 1.5       # cluster_recs_from_same_cam(): min distance to consider objects for 1 camera and same classid but difft timestamps the same object
INTRA_FRAME_CLUSTER_LARGE_SCALE_FACTOR = 10    # factor to multiply INTRA_FRAME_PERIOD_CLUST_DIST_IN_M by to ensure 2 objects dont match for single-camera clustering
#multiple camera overlapping or don't match:
CLUSTER_DIFFT_CAMERAS_LARGE_SCALE_FACTOR = 10.0 #get_cluster(): 

CARRY_OVER_LIST_PRUNE_TIME_IN_SEC = 2.5      #secs to maintain an entry on the carryover list
# HOLD_FOR_PARKED_CAR_PRUNE_TIME_IN_SEC = 30.0
# HOLD_FOR_PULLED_CAR_PRUNE_TIME_IN_SEC = 30.0
HOLD_FOR_PARKED_CAR_PRUNE_TIME_IN_SEC = 0.0
HOLD_FOR_PULLED_CAR_PRUNE_TIME_IN_SEC = 0.0

# Distance normalization thresholds
DEFAULT_DIST_NORM_XRANGE = 1
DEFAULT_DIST_NORM_YRANGE = 1
DEFAULT_DIST_NORM_MINX = 0
DEFAULT_DIST_NORM_MINY = 0

# Snapping to map thresholds
MAX_DIST_SNAP_MAP = 20.0

# Camera and frame rate related variables
# ---------------------------------
# Frame periodicity
RESAMPLE_TIME_IN_SEC = 0.5
INPUT_QUEUE_WAIT_SEC = 0.5


# Other constants
# -----------------------------------
VEH_KEY_STR_FORMAT = "{}"
UNK_VEH_KEY_STR_FORMAT = VEH_KEY_STR_FORMAT.format('')
SYN_VEHICLE_STRUCT = {"make": "UNKNOWN",
                      "model": "UNKNOWN",
                      "color": "UNKNOWN",
                      "confidence": 0.0,
                      "license": "UNKNOWN",
                      "licenseState": "UNKNOWN",
                      "type": "UNKNOWN"}
