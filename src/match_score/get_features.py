from __future__ import print_function  # Python 2/3 compatibility
import numpy as np
from src.utils import logger

log, _ = logger.getlogger()


def distance_feature(lat1, lon1, lat2, lon2):
    """
    Calculates Haversine distance between two GPS coordinates
    :param lat1: latitude of coordinate 1
    :param lon1: longitude of coordinate 1
    :param lat2: latitude of coordinate 2
    :param lon2: longitude of coordinate 2
    :return: haversine distance between two coordinates
    """
    try:
        R = 3958.76 # Earth radius in miles
        dLat, dLon, lat1, lat2 = np.radians(lat2 - lat1), np.radians(lon2 - lon1), np.radians(lat1), np.radians(lat2)
        a = np.sin(dLat/2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dLon/2) ** 2
        c = 2*np.arcsin(np.sqrt(a))
        return float(R * c)
    except Exception as e:
        # log.error("Distance ERROR: {} Params: {} {} {} {}".format(e.__str__(), lat1, lon1, lat2, lon2))
        return None


def deviation_ratio(user_cal_feature, feature):
    """
    Ratio of user feature deviation from the ldp feature
    :param user_cal_feature: user feature value
    :param feature: feature value
    :return: deviation ratio
    """
    try:
        return float(np.maximum(user_cal_feature/feature, 0.0002))
    except Exception as e:
        # log.error("Deviation Ratio ERROR: {} Params: {} {}".format(e.__str__(), user_cal_feature, feature))
        return None


def deviation_diff(user_cal_feature, feature):
    """
    Difference of user feature deviation from the ldp feature
    :param user_cal_feature: user feature value
    :param feature: feature value
    :return: deviation difference
    """
    try:
        return float(feature - user_cal_feature)
    except Exception as e:
        # log.error("Deviation Difference ERROR: {} Params: {} {}".format(e.__str__(), feature, user_cal_feature))
        return None
