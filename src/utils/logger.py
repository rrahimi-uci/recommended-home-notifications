from __future__ import print_function

import sys, logging, os
from src.utils import constants


def get_logger(name, level=logging.INFO):
    logger = logging.getLogger(name)
    logger.setLevel(level)
    if logger.handlers:
        # or else, as I found out, we keep adding handlers and duplicate messages
        pass
    else:
        ch = logging.StreamHandler(sys.stderr)
        ch.setLevel(level)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        logger.addHandler(ch)
    return logger


def getlogger():
    pidInfo = "PID:{}:".format(os.getpid())
    log = get_logger(constants.DeploymentConstants.APPLICATION_NAME, logging.INFO)
    return log, pidInfo
