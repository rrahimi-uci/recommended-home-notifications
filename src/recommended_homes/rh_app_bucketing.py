import os
import sys
import gc
import argparse
import boto3
import json
from datetime import datetime, timedelta

from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from src.bucketing import eligiblilty_filtering
from src.bucketing import optimizely_variation
from src.utils import constants
from src.utils import logger

log, _ = logger.getlogger()
sf = boto3.client('stepfunctions', region_name='us-west-2')


def get_arguments(args):
    parser = argparse.ArgumentParser(description='args for bucketing')
    parser.add_argument("--task_token", help="sf token - required")
    parser.add_argument("--env", help="environment - required")
    args = parser.parse_args(args)
    log.info("Arguments:%s", args)
    return args


def driver(args):
    """
    Driver method to bucket Recommended Homes users for Native App
    :return: None
    """
    try:
        args = get_arguments(args)
        if args.env != 'local':
            target_date = datetime.strftime(datetime.now() - timedelta(days=1), '%Y%m%d')
        else:
            target_date = constants.JobConstants.SAMPLE_EVENT_DATE

        spark = SparkSession.builder.getOrCreate()

        # get experiment variation of eligible users
        optimizely_variation.bucketing_users_app_v1(args.env, spark, target_date)
        gc.collect()
        log.info("Candidates with Recommended Homes App variation uploaded to S3!")

        # send success token to step function activity
        response = sf.send_task_success(taskToken=args.task_token,
                                        output=json.dumps(
                                            {'message': 'Recommended Homes App Bucketing Completed!'}))

    except Exception as e:
        _ = sf.send_task_failure(taskToken=args.task_token)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        f_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        log.error("Filename = {} || Line Number = {} || Recommended Homes App Bucketing Driver ERROR: {}".format(
            f_name, exc_tb.tb_lineno, e.__str__()))
        raise


if __name__ == '__main__':
    driver(sys.argv[1:])