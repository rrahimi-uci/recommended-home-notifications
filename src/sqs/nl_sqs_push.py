import os
import sys
import boto3
import json
from datetime import datetime, timedelta

from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from src.sqs import sqs_push
from src.utils import constants

sf = boto3.client('stepfunctions', region_name='us-west-2')

# Set up Glue, spark context, logger, step functions client
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
log = glueContext.get_logger()

# Parse the input arguments for the glue job
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'task_token', 'ENV'])
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


def driver():
    try:
        if args['ENV'] != 'local':
            target_date = datetime.strftime(datetime.now() - timedelta(days=1), '%Y%m%d')
        else:
            target_date = constants.JobConstants.SAMPLE_EVENT_DATE

        new_listings_base_path = constants.JobConstants.NEW_LISTINGS_OUTPUT_PATH
        new_listings_base_local_path = constants.JobConstants.NEW_LISTINGS_OUTPUT_LOCAL_PATH
        queue = constants.DataConstants.SQS_QUEUE
        queue = queue.replace("$env", args['ENV'])

        sqs = sqs_push.SQS(args['ENV'], queue, "new_listings", spark, target_date, new_listings_base_path,
                           new_listings_base_local_path)
        sqs.push_recommendations()
        response = sf.send_task_success(taskToken=args['task_token'], output=json.dumps(
            {'message': 'New Listings Recommended Notifications SQS Push completed'}))
        log.info("Pushed all the new listings recommendations to SQS!")

    except Exception as e:
        _ = sf.send_task_failure(taskToken=args['task_token'])
        exc_type, exc_obj, exc_tb = sys.exc_info()
        f_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        log.error("Filename = {} || Line Number = {} || New Listings SQS Push ERROR: {}".format(
            f_name, exc_tb.tb_lineno, e.__str__()))


if __name__ == '__main__':
    driver()
