import sys
import gc
import argparse
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

        recommended_homes_base_path = constants.JobConstants.RECOMMENDED_FILTERS_OUTPUT_PATH
        recommended_homes_base_local_path = constants.JobConstants.RECOMMENDED_FILTERS_OUTPUT_LOCAL_PATH
        queue = constants.DataConstants.SQS_QUEUE
        queue = queue.replace("$env", args['ENV'])

        sqs = sqs_push.SQS(args['ENV'], queue, "recommended_homes_email", spark, target_date, recommended_homes_base_path,
                           recommended_homes_base_local_path)
        sqs.push_recommendations()
        response = sf.send_task_success(taskToken=args['task_token'], output=json.dumps(
            {'message': 'Recommended Homes SQS Push for Email Campaign completed'}))
        log.info("Pushed all the recommendations for Email Campaign to SQS!")

    except Exception as e:
        response = sf.send_task_failure(taskToken=args['task_token'])
        log.error("Recommended Homes SQS Push for Email Campaign ERROR: {}".format(e.__str__()))


if __name__ == '__main__':
    driver()
