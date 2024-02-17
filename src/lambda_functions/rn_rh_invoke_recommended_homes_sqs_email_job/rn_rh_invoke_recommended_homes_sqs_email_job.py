import os
import sys
import logging
import boto3

logger = logging.getLogger('rdc_recommended_notifications')
logging.getLogger().addHandler(logging.StreamHandler(sys.stderr))
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger.setLevel(logging.INFO)

sf = boto3.client('stepfunctions')
glue = boto3.client('glue')
activity = os.environ['ACTIVITY_ARN']
env = os.environ['ENV']
library_path = os.environ['LIBRARY_PATH']


def lambda_handler(event, context):
    task = sf.get_activity_task(activityArn=activity, workerName="recommended-homes-sqs-push-email")
    logger.info(f"Task: {task}")
    response = glue.start_job_run(JobName='rn-recommended-homes-sqs-push-email',
                                    Arguments={
                                        '--task_token': task['taskToken'],
                                        '--ENV': env,
                                        '--extra-py-files': library_path
                                    })
