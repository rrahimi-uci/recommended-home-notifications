import os
import boto3
import logging
import sys

logger = logging.getLogger('match_score')
logging.getLogger().addHandler(logging.StreamHandler(sys.stderr))
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger.setLevel(logging.INFO)

client = boto3.client('stepfunctions')
activity = os.environ['ACTIVITY_ARN']


def lambda_handler(event, context):
    task = client.get_activity_task(activityArn=activity, workerName="recommended-notifications-crawler-activity")
    logger.info(f"Task: {task}")
    response = client.send_task_failure(taskToken=task['taskToken'])
