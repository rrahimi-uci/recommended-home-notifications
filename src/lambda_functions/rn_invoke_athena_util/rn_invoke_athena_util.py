import os
import sys
import logging
import boto3
import json
import create_datasets as cd

logger = logging.getLogger('rdc-recommended-notifications')
logging.getLogger().addHandler(logging.StreamHandler(sys.stderr))
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger.setLevel(logging.INFO)

sf = boto3.client('stepfunctions')
activity = os.environ['ACTIVITY_ARN']
env = os.environ['ENV']


def lambda_handler(event, context):
    task = sf.get_activity_task(activityArn=activity, workerName="invoke-athena-query")
    logger.info(f"Task: {task}")
    try:
        cd.driver()
        response = sf.send_task_success(taskToken=task['taskToken'], output=json.dumps(
            {'message': 'Recommended Notifications Athena query execution initiated'}))
    except Exception as e:
        cd.failure_cleanup()
        response = sf.send_task_failure(taskToken=task['taskToken'])
        logger.error("Member Email Map ERROR: {}".format(e.__str__()))
