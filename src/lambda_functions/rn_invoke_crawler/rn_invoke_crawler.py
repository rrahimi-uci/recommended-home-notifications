import os
import sys
import logging
import boto3

logger = logging.getLogger('recommended_notifications')
logging.getLogger().addHandler(logging.StreamHandler(sys.stderr))
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger.setLevel(logging.INFO)

client = boto3.client('glue')
glue = boto3.client(service_name='glue', region_name='us-west-2',
                    endpoint_url='https://glue.us-west-2.amazonaws.com')
sf = boto3.client('stepfunctions')
activity = os.environ['ACTIVITY_ARN']


def lambda_handler(event, context):
    logger.info("Glue Crawler initiated")

    class CrawlerException(Exception):
        pass
    try:
        response = client.start_crawler(Name='rdc-recommended-notifications')
    except CrawlerRunningException as c:
        raise CrawlerException('Crawler In Progress!')
        logger.info("Crawler In Progress!")
    except Exception as e:
        # send activity failure token
        task = sf.get_activity_task(activityArn=activity, workerName='rdc-recommended-notifications')
        response = sf.send_task_failure(taskToken=task['taskToken'])
        logger.error('Error encountered while invoking crawler')
