import json
import os
import sys
import logging
import boto3

client = boto3.client('glue')
glue = boto3.client(service_name='glue', region_name='us-west-2',
                    endpoint_url='https://glue.us-west-2.amazonaws.com')

logger = logging.getLogger('recommended_notifications')
logging.getLogger().addHandler(logging.StreamHandler(sys.stderr))
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger.setLevel(logging.INFO)

client_sf = boto3.client('stepfunctions')
activity = os.environ['ACTIVITY_ARN']


def lambda_handler(event, context):
    class CrawlerException(Exception):
        pass

    response = client.get_crawler_metrics(CrawlerNameList=['rdc-recommended-notifications'])

    logger.info(f"Response: {response}")
    logger.info(f"CrawlerName: {response['CrawlerMetricsList'][0]['CrawlerName']}")
    logger.info(f"TimeLeftSeconds: {response['CrawlerMetricsList'][0]['TimeLeftSeconds']}")
    logger.info(f"StillEstimating: {response['CrawlerMetricsList'][0]['StillEstimating']}")

    if response['CrawlerMetricsList'][0]['StillEstimating']:
        raise CrawlerException('Crawler In Progress!')
    elif response['CrawlerMetricsList'][0]['TimeLeftSeconds'] > 0:
        raise CrawlerException('Crawler In Progress!')
    else:
        # send activity success token
        task = client_sf.get_activity_task(activityArn=activity,
                                           workerName="rdc-recommended-notifications-crawler-activity")
        logger.info(f"Task: {task}")
        response = client_sf.send_task_success(taskToken=task['taskToken'], output=json.dumps(
            {'message': 'Recommended Notifications Crawler Completed'}))
