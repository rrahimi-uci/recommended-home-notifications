import os
import sys
import logging
import boto3
from botocore.client import Config

from datetime import datetime, timedelta

logger = logging.getLogger('rdc_recommended_notifications')
logging.getLogger().addHandler(logging.StreamHandler(sys.stderr))
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger.setLevel(logging.INFO)

conf = Config(
    retries={
      'max_attempts': 10,
      'mode': 'standard'
   }
)

sf = boto3.client('stepfunctions',  config=conf)
sm = boto3.client('sagemaker',  config=conf)
activity = os.environ['ACTIVITY_ARN']
env = os.environ['ENV']
user_groups = os.environ['USER_GROUPS']


def lambda_handler(event, context):
    target_date = datetime.strftime(datetime.now() - timedelta(days=1), '%Y%m%d')
    for i in range(0, int(user_groups), 10):
        start_user_group = i
        end_user_group = start_user_group + min(10, int(user_groups) - start_user_group) - 1
        response = sm.create_transform_job(
            TransformJobName=f'new-listings-candidate-ranking-{start_user_group}-to-{end_user_group}-{target_date}',
            ModelName=f'rdc-match-score-batch-{env}',
            ModelClientConfig={
                'InvocationsTimeoutInSeconds': 3600,
                'InvocationsMaxRetries': 0
            },
            BatchStrategy='MultiRecord',
            MaxPayloadInMB=0,
            TransformInput={
                'DataSource': {
                    'S3DataSource': {
                        'S3DataType': 'S3Prefix',
                        'S3Uri': "s3://rdc-recommended-notifications-{}/user_groups.txt".format(env)
                    }
                },
                'ContentType': 'text/csv',
                'CompressionType': 'None',
                'SplitType': 'Line'
            },
            TransformOutput={
                'S3OutputPath': "s3://rdc-recommended-notifications-{}/batch-transform-logs/".format(env)
            },
            TransformResources={
                'InstanceType': 'ml.m4.2xlarge',
                'InstanceCount': 2
            },
            Environment={
                'ENV': env,
                'START_USER_GROUP': str(start_user_group),
                'END_USER_GROUP': str(end_user_group)
            },
            Tags=[
                {
                    'Key': 'owner',
                    'Value': 'dlrecommendationeng@move.com'
                },
                {
                    'Key': 'product',
                    'Value': 'ir_platform'
                },
                {
                    'Key': 'component',
                    'Value': 'rdc-recommended-notifications'
                },
                {
                    'Key': 'classification',
                    'Value': 'internal'
                },
                {
                    'Key': 'environment',
                    'Value': env
                },
            ],
        )
        logger.info(f"Batch Transformations Response: {response}")
