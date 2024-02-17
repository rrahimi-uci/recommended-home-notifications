import os
import sys
import logging
import boto3
from datetime import datetime, timedelta
import constants

logger = logging.getLogger('rdc_recommended_notifications')
logging.getLogger().addHandler(logging.StreamHandler(sys.stderr))
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger.setLevel(logging.INFO)

sm = boto3.client('sagemaker')
activity = os.environ['ACTIVITY_ARN']
env = os.environ['ENV']
user_groups = os.environ['USER_GROUPS']


def lambda_handler(event, context):
    """
    Lambda to get the status of the sagemaker batch transform jobs
    :param event: lambda event
    :param context: lambda context
    :return: json of the transform job status
    """
    target_date = datetime.strftime(datetime.now() - timedelta(days=1), '%Y%m%d')
    batch_job_responses = {}
    failed_status = constants.LambdaConstants.FAILED_STATUS_TYPE
    success_status = constants.LambdaConstants.SUCCESS_STATUS_TYPE

    for i in range(0, int(user_groups), 10):
        start_user_group = i
        end_user_group = start_user_group + min(10, int(user_groups) - start_user_group) - 1
        job_name = f'new-listings-candidate-ranking-{start_user_group}-to-{end_user_group}-{target_date}'
        job_response = sm.describe_transform_job(TransformJobName=job_name)['TransformJobStatus']

        batch_job_responses[job_name] = job_response
        logger.info(f"JOB: {job_name} STATUS: {job_response}")

    job_status_set = set(batch_job_responses.values())
    if job_status_set == success_status:
        return {"TransformStatus": "Completed"}
    elif len(job_status_set.intersection(failed_status)) > 0:
        return {"TransformStatus": "Failed"}
    else:
        return {"TransformStatus": "InProgress"}
