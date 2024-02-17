from __future__ import print_function  # Python 2/3 compatibility
import os, errno, time
from datetime import datetime, timedelta
import boto3, botocore
from botocore import exceptions
from src.utils import logger

log, pidInfo = logger.getlogger()


def get_training_days(target_date, training_days):
    """
    Helper method to get the training days from the target date
    :param target_date: target date
    :param training_days: number of training days
    :return: training start date
    """
    start_date = datetime.strptime(str(target_date), '%Y%m%d')
    training_start_date = int((start_date - timedelta(days=training_days)).strftime('%Y%m%d'))
    return training_start_date


def role_arn_to_session(**args):
    """
    Usage :
        session = role_arn_to_session(
            RoleArn='arn:aws:iam::012345678901:role/example-role',
            RoleSessionName='ExampleSessionName')
        client = session.client('sqs')
    """
    client = boto3.client('sts')
    response = client.assume_role(**args)
    return boto3.Session(
        aws_access_key_id=response['Credentials']['AccessKeyId'],
        aws_secret_access_key=response['Credentials']['SecretAccessKey'],
        aws_session_token=response['Credentials']['SessionToken'])


def wait_for_query_to_complete(client, QueryExecutionId):
    """
    Helper method to wait and complete the query execution to execute the queries sequentially
    :param client: boto3 client is passed to maintain the same session
    :param QueryExecutionId: Execution ID of the query for which the status is to be monitored
    :return: no return object
    """
    status = "QUEUED"  # assumed
    error_count = 0
    while status in "QUEUED','RUNNING":  # can be QUEUED | RUNNING | SUCCEEDED | FAILED | CANCELLED
        try:
            response = client.get_query_execution(QueryExecutionId=QueryExecutionId)
            status = response["QueryExecution"]["Status"]["State"]
            time.sleep(10)
            log.info("%s - Query Execution Status: %s ", pidInfo, status)
        except botocore.exceptions.ClientError as ce:
            error_count = error_count + 1
            if (error_count > 3):
                status = "FAILED"
                break  # out of the loop
        except Exception as e:
            log.error("%s - error message: %s ", pidInfo, e.__traceback__)

    if status == "FAILED" or status == "CANCELLED":
        log.error("%s - Query Execution Status: %s ", pidInfo, status)
        raise Exception('Query Execution Failed/Cancelled')


def create_dir(dirPath):
    if os.path.exists(dirPath):
        return
    try:
        os.makedirs(dirPath)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise  # raises the error again


def get_current_day(withHyPhen=True):
    if (withHyPhen):
        return datetime.strftime(datetime.now(), '%Y-%m-%d')
    return datetime.strftime(datetime.now(), '%Y%m%d')


def get_push_dynamodb_output_timestamp(out_path, data_type):
    return os.path.join(out_path, "output",data_type, "push_dynamodb", "dt=" + datetime.now().strftime("%m%d%Y-%H:%M:%S"))


def path_exists(spark, path):
    sc = spark.sparkContext
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jvm.java.net.URI.create("s3://" + path.split("/")[2]),
                                                sc._jsc.hadoopConfiguration(), )
    return fs.exists(sc._jvm.org.apache.hadoop.fs.Path(path))
