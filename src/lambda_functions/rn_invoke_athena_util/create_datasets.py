import argparse
import time
from datetime import datetime, timedelta
import boto3, botocore
from botocore import exceptions
import queries as athena_queries
import constants as constants
import logger as logger

log, pidInfo = logger.getlogger()


def get_arguments(args):
    """
    Helper method to get system arguments
    :param args:
    :return: args namespace object
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", help='target date - required')
    args = parser.parse_args(args)
    return args


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
            log.error("%s - error message: %s ", pidInfo, e.__str__())

    if status == "FAILED" or status == "CANCELLED":
        log.error("%s - Query Execution Status: %s ", pidInfo, status)
        raise Exception('Query Execution Failed/Cancelled')


def cleanup(session, prefix):
    """
    Clean up the S3 files at the given prefix
    :param session: boto3 session
    :param prefix: s3 prefix to clean up
    :return: no return object
    """
    try:
        s3 = session.resource("s3")
        bucket = s3.Bucket(constants.DeploymentConstants.S3_DATAENG_BUCKET_NAME)
        bucket.objects.filter(Prefix=prefix).delete()
        log.info("%s - Deleted partitioned files", pidInfo)
    except botocore.exceptions.ClientError as e:
        log.error("%s Botocore Error: %s ", pidInfo, e.response['Error'])
    except Exception as e:
        log.error("%s Error: %s",pidInfo, e.__str__())


def failure_cleanup():
    # assuming data eng prod role from all the accounts rdc_dev, rdc_qa, rdc_prod
    session = role_arn_to_session(
        RoleArn=constants.DeploymentConstants.ROLE_ARN_PROD,
        RoleSessionName='DataGenerationSession')
    log.info("%s - Role Assumed: %s ", pidInfo, constants.DeploymentConstants.ROLE_ARN_PROD)

    target_date = datetime.strftime(datetime.now() - timedelta(days=2), '%Y%m%d')
    # clean up athena tables and s3 files
    cleanup_prefix = "dataproducts/notifications/member_email_map/target_date={}/".format(target_date)

    cleanup(session, constants.DeploymentConstants.ATHENA_TABLES_PREFIX)
    cleanup(session, cleanup_prefix)


def run_query(query):
    """
    Helper method to run each query using the boto3 client
    :param query: query to be executed
    :return: query response
    """
    session = role_arn_to_session(
        RoleArn=constants.DeploymentConstants.ROLE_ARN_PROD,
        RoleSessionName='DataGenerationSession')
    log.info("%s - Role Assumed: %s ", pidInfo, constants.DeploymentConstants.ROLE_ARN_PROD)

    client = session.client('athena', region_name='us-west-2')
    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': 'dataproducts'
        },
        ResultConfiguration={
            'OutputLocation': constants.DeploymentConstants.S3_DATAENG_BUCKET
        }
    )

    return response


def query_builder(target_date, drop_table_date, member_email_map_output_folder):
    """
    Helper method to build query strings
    :param target_date: target date for which training data is generated
    :param drop_table_date: drop the old tables data (7 days prior to the target date)
    :param member_email_map_output_folder:  path to store member id email mapping
    :return: list of queries
    """
    drop_query1 = athena_queries.drop_query1
    drop_query1 = drop_query1.replace('$drop_table_date', str(drop_table_date))

    # cleanup current day data if exists
    drop_query1_today = athena_queries.drop_query1
    drop_query1_today = drop_query1_today.replace('$drop_table_date', str(target_date))

    # query to generate listings data partitioned by state
    query1 = athena_queries.query1
    query1 = query1.replace('$target_date', str(target_date))
    query1 = query1.replace('$member_email_map_output_folder', str("'" + member_email_map_output_folder + "'"))

    queries = [drop_query1_today, drop_query1, query1]

    return queries


def execute_queries(target_date, drop_table_date, queries):
    """
    Execute all the queries in the required order (listings data followed by training data)
    :param target_date: target date for which training data is generated
    :param drop_table_date: drop the old tables data (7 days prior to the target date)
    :param queries: listings and visitor queries
    """
    # assuming data eng prod role from all the accounts rdc_dev, rdc_qa, rdc_prod
    session = role_arn_to_session(
        RoleArn=constants.DeploymentConstants.ROLE_ARN_PROD,
        RoleSessionName='DataGenerationSession')
    log.info("%s - Role Assumed: %s ", pidInfo, constants.DeploymentConstants.ROLE_ARN_PROD)

    client = session.client('athena', region_name='us-west-2')

    # clean up athena tables and s3 files
    cleanup_prefix = "dataproducts/notifications/member_email_map/target_date={}/".format(target_date)
    cleanup_prefix_previous = "dataproducts/notifications/member_email_map/target_date={}/".format(drop_table_date)

    cleanup(session, constants.DeploymentConstants.ATHENA_TABLES_PREFIX)
    cleanup(session, cleanup_prefix_previous)
    cleanup(session, cleanup_prefix)

    for query in queries:
        response = run_query(query)
        wait_for_query_to_complete(client, response['QueryExecutionId'])
        log.info("%s - Query Execution Completed for Query ID: %s ", pidInfo, response['QueryExecutionId'])


def driver():
    """
    Driver method
    """
    target_date = datetime.strftime(datetime.now() - timedelta(days=7), '%Y%m%d')
    drop_table_date = datetime.strftime((datetime.now() - timedelta(8)), '%Y%m%d')
    member_email_map_output_folder = "{}/target_date={}".format(constants.DeploymentConstants.S3_DATAENG_BUCKET, target_date)

    queries = query_builder(target_date, drop_table_date, member_email_map_output_folder)
    execute_queries(target_date, drop_table_date, queries)
    log.info("cool off time if any queries are not executed completely - sleeping ...")
    time.sleep(10)
    log.info("member id and email mapping completed!")
