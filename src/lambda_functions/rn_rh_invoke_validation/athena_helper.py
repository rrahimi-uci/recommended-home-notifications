import boto3, botocore
from constants import *
from logger import *
from botocore.exceptions import ClientError
import os, time, sys


log = get_logger('')
pidInfo = 'PID:{}:'.format(os.getpid())


class AWSAthenaHelper(object):
    def __init__(self, env):
        self.session = None
        self.aws_constant = AwsConfig(env)
        self.query_execution_id = None
        self.client = boto3.client('athena', region_name=self.aws_constant.get_Region())

    def role_arn_to_session(self, **args):
        """
        Usage :
            session = role_arn_to_session(
                RoleArn='arn:aws:iam::012345678901:role/example-role',
                RoleSessionName='ExampleSessionName')
            client = session.client('sqs')
        """
        local_client = boto3.client('sts')
        response = local_client.assume_role(**args)
        return boto3.Session(
            aws_access_key_id=response['Credentials']['AccessKeyId'],
            aws_secret_access_key=response['Credentials']['SecretAccessKey'],
            aws_session_token=response['Credentials']['SessionToken'])

    def cleanup(self):
        """
        Clean up the S3 files at the given prefix
        :param session: boto3 session
        :param target_date: s3 files which are in the same location
        :return: no return object
        """
        try:
            s3 = self.session.resource("s3")
            bucket = s3.Bucket("move-dataeng-dapexternal-prod")
            bucket.objects.filter(Prefix="dataproducts/seo_data_transformer_validation/tables/").delete()
            #log.info("%s - Deleted tables", pidInfo)
        except botocore.exceptions.ClientError as e:
            log.error("%s Botocore Error: %s ", pidInfo, e.response['Error'])
        except Exception as e:
            log.error("%s Error: %s", pidInfo, e)

    def wait_for_query_to_complete(self, QueryExecutionId):
        """
        Helper method to wait and complete the query execution to execute the queries sequentially
        :param client: boto3 client is passed to maintain the same session
        :param QueryExecutionId: Execution ID of the query for which the status is to be monitored
        :return: no return object
        """
        status = "QUEUED"  # assumed
        error_count = 0
        response = None
        while status in ["QUEUED", "RUNNING"]:  # can be QUEUED | RUNNING | SUCCEEDED | FAILED | CANCELLED
            try:
                response = self.client.get_query_execution(QueryExecutionId=QueryExecutionId)
                status = response["QueryExecution"]["Status"]["State"]
                if status not in ["QUEUED", "RUNNING"]:
                    break
                time.sleep(10)
                log.debug("%s - Query Execution Status: %s ", pidInfo, status)
            except botocore.exceptions.ClientError as ce:
                log.error(ce.message)
                error_count = error_count + 1
                if (error_count > 3):
                    status = "FAILED"
                    break
            except Exception as e:
                log.error("%s - error message: %s ", pidInfo, e.message)
        log.debug("response is => ")
        log.debug(response)
        if (status == "FAILED" or status == "CANCELLED"):
            log.error("%s - Query Execution Status: %s ", pidInfo, status)
            log.error(response)
            raise Exception('Query Execution Failed/Cancelled')

    def run(self, query_list, bucket_path, input_db):
        query_response_map = {}
        for i in query_list:
            log.info(" the query {} with bucket_path {}  and db {}\n".format(i, bucket_path, input_db))
            response = self.client.start_query_execution(
                QueryString=i,
                QueryExecutionContext={
                    'Database': input_db
                },
                ResultConfiguration={
                    'OutputLocation': bucket_path
                }
            )
            self.wait_for_query_to_complete(response['QueryExecutionId'])
            respon = self.client.get_query_results(QueryExecutionId=response['QueryExecutionId'], MaxResults=200)
            if 'NextToken' in respon:
                self.query_execution_id = response['QueryExecutionId']
            query_response_map[i] = respon
        return query_response_map

    def parse_query(self, responseMapRow):

        result = responseMapRow['ResultSet']['Rows']
        response_parse = []
        if result is None or len(result) == 1:
            return response_parse
        for dict_value in result[1:]:
            tmp = []
            for data in dict_value['Data']:
                tmp.append(list(data.values())[0])
            response_parse.append(tmp)
        # sort based on first element
        return response_parse

    def pagination_helper(self, next_token):
        response = self.client.\
                    get_query_results(QueryExecutionId= self.query_execution_id, NextToken=next_token)
        return response


class AWSAthenaHelperGenereic(AWSAthenaHelper):
    def __init__(self, env):
        super(AWSAthenaHelperGenereic, self).__init__(env)
        self.client = boto3.client('athena', region_name=self.aws_constant.get_Region())


class AWSAthenaHelperSession(AWSAthenaHelper):
    def __init__(self, role, env):
        super(AWSAthenaHelperSession, self).__init__(env)
        self.session = self.role_arn_to_session(
                    RoleArn=role,
                    RoleSessionName='DataGenerationSession')
        self.client = self.session.client('athena', region_name=self.aws_constant.get_Region())
        log.info(" client connected")
        self.cleanup()
