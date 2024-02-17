import boto3, botocore
from constants import *
import os, sys
import logging

logger = logging.getLogger('match_score')
logging.getLogger().addHandler(logging.StreamHandler(sys.stderr))
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger.setLevel(logging.INFO)
pidInfo = 'PID:{}:'.format(os.getpid())


class AWSS3Helper(object):
    SUFFIX = 'parquet'
    def __init__(self, env):
        self.session = None
        self.aws_constant = AwsConfig(env)
        self.client = boto3.client('s3', region_name=self.aws_constant.get_Region())

    def write_to_s3(self, bucket, key, data):
        self.client.put_object(Body=data, Bucket=bucket, Key=key)

    def get_list_of_files(self, bucket_name, prefix):
        key_list = []
        bucket_content_list = self.client.list_objects(Bucket=bucket_name, Prefix=prefix)
        if bucket_content_list.get('Contents',None):
            for obj in bucket_content_list['Contents']:
                if obj['Key'].endswith(self.SUFFIX):
                    key_list.append(obj['Key'])
        return key_list

    def copy_bucket(self, key, old_bucket, new_key):
        #print(' {} the key is {}'.format(key, old_bucket))
        copy_source = {
            'Bucket': old_bucket,
            'Key': key
        }
        '''self.client.meta.client.copy(copy_source, new_bucket, key)
        self.clien.meta.client.copy_object(
            ACL='public-read',
            Bucket=new_bucket,
            CopySource={'Bucket': old_bucket, 'Key': key},
            Key=key
        )'''
        self.client.copy_object(CopySource=copy_source, Bucket=old_bucket, Key=key.replace('output', new_key))

    def get_key_content(self, bucket, key):
        content = self.client.get_object(Bucket=bucket, Key=key)['Body']\
            .read().decode(encoding="utf-8",errors="ignore")
        #print(" the content is {}".format(content))
        for line in content.splitlines():
            yield line

