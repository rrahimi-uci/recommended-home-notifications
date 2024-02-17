import os
import sys
import logging
import boto3

logger = logging.getLogger('match_score')
logging.getLogger().addHandler(logging.StreamHandler(sys.stderr))
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger.setLevel(logging.INFO)

sf = boto3.client('stepfunctions')
emr_client = boto3.client("emr")
activity = os.environ['ACTIVITY_ARN']
env = os.environ['ENV']
library_path = os.environ['LIBRARY_PATH']
security_group_id = os.environ['SECURITY_GROUP_ID']
subnet_id = os.environ['SUBNET_ID']
ec2_key_pair = os.environ['EC2_KEY_PAIR']


def lambda_handler(event, context):
    task = sf.get_activity_task(activityArn=activity, workerName="recommended-homes-bucketing-app")
    logger.info(f"Task: {task}")
    cluster_id = emr_client.run_job_flow(
        Name='recommended-homes-bucketing-app-job',
        ServiceRole='DataPipelineDefaultRole',
        JobFlowRole='DataPipelineDefaultResourceRole',
        VisibleToAllUsers=True,
        LogUri=f's3://rdc-recommended-notifications-{env}/logs/',
        ReleaseLabel='emr-5.25.0',
        Instances={
            'InstanceGroups': [
                {
                    'Name': 'Master nodes',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': 'r5.2xlarge',
                    'InstanceCount': 1,
                },
                {
                    'Name': 'Slave nodes',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'CORE',
                    'InstanceType': 'r5.2xlarge',
                    'InstanceCount': 3,
                }
            ],
            'Ec2KeyName': f'{ec2_key_pair}',
            'KeepJobFlowAliveWhenNoSteps': False,
            'TerminationProtected': False,
            'Ec2SubnetId': f'{subnet_id}',
            'AdditionalMasterSecurityGroups': [
                f'{security_group_id}',
            ],
            'AdditionalSlaveSecurityGroups': [
                f'{security_group_id}',
            ]
        },
        Applications=[{
            'Name': 'Spark'
        }],
        Configurations=[{
            "Classification":"spark-env",
            "Properties":{},
            "Configurations":[{
                "Classification":"export",
                "Properties":{
                    "PYSPARK_PYTHON":"/usr/bin/python3",
                    "PYSPARK_DRIVER_PYTHON":"/usr/bin/python3"
                }
            }]
        }],
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
                'Value': f'{env}'
            },
        ],
        BootstrapActions=[{
            'Name': 'Install',
            'ScriptBootstrapAction': {
                'Path': f's3://rdc-recommended-notifications-{env}/emr_transient_bootstrap.sh',
                'Args': [
                    f'{env}',
                ]
            }
        }],
        Steps=[{
                'Name': 'Recommended Homes Bucketing App',
                'ActionOnFailure': 'TERMINATE_CLUSTER',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': [
                        "bash",
                        "-c",
                        f"""aws s3 cp s3://rdc-recommended-notifications-{env}/rn_rh_bucketing_app_driver.sh .;
                        chmod 755 rn_rh_bucketing_app_driver.sh;
                        ./rn_rh_bucketing_app_driver.sh {task['taskToken']} {env};
                        rm rn_rh_bucketing_app_driver.sh"""
                    ]
                }
            }
        ],
    )
    return "Started cluster {}".format(cluster_id)
