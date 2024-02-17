#!/bin/bash

set -e

echo "Setup Account Number Hash..."
declare -A ACCOUNT_NUMBERS

ACCOUNT_NUMBERS["moverdc-dev"]="425555124585"
ACCOUNT_NUMBERS["moverdc-qa"]="337683724535"
ACCOUNT_NUMBERS["moverdc-prod"]="747559966630"

echo "Getting AWS Account Number For Environment..."
ACCOUNT_NUMBER=${ACCOUNT_NUMBERS[moverdc-${ACCOUNT}]};

declare -A TAG_SET
TAG_SET["owner"]="dl-disc-personalization-eng@move.com"
TAG_SET["product"]="ir_platform"
TAG_SET["component"]="rdc-recommended-notifications"
TAG_SET["environment"]="${ACCOUNT}"
TAG_SET["classification"]="internal"

echo "Getting AWS Credentials..."
aws sts assume-role --role-arn=arn:aws:iam::${ACCOUNT_NUMBER}:role/User --role-session-name="userRole" --region=us-west-2 > ${WORKSPACE}/credentials.txt
export AWS_SECRET_ACCESS_KEY=`grep SecretAccessKey credentials.txt |cut -d"\"" -f4`
export AWS_SESSION_TOKEN=`grep SessionToken credentials.txt |cut -d"\"" -f4`
export AWS_ACCESS_KEY_ID=`grep AccessKeyId credentials.txt |cut -d"\"" -f4`
export AWS_SECURITY_TOKEN=$AWS_SESSION_TOKEN

cd ${WORKSPACE}
OUTPUTFILE=output.txt

if [ ${STACKTYPE} == "new-listings" ]; then
  TEMPLATE=file://${WORKSPACE}/deployment/cloudformation-template-nl.yml
elif [ ${STACKTYPE} == "new-listings-email" ]; then
  TEMPLATE=file://${WORKSPACE}/deployment/cloudformation-template-nl-email.yml
elif [ ${STACKTYPE} == "recommended-homes" ]; then
  TEMPLATE=file://${WORKSPACE}/deployment/cloudformation-template-rh.yml
elif [ ${STACKTYPE} == "recommended-homes-email" ]; then
  TEMPLATE=file://${WORKSPACE}/deployment/cloudformation-template-rh-email.yml
elif [ ${STACKTYPE} == "iam" ]; then
  TEMPLATE=file://${WORKSPACE}/deployment/cloudformation-template-iam.yml
elif [ ${STACKTYPE} == "glue" ]; then
  TEMPLATE=file://${WORKSPACE}/deployment/cloudformation-template-glue.yml
fi

PipelineStartDateTime="ONDEMAND_ACTIVATION_TIME"
STACKNAME="${PROJECT}-${STACKTYPE}"
if [ ${STACKTYPE} == "iam" ]; then
  STACKNAME="${PROJECT}-iam"
elif [ ${STACKTYPE} == "glue" ]; then
  STACKNAME="${PROJECT}-glue"
fi
echo "stack name: ${STACKNAME}"
echo "use template file: ${TEMPLATE}"
echo "Performing Cloudformation Action -> ${ACTION}"

zip_lambdas(){

cd ${WORKSPACE}
echo " The workspace is ${WORKSPACE}"

cd src
# make a directory for saving lambdas zip source codes
mkdir -p lambda_functions_zip;

# zipping lambdas source code
cat << EOF > rn_zip_lambda_codes.py
#!/usr/bin/python
# This module zips lambdas
from shutil import make_archive


lambda_functions_list = ['rn_check_crawler_status', 'rn_crawler_activity_failed',
                         'rn_nl_job_cleanup', 'rn_invoke_crawler',
                         'rn_nl_invoke_validation', 'rn_invoke_athena_util', 'rn_rh_invoke_recommended_homes_filtering_glue_job',
                         'rn_rh_invoke_recommended_homes_sqs_job', 'rn_rh_job_cleanup', 'rn_rh_invoke_validation',
                         'rn_nl_invoke_new_listings_candidate_generation_job', 'rn_nl_invoke_new_listings_candidate_ranking_job',
                         'rn_nl_invoke_new_listings_sqs_job', 'rn_nl_invoke_new_listings_candidate_ranking_status_check',
                         'rn_rh_invoke_recommended_homes_offline_metrics_job', 'rn_rh_invoke_recommended_homes_bucketing_job',
                         'rn_nl_invoke_new_listings_offline_metrics_job', 'rn_nl_invoke_new_listings_bucketing_job',
                         'rn_nl_invoke_new_listings_bucketing_app_job', 'rn_rh_invoke_recommended_homes_bucketing_app_job',
                         'rn_nl_invoke_new_listings_sqs_app_job', 'rn_rh_invoke_recommended_homes_sqs_app_job',
                         'rn_nl_invoke_new_listings_bucketing_email_job', 'rn_nl_invoke_new_listings_sqs_email_job',
                         'rn_rh_invoke_recommended_homes_bucketing_email_job', 'rn_rh_invoke_recommended_homes_sqs_email_job']

lambda_source_dir = 'lambda_functions/'
lambda_source_zip = 'lambda_functions_zip/'

def zip_lambdas():
    for lambdas in lambda_functions_list:
        make_archive(lambda_source_zip + lambdas, 'zip', lambda_source_dir + lambdas)

if __name__ == "__main__":
    zip_lambdas()

EOF

chmod 755 rn_zip_lambda_codes.py

./rn_zip_lambda_codes.py

cd ..
}

deployCode() {
    echo "Zipping Code According To TAG..."
    zip -r rdc-recommended-notifications.zip ./*

    echo "Ensuring S3 Bucket and Pushing S3..."
    aws s3 mb s3://rdc-recommended-notifications-${ACCOUNT} --region us-west-2 || true
    aws s3api put-bucket-tagging --bucket rdc-recommended-notifications-${ACCOUNT} --tagging "TagSet=[{Key=owner,Value=${TAG_SET[owner]}}, {Key=product,Value=${TAG_SET[product]}}, {Key=component,Value=${TAG_SET[component]}}, {Key=environment,Value=${TAG_SET[environment]}}, {Key=classification,Value=${TAG_SET[classification]}} ]"
    aws s3 cp rdc-recommended-notifications.zip s3://rdc-recommended-notifications-${ACCOUNT}/rdc-recommended-notifications.zip
    aws s3 cp src/ s3://rdc-recommended-notifications-${ACCOUNT}/src/ --recursive
    aws s3 mv src/lambda_functions_zip/ s3://rdc-recommended-notifications-${ACCOUNT}/src/lambda_functions/ --recursive
    aws s3 cp deployment/ s3://rdc-recommended-notifications-${ACCOUNT}/ --recursive --exclude "*" --include "step-functions-*"
    aws s3 cp deployment/ s3://rdc-recommended-notifications-${ACCOUNT}/ --recursive --exclude "*" --include "*.sh"
    aws s3 cp src/utils/user_groups.txt s3://rdc-recommended-notifications-${ACCOUNT}/
}

echo ${PROJECT}
TAGS_LIST="Key=owner,Value=${TAG_SET['owner']} Key=product,Value=${TAG_SET['product']} Key=component,Value=${TAG_SET['component']} Key=environment,Value=${TAG_SET['environment']}  Key=classification,Value=${TAG_SET['classification']}"

if [ ${ACTION} == "deploy-code" ]; then
  zip_lambdas
  deployCode
fi

if [ ${ACTION} == "create-stack" ]; then
  zip_lambdas
  deployCode
  if [ ${STACKTYPE} == "new-listings" ]; then
    if [ ${ACCOUNT} == "prod" ]; then
        PipelineStartDateTime=`date '+%Y-%m-%d' -d "+1 days"`T10:00:00
    fi
    aws cloudformation ${ACTION} --stack-name ${STACKNAME} --template-body ${TEMPLATE} --capabilities CAPABILITY_NAMED_IAM --parameters ParameterKey=Env,ParameterValue=${ACCOUNT} ParameterKey=StartDateTime,ParameterValue=${PipelineStartDateTime} --tags ${TAGS_LIST} --region us-west-2 2>&1
  elif [ ${STACKTYPE} == "new-listings-email" ]; then
    if [ ${ACCOUNT} == "prod" ]; then
        PipelineStartDateTime=`date '+%Y-%m-%d' -d "+1 days"`T10:00:00
    fi
    aws cloudformation ${ACTION} --stack-name ${STACKNAME} --template-body ${TEMPLATE} --capabilities CAPABILITY_NAMED_IAM --parameters ParameterKey=Env,ParameterValue=${ACCOUNT} ParameterKey=StartDateTime,ParameterValue=${PipelineStartDateTime} --tags ${TAGS_LIST} --region us-west-2 2>&1
  elif [ ${STACKTYPE} == "recommended-homes" ]; then
    if [ ${ACCOUNT} == "prod" ]; then
        PipelineStartDateTime=`date '+%Y-%m-%d' -d "+1 days"`T10:00:00
    fi
    aws cloudformation ${ACTION} --stack-name ${STACKNAME} --template-body ${TEMPLATE} --capabilities CAPABILITY_NAMED_IAM --parameters ParameterKey=Env,ParameterValue=${ACCOUNT} ParameterKey=StartDateTime,ParameterValue=${PipelineStartDateTime} --tags ${TAGS_LIST} --region us-west-2 2>&1
  elif [ ${STACKTYPE} == "recommended-homes-email" ]; then
    if [ ${ACCOUNT} == "prod" ]; then
        PipelineStartDateTime=`date '+%Y-%m-%d' -d "+1 days"`T10:00:00
    fi
    aws cloudformation ${ACTION} --stack-name ${STACKNAME} --template-body ${TEMPLATE} --capabilities CAPABILITY_NAMED_IAM --parameters ParameterKey=Env,ParameterValue=${ACCOUNT} ParameterKey=StartDateTime,ParameterValue=${PipelineStartDateTime} --tags ${TAGS_LIST} --region us-west-2 2>&1
  elif [ ${STACKTYPE} == "iam" ]; then
    aws cloudformation ${ACTION} --stack-name ${STACKNAME} --template-body ${TEMPLATE} --capabilities CAPABILITY_NAMED_IAM --parameters ParameterKey=Env,ParameterValue=${ACCOUNT} --tags ${TAGS_LIST} --region us-west-2 2>&1
  elif [ ${STACKTYPE} == "glue" ]; then
    aws cloudformation ${ACTION} --stack-name ${STACKNAME} --template-body ${TEMPLATE} --capabilities CAPABILITY_NAMED_IAM --parameters ParameterKey=Env,ParameterValue=${ACCOUNT} --tags ${TAGS_LIST} --region us-west-2 2>&1
  fi
fi
if [ ${ACTION} == "update-stack" ]; then
  aws cloudformation ${ACTION} --stack-name ${STACKNAME} --template-body ${TEMPLATE} --capabilities CAPABILITY_NAMED_IAM --parameters ParameterKey=Env,ParameterValue=${ACCOUNT} --tags ${TAGS_LIST} --region us-west-2 2>&1
fi
if [ ${ACTION} == "delete-stack" ]; then
  aws cloudformation ${ACTION} --stack-name ${STACKNAME} --region us-west-2 2>&1
fi
if [ ${ACTION} == "tag-resources" ]; then
  aws s3api put-bucket-tagging --bucket rdc-recommended-notifications-${ACCOUNT} --tagging "TagSet=[{Key=owner,Value=${TAG_SET[owner]}}, {Key=product,Value=${TAG_SET[product]}}, {Key=component,Value=${TAG_SET[component]}}, {Key=environment,Value=${TAG_SET[environment]}}, {Key=classification,Value=${TAG_SET[classification]}} ]" | tee ${WORKSPACE}/${OUTPUTFILE}
fi
if [ ${ACTION} == "add-iam-policy" ]; then
  #aws iam put-role-policy --role-name DataPipelineDefaultResourceRole --policy-name SageMakerAccess --policy-document file://${WORKSPACE}/deployment/sagemaker-role.json
  aws iam put-role-policy --role-name DataPipelineDefaultResourceRole --policy-name GlueUpgrade --policy-document file://${WORKSPACE}/deployment/glue-upgrade.json
  #aws iam attach-role-policy --role-name DataPipelineDefaultResourceRole --policy-arn arn:aws:iam::aws:policy/AWSStepFunctionsFullAccess
fi
if [ ${ACTION} == "update-assume-role-policy" ]; then
  aws iam update-assume-role-policy --role-name DataPipelineDefaultResourceRole --policy-document file://${WORKSPACE}/deployment/trust-relation.json
fi
if [ ${ACTION} == "update-dynamo-billing" ]; then
    aws dynamodb update-table --table-name ${DYNAMODBTABLE} --region us-west-2 --billing-mode PAY_PER_REQUEST
fi
if [ ${ACTION} == "put-bucket-policy" ]; then
    aws s3api put-bucket-policy --bucket rdc-recommended-notifications-${ACCOUNT} --policy file://${WORKSPACE}/deployment/bucket-policy.json --region=us-west-2
fi
