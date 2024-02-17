import os
import sys
import boto3
import json
from datetime import datetime, timedelta

from pyspark.context import SparkContext
from pyspark.sql.functions import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from src.recommended_homes import rh_filter_recommendations
from src.utils import offline_metrics_helper
from src.utils import constants

sf = boto3.client('stepfunctions', region_name='us-west-2')

# Set up Glue, spark context, logger, step functions client
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
log = glueContext.get_logger()

# Parse the input arguments for the glue job
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'task_token', 'ENV'])
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


def path_exists(spark, path):
        """
        Check if s3 path exists
        :param path: S3 Path
        :return: Boolean true if exists else false
        """
        sc = spark.sparkContext
        fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jvm.java.net.URI.create("s3://" + path.split("/")[2]),
                                                         sc._jsc.hadoopConfiguration(),)
        return fs.exists(sc._jvm.org.apache.hadoop.fs.Path(path))

def driver():
    """
    Driver method to post process the New Listings Recommendations using Real Profile
    :return: None
    """
    try:
        env = args['ENV']
        lag = 7
        target_date = datetime.strftime(datetime.now() - timedelta(days=8), '%Y%m%d')
        # As data is not available for all the previous dates on the launch date
        if env != 'local':
            candidates_ranked_path = "{}candidates_ranked/target_date={}".format(
                constants.JobConstants.NEW_LISTINGS_OUTPUT_PATH, target_date)
            candidates_ranked_path = candidates_ranked_path.replace("$env", env)
        else:
            target_date_input = constants.JobConstants.SAMPLE_EVENT_DATE
            candidates_ranked_path = "{}candidates_ranked/target_date={}".format(
                constants.JobConstants.NEW_LISTINGS_OUTPUT_LOCAL_PATH, target_date_input)

        if path_exists(spark, candidates_ranked_path):
            # Get New Listings Recommendations filtered with Real Profile
            candidates_ranked_df = spark.read.parquet(candidates_ranked_path)
            candidates_ranked_df = candidates_ranked_df.filter(candidates_ranked_df.user_id != '$CUSTOMER_ID_$')
            candidates_ranked_df = candidates_ranked_df.filter(candidates_ranked_df.user_id != 'unknown')
            candidates_ranked_df = candidates_ranked_df.filter(candidates_ranked_df.listing_id != 'unknown')
            candidates_ranked_df = candidates_ranked_df.drop_duplicates(['user_id', 'listing_id'])
            candidates_ranked_df = candidates_ranked_df.withColumnRenamed('seq_id', 'rank')
            candidates_ranked_df = candidates_ranked_df.select('user_id', 'listing_id', 'rank')

            # Get actual interactions, (ground truth)
            interactions_df = rh_filter_recommendations.get_interaction_data(target_date, lag)
            log.info("Completed reading predictions and ground truth")

            for top_rank in ['1', '3', '12']:
                mAP_mAR_metrics_df, ndcg_metrics_df = offline_metrics_helper.generate_metrics(candidates_ranked_df,
                                                                                            interactions_df,
                                                                                            top_rank,
                                                                                            '2')
                mAR_mARP_metrics_df = mAP_mAR_metrics_df.withColumn("row_id", monotonically_increasing_id())
                ndcg_metrics_df = ndcg_metrics_df.withColumn("row_id", monotonically_increasing_id()).select('mean_ndcg',
                                                                                                            'row_id')
                rank_df = mAR_mARP_metrics_df.join(ndcg_metrics_df, on="row_id").drop("row_id")

                metrics_path = "{}metrics/target_date={}/rank={}".format(
                    constants.JobConstants.NEW_LISTINGS_OUTPUT_PATH,
                    target_date, top_rank)
                metrics_path = metrics_path.replace("$env", env)

                print("Writing metrics to S3 Path {} for Top Rank {}!".format(metrics_path, top_rank))
                rank_df.write.parquet(metrics_path)

                # Explicit Cleanup for OOM
                mAP_mAR_metrics_df = None
                ndcg_metrics_df = None
                mAR_mARP_metrics_df = None
                rank_df = None

            log.info("New Listings Recommendations - V2 Offline Metrics Done!")
        else:
            log.info("New Listings Recommedations - Path {} does not exit!".format(candidates_ranked_path))
            
        # send success token to step function activity
        response = sf.send_task_success(taskToken=args['task_token'],
                                        output=json.dumps({'message': 'New Listings Recommendations - V2 Offline Metrics Completed!'}))
    except Exception as e:
        _ = sf.send_task_failure(taskToken=args['task_token'])
        exc_type, exc_obj, exc_tb = sys.exc_info()
        f_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        log.error(
            "Filename = {} || Line Number = {} ||New Listings Recommendations - V2 Offline Metrics ERROR: {}".format(
                f_name, exc_tb.tb_lineno, e.__str__()))


if __name__ == '__main__':
    driver()