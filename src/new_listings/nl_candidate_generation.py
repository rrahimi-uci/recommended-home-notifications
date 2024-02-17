import os
import gc
import sys
import boto3
import json
from datetime import datetime, timedelta

from pyspark import SparkContext
from pyspark.sql import SparkSession
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from src.real_profile import candidate_generation
from src.match_score import feature_engineering
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


def driver():
    """
    Driver method to generate new listings candidates for the active users in the last 30 days
    :return: None
    """
    try:
        if args['ENV'] != 'local':
            target_date_input = datetime.strftime(datetime.now() - timedelta(days=2), '%Y%m%d')
            target_date_output = datetime.strftime(datetime.now() - timedelta(days=1), '%Y%m%d')
        else:
            target_date_input = constants.JobConstants.SAMPLE_EVENT_DATE
            target_date_output = constants.JobConstants.SAMPLE_EVENT_DATE

        target_status = constants.DataConstants.FOR_SALE_STATUS
        number_of_geos = constants.DataConstants.NUMBER_OF_GEOS_NL
        standard_deviation_factor = constants.DataConstants.STANDARD_DEVIATION_NL
        output_base_path = constants.JobConstants.NEW_LISTINGS_OUTPUT_PATH
        output_base_local_path = constants.JobConstants.NEW_LISTINGS_OUTPUT_LOCAL_PATH
        lag = constants.DataConstants.LAG_5
        lag_180 = constants.DataConstants.LAG_180
        user_groups = constants.DataConstants.USER_GROUPS_NL

        cg_median = candidate_generation.CandidateGeneration(args['ENV'], spark, target_date_input, target_date_output,
                                                             target_status, number_of_geos, standard_deviation_factor,
                                                             lag_180, user_groups, output_base_path,
                                                             output_base_local_path, None, None, None, None, None, None,
                                                             None)

        cg_median.generate_listings()
        cg_median.get_median_listing_price()
        listing_price_median_df = cg_median.listing_price_median_df

        cg = candidate_generation.CandidateGeneration(args['ENV'], spark, target_date_input, target_date_output,
                                                      target_status, number_of_geos, standard_deviation_factor, lag,
                                                      user_groups, output_base_path, output_base_local_path, None, None,
                                                      None, None, None, None, listing_price_median_df)

        # Get the listing attributes of all the active listings
        cg.get_rdc_biz_data()
        cg.generate_listings()
        cg.generate_ldp_metrics()
        cg.write_generated_listings()

        # Get the real profiles of all the users active in the last 30 days
        cg.get_real_profiles()

        # Generate the candidates for each user
        cg.generate_candidates()
        cg.tag_viewed_saved_listings()
        cg.write_generated_candidates()
        log.info("Candidate Generation Completed!")

        cg_median = None
        cg = None
        gc.collect()
        # Generate feature vectors for the generated candidates
        fe = feature_engineering.FeatureEngineering(args['ENV'], spark, target_date_input, target_date_output,
                                                    output_base_path, output_base_local_path, None, None, None)
        fe.lookup_files()
        fe.load_udfs()
        fe.get_match_score_features()
        gc.collect()
        fe.get_candidate_features()
        gc.collect()
        fe.write_candidates_features()
        gc.collect()
        log.info("Feature Engineering Completed!")

        # send success token to step function activity
        response = sf.send_task_success(taskToken=args['task_token'],
                                        output=json.dumps({'message': 'New Listings Candidate Generation done!'}))
    except Exception as e:
        _ = sf.send_task_failure(taskToken=args['task_token'])
        exc_type, exc_obj, exc_tb = sys.exc_info()
        f_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        log.error("Filename = {} || Line Number = {} || New Listings Candidate Generation ERROR: {}".format(
            f_name, exc_tb.tb_lineno, e.__str__()))


if __name__ == '__main__':
    driver()
