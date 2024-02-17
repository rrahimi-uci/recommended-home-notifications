import sys
import gc
import boto3
import json
from datetime import datetime, timedelta

from pyspark.context import SparkContext
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from src.real_profile import candidate_generation
from src.recommended_homes import rh_filter_recommendations
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
    Driver method to post process the Recommended Homes using Real Profile
    :return: None
    """
    try:
        if args['ENV'] != 'local':
            target_date_input = datetime.strftime(datetime.now() - timedelta(days=2), '%Y%m%d')
            target_date_output = datetime.strftime(datetime.now() - timedelta(days=1), '%Y%m%d')
            target_date_rh = datetime.strftime(datetime.now() - timedelta(days=1), '%Y%m%d')
        else:
            target_date_input = constants.JobConstants.SAMPLE_EVENT_DATE
            target_date_output = constants.JobConstants.SAMPLE_EVENT_DATE
            target_date_rh = constants.JobConstants.SAMPLE_EVENT_DATE

        target_status = constants.DataConstants.FOR_SALE_STATUS
        number_of_geos = constants.DataConstants.NUMBER_OF_GEOS_RH
        standard_deviation_factor = constants.DataConstants.STANDARD_DEVIATION_RH
        recommended_homes_base_path = constants.JobConstants.RECOMMENDED_FILTERS_OUTPUT_PATH
        recommended_homes_base_local_path = constants.JobConstants.RECOMMENDED_FILTERS_OUTPUT_LOCAL_PATH
        lag_90 = constants.DataConstants.LAG_90
        lag_180 = constants.DataConstants.LAG_180
        user_groups = constants.DataConstants.USER_GROUPS_RH
        new_listings_base_path = constants.JobConstants.NEW_LISTINGS_OUTPUT_PATH
        new_listings_base_local_path = constants.JobConstants.NEW_LISTINGS_OUTPUT_LOCAL_PATH
        notification_type = constants.ABTesting.VARIANT1

        # Get the median listing price at a zip code level in the last 180 days
        # This will be used to fill null values in real profile
        cg_median = candidate_generation.CandidateGeneration(args['ENV'], spark, target_date_input, target_date_output,
                                                             target_status, number_of_geos, standard_deviation_factor,
                                                             lag_180, user_groups, recommended_homes_base_path,
                                                             recommended_homes_base_local_path, None, None, None, None,
                                                             None, None, None)

        cg_median.generate_listings()
        cg_median.get_median_listing_price()
        listing_price_median_df = cg_median.listing_price_median_df
        cg_median = None

        # Invoke the candidate generation class to generate the listings and real profiles of the active users
        cg = candidate_generation.CandidateGeneration(args['ENV'], spark, target_date_input, target_date_output,
                                                      target_status, number_of_geos, standard_deviation_factor, lag_90,
                                                      user_groups, recommended_homes_base_path,
                                                      recommended_homes_base_local_path, None, None,
                                                      None, None, None, None, listing_price_median_df)

        # Get the listing attributes of all the active listings
        cg.get_rdc_biz_data()
        cg.generate_listings()
        gc.collect()
        cg.generate_ldp_metrics()
        gc.collect()
        cg.write_generated_listings()
        gc.collect()

        # Get the real profiles of all the users active in the last 30 days
        cg.get_real_profiles()
        cg = None
        gc.collect()

        # Get all the recommendations and the recommended listings and merge with the listing attributes
        rh_filter_recommendations.get_recommendations(target_date_output, target_date_rh, args['ENV'])

        # Invoke the candidate generation class to generate the listings and real profiles of the active users
        cg = candidate_generation.CandidateGeneration(args['ENV'], spark, target_date_input, target_date_output,
                                                      target_status, number_of_geos, standard_deviation_factor, lag_90,
                                                      user_groups, recommended_homes_base_path,
                                                      recommended_homes_base_local_path, None, None,
                                                      None, None, None, None, listing_price_median_df)
        
        cg.get_rdc_biz_data()

        # Filter the recommended listings using real profile filters
        cg.candidates_df = rh_filter_recommendations.filter_recommendations(target_date_output, args['ENV'])
        recommended_filters_df = cg.tag_viewed_saved_listings()
        cg = None
        
        rh_filter_recommendations.write_recommendations_to_s3(target_date_output, args['ENV'], recommended_filters_df)

        gc.collect()

        log.info("Recommended Homes Filtering Done!")

        # send success token to step function activity
        response = sf.send_task_success(taskToken=args['task_token'],
                                        output=json.dumps(
                                            {'message': 'Recommended Homes Filtering Completed!'}))

    except Exception as e:
        _ = sf.send_task_failure(taskToken=args['task_token'])
        log.error("Recommended Homes Filtering Driver ERROR: {}".format(e.__str__()))
        raise


if __name__ == '__main__':
    driver()
