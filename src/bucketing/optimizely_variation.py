import gc
import sys
import os
import logging
import time

from optimizely import optimizely
from optimizely import event_dispatcher as optimizely_event_dispatcher
from optimizely.event import event_processor
from optimizely import logger
from pyspark.sql.types import StringType
from six.moves import queue

from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Row

from src.utils import constants
from src.utils import logger as lg
from src.real_profile import sql_queries

import pyspark.sql.functions as F 
from pyspark.sql import Window

log, _ = lg.getlogger()


def get_variation(df_rows, env, experience_type):
    """
    Get the variation of the user in the experiment
    :param df_rows: rows in a dataframe with user_id, experiment_name
    :param env: environment
    :return: variation group
    """
    try:
        # Update the optimizely sdk key by environment
        op_key = constants.ABTesting.OPTIMIZELY_DEV
        if env == 'prod':
            op_key = constants.ABTesting.OPTIMIZELY_PROD

        # Setup Optimizely client for parallel processing
        event_dispatcher = optimizely_event_dispatcher.EventDispatcher
        batch_processor = event_processor.BatchEventProcessor(
            event_dispatcher,
            batch_size=250,
            flush_interval=60,
            timeout_interval=120,
            start_on_init=True,
            event_queue=queue.Queue(maxsize=10000),
            logger=logger.SimpleLogger(min_level=logging.DEBUG)
        )

        op_client = optimizely.Optimizely(
            sdk_key=op_key,
            event_processor=batch_processor,
            logger=logger.SimpleLogger(min_level=logging.DEBUG)
        )

        log.info('Is Optimizely client valid: {}'.format(op_client.is_valid))

        if experience_type == 'web':
            for df_row in df_rows:
                enabled = op_client.is_feature_enabled(df_row.experiment_name, df_row.user_id)
                variation_group = op_client.get_feature_variable_string(df_row.experiment_name, 'Variation', df_row.user_id)
                yield Row(user_id=df_row.user_id, experiment_name=df_row.experiment_name, variation=variation_group)
        elif experience_type == 'native_app':
            for df_row in df_rows:
                enabled = op_client.is_feature_enabled(df_row.experiment_name, df_row.user_id)
                variation_group = op_client.get_feature_variable_string(df_row.experiment_name, 'Variation', df_row.user_id)
                yield Row(user_id=df_row.user_id, listing_state=df_row.listing_state, experiment_name=df_row.experiment_name, variation=variation_group)

        # ensures the queued events are flushed as soon as possible to avoid any data loss
        batch_processor.stop()
        time.sleep(10)
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        f_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        log.error("Filename = {} || Line Number = {} || Get Variation Error: {}".format(f_name, exc_tb.tb_lineno,
                                                                                       e.__str__()))


def bucketing_users_v1(env, spark, target_date, base_path, base_local_path):
    """
    Method to bucket v1 users from Optimizely
    :param env: Environment
    :param spark: Spark Session
    :param target_date: date on which the bucketing has to be done
    :param base_path: recommended homes base path
    :param base_local_path: recommended homes local base path
    :return: None
    """
    try:
        if env != 'local':
            eligible_users_output_path = "{}eligible_users/target_date={}".format(base_path, target_date)
            eligible_users_output_path = eligible_users_output_path.replace("$env", env)
            eligible_candidates_path = "{}eligible_candidates/target_date={}".format(base_path, target_date)
            eligible_candidates_path = eligible_candidates_path.replace("$env", env)

            # Location of eligible users historically for new listings
            nl_eligible_users_hist_path = "{}eligible_users".format(constants.JobConstants.NEW_LISTINGS_OUTPUT_PATH)
            nl_eligible_users_hist_path = nl_eligible_users_hist_path.replace("$env", env)

            # Location of eligible users historically for recommended homes
            rh_eligible_users_hist_path = "{}eligible_users".format(
                constants.JobConstants.RECOMMENDED_FILTERS_OUTPUT_PATH)
            rh_eligible_users_hist_path = rh_eligible_users_hist_path.replace("$env", env)
        else:
            eligible_users_output_path = "{}eligible_users/target_date={}".format(base_local_path, target_date)
            eligible_candidates_path = "{}eligible_candidates/target_date={}".format(base_local_path, target_date)

            nl_eligible_users_hist_path = "{}eligible_users".format(
                constants.JobConstants.NEW_LISTINGS_OUTPUT_LOCAL_PATH)
            rh_eligible_users_hist_path = "{}eligible_users".format(
                constants.JobConstants.RECOMMENDED_FILTERS_OUTPUT_PATH)

        experiment_name_abc = constants.ABTesting.EXPERIMENT_NAME_ABC
        experiment_name_aaa = constants.ABTesting.EXPERIMENT_NAME_AAA
        experience_type = "web"

        # Read A/B test eligible users historically
        nl_eligible_users_hist_df = spark.read.parquet(nl_eligible_users_hist_path).filter(
            col('experiment_name') == constants.ABTesting.EXPERIMENT_NAME_AB).select('user_id').distinct()

        # Read A/A/A test eligible users historically
        rh_eligible_users_hist_aaa_df = spark.read.parquet(rh_eligible_users_hist_path).filter(
            col('experiment_name') == constants.ABTesting.EXPERIMENT_NAME_AAA).select('user_id').distinct()

        # Read A/B/C test eligible users historically
        rh_eligible_users_hist_abc_df = spark.read.parquet(rh_eligible_users_hist_path).filter(
            col('experiment_name') == constants.ABTesting.EXPERIMENT_NAME_ABC).select('user_id').distinct()

        log.info("All users of  recommended homes users are eligible for A/B/C experiment!")

        # Remove A/B test users from the V1 pipeline eligible users
        eligible_users_df = spark.read.parquet(eligible_candidates_path)
        eligible_users_df = eligible_users_df.join(nl_eligible_users_hist_df, on='user_id', how='left_anti')

        log.info("Holding out the New Mexico State users for the AAA test")

        # Remove the filtering logic for New Mexico state once the A/B/C test is completed
        # Bucketing users who are in A/A/A test and not in A/B/C test
        nm_users_df = eligible_users_df.filter(col('state') == 'NM').select('user_id').distinct()
        nm_users_df = nm_users_df.join(rh_eligible_users_hist_abc_df, on='user_id', how='left_anti')
        nm_users_df = nm_users_df.withColumn("experiment_name", lit(experiment_name_aaa))
        nm_users_df = nm_users_df.rdd.mapPartitions(lambda rows: get_variation(rows, env, experience_type)).toDF()

        # Bucketing users who are in A/B/C test and not in A/A/A test
        eligible_users_df = eligible_users_df.filter(col('state') != 'NM').select('user_id').distinct()
        eligible_users_df = eligible_users_df.join(rh_eligible_users_hist_aaa_df, on='user_id', how='left_anti')
        eligible_users_df = eligible_users_df.withColumn("experiment_name", lit(experiment_name_abc))
        eligible_users_df = eligible_users_df.rdd.mapPartitions(lambda rows: get_variation(rows, env, experience_type)).toDF()

        eligible_users_df = eligible_users_df.select(['user_id', 'experiment_name', 'variation']).union(
            nm_users_df.select(['user_id', 'experiment_name', 'variation']))
        eligible_users_df = eligible_users_df.drop_duplicates(['user_id'])

        log.info("Writing eligible users of V1 at {}!".format(eligible_users_output_path))
        eligible_users_df.write.parquet(eligible_users_output_path)

        # Explicit Cleanup for OOM
        eligible_users_df = None
        gc.collect()
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        f_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        log.error("Filename = {} || Line Number = {} || V1 Bucketing Error: {}".format(f_name, exc_tb.tb_lineno,
                                                                                       e.__str__()))


def write_candidates_variation(env, spark, target_date, variation_group, base_path, base_local_path):
    """
    Method to write the bucketed candidates to S3
    :param env: Environment
    :param spark: Spark Session
    :param target_date: date on which the bucketing has to be done
    :param variation_group: A/B/C variation group
    :param base_path: recommended homes base path
    :param base_local_path: recommended homes local base path
    :return: None
    """
    try:
        if env != 'local':
            eligible_users_path = "{}eligible_users/target_date={}".format(base_path, target_date)
            eligible_users_path = eligible_users_path.replace("$env", env)
            eligible_candidates_path = "{}eligible_candidates/target_date={}".format(base_path, target_date)
            eligible_candidates_path = eligible_candidates_path.replace("$env", env)
            bucketed_candidates_output_path = "{}bucketed_candidates/target_date={}".format(base_path, target_date)
            bucketed_candidates_output_path = bucketed_candidates_output_path.replace("$env", env)
        else:
            eligible_users_path = "{}eligible_users/target_date={}".format(base_local_path, target_date)
            eligible_candidates_path = "{}eligible_candidates/target_date={}".format(base_local_path, target_date)
            bucketed_candidates_output_path = "{}bucketed_candidates/target_date={}".format(base_local_path, target_date)

        eligible_users_df = spark.read.parquet(eligible_users_path)
        eligible_candidates_df = spark.read.parquet(eligible_candidates_path)

        # send all variations for AAA experiment users irrespective of the variation group
        if variation_group == 'V1':
            eligible_users_df.createOrReplaceTempView("usersView")
            v1_bucketing_query = sql_queries.v1_bucketing_candidates
            v1_bucketing_query = v1_bucketing_query.replace("$experiment_abc", constants.ABTesting.EXPERIMENT_NAME_ABC)
            v1_bucketing_query = v1_bucketing_query.replace("$experiment_aaa", constants.ABTesting.EXPERIMENT_NAME_AAA)
            v1_bucketing_query = v1_bucketing_query.replace("$variation_group", constants.ABTesting.VARIANT1)
            eligible_users_df = spark.sql(v1_bucketing_query)

        else:
            eligible_users_df = eligible_users_df.filter(col('variation') == variation_group)

        log.info("Merging Eligible candidates with eligible users")

        eligible_candidates_df = eligible_candidates_df.join(eligible_users_df, on='user_id', how='inner')
        eligible_users_df = None

        log.info("Writing Optimizely bucketed users to s3 at: {}".format(bucketed_candidates_output_path))
        eligible_candidates_df.write.parquet(bucketed_candidates_output_path)
        eligible_candidates_df = None
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        f_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        log.error("Filename = {} || Line Number = {} || Writing Bucketed Users Error: {}".format(f_name, exc_tb.tb_lineno,
                                                                                       e.__str__()))


def bucketing_users_v2(env, spark, target_date, base_path, base_local_path, recommended_homes_base_path,
                       recommended_homes_base_local_path):
    """
    Method to invoke Optimizely client to get the variation of the users
    :return: ranked candidates data frame with variation group column
    """
    try:
        if env != 'local':
            eligible_users_output_path = "{}eligible_users/target_date={}".format(base_path, target_date)
            eligible_users_output_path = eligible_users_output_path.replace("$env", env)
            eligible_candidates_path = "{}eligible_candidates/target_date={}".format(base_path, target_date)
            eligible_candidates_path = eligible_candidates_path.replace("$env", env)

            # Location of eligible users historically for new listings
            nl_eligible_users_hist_path = "{}eligible_users".format(constants.JobConstants.NEW_LISTINGS_OUTPUT_PATH)
            nl_eligible_users_hist_path = nl_eligible_users_hist_path.replace("$env", env)

            # Location of eligible users historically for recommended homes
            rh_eligible_users_hist_path = "{}eligible_users".format(
                constants.JobConstants.RECOMMENDED_FILTERS_OUTPUT_PATH)
            rh_eligible_users_hist_path = rh_eligible_users_hist_path.replace("$env", env)

        else:
            eligible_users_output_path = "{}eligible_users/target_date={}".format(base_local_path, target_date)
            eligible_candidates_path = "{}eligible_candidates/target_date={}".format(base_local_path, target_date)

            nl_eligible_users_hist_path = "{}eligible_users".format(
                constants.JobConstants.NEW_LISTINGS_OUTPUT_LOCAL_PATH)
            rh_eligible_users_hist_path = "{}eligible_users".format(
                constants.JobConstants.RECOMMENDED_FILTERS_OUTPUT_PATH)

        experiment_name_exclusive = constants.ABTesting.EXPERIMENT_NAME_AB
        experience_type = 'web'

        log.info("Splitting users into common users and exclusive new listings users for A/B/C and A/B experiment!")

        # Alternate implementation for rdd map implementation for getting the variation of the user
        # variation_udf = udf(lambda user_id: get_variation(user_id, experiment_name_exclusive), StringType())
        # spark.udf.register("variation_udf", variation_udf)
        # log.info("Variation UDF created!")
        # nl_eligible_users_df = nl_eligible_users_df.withColumn("variation", variation_udf(col('user_id')))

        nl_eligible_users_df = spark.read.parquet(eligible_candidates_path)
        nl_eligible_users_df = nl_eligible_users_df.select('user_id').distinct()

        # Read A/B/C and A/A/A test eligible users historically
        rh_eligible_users_hist_df = spark.read.parquet(rh_eligible_users_hist_path).select(
            ['user_id', 'experiment_name', 'variation']).distinct()
        rh_eligible_users_aaa_df = rh_eligible_users_hist_df.filter(
            col('experiment_name') != constants.ABTesting.EXPERIMENT_NAME_ABC)
        rh_eligible_users_abc_df = rh_eligible_users_hist_df.filter(
            col('experiment_name') == constants.ABTesting.EXPERIMENT_NAME_ABC)

        # Read A/B test eligible users historically
        nl_eligible_users_hist_df = spark.read.parquet(nl_eligible_users_hist_path).select(
            ['user_id', 'experiment_name', 'variation']).distinct()
        nl_eligible_users_ab_df = nl_eligible_users_hist_df.filter(
            col('experiment_name') == constants.ABTesting.EXPERIMENT_NAME_AB)

        log.info("All common users with recommended homes are eligible for A/B/C experiment!")

        users_df = nl_eligible_users_df.join(rh_eligible_users_aaa_df, on='user_id', how='left_anti')
        users_df = users_df.join(nl_eligible_users_ab_df, on='user_id', how='left_anti')
        users_df = users_df.join(rh_eligible_users_abc_df, on='user_id', how='inner')

        log.info("Only Exclusive V2 users are eligible for A/B test!")

        nl_eligible_users_df = nl_eligible_users_df.join(rh_eligible_users_hist_df, on='user_id', how='left_anti')

        # explict cleanup for OOM
        rh_eligible_users_hist_df = None
        rh_eligible_users_aaa_df = None
        rh_eligible_users_abc_df = None

        # Bucketing A/B test users
        nl_eligible_users_df = nl_eligible_users_df.select('user_id').withColumn(
            "experiment_name", lit(experiment_name_exclusive))
        nl_eligible_users_df = nl_eligible_users_df.rdd.mapPartitions(lambda rows: get_variation(rows, env, experience_type)).toDF()

        # Union A/B/C and A/B test eligible users of V2 pipeline
        users_df = users_df.select(['user_id', 'experiment_name', 'variation']).union(
            nl_eligible_users_df.select(['user_id', 'experiment_name', 'variation']))
        nl_eligible_users_df = None

        log.info("Writing eligible users of V2 to {}!".format(eligible_users_output_path))
        users_df.write.parquet(eligible_users_output_path)
        
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        f_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        log.error("Filename = {} || Line Number = {} || V2 Bucketing Error: {}".format(f_name, exc_tb.tb_lineno,
                                                                                       e.__str__()))


def bucketing_users_v2_web_all(env, spark, target_date, base_path, base_local_path):

    """
    Method to invoke Optimizely client to get the variation of the users
    :return: ranked candidates data frame with variation group column
    """
    try:
        if env != 'local':
            eligible_users_output_path = "{}eligible_users/target_date={}".format(base_path, target_date)
            eligible_users_output_path = eligible_users_output_path.replace("$env", env)
            eligible_candidates_path = "{}eligible_candidates/target_date={}".format(base_path, target_date)
            eligible_candidates_path = eligible_candidates_path.replace("$env", env)

        else:
            eligible_users_output_path = "{}eligible_users/target_date={}".format(base_local_path, target_date)
            eligible_candidates_path = "{}eligible_candidates/target_date={}".format(base_local_path, target_date)

        experiment_name_exclusive = constants.ABTesting.EXPERIMENT_NAME_AB
        experience_type = 'web'

        log.info("A/B Testing has concluded and all the traffic is now ramped up to the Winner - Recommended Homes V2!")

        nl_eligible_users_df = spark.read.parquet(eligible_candidates_path)
        nl_eligible_users_df = nl_eligible_users_df.select('user_id').distinct()

        log.info("All users with recommended homes V2 are eligible for web push notifications!")

        # Bucketing A/B test users
        nl_eligible_users_df = nl_eligible_users_df.select('user_id').withColumn(
            "experiment_name", lit(experiment_name_exclusive))
        nl_eligible_users_df = nl_eligible_users_df.rdd.mapPartitions(lambda rows: get_variation(rows, env, experience_type)).toDF()

        log.info("Writing eligible users of V2 to {}!".format(eligible_users_output_path))
        nl_eligible_users_df.write.parquet(eligible_users_output_path)

    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        f_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        log.error("Filename = {} || Line Number = {} || V2 Bucketing Error: {}".format(f_name, exc_tb.tb_lineno,
                                                                                       e.__str__()))


def bucketing_users_app_domzip(env, spark, target_date):
    try:
        log.info("Getting DomZip Eligible Users!")
        if env == 'local':
            app_eligible_users_path = f"{constants.JobConstants.APP_ELIGIBLE_USERS_OUTPUT_LOCAL_PATH}/target_date={target_date}"
            app_bucketed_users_output_path = f"{constants.JobConstants.APP_BUCKETED_USERS_OUTPUT_LOCAL_PATH}/target_date={target_date}"
        else:
            app_eligible_users_path = f"{constants.JobConstants.APP_ELIGIBLE_USERS_OUTPUT_PATH}/target_date={target_date}"
            app_eligible_users_path = app_eligible_users_path.replace('$env', env)

            app_bucketed_users_output_path = f"{constants.JobConstants.APP_BUCKETED_USERS_OUTPUT_PATH}/target_date={target_date}"
            app_bucketed_users_output_path = app_bucketed_users_output_path.replace('$env', env)

        app_eligible_users_df = spark.read.parquet(app_eligible_users_path)
        app_experiment_name = constants.ABTesting.APP_EXPERIMENT_NAME_ABC
        experience_type = 'native_app'

        app_eligible_users_df = app_eligible_users_df.withColumn('experiment_name', lit(app_experiment_name))

        app_eligible_users_df = app_eligible_users_df.filter('user_id is not null')
        app_eligible_users_df = app_eligible_users_df.rdd.mapPartitions(lambda rows: get_variation(rows, env, experience_type)).toDF()

        app_eligible_users_df = app_eligible_users_df.drop_duplicates()

        log.info("Writing DomZip Buckted Users to S3!")
        app_eligible_users_df.write.parquet(app_bucketed_users_output_path)
        app_eligible_users_df = None
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        f_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        log.error("Filename = {} || Line Number = {} || DomZip Bucketing Error: {}".format(f_name, exc_tb.tb_lineno,
                                                                                       e.__str__()))

def bucketing_users_app_v1(env, spark, target_date):
    try:
        log.info("Generating Eligible V1 Variation for App!")

        if env == 'local':
            app_bucketed_users_path = f"{constants.JobConstants.APP_BUCKETED_USERS_OUTPUT_LOCAL_PATH}/target_date={target_date}"
            rn_rh_ranked_candidates_path = f"{constants.JobConstants.RECOMMENDED_FILTERS_OUTPUT_LOCAL_PATH}candidates_ranked"
            rn_rh_eligible_users_app_output_path = ""
        else:
            app_bucketed_users_path = f"{constants.JobConstants.APP_BUCKETED_USERS_OUTPUT_PATH}/target_date={target_date}"
            app_bucketed_users_path = app_bucketed_users_path.replace('$env', env)
            rn_rh_ranked_candidates_path = f"{constants.JobConstants.RECOMMENDED_FILTERS_OUTPUT_PATH}candidates_ranked/target_date={target_date}"
            rn_rh_ranked_candidates_path = rn_rh_ranked_candidates_path.replace('$env', env)
            rn_rh_eligible_users_app_output_path = f"{constants.JobConstants.RECOMMENDED_FILTERS_OUTPUT_PATH}eligible_candidates_app/target_date={target_date}"
            rn_rh_eligible_users_app_output_path = rn_rh_eligible_users_app_output_path.replace('$env', env)

        log.info("Getting DomZip Bucketed Users!")
        dom_zip_bucketed_users_df = spark.read.parquet(app_bucketed_users_path)
        
        log.info("Getting Recommended Homes Users for App!")
        # getting v1 users
        rh_eligible_users = spark.read.parquet(rn_rh_ranked_candidates_path)

        rh_eligible_users = rh_eligible_users.filter("user_id != '$CUSTOMER_ID_$'")
        rh_eligible_users = rh_eligible_users.filter("user_id != 'unknown'")
        rh_eligible_users = rh_eligible_users.filter("listing_id != 'unknown'")
        rh_eligible_users = rh_eligible_users.filter("user_id is not null")
        rh_eligible_users = rh_eligible_users.filter("listing_id is not null")
        rh_eligible_users = rh_eligible_users.drop_duplicates(['user_id', 'listing_id'])
       
        rh_eligible_users = rh_eligible_users.join(dom_zip_bucketed_users_df, on=['user_id', 'listing_state']).filter("variation == 'V1'")

        dom_zip_bucketed_users_df  = None

        log.info("Writing Recommended Homes Eligible Users for App!")
        rh_eligible_users.write.parquet(rn_rh_eligible_users_app_output_path)
    
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        f_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        log.error("Filename = {} || Line Number = {} || V1 App Bucketing Error: {}".format(f_name, exc_tb.tb_lineno,
                                                                                       e.__str__()))

def bucketing_users_app_v2(env, spark, target_date):
    try:
        log.info("Generating Eligible V2 Variation for App!")

        if env == 'local':
            app_bucketed_users_path = f"{constants.JobConstants.APP_BUCKETED_USERS_OUTPUT_LOCAL_PATH}/target_date={target_date}"
            rn_nl_ranked_candidates_path = f"{constants.JobConstants.NEW_LISTINGS_OUTPUT_LOCAL_PATH}candidates_ranked"
            rn_nl_eligible_users_app_output_path = ""
        else:
            app_bucketed_users_path = f"{constants.JobConstants.APP_BUCKETED_USERS_OUTPUT_PATH}/target_date={target_date}"
            app_bucketed_users_path = app_bucketed_users_path.replace('$env', env)
            rn_nl_ranked_candidates_path = f"{constants.JobConstants.NEW_LISTINGS_OUTPUT_PATH}candidates_ranked/target_date={target_date}"
            rn_nl_ranked_candidates_path = rn_nl_ranked_candidates_path.replace('$env', env)
            rn_nl_eligible_users_app_output_path = f"{constants.JobConstants.NEW_LISTINGS_OUTPUT_PATH}eligible_candidates_app/target_date={target_date}"
            rn_nl_eligible_users_app_output_path = rn_nl_eligible_users_app_output_path.replace('$env', env)


        log.info("Getting DomZip Bucketed Users!")
        dom_zip_bucketed_users_df = spark.read.parquet(app_bucketed_users_path)

        log.info("Getting New Listings Users for App!")
        # getting v2 users
        nl_eligible_users = spark.read.parquet(rn_nl_ranked_candidates_path)
        nl_eligible_users = nl_eligible_users.filter("user_id != '$CUSTOMER_ID_$'")
        nl_eligible_users = nl_eligible_users.filter("user_id != 'unknown'")
        nl_eligible_users = nl_eligible_users.filter("listing_id != 'unknown'")
        nl_eligible_users = nl_eligible_users.filter("user_id is not null")
        nl_eligible_users = nl_eligible_users.filter("listing_id is not null")
        nl_eligible_users = nl_eligible_users.drop_duplicates(['user_id', 'listing_id'])

        nl_eligible_users = nl_eligible_users.join(dom_zip_bucketed_users_df, on=['user_id', 'listing_state']).filter("variation == 'V2'")

        dom_zip_bucketed_users_df = None

        log.info(f"Writing New Listings Eligible Users for App to {rn_nl_eligible_users_app_output_path}!")
        nl_eligible_users.write.parquet(rn_nl_eligible_users_app_output_path)
    
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        f_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        log.error("Filename = {} || Line Number = {} || V2 App Bucketing Error: {}".format(f_name, exc_tb.tb_lineno,
                                                                                       e.__str__()))

def bucketing_users_email_V1(env, spark, target_date):
    try:
        log.info("Generating V1 users Eligible for Email!")
        
        if env == 'local':
            email_eligible_users_path = f"{constants.JobConstants.EMAIL_ELIGIBLE_USERS_OUTPUT_LOCAL_PATH}/target_date={target_date}"
            rn_rh_ranked_candidates_path = f"{constants.JobConstants.RECOMMENDED_FILTERS_OUTPUT_LOCAL_PATH}candidates_ranked"
            rn_rh_eligible_users_email_output_path = ""
        else:
            email_eligible_users_path = f"{constants.JobConstants.EMAIL_ELIGIBLE_USERS_OUTPUT_PATH}/target_date={target_date}"
            email_eligible_users_path = email_eligible_users_path.replace('$env', env)
            rn_rh_ranked_candidates_path = f"{constants.JobConstants.RECOMMENDED_FILTERS_OUTPUT_PATH}candidates_ranked/target_date={target_date}"
            rn_rh_ranked_candidates_path = rn_rh_ranked_candidates_path.replace('$env', env)
            rn_rh_eligible_users_email_output_path = f"{constants.JobConstants.RECOMMENDED_FILTERS_OUTPUT_PATH}eligible_candidates_email/target_date={target_date}"
            rn_rh_eligible_users_email_output_path = rn_rh_eligible_users_email_output_path.replace('$env', env)
        
        log.info("Getting Email Eligible Users!")
        email_eligible_users = spark.read.parquet(email_eligible_users_path)
        email_eligible_users = email_eligible_users.withColumnRenamed('customer_id', 'user_id')

        log.info("Getting Recommended Homes Users for Email!")
        # getting v1 users
        rh_eligible_users = spark.read.parquet(rn_rh_ranked_candidates_path)

        rh_eligible_users = rh_eligible_users.filter("user_id != '$CUSTOMER_ID_$'")
        rh_eligible_users = rh_eligible_users.filter("user_id != 'unknown'")
        rh_eligible_users = rh_eligible_users.filter("listing_id != 'unknown'")
        rh_eligible_users = rh_eligible_users.filter("user_id is not null")
        rh_eligible_users = rh_eligible_users.filter("listing_id is not null")
        rh_eligible_users = rh_eligible_users.drop_duplicates(['user_id', 'listing_id'])

        w = Window.partitionBy('user_id').orderBy('seq_id')
        rh_eligible_users = rh_eligible_users.withColumn('reccity', F.first('listing_city').over(w)) \
            .withColumn('recstate', F.first('listing_state').over(w))
        rh_eligible_users = rh_eligible_users.join(email_eligible_users, on=['user_id'])
        rh_eligible_users = rh_eligible_users.withColumn("variation", lit("V1"))
        rh_eligible_users = rh_eligible_users.withColumn("experiment_name", lit("recommended_homes_email-v1"))

        log.info("Writing Recommended Homes Eligible Users for Email!")
        rh_eligible_users.write.parquet(rn_rh_eligible_users_email_output_path)
        email_eligible_users = None
        rh_eligible_users = None
        gc.collect()

    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        f_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        log.error("Filename = {} || Line Number = {} || V1 Email bucketing Error: {}".format(f_name, exc_tb.tb_lineno,
                                                                                       e.__str__()))

def bucketing_users_email_V2(env, spark, target_date):
    try:
        log.info("Generating V2 users Eligible for Email!")

        if env == 'local':
            email_eligible_users_path = f"{constants.JobConstants.EMAIL_ELIGIBLE_USERS_OUTPUT_LOCAL_PATH}/target_date={target_date}"
            rn_nl_ranked_candidates_path = f"{constants.JobConstants.NEW_LISTINGS_OUTPUT_LOCAL_PATH}candidates_ranked"
            rn_nl_eligible_users_email_output_path = ""
        else:
            email_eligible_users_path = f"{constants.JobConstants.EMAIL_ELIGIBLE_USERS_OUTPUT_PATH}/target_date={target_date}"
            email_eligible_users_path = email_eligible_users_path.replace('$env', env)
            rn_nl_ranked_candidates_path = f"{constants.JobConstants.NEW_LISTINGS_OUTPUT_PATH}candidates_ranked/target_date={target_date}"
            rn_nl_ranked_candidates_path = rn_nl_ranked_candidates_path.replace('$env', env)
            rn_nl_eligible_users_email_output_path = f"{constants.JobConstants.NEW_LISTINGS_OUTPUT_PATH}eligible_candidates_email/target_date={target_date}"
            rn_nl_eligible_users_email_output_path = rn_nl_eligible_users_email_output_path.replace('$env', env)
        
        log.info("Getting Email Eligible Users!")
        email_eligible_users = spark.read.parquet(email_eligible_users_path)
        email_eligible_users = email_eligible_users.withColumnRenamed('customer_id', 'user_id')

        log.info("Getting New Listings Users for Email!")
        # getting v2 users
        nl_eligible_users = spark.read.parquet(rn_nl_ranked_candidates_path)
        nl_eligible_users = nl_eligible_users.filter("user_id != '$CUSTOMER_ID_$'")
        nl_eligible_users = nl_eligible_users.filter("user_id != 'unknown'")
        nl_eligible_users = nl_eligible_users.filter("listing_id != 'unknown'")
        nl_eligible_users = nl_eligible_users.filter("user_id is not null")
        nl_eligible_users = nl_eligible_users.filter("listing_id is not null")
        nl_eligible_users = nl_eligible_users.drop_duplicates(['user_id', 'listing_id'])

        w = Window.partitionBy('user_id').orderBy('seq_id')
        nl_eligible_users = nl_eligible_users.withColumn('reccity', F.first('listing_city').over(w)) \
            .withColumn('recstate', F.first('listing_state').over(w))
        nl_eligible_users = nl_eligible_users.join(email_eligible_users, on=['user_id'])
        nl_eligible_users = nl_eligible_users.withColumn("variation", lit("V2"))
        nl_eligible_users = nl_eligible_users.withColumn("experiment_name", lit("recommended_homes_email-v2"))

        log.info(f"Writing New Listings Eligible Users for Email!")
        nl_eligible_users.write.parquet(rn_nl_eligible_users_email_output_path)

        email_eligible_users = None
        nl_eligible_users = None
        gc.collect()
            
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        f_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        log.error("Filename = {} || Line Number = {} || V2 Email bucketing Error: {}".format(f_name, exc_tb.tb_lineno,
                                                                                       e.__str__()))