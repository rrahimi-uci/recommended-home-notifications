from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.window import Window
from pyspark.sql.functions import *

from src.utils import constants
from src.utils import logger
from src.utils.schema import Schema
from src.utils import helper

logger, _ = logger.getlogger()


def get_recommendations(target_date_output, target_date_input, env):
    """
    Method to get recommendations on a target day
    :param target_date_output: target date of the output folder for notifications
    :param target_date_input: target date of the input folder to read the recommendations
    :param env: environment
    :return: recommendations data frame for each state
    """
    logger.info("Get active visitors at a day level!")

    spark_session = SparkSession.builder.getOrCreate()

    for state in constants.DataConstants.STATE_LIST:
        if env != 'local':
            recommendations_path = "{}/dt={}/dwell_time/{}".format(constants.JobConstants.RECOMMENDED_HOMES_PATH,
                                                                   target_date_input, state)
            recommendations_output_path = "{}recommendations/target_date={}/state={}".format(
                constants.JobConstants.RECOMMENDED_FILTERS_OUTPUT_PATH, target_date_output, state)
            recommendations_output_path = recommendations_output_path.replace("$env", env)

        else:
            if state != constants.DataConstants.STATE_LOCAL:
                return
            recommendations_path = "{}/dt={}/dwell_time/{}".format(
                constants.JobConstants.RECOMMENDED_HOMES_LOCAL_PATH, target_date_input, state)
            recommendations_output_path = "{}recommendations/target_date={}/state={}".format(
                constants.JobConstants.RECOMMENDED_FILTERS_OUTPUT_LOCAL_PATH, target_date_output, state)

        logger.info("Recommendations Path: {}".format(recommendations_path))

        if not helper.path_exists(spark_session, recommendations_path):
            logger.info("Recommended Homes Path not available: {}".format(recommendations_output_path))
            continue
        # window_row_number = Window.partitionBy('user_id').orderBy(monotonically_increasing_id())

        recommendations_df = spark_session.read.schema(Schema.recommended_homes_schema).json(recommendations_path) \
            .select('user_id', 'recommendations')

        recommendations_df = recommendations_df.select(recommendations_df.user_id,
                                                       posexplode_outer(recommendations_df.recommendations)
                                                       .alias('row_id', 'recommendations'))
        recommendations_df = recommendations_df.select(recommendations_df.user_id,
                                                       recommendations_df.row_id,
                                                       recommendations_df.recommendations.listing_id.alias(
                                                           'listing_id'))

        recommendations_df = recommendations_df.drop_duplicates(['user_id', 'listing_id'])
        # recommendations_df = recommendations_df.withColumn('row_id', row_number().over(window_row_number))

        logger.info("Recommended Homes Output Path: {}".format(recommendations_output_path))
        recommendations_df.write.parquet(recommendations_output_path)
        recommendations_df = None


def filter_recommendations(target_date, env):
    """
    Filter the recommendations using real profile of the user_id
    :param target_date: target date of the output folder
    :param env: environment
    :return: Spark dataframe
    """
    logger.info("Generating recommended filters!")

    spark_session = SparkSession.builder.getOrCreate()

    if env != 'local':
        recommendations_path = "{}recommendations/target_date={}".format(
            constants.JobConstants.RECOMMENDED_FILTERS_OUTPUT_PATH,
            target_date)
        recommendations_path = recommendations_path.replace("$env", env)
        recommended_listings_path = "{}listings/target_date={}".format(
            constants.JobConstants.RECOMMENDED_FILTERS_OUTPUT_PATH,
            target_date)
        recommended_listings_path = recommended_listings_path.replace("$env", env)
        real_profile_path = "{}users/target_date={}".format(constants.JobConstants.RECOMMENDED_FILTERS_OUTPUT_PATH,
                                                            target_date)
        real_profile_path = real_profile_path.replace("$env", env)
        recommended_filters_output_path = "{}candidates_ranked/target_date={}".format(
            constants.JobConstants.RECOMMENDED_FILTERS_OUTPUT_PATH,
            target_date)
        recommended_filters_output_path = recommended_filters_output_path.replace("$env", env)
    else:
        recommendations_path = "{}recommendations/target_date={}".format(
            constants.JobConstants.RECOMMENDED_FILTERS_OUTPUT_LOCAL_PATH,
            target_date)
        recommended_listings_path = "{}listings/target_date={}".format(
            constants.JobConstants.RECOMMENDED_FILTERS_OUTPUT_LOCAL_PATH,
            target_date)
        real_profile_path = "{}users/target_date={}".format(
            constants.JobConstants.RECOMMENDED_FILTERS_OUTPUT_LOCAL_PATH,
            target_date)

        recommended_filters_output_path = "{}candidates_ranked/target_date={}".format(
            constants.JobConstants.RECOMMENDED_FILTERS_OUTPUT_LOCAL_PATH,
            target_date)

    logger.info("Reading Real Profile Data: {}".format(real_profile_path))
    real_profile_df = spark_session.read.parquet(real_profile_path)

    logger.info("Reading Recommendations Data: {}".format(recommendations_path))
    recommendations_df = spark_session.read.parquet(recommendations_path)

    logger.info("Reading Recommended Listings Data: {}".format(recommended_listings_path))
    recommended_listings_df = spark_session.read.parquet(recommended_listings_path)

    recommendations_df = recommendations_df.join(recommended_listings_df,
                                                 on=['listing_id'],
                                                 how='inner')
    real_profile_df = real_profile_df.withColumnRenamed('zip_code', 'listing_postal_code')
    recommended_filters_df = recommendations_df.join(real_profile_df,
                                                     on=['user_id', 'listing_postal_code'],
                                                     how='inner')

    window_seq_id = Window.partitionBy('user_id', 'state').orderBy('row_id')

    recommended_filters_df = recommended_filters_df.withColumn('seq_id', row_number().over(window_seq_id))

    recommended_filters_df = recommended_filters_df.drop(*['price_floor', 'price_ceiling', 'overall_mean',
                                                           'row_id', 'overall_std_dev', 'score', 'zip_code'])
    recommended_filters_df = recommended_filters_df.filter("seq_id <= 10")

    logger.info("Recommended Filters Schema: {}".format(recommended_filters_df.printSchema()))

    return recommended_filters_df

def write_recommendations_to_s3(target_date, env, recommended_filters_df):
    """
    Write Filtered recommendations to s3
    :param target_date: target date of the output folder
    :param env: environment
    :param recommended_filters_df: recommended homes dataframe filtered with Real Profile
    :return: Spark dataframe
    """
    if env != 'local':
        recommended_filters_output_path = "{}candidates_ranked/target_date={}".format(
                constants.JobConstants.RECOMMENDED_FILTERS_OUTPUT_PATH,
                target_date)
        recommended_filters_output_path = recommended_filters_output_path.replace("$env", env)
    else:
        recommended_filters_output_path = "{}candidates_ranked/target_date={}".format(
            constants.JobConstants.RECOMMENDED_FILTERS_OUTPUT_LOCAL_PATH,
            target_date)

    recommended_filters_df.write.partitionBy('user_group').parquet(recommended_filters_output_path)
    recommended_filters_df = None
    logger.info("Recommended Filters written at: {}".format(recommended_filters_output_path))


def get_interaction_data(start_date, num_covered_days):
    """
    Get Interaction Data in the last covered days
    :param start_date: date on which the offline metrics are captured
    :param num_covered_days: number of days to look back
    :return:
    """
    spark_session = SparkSession.builder.getOrCreate()

    rdc_biz_data_path = constants.JobConstants.RDC_BIZ_DATA_PATH
    end_date = datetime.strftime((datetime.strptime(start_date, '%Y%m%d') + timedelta(num_covered_days)), '%Y%m%d')
    dt = start_date
    paths = []
    logger.info("Reading interaction data between {} and {} for offline metrics".format(start_date, end_date))

    while dt < end_date:
        path = "{}/event_date={}".format(rdc_biz_data_path, dt)
        paths.append(path)
        dt = datetime.strftime((datetime.strptime(dt, '%Y%m%d') + timedelta(1)), '%Y%m%d')

    rdc_biz_df = spark_session.read.option('basePath', rdc_biz_data_path).parquet(*paths).select('member_id',
                                                                                                 'rdc_visitor_id',
                                                                                                 'listing_id_persist',
                                                                                                 'zip',
                                                                                                 'datetime_mst')
    rdc_biz_df.createOrReplaceTempView('biz_table')

    rdc_biz_df = spark_session.sql("""
                            SELECT coalesce(nullif(member_id, ""), rdc_visitor_id) as user_id,
                            listing_id_persist as listing_id, zip as zipcode
                            FROM biz_table
                            """)

    for c in rdc_biz_df.columns:
        rdc_biz_df = rdc_biz_df.filter(col(c).isNotNull())
        rdc_biz_df = rdc_biz_df.filter(~col(c).isin(['unknown', 'UNKNOWN', 'NULL', 'null', "", " "]))
        if c == 'zipcode':
            rdc_biz_df = rdc_biz_df.filter(length(col(c)) == 5)
        if c == 'user_id':
            rdc_biz_df = rdc_biz_df.filter(col(c) != '$CUSTOMER_ID_$')
            rdc_biz_df = rdc_biz_df.filter(col(c) != '$customer_id_$')

    rdc_biz_df = rdc_biz_df.drop_duplicates(['user_id', 'listing_id'])
    rdc_biz_df = rdc_biz_df.select(['user_id', 'listing_id'])
    return rdc_biz_df


