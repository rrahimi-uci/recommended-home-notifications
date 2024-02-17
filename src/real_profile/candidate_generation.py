import os
import sys
from datetime import datetime, timedelta
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from src.utils import constants
from src.utils import logger
from src.utils.schema import Schema
from src.real_profile import sql_queries

log, _ = logger.getlogger()


class CandidateGeneration(object):
    """
    Candidate Generation class to generate candidates for the active users
    """

    def __init__(self, env, spark, target_date_input, target_date_output, target_status, number_of_geos,
                 standard_deviation_factor, lag, user_groups, output_base_path, output_base_local_path, real_profile_df,
                 candidates_df, ldp_metrics_df, listings_df, rdc_biz_df, member_email_df, listing_price_median_df):
        self.logger = log
        self.env = env
        self.spark = spark
        self.target_date_input = target_date_input
        self.target_date_output = target_date_output
        self.target_status = target_status
        self.target_date_timestamp = target_date_output + ' 23:59:59'
        self.number_of_geos = number_of_geos
        self.standard_deviation_factor = standard_deviation_factor
        self.lag = lag
        self.user_groups = user_groups
        self.output_base_path = output_base_path
        self.output_base_local_path = output_base_local_path
        self.real_profile_df = real_profile_df
        self.candidates_df = candidates_df
        self.ldp_metrics_df = ldp_metrics_df
        self.listings_df = listings_df
        self.rdc_biz_df = rdc_biz_df
        self.member_email_df = member_email_df
        self.listing_price_median_df = listing_price_median_df

    def get_real_profiles(self):
        """
        Method to get real profiles of active visitors in the last 30 days
        :return: real profile data frame
        """
        self.logger.info("Get real profiles of active visitors in the last 30 days!")

        if self.env != 'local':
            real_profile_path = constants.JobConstants.REAL_PROFILE_PATH
            real_profile_path = real_profile_path.replace("$env", self.env)
            users_output_path = "{}users/target_date={}".format(self.output_base_path, self.target_date_output)
            users_output_path = users_output_path.replace("$env", self.env)
        else:
            real_profile_path = constants.JobConstants.REAL_PROFILE_LOCAL_PATH
            users_output_path = "{}users/target_date={}".format(
                self.output_base_local_path, self.target_date_output)

        self.logger.info("Real Profile Path: {}".format(real_profile_path))

        start_date = datetime.strftime((datetime.strptime(self.target_date_input, '%Y%m%d') - timedelta(29)), '%Y%m%d')
        paths = []
        dt = start_date

        while self.target_date_input >= dt:
            path = "{}/target_date={}".format(real_profile_path, dt)
            dt = datetime.strftime((datetime.strptime(dt, '%Y%m%d') + timedelta(1)), '%Y%m%d')
            if self.path_exists(path):
                paths.append(path)

        log.info("Paths: {}".format(paths))
        self.real_profile_df = self.spark.read.schema(Schema.real_profile_schema).option("basePath", real_profile_path)\
            .json(paths).select('user_id', 'zip_stats', 'price_stats', 'target_date')

        self.real_profile_df = self.real_profile_df.filter(~col('user_id').isin(['unknown', 'NULL', 'null',
                                                                                '$CUSTOMER_ID_$', '$customer_id_$', '']))

        log.info("Getting the latest records in Real Profile Dataframe!")
        # Get the latest real profile in the last 30 days for each user id
        window_latest_rp = Window.partitionBy('user_id').orderBy(desc('target_date'))
        self.real_profile_df = self.real_profile_df.withColumn("latest_rp", row_number().over(window_latest_rp))
        self.real_profile_df = self.real_profile_df.filter(col('latest_rp') == 1)

        # Add row numbers and create user group partitions
        window_user_group = Window.orderBy(lit('user_group'))
        self.real_profile_df = self.real_profile_df.withColumn("row_id", row_number().over(window_user_group))
        self.real_profile_df = self.real_profile_df.withColumn("user_group",
                                                               col("row_id") % self.user_groups)

        log.info("Applying Logic on Real Profile Dataframe!")
        # Explode the zip codes to get the top 3 zip codes for each user id
        self.real_profile_df = self.real_profile_df.select(self.real_profile_df.user_id,
                                                           explode_outer(self.real_profile_df.zip_stats).alias(
                                                               'zip_stat'), self.real_profile_df.price_stats,
                                                           self.real_profile_df.user_group)

        self.real_profile_df = self.real_profile_df.select(self.real_profile_df.user_id,
                                                           self.real_profile_df.zip_stat.getItem('category_name')
                                                           .alias('zip_code'),
                                                           self.real_profile_df.zip_stat.getItem('score')
                                                           .alias('score'),
                                                           self.real_profile_df.price_stats.getItem('overall_mean')
                                                           .alias('overall_mean'),
                                                           self.real_profile_df.price_stats.getItem('overall_std_dev')
                                                           .alias('overall_std_dev'),
                                                           self.real_profile_df.user_group)

        window_top_scores = Window.partitionBy('user_id').orderBy(desc('score'))
        self.real_profile_df = self.real_profile_df.withColumn("rn", row_number().over(window_top_scores))
        self.real_profile_df = self.real_profile_df.filter(col('rn') <= self.number_of_geos)

        # Add the median of listing price column for the respective zip codes
        self.real_profile_df = self.real_profile_df.join(self.listing_price_median_df, ['zip_code'], how='left')
        self.real_profile_df = self.real_profile_df.na.fill(0, ['listing_price_median'])

        self.real_profile_df = self.real_profile_df.withColumn("overall_std_dev",
                                                               when(col('overall_std_dev') < 0.1 * col('overall_mean'),
                                                                    least(0.1 * col('overall_mean'),
                                                                          col('listing_price_median')))
                                                               .otherwise(col('overall_std_dev')))

        self.real_profile_df = self.real_profile_df.withColumn("overall_std_dev",
                                                               when(col('overall_std_dev') > 0.67 * col('overall_mean'),
                                                                    least(0.67 * col('overall_mean'),
                                                                          col('listing_price_median')))
                                                               .otherwise(col('overall_std_dev')))

        self.real_profile_df = self.real_profile_df.withColumn("overall_std_dev", when(col('overall_std_dev') == 0,
                                                                                       0.1 * col('overall_mean'))
                                                               .otherwise(col('overall_std_dev')))

        self.real_profile_df = self.real_profile_df.withColumn("score", col('score').cast(DoubleType()))
        self.real_profile_df = self.real_profile_df.withColumn("overall_mean", col('overall_mean').cast(DoubleType()))
        self.real_profile_df = self.real_profile_df.withColumn("overall_std_dev", col('overall_std_dev').cast(DoubleType()))

        # Apply filters to get the price range for each user id in each zip code
        self.real_profile_df = self.real_profile_df.withColumn("price_floor", (col('overall_mean') -
                                                               self.standard_deviation_factor * col('overall_std_dev')))
        self.real_profile_df = self.real_profile_df.withColumn("price_ceiling", (col('overall_mean') +
                                                             self.standard_deviation_factor * col('overall_std_dev')))
        self.real_profile_df = self.real_profile_df.drop(*['rn'])

        # Add associated emails to each user id
        # self.real_profile_df = self.real_profile_df.join(self.member_email_df, ['user_id'], how='left')
        # self.real_profile_df = self.real_profile_df.where(self.real_profile_df.email_address.isNotNull())

        # Write the real profile features of users to s3
        self.real_profile_df.write.parquet(users_output_path)
        self.logger.info("Processed Real Profiles!")

        return self.real_profile_df

    def path_exists(self, path):
        """
        Check if s3 path exists
        :param path: S3 Path
        :return: Boolean true if exists else false
        """
        sc = self.spark.sparkContext
        fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jvm.java.net.URI.create("s3://" + path.split("/")[2]),
                                                         sc._jsc.hadoopConfiguration(),)
        return fs.exists(sc._jvm.org.apache.hadoop.fs.Path(path))

    def generate_listings(self):
        """
        Generate listings' details for all the active listings
        :param aaa_test_filter: A/A/A test flag to restrict recommendations from New Mexico
        :return: listings data frame
        """
        self.logger.info("Generate listings data!")

        cols = sql_queries.listing_columns
        current_year = datetime.now().year
        start_year = current_year - 1
        if self.env != 'local':
            listings_path = constants.JobConstants.LISTING_HISTORY_PATH
            self.listings_df = self.spark.read.parquet("{}/year_yyyy=9999".format(listings_path)).select(*cols)\
                .withColumn("listing_photo_count", col("listing_photo_count").cast(LongType()))
            while current_year > start_year:
                year_path = "{}/year_yyyy={}".format(listings_path, start_year)
                start_year += 1
                year_df = self.spark.read.parquet(year_path).select(*cols)\
                    .withColumn("listing_photo_count", col("listing_photo_count").cast(LongType()))
                self.listings_df = self.listings_df.union(year_df)
        else:
            listings_path = constants.JobConstants.DAILY_LISTINGS_LOCAL_PATH
            self.listings_df = self.spark.read.parquet(listings_path).select(*cols)

        self.listings_df.createOrReplaceTempView("listingHistory")

        self.logger.info("Listing History Schema: {}".format(self.listings_df.printSchema()))

        listings_details = sql_queries.listing_history_details

        listings_details = listings_details.replace('$target_date_timestamp', self.target_date_timestamp)

        self.logger.info("Listing details: {}".format(listings_details))

        self.listings_df = self.spark.sql(listings_details)

        self.logger.info("Listing details Count: {}".format(self.listings_df.count()))

        # Filter the contingent and pending listings
        self.listings_df = self.listings_df.filter((col('is_contingent').isNotNull())
                                                   & (col('is_da_pending').isNotNull())
                                                   & (col('is_contingent') != 'y')
                                                   & (col('is_da_pending') != 'y'))
        self.listings_df = self.listings_df.filter((length(col('listing_address')) > 0)
                                                   & (length(col('listing_city')) > 0)
                                                   & (length(col('listing_state')) > 0)
                                                   & (col('listing_status') == self.target_status))
        self.listings_df = self.listings_df.filter(lower(col('listing_type')).isin(constants.DataConstants.LISTING_TYPES))

        # Get the listings active in the LAG days
        self.listings_df = self.listings_df.withColumn("run_date_yyyymmdd",
                                                       unix_timestamp(lit(self.target_date_timestamp),
                                                                      'yyyyMMdd HH:mm:ss').cast('timestamp'))
        self.listings_df = self.listings_df.filter(col('listing_start_datetime_mst')
                                                   .between(date_sub(col('run_date_yyyymmdd'),
                                                            self.lag), col('run_date_yyyymmdd')))

        # Process the listings to generate features
        self.listings_df = self.listings_df.withColumn("dom", datediff(col('run_date_yyyymmdd'),
                                                                       col('listing_start_datetime_mst')))
        self.listings_df = self.listings_df.withColumn("listing_photo_url", when(col('listing_photo_url').like('%.jpg'),
                                           regexp_replace(col('listing_photo_url'), ".jpg", "od-w640_h480_q80_r4.jpg"))
                                                       .when(col('listing_photo_url').like('%not available%'),
                                                             'not available')
                                                       .otherwise(col('listing_photo_url')))
        self.listings_df = self.listings_df.withColumn("listing_photo_url",
                                                       regexp_replace('listing_photo_url', 'http://',
                                                                      'https://'))
        self.listings_df = self.listings_df.withColumn("listing_square_feet",
                                                       when(col('listing_square_feet').like('0.0'), None)
                                                       .otherwise(col('listing_square_feet')))
        self.listings_df = self.listings_df.withColumn("listing_lot_square_feet",
                                                       when(col('listing_lot_square_feet').like('0.0'), None)
                                                       .otherwise(col('listing_lot_square_feet')))
        self.listings_df = self.listings_df.withColumn("listing_state", upper(col('listing_state')))

        # Get the latest listing for each property id
        window_row = Window.partitionBy('property_id').orderBy(desc('listing_start_datetime_mst'),
                                                              desc('listing_end_datetime_mst'),
                                                              desc('effective_from_datetime_mst'))
        self.listings_df = self.listings_df.withColumn("listing_rank", row_number().over(window_row))
        self.listings_df = self.listings_df.filter(col('listing_rank') == 1)

        # Cleanup unused columns
        self.listings_df = self.listings_df.drop(*['listing_rank', 'is_contingent', 'is_da_pending',
                                                   'listing_end_datetime_mst', 'effective_from_datetime_mst',
                                                   'run_date_yyyymmdd', 'effective_to_datetime_mst'])
        self.listings_df = self.listings_df.withColumn("listing_current_price",
                                                       col("listing_current_price").cast(DoubleType()))
        self.listings_df = self.listings_df.withColumn("listing_start_datetime_mst",
                                                       col("listing_start_datetime_mst").cast(StringType()))
        self.listings_df = self.listings_df.withColumnRenamed('rdc_property_url', 'ldp_url')


        states = constants.DataConstants.STATE_LIST
        self.listings_df = self.listings_df.filter(col('listing_state').isin(states))

        self.logger.info("Listings DF Schema: {}".format(self.listings_df.printSchema()))
        return self.listings_df

    def write_generated_listings(self):
        """
        Write the generated listings to s3
        :return: None
        """
        if self.env != 'local':
            listings_output_path = "{}listings/target_date={}".format(
                self.output_base_path, self.target_date_output)
            listings_output_path = listings_output_path.replace("$env", self.env)
        else:
            listings_output_path = "{}listings/target_date={}".format(
                self.output_base_local_path, self.target_date_output)
        self.listings_df.write.parquet(listings_output_path)
        self.listings_df = None

    def get_rdc_biz_data(self):
        """
        Get the data frame for the rdc biz data that can used for generating LDP metrics, saved viewed homes
        :return: rdc_biz_df data frame
        """
        self.logger.info("Get rdc biz data till the lag day")
        paths = []
        start_date = datetime.strftime((datetime.strptime(self.target_date_output, '%Y%m%d') -
                                        timedelta(self.lag)), '%Y%m%d')
        dt = start_date
        if self.env != 'local':
            rdc_biz_data_path = constants.JobConstants.RDC_BIZ_DATA_PATH
            while self.target_date_output > dt:
                path = "{}/event_date={}".format(rdc_biz_data_path, dt)
                dt = datetime.strftime((datetime.strptime(dt, '%Y%m%d') + timedelta(1)), '%Y%m%d')
                paths.append(path)
        else:
            rdc_biz_data_path = constants.JobConstants.RDC_BIZ_DATA_LOCAL_PATH
            dt = constants.JobConstants.SAMPLE_EVENT_DATE
            path = "{}/event_date={}".format(rdc_biz_data_path, dt)
            paths.append(path)

        self.rdc_biz_df = self.spark.read.option("basePath", rdc_biz_data_path).parquet(*paths).select(
                                                                        'listing_id',
                                                                        'page_type_group',
                                                                        'social_shares',
                                                                        'saved_items',
                                                                        'member_id',
                                                                        'rdc_visitor_id',
                                                                        'current_page_url',
                                                                        'event_date',
                                                                        'property_status',
                                                                        'page_type')
        return self.rdc_biz_df

    def generate_ldp_metrics(self):
        """
        Generate LDP metrics for the candidate listings
        :return: listings_df merged with ldp_metrics_df data frame
        """
        self.logger.info("Generate ldp page views!")
        self.rdc_biz_df.createOrReplaceTempView("bizTable")
        self.logger.info("Completed reading event data!")

        self.ldp_metrics_df = self.spark.sql(sql_queries.ldp_metrics)

        self.logger.info("LDP Metrics Generated!")

        # Merge the ldp metrics data frame and write to s3
        self.listings_df = self.listings_df.join(self.ldp_metrics_df, ['listing_id'], how='left')
        self.ldp_metrics_df = None
        return self.listings_df

    def generate_candidates(self):
        """
        Method to generate candidate listings for each user of a user group
        :return: candidates data frame
        """
        try:
            if self.env != 'local':
                users_path = "{}users/target_date={}".format(
                    self.output_base_path, self.target_date_output)
                users_path = users_path.replace("$env", self.env)
                listings_path = "{}listings/target_date={}".format(self.output_base_path,
                                                                    self.target_date_output)
                listings_path = listings_path.replace("$env", self.env)
                candidates_path = "{}candidates/target_date={}".format(
                    self.output_base_path, self.target_date_output)
                candidates_path = candidates_path.replace("$env", self.env)
            else:
                users_path = "{}users/target_date={}".format(
                    self.output_base_local_path, self.target_date_output)
                listings_path = "{}listings/target_date={}".format(
                    self.output_base_local_path, self.target_date_output)
                candidates_path = "{}candidates/target_date={}".format(
                    self.output_base_local_path, self.target_date_output)

            # Generate candidates using real profile of the user and the active listings in their preferred zip codes
            user_group_df = self.spark.read.parquet(users_path)
            listings_df = self.spark.read.parquet(listings_path)
            self.candidates_df = user_group_df.join(listings_df, user_group_df.zip_code == listings_df.listing_postal_code,
                                                    'inner')
            self.candidates_df = self.candidates_df.where(
                self.candidates_df.listing_current_price.between(self.candidates_df.price_floor,
                                                                 self.candidates_df.price_ceiling))
            self.candidates_df = self.candidates_df.drop(*['price_floor', 'price_ceiling', 'overall_mean',
                                                           'overall_std_dev', 'score', 'zip_code'])

            self.logger.info("Generated candidates!")
            return self.candidates_df
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            f_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            log.error("Filename = {} || Line Number = {} || Candidate Generation ERROR: {}".format(
                f_name, exc_tb.tb_lineno, e.__str__()))
            return None

    def get_emails(self):
        """
        Get emails for the user ids
        :return: member id email data frame
        """
        if self.env != 'local':
            member_email_path = "{}/target_date={}".format(constants.DeploymentConstants.S3_DATAENG_BUCKET,
                                                           self.target_date_output)
        else:
            member_email_path = constants.DeploymentConstants.S3_DATAENG_BUCKET_LOCAL

        self.member_email_df = self.spark.read.parquet(member_email_path)
        self.member_email_df = self.member_email_df.select(col('member_id').alias('user_id'),
                                                           col('member_profile__email_address').alias('email_address'))
        return self.member_email_df

    def get_median_listing_price(self):
        """
        Generate median of listing price at a zip code level for all the active listings
        :return: zip code median listing price data frame
        """
        self.logger.info("Generate median of listing price data at a zip code level!")

        window_zip_code = Window.partitionBy('listing_postal_code')
        magic_percentile = expr('percentile_approx(listing_current_price, 0.5)')
        self.listing_price_median_df = self.listings_df.withColumn("listing_price_median", magic_percentile.over(
                                                                    window_zip_code)).select(
                                                                    'listing_postal_code', 'listing_price_median')
        self.listing_price_median_df = self.listing_price_median_df.withColumnRenamed("listing_postal_code", "zip_code")
        self.listing_price_median_df = self.listing_price_median_df.drop_duplicates()
        self.listing_price_median_df = self.listing_price_median_df.filter("""listing_price_median > 9999 """)
        return self.listing_price_median_df

    def tag_viewed_saved_listings(self):
        """
        Add viewed and saved tags to the candidates data frame
        :return: candidates data frame
        """
        self.rdc_biz_df.createOrReplaceTempView('biz_data')

        interactions_df = self.spark.sql(sql_queries.interactions)
        saved_items_df = self.spark.sql(sql_queries.saved_items)

        interactions_cond = [self.candidates_df.user_id == interactions_df.interactions_user_id,
                             self.candidates_df.listing_id == interactions_df.interactions_listing_id]

        self.candidates_df = self.candidates_df.join(interactions_df, interactions_cond, how='left')

        saved_items_cond = [self.candidates_df.user_id == saved_items_df.saved_items_user_id,
                            self.candidates_df.listing_id == saved_items_df.saved_items_listing_id]

        self.candidates_df = self.candidates_df.join(saved_items_df, saved_items_cond, how='left')

        # tag the candidates as viewed or saved as per the interactions data
        self.candidates_df = self.candidates_df.withColumn('viewed', when(
            (col('interactions_user_id') != 'null') & (col('interactions_listing_id') != 'null'), 'y').otherwise('n'))
        self.candidates_df = self.candidates_df.withColumn('saved_home', when(
            (col('saved_items') != 'null') & (col('saved_items') == 'y'), 'y').otherwise('n'))

        self.candidates_df = self.candidates_df.drop(
            *['interactions_user_id', 'interactions_listing_id', 'saved_items_user_id', 'saved_items_listing_id'])

        self.logger.info("Tagged Candidates!")
        return self.candidates_df

    def write_generated_candidates(self):
        """
        Write the candidates data frame to s3
        :return: None
        """
        if self.env != 'local':
            candidates_path = "{}candidates/target_date={}".format(
                self.output_base_path, self.target_date_output)
            candidates_path = candidates_path.replace("$env", self.env)
        else:
            candidates_path = "{}candidates/target_date={}".format(
                self.output_base_local_path, self.target_date_output)
        self.candidates_df.write.parquet(candidates_path)
