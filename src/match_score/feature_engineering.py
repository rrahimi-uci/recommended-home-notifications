import os
import sys
from datetime import datetime, timedelta
import numpy as np
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from src.utils import constants, logger
import src.match_score.get_features as get_features

log, _ = logger.getlogger()


class FeatureEngineering(object):
    """
    Feature Engineering class to generate the feature vectors of the user listing interactions
    """
    def __init__(self, env, spark, target_date_input, target_date_output, candidates_base_path,
                 candidates_base_local_path, user_features_df, listing_features_df, candidates_df):
        self.target_date_input = target_date_input
        self.target_date_output = target_date_output
        self.env = env
        self.spark = spark
        self.candidates_base_path = candidates_base_path
        self.candidates_base_local_path = candidates_base_local_path
        self.user_features_df = user_features_df
        self.listing_features_df = listing_features_df
        self.candidates_df = candidates_df
        self.con_city_dict = None
        self.ldp_city_dict = None
        self.con_zip_dict = None
        self.ldp_zip_dict = None
        self.deviation_ratio_udf = None
        self.deviation_diff_udf = None
        self.distance_udf = None

    def load_udfs(self):
        """
        Load all the UDFs required for run time features
        :return: None
        """
        self.deviation_ratio_udf = udf(lambda user_feature, feature:
                                       get_features.deviation_ratio(user_feature, feature), FloatType())
        self.spark.udf.register("deviation_ratio_udf", self.deviation_ratio_udf)

        self.deviation_diff_udf = udf(lambda user_feature, feature:
                                      get_features.deviation_diff(user_feature, feature), FloatType())
        self.spark.udf.register("deviation_diff_udf", self.deviation_diff_udf)

        self.distance_udf = udf(lambda lat1, lon1, lat2, lon2:
                                get_features.distance_feature(lat1, lon1, lat2, lon2), FloatType())
        self.spark.udf.register("distance_udf", self.distance_udf)

    def lookup_files(self):
        """
        Load the lookup tables for lookup features
        :return: No return object
        """
        if self.env != 'local':
            con_city_path = constants.ModelConstants.LOOKUP_CONSUMER_CITY
            ldp_city_path = constants.ModelConstants.LOOKUP_LDP_CITY
            con_zip_path = constants.ModelConstants.LOOKUP_CONSUMER_ZIP
            ldp_zip_path = constants.ModelConstants.LOOKUP_LDP_ZIP
        else:
            con_city_path = constants.ModelConstants.LOOKUP_CONSUMER_CITY_LOCAL
            ldp_city_path = constants.ModelConstants.LOOKUP_LDP_CITY_LOCAL
            con_zip_path = constants.ModelConstants.LOOKUP_CONSUMER_ZIP_LOCAL
            ldp_zip_path = constants.ModelConstants.LOOKUP_LDP_ZIP_LOCAL

        con_city_path = con_city_path.replace("$env", self.env)
        self.con_city_dict = self.spark.read.csv(con_city_path, header=True)
        self.con_city_dict = self.con_city_dict.drop(*['web_uus_sum'])
        self.con_city_dict = self.con_city_dict.withColumnRenamed("con_pct_web_uls_median",
                                                                  "con_pct_web_uls_median_city")

        ldp_city_path = ldp_city_path.replace("$env", self.env)
        self.ldp_city_dict = self.spark.read.csv(ldp_city_path, header=True)
        self.ldp_city_dict = self.ldp_city_dict.drop(*['web_uus_sum'])
        self.ldp_city_dict = self.ldp_city_dict.withColumnRenamed("ldp_pct_web_uls_median",
                                                                  "ldp_pct_web_uls_median_city")

        con_zip_path = con_zip_path.replace("$env", self.env)
        self.con_zip_dict = self.spark.read.csv(con_zip_path, header=True)
        self.con_zip_dict = self.con_zip_dict.drop(*['web_uus_sum'])
        self.con_zip_dict = self.con_zip_dict.withColumnRenamed("con_pct_web_uls_median",
                                                                "con_pct_web_uls_median_zip")

        ldp_zip_path = ldp_zip_path.replace("$env", self.env)
        self.ldp_zip_dict = self.spark.read.csv(ldp_zip_path, header=True)
        self.ldp_zip_dict = self.ldp_zip_dict.drop(*['web_uus_sum'])
        self.ldp_zip_dict = self.ldp_zip_dict.withColumnRenamed("ldp_pct_web_uls_median",
                                                                "ldp_pct_web_uls_median_zip")

        log.info("Lookup files loaded on memory...")

    def get_match_score_features(self):
        """
        Get the match score features for the users and listing
        :return: user_features_df, listing_features_df
        """
        if self.env != 'local':
            user_features_path = constants.JobConstants.USER_FEATURES_PATH
            user_features_path = user_features_path.replace("$env", self.env)
            listing_features_path = "{}/target_date={}".format(constants.JobConstants.LISTING_FEATURES_PATH,
                                                               self.target_date_input)
            listing_features_path = listing_features_path.replace("$env", self.env)
        else:
            user_features_path = constants.JobConstants.USER_FEATURES_LOCAL_PATH
            listing_features_path = constants.JobConstants.LISTING_FEATURES_LOCAL_PATH

        log.info("Users path: {}".format(user_features_path))
        start_date = datetime.strftime((datetime.strptime(self.target_date_input, '%Y%m%d') - timedelta(31)), '%Y%m%d')
        self.user_features_df = self.spark.read.parquet(user_features_path).filter(col('target_date') > start_date)
        window_latest_ms = Window.partitionBy('user_id').orderBy(desc('target_date'))
        self.user_features_df = self.user_features_df.withColumn("latest_ms", row_number().over(window_latest_ms))
        self.user_features_df = self.user_features_df.filter(col('latest_ms') == 1)
        self.user_features_df = self.user_features_df.drop(*['latest_ms'])
        log.info("User Features loaded!")

        log.info("Listings path: {}".format(listing_features_path))
        self.listing_features_df = self.spark.read.parquet(listing_features_path)
        log.info("Listing Features loaded!")
        return self.user_features_df, self.listing_features_df

    def get_candidate_features(self):
        """
        Generate the feature vectors for the interactions
        :return: None
        """
        try:
            if self.env != 'local':
                candidates_path = "{}candidates/target_date={}".format(
                    self.candidates_base_path, self.target_date_output)

                candidates_path = candidates_path.replace("$env", self.env)
                candidates_features_path = "{}features/target_date={}".format(
                    self.candidates_base_path, self.target_date_output)


            else:
                candidates_path = "{}candidates/target_date={}".format(
                    self.candidates_base_local_path, self.target_date_output)

            log.info("Candidates path: {}".format(candidates_path))
            self.candidates_df = self.spark.read.parquet(candidates_path)
            log.info("Candidates loaded!")

            listings_df = self.candidates_df.select('listing_id').drop_duplicates()
            listings_df = self.listing_features_df.join(listings_df, ['listing_id'])

            users_df = self.candidates_df.select('user_id').drop_duplicates()
            users_df = self.user_features_df.join(users_df, ['user_id'])

            log.info("User and listings features loaded!")

            # # create a data frame of interactions with features
            self.candidates_df = self.candidates_df.join(listings_df, ['listing_id'])
            listings_df = None
            self.candidates_df = self.candidates_df.join(users_df, ['user_id'])
            users_df = None

            log.info("User and listings features merged!")
            # # get run time features and eliminate columns not required for the model

            features = ['price', 'lot_sqft', 'bath', 'bed', 'sqft']
            for feature in features:
                self.candidates_df = self.candidates_df.withColumn("{}_ratio_cal_{}_t90".format(feature, feature),
                                                           self.deviation_ratio_udf("cal_{}_t90".format(feature),
                                                                                    feature))

                self.candidates_df = self.candidates_df.withColumn("{}_diff_cal_{}_t90".format(feature, feature),
                                                           self.deviation_diff_udf("cal_{}_t90".format(feature),
                                                                                   feature))

                self.candidates_df = self.candidates_df.withColumn("{}_ratio_cal_{}_t90_20".format(feature, feature),
                                                           self.deviation_ratio_udf("cal_{}_t90_20".format(feature),
                                                                                    feature))

                self.candidates_df = self.candidates_df.withColumn("{}_diff_cal_{}_t90_20".format(feature, feature),
                                                           self.deviation_diff_udf("cal_{}_t90_20".format(feature),
                                                                                   feature))

                log.info("{} feature done!".format(feature))

            self.candidates_df = self.candidates_df.withColumn('listing_to_interest_distance',
                                                       self.distance_udf('lat', 'lon', 'cal_lat_t90', 'cal_lon_t90'))
            log.info("distance feature done!")

            self.candidates_df = self.candidates_df.join(self.con_city_dict, ['consumer_ip_city', 'ldp_city'], how='left')
            self.candidates_df = self.candidates_df.join(self.ldp_city_dict, ['ldp_city', 'consumer_ip_city'], how='left')
            self.candidates_df = self.candidates_df.join(self.con_zip_dict, ['consumer_ip_zip', 'ldp_zip'], how='left')
            self.candidates_df = self.candidates_df.join(self.ldp_zip_dict, ['ldp_zip', 'consumer_ip_zip'], how='left')
            log.info("Run time features loaded!")

            self.candidates_df = self.candidates_df.withColumn("con_pct_web_uls_median_city",
                                                     col('con_pct_web_uls_median_city').cast(DoubleType()))
            self.candidates_df = self.candidates_df.withColumn("ldp_pct_web_uls_median_city",
                                                     col('ldp_pct_web_uls_median_city').cast(DoubleType()))
            self.candidates_df = self.candidates_df.withColumn("con_pct_web_uls_median_zip",
                                                     col('con_pct_web_uls_median_zip').cast(DoubleType()))
            self.candidates_df = self.candidates_df.withColumn("ldp_pct_web_uls_median_zip",
                                                     col('ldp_pct_web_uls_median_zip').cast(DoubleType()))

            # _candidates_df = _candidates_df.replace(np.inf, np.nan)
            # _candidates_df = _candidates_df.replace(-np.inf, np.nan)
            self.candidates_df = self.candidates_df.fillna(value=np.nan)
            self.candidates_df = self.candidates_df.drop(*['consumer_ip_zip', 'consumer_ip_city',
                                                           'ldp_zip', 'ldp_city', 'run_date_yyyymmdd', 'target_date'])

        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            f_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            log.error("Filename = {} || Line Number = {} || Generating Features ERROR: {}".format(
                f_name, exc_tb.tb_lineno, e.__str__()))

    def write_candidates_features(self):
        """
        Write the feature vectors for the interactions to S3
        :return: None
        """
        try:
            if self.env != 'local':
                candidates_features_path = "{}features/target_date={}".format(
                    self.candidates_base_path, self.target_date_output)

                candidates_features_path = candidates_features_path.replace("$env", self.env)

            else:
                candidates_features_path = "{}features/target_date={}".format(
                    self.candidates_base_local_path, self.target_date_output)
            self.candidates_df.write.partitionBy("user_group").parquet(candidates_features_path)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            f_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            log.error("Filename = {} || Line Number = {} || Writing Features ERROR: {}".format(f_name, exc_tb.tb_lineno,
                                                                                               e.__str__()))
