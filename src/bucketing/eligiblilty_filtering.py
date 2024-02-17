import gc
import os
import sys
from datetime import datetime, timedelta
from pyspark.sql.functions import *
from pyspark.sql.window import Window

from src.utils import constants
from src.utils.schema import Schema
from src.utils import logger

log, _ = logger.getlogger()


class Eligibility(object):
    """
    Get the eligible users based on the criteria:
    a. Users who do not have saved searches
    b. Users who have opted in for push notifications
    """
    def __init__(self, env, target_date, spark, base_path, base_local_path, saved_search_df,
                 web_opted_df, eligible_candidates_df):
        self.env = env
        self.target_date = target_date
        self.spark = spark
        self.base_path = base_path
        self.base_local_path = base_local_path
        self.saved_search_df = saved_search_df
        self.web_opted_df = web_opted_df
        self.eligible_candidates_df = eligible_candidates_df

        self.app_push_opted_df = None
        self.app_eligible_users_df = None
        self.users_with_notif_30_df = None
        self.domzip_users_without_notif_df = None
        self.users_domzip_df = None

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

    def saved_search_users(self):
        """
        Method to get the list of users who have saved searches over the last 30 days
        :return: Data frame of users who have saved searches
        """
        start_date = datetime.strftime((datetime.strptime(self.target_date, '%Y%m%d') - timedelta(30)), '%Y%m%d')
        paths = []
        dt = start_date

        if self.env != 'local':
            saved_search_path = constants.JobConstants.ANALYTICAL_PROFILE_PATH
            while self.target_date >= dt:
                path = "{}/snapshot_date_mst_yyyymmdd={}".format(saved_search_path, dt)
                dt = datetime.strftime((datetime.strptime(dt, '%Y%m%d') + timedelta(1)), '%Y%m%d')
                if self.path_exists(path):
                    paths.append(path)
        else:
            saved_search_path = constants.JobConstants.ANALYTICAL_PROFILE_LOCAL_PATH
            dt = constants.JobConstants.SAMPLE_EVENT_DATE
            paths.append("{}/snapshot_date_mst_yyyymmdd={}".format(saved_search_path, dt))

        self.saved_search_df = self.spark.read.option("basePath", saved_search_path).parquet(*paths)\
            .select('snapshot_date_mst_yyyymmdd', 'is_saved_search', 'member_id')
        self.saved_search_df = self.saved_search_df.filter("""is_saved_search = 'y' and length(member_id) > 1""")
        self.saved_search_df = self.saved_search_df.select(self.saved_search_df.member_id.alias('user_id')).distinct()

        log.info("Saved Search Users Loaded!")
        return self.saved_search_df

    def web_push_opted_users(self):
        """
        Method to get the list of users who have opted in for web pushes over the last 30 days
        :return: Data frame of users who have opted in for web and app push notifications
        """
        if self.env != 'local':
            web_opted_path = "{}/event_date={}".format(constants.JobConstants.PUSH_NOTIFICATION_OPTED_PATH,
                                                       self.target_date)
        else:
            dt = constants.JobConstants.SAMPLE_EVENT_DATE
            web_opted_path = "{}/event_date={}".format(constants.JobConstants.PUSH_NOTIFICATION_OPTED_LOCAL_PATH, dt)

        self.web_opted_df = self.spark.read.schema(Schema.push_opted_schema).json(web_opted_path)
        self.web_opted_df = self.web_opted_df.select(self.web_opted_df.userId.alias('user_id'),
                                                     self.web_opted_df.optIns.web.alias('web'),
                                                     self.web_opted_df.optIns.app.alias('app'))
        self.web_opted_df = self.web_opted_df.filter("""web == true """).select('user_id')
        self.web_opted_df = self.web_opted_df.withColumn('user_id', regexp_replace(col('user_id'), 'visitor_', ''))

        log.info("Opted In Users Loaded!")
        return self.web_opted_df

    def web_push_eligible_candidates(self):
        """
        Method to filter eligible users from the pool of recommendations
        :return:
        """
        if self.env != 'local':
            candidates_ranked_path = "{}candidates_ranked/target_date={}".format(self.base_path, self.target_date)
            candidates_ranked_path = candidates_ranked_path.replace("$env", self.env)
            eligible_candidates_path = "{}eligible_candidates/target_date={}".format(self.base_path, self.target_date)
            eligible_candidates_path = eligible_candidates_path.replace("$env", self.env)
        else:
            target_date_input = constants.JobConstants.SAMPLE_EVENT_DATE
            candidates_ranked_path = "{}candidates_ranked/target_date={}".format(self.base_local_path,
                                                                                 target_date_input)
            eligible_candidates_path = "{}eligible_candidates/target_date={}".format(self.base_local_path,
                                                                                     target_date_input)

        self.eligible_candidates_df = self.spark.read.parquet(candidates_ranked_path)

        self.eligible_candidates_df = self.eligible_candidates_df.filter(
            self.eligible_candidates_df.user_id != '$CUSTOMER_ID_$')
        self.eligible_candidates_df = self.eligible_candidates_df.filter(
            self.eligible_candidates_df.user_id != 'unknown')
        self.eligible_candidates_df = self.eligible_candidates_df.filter(
            self.eligible_candidates_df.listing_id != 'unknown')
        self.eligible_candidates_df = self.eligible_candidates_df.drop_duplicates(['user_id', 'listing_id'])

        log.info("Discarding users who have save searches!")
        opted_not_saved_users_df = self.web_opted_df.join(self.saved_search_df, on='user_id', how='left_anti')

        log.info("Filtering users who opted for push notifications!")
        self.eligible_candidates_df = self.eligible_candidates_df.join(opted_not_saved_users_df, on='user_id',
                                                                       how='inner')

        self.saved_search_df = None
        self.web_opted_df = None
        self.eligible_candidates_df.write.parquet(eligible_candidates_path)
        log.info("Eligible Users Loaded and written to S3 at {}!".format(eligible_candidates_path))

        return self.eligible_candidates_df

    def web_push_execute(self):
        """
        Sequence of execution of methods
        a. find users who have saved searches
        b. opted in users for push notifications
        c. filter eligible users from ranked candidates of users
        :return:
        """
        self.saved_search_users()
        gc.collect()
        self.web_push_opted_users()
        gc.collect()
        self.web_push_eligible_candidates()

        return self.eligible_candidates_df

    def app_push_opted_users(self):
        """
        Method to get users who have opted-in for app push notifications
        
        :return:
        """
        if self.env == "local":
            app_opted_path = constants.JobConstants.PUSH_NOTIFICATION_OPTED_LOCAL_PATH
        else:
            app_opted_path = constants.JobConstants.PUSH_NOTIFICATION_OPTED_PATH
        
        log.info("Getting Users who opted in for App Push Notification!")
        self.app_push_opted_df = self.spark.read.schema(Schema.push_opted_schema).json(app_opted_path)
        self.app_push_opted_df = self.app_push_opted_df.select(self.app_push_opted_df.userId.alias('user_id'), 
                                                                self.app_push_opted_df.optIns.app.alias('app'))
        self.app_push_opted_df = self.app_push_opted_df.filter("app == true").select('user_id')
        log.info("App Push Opted-In Users loaded!")

    def get_users_with_domzip(self):
        """
        Method to get Users with their respective dominant zip for last 30 days.
        
        :return:
        """
        if self.env == "local":
            domzip_path_base_path = constants.JobConstants.USERS_WITH_DOMZIP_LOCAL_PATH
        else:
            domzip_path_base_path = constants.JobConstants.USERS_WITH_DOMZIP_PATH

        log.info("Getting Users with Respective Domzip for last 30 days!")
        users_domzip_paths = []
        _date = self.target_date
        for _ in range(30):
            if self.path_exists(f"{domzip_path_base_path}/run_date_yyyymmdd={_date}"):
                users_domzip_paths.append(f"{domzip_path_base_path}/run_date_yyyymmdd={_date}")
            _date = (datetime.strptime(_date, "%Y%m%d") - timedelta(days=1)).strftime("%Y%m%d")
        
        self.users_domzip_df = self.spark.read.schema(Schema.users_domzip_schema) \
                                                .option("basePath", domzip_path_base_path) \
                                                .parquet(*users_domzip_paths)
        # performing gradual rollout
        self.users_domzip_df = self.users_domzip_df

        self.users_domzip_df = self.users_domzip_df.select('member_id', 'listing_state').drop_duplicates()
        log.info("Users with DomZip loaded!")

    def get_users_with_notif_30(self):
        """
        Method to get Users who recieved push notifications in last 30 days.
        
        :return:
        """
        if self.env == "local":
            users_with_notif_30_base_path = constants.JobConstants.USERS_WITH_NOTIF_LOCAL_PATH
        else:
            users_with_notif_30_base_path = constants.JobConstants.USERS_WITH_NOTIF_PATH
        
        log.info("Getting Users who recieved app push notifications in Last 30 days!")
        users_with_notif_30_paths = []
        _date = self.target_date
        for _ in range(30):
            if self.path_exists(f"{users_with_notif_30_base_path}/year={_date[:4]}/month={_date[4:6]}/day={_date[6:]}"):
                users_with_notif_30_paths.append(f"{users_with_notif_30_base_path}/year={_date[:4]}/month={_date[4:6]}/day={_date[6:]}")
            _date = (datetime.strptime(_date, "%Y%m%d") - timedelta(days=1)).strftime("%Y%m%d")
            
        self.users_with_notif_30_df = self.spark.read.schema(Schema.users_with_notif_schema)  \
                                                    .option("basePath", users_with_notif_30_base_path) \
                                                    .parquet(*users_with_notif_30_paths) \
                                                    .filter("campaign_id in ('2fbfc9f2-a162-42a3-a036-2687d1e377fe', \
                                                                            'ee341ac8-d3ec-44aa-a288-a5b23e011dfc', \
                                                                            'fe840cfe-f507-4a60-976d-8963355cabae', \
                                                                            '147f3499-f628-4be6-9328-5412cf790121',\
                                                                            'c8654f56-fdc3-8055-a9a2-353867e1f067') \
                                                            or canvas_id in ('de9d7ccf-b427-4d62-a765-129de45f0d90')")
        
        self.users_with_notif_30_df = self.users_with_notif_30_df.select('external_user_id').distinct()
        self.users_with_notif_30_df = self.users_with_notif_30_df.withColumnRenamed('external_user_id', 'member_id')
        log.info("Users who recieve app push notifications in Last 30 days are loaded!")

    def get_app_push_eligible_users(self):
        """
        Mehod to execute sequece of steps to generate eligible users for app.
        Reference: https://moveinc.atlassian.net/wiki/spaces/PERS/pages/7168988273/Recommended+Homes+v2
        Also, we have to get Users with Domzip who didn't recieve notifications in last  30 days, so we did left anti join.

        :return:
        """
        try:
            if self.env == 'local':
                app_eligible_users_output_path = constants.JobConstants.APP_ELIGIBLE_USERS_OUTPUT_LOCAL_PATH
            else:
                app_eligible_users_output_path = constants.JobConstants.APP_ELIGIBLE_USERS_OUTPUT_PATH
                app_eligible_users_output_path = app_eligible_users_output_path.replace('$env', self.env)

            self.app_push_opted_users()
            gc.collect()
            self.get_users_with_domzip()
            gc.collect()
            self.get_users_with_notif_30()
            gc.collect()

            self.domzip_users_without_notif_df =  self.users_domzip_df.join(self.users_with_notif_30_df, 
                                                        on=['member_id'], how='left_anti')
            self.app_eligible_users_df = self.domzip_users_without_notif_df.join(self.app_push_opted_df, 
                                                        self.domzip_users_without_notif_df.member_id == self.app_push_opted_df.user_id)
            
            self.app_eligible_users_df = self.app_eligible_users_df.select('user_id', 'listing_state').drop_duplicates()
            
            log.info("Writing DomZip App Push Eligible Users to datalake!")
            self.app_eligible_users_df.write.parquet(f"{app_eligible_users_output_path}/target_date={self.target_date}")
            log.info("DomZip App Push Eligible Users written to DataLake!")
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            f_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            log.error(f"Filename = {f_name} || Line Number = {exc_tb.tb_lineno} || Error: {e}")
            # re-raise the error
            raise

    def email_push_eligible_users(self):
        """Get users opted in for email campaign and haven't received the home alert emails in last 2 days.
        """
        try:
            if self.env == 'local':
                users_with_email_notifications_base_path = constants.JobConstants.EMAIL_NOTIFICATIONS_LOCAL_PATH
                email_push_eligible_users_path = ""
            else:
                users_with_email_notifications_base_path = constants.JobConstants.EMAIL_NOTIFICATIONS_PATH
                email_push_eligible_users_path = constants.JobConstants.EMAIL_ELIGIBLE_USERS_OUTPUT_PATH
                email_push_eligible_users_path = email_push_eligible_users_path.replace('$env', self.env)
            
            users_with_email_notifications_paths = []
            _date = self.target_date
            for _ in range(180):
                if self.path_exists(f"{users_with_email_notifications_base_path}/year={_date[:4]}/month={_date[4:6]}/day={_date[6:]}"):
                    users_with_email_notifications_paths.append(f"{users_with_email_notifications_base_path}/year={_date[:4]}/month={_date[4:6]}/day={_date[6:]}")
                _date = (datetime.strptime(_date, "%Y%m%d") - timedelta(days=1)).strftime("%Y%m%d")
            
            log.info("Getting users who opted-in for email campaign and didn't receive home alert notificaitons in last 2 days.")
            self.users_email_df = self.spark.read.schema(Schema.users_sent_email_schema) \
                                            .option("basePath", users_with_email_notifications_base_path) \
                                            .parquet(*users_with_email_notifications_paths)
            
            _opted_in_df = self.users_email_df.filter("customer_id is not null and \
                                                trim(customer_id)!='' and \
                                                campaign_id is not null and \
                                                trim(campaign_id)!=''") \
                                    .select('customer_id', 'email').distinct()

            start_date = (datetime.strptime(self.target_date, "%Y%m%d") - timedelta(days=2)).strftime("%Y%m%d")
            _users_with_home_alerts_email_df = self.users_email_df.filter(f"customer_id is not null  \
                                                                and trim(customer_id)!='' \
                                                                and campaign_id in ( \
                                                                '12908962','12905042','12153702','12153622', \
                                                                '12940062','12153862','13324642','12153542', \
                                                                '12677522','12153782','12677822','12678022') \
                                                                 and concat(year, month, day) between {start_date} and {self.target_date} ") \
                                                        .select('customer_id', 'email').distinct()
            
            self.users_email_df = _opted_in_df.exceptAll(_users_with_home_alerts_email_df)

            log.info("Writing Email Eligible users to datalake!")
            self.users_email_df.write.parquet(f"{email_push_eligible_users_path}/target_date={self.target_date}")
            
            _opted_in_df = None
            _users_with_home_alerts_email_df = None
            self.users_email_df = None
            gc.collect()
            log.info("Email Eligible Users wrtitten to datalake!")
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            f_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            log.error(f"Filename = {f_name} || Line Number = {exc_tb.tb_lineno} || Error: {e}")
            # re-raise the error
            raise