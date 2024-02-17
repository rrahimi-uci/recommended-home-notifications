import boto3

from pyspark.sql.functions import *
from pyspark.sql.types import StringType

from src.utils import sqs_helper
from src.utils import logger
from src.utils import constants

log, pidInfo = logger.getlogger()
sqs_client = boto3.resource('sqs', region_name='us-west-2')


class SQS:
    """
    AWS SQS service helper methods to create and remove queues, send and receive messages
    """
    def __init__(self, env, queue_name, event_type, spark, target_date, recommendations_base_path,
                 recommendations_base_local_path):
        self.env = env
        self.queue_name = queue_name
        self.event_type = event_type
        self.spark = spark
        self.target_date = target_date
        self.recommendations_base_path = recommendations_base_path
        self.recommendations_base_local_path = recommendations_base_local_path
        self.candidates_ranked_df = None

    def push_recommendations(self):
        if self.env != 'local':
            if self.event_type.endswith('app'):
                candidates_ranked_path = "{}eligible_candidates_app/target_date={}".format(
                    self.recommendations_base_path, self.target_date)
            elif self.event_type.endswith('email'):
                candidates_ranked_path = "{}eligible_candidates_email/target_date={}".format(
                    self.recommendations_base_path, self.target_date)
            else:
                candidates_ranked_path = "{}bucketed_candidates/target_date={}".format(
                    self.recommendations_base_path, self.target_date)

            candidates_ranked_path = candidates_ranked_path.replace("$env", self.env)

        else:
            if self.event_type.endswith('app'):
                candidates_ranked_path = "{}eligible_candidates_app/target_date={}".format(
                self.recommendations_base_local_path, self.target_date)
            elif self.event_type.endswith('email'):
                candidates_ranked_path = "{}eligible_candidates_email/target_date={}".format(
                    self.recommendations_base_local_path, self.target_date)
            else:
                candidates_ranked_path = "{}bucketed_candidates/target_date={}".format(
                    self.recommendations_base_local_path, self.target_date)

        log.info("Candidates path: {}".format(candidates_ranked_path))

        self.candidates_ranked_df = self.spark.read.parquet(candidates_ranked_path)

        if self.event_type.endswith('email'):
            self.candidates_ranked_df = self.candidates_ranked_df.withColumn("recommendation",
                                                                            struct('listing_id',
                                                                                    'property_id',
                                                                                    'email',
                                                                                    'listing_address',
                                                                                    'listing_status',
                                                                                    'listing_type',
                                                                                    'listing_photo_url',
                                                                                    'listing_photo_count',
                                                                                    'listing_square_feet',
                                                                                    'listing_lot_square_feet',
                                                                                    'ldp_url',
                                                                                    'listing_city',
                                                                                    'listing_state',
                                                                                    'listing_postal_code',
                                                                                    'listing_current_price',
                                                                                    'listing_start_datetime_mst',
                                                                                    'listing_number_of_bath_rooms',
                                                                                    'listing_number_of_bed_rooms',
                                                                                    'dom',
                                                                                    'ldp_pageview_count',
                                                                                    'saved_home',
                                                                                    'shares_count',
                                                                                    'saves_count',
                                                                                    'viewed',
                                                                                    'seq_id',
                                                                                    'reccity',
                                                                                    'recstate')).select(
                'user_id', 'recommendation', 'variation', 'experiment_name')
        else:
            self.candidates_ranked_df = self.candidates_ranked_df.withColumn("recommendation",
                                                                            struct('listing_id',
                                                                                    'property_id',
                                                                                    'listing_address',
                                                                                    'listing_status',
                                                                                    'listing_photo_url',
                                                                                    'listing_photo_count',
                                                                                    'listing_square_feet',
                                                                                    'listing_lot_square_feet',
                                                                                    'ldp_url',
                                                                                    'listing_city',
                                                                                    'listing_state',
                                                                                    'listing_postal_code',
                                                                                    'listing_current_price',
                                                                                    'listing_start_datetime_mst',
                                                                                    'listing_number_of_bath_rooms',
                                                                                    'listing_number_of_bed_rooms',
                                                                                    'dom',
                                                                                    'ldp_pageview_count',
                                                                                    'shares_count',
                                                                                    'saves_count',
                                                                                    'viewed',
                                                                                    'seq_id')).select(
                'user_id', 'recommendation', 'variation', 'experiment_name')

        self.candidates_ranked_df = self.candidates_ranked_df.filter("""variation is not null""")
        self.candidates_ranked_df = self.candidates_ranked_df.groupBy('user_id', 'variation',
                                                                      'experiment_name').agg(
            collect_list('recommendation').alias('recommendations'))

        queue_name = self.queue_name
        event_template = sqs_helper.get_schema(self.event_type)
        self.candidates_ranked_df = self.candidates_ranked_df.toJSON().map(
            lambda line: sqs_helper.send_message(line, event_template, queue_name))
        pushed_items = self.candidates_ranked_df.map(lambda line: line[1]).sum()

        log.info("Pushed Items: {}".format(pushed_items))
    
    def push_dom_zip_notifications(self):
        if self.env != 'local':
            domzip_bucketed_users_path = "{}/target_date={}".format(constants.JobConstants.APP_BUCKETED_USERS_OUTPUT_PATH,
                                                                     self.target_date)
            domzip_bucketed_users_path = domzip_bucketed_users_path.replace('$env', self.env)
        else:
            domzip_bucketed_users_path = "{}".format(constants.JobConstants.APP_BUCKETED_USERS_OUTPUT_LOCAL_PATH)

        log.info("Getting DomZip App Bucketed Users from {}".format(domzip_bucketed_users_path))

        domzip_bucketed_users_df = self.spark.read.parquet(domzip_bucketed_users_path)
        domzip_bucketed_users_df = domzip_bucketed_users_df.filter("variation == 'C'")

        domzip_bucketed_users_df = domzip_bucketed_users_df.withColumn("recommendations", lit(""))
        domzip_bucketed_users_df = domzip_bucketed_users_df.select('user_id', 'recommendations', 'variation', 'experiment_name')
        domzip_bucketed_users_df = domzip_bucketed_users_df.drop_duplicates()

        queue_name = self.queue_name
        event_template = sqs_helper.get_schema(self.event_type)
        domzip_bucketed_users_df = domzip_bucketed_users_df.toJSON().map(
            lambda line: sqs_helper.send_message(line, event_template, queue_name))
        pushed_items = domzip_bucketed_users_df.map(lambda line: line[1]).sum()

        log.info("Pushed DomZip Items: {}".format(pushed_items))

        domzip_bucketed_users_df = None
