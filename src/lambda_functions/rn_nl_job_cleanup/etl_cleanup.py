import boto3, botocore
from botocore import exceptions
from datetime import datetime, timedelta


class Cleanup(object):
    """
    Class object to clear all the s3 locations of all the ETL output paths for the target date
    """
    def _cleanup(self, logger, env, prefix, glue_file_path):
        """
        Clean up the S3 files at the given prefix
        :param logger: logger method
        :param env: environment
        :param prefix: s3 prefix to clean up
        :param glue_file_path: glue file path
        :return: None
        """
        try:
            s3 = boto3.resource("s3")
            bucket = s3.Bucket("rdc-recommended-notifications-" + env)
            bucket.objects.filter(Prefix=prefix).delete()
            s3.Object("rdc-recommended-notifications-" + env, glue_file_path).delete()
            logger.info("Deleted files in prefix: {}".format(prefix))
        except botocore.exceptions.ClientError as e:
            logger.error("Botocore Error: {}".format(e.response['Error']))
        except Exception as e:
            logger.error("Error: {}".format(e.__traceback__))

    def cleanup_new_listings(self, logger, env, target_date):
        """
        Cleanup the paths for the target date to avoid future failures due to any previous runs'
        which might have any corrupted data for the target date
        :return:
        """
        users_path = "notifications/new_listings/nl_users/target_date={}".format(target_date)
        glue_file_path = "{}_$folder$".format(users_path)
        self._cleanup(logger, env, users_path, glue_file_path)

        listings_path = "notifications/new_listings/nl_listings/target_date={}".format(target_date)
        glue_file_path = "{}_$folder$".format(listings_path)
        self._cleanup(logger, env, listings_path, glue_file_path)

        candidates_path = "notifications/new_listings/nl_candidates/target_date={}".format(target_date)
        glue_file_path = "{}_$folder$".format(candidates_path)
        self._cleanup(logger, env, candidates_path, glue_file_path)

        candidates_features_path = "notifications/new_listings/nl_features/target_date={}".format(target_date)
        glue_file_path = "{}_$folder$".format(candidates_path)
        self._cleanup(logger, env, candidates_features_path, glue_file_path)

        candidates_ranked_path = "notifications/new_listings/nl_candidates_ranked/target_date={}".format(target_date)
        glue_file_path = "{}_$folder$".format(candidates_path)
        self._cleanup(logger, env, candidates_ranked_path, glue_file_path)

        eligible_candidates_path = "notifications/new_listings/nl_eligible_candidates/target_date={}".format(target_date)
        glue_file_path = "{}_$folder$".format(candidates_path)
        self._cleanup(logger, env, eligible_candidates_path, glue_file_path)

        eligible_users_path = "notifications/new_listings/nl_eligible_users/target_date={}".format(target_date)
        glue_file_path = "{}_$folder$".format(candidates_path)
        self._cleanup(logger, env, eligible_users_path, glue_file_path)

        bucketed_candidates_path = "notifications/new_listings/nl_bucketed_candidates/target_date={}".format(target_date)
        glue_file_path = "{}_$folder$".format(candidates_path)
        self._cleanup(logger, env, bucketed_candidates_path, glue_file_path)
        
        app_eligible_candidates_path = "notifications/new_listings/nl_eligible_candidates_app/target_date={}".format(target_date)
        glue_file_path = "{}_$folder$".format(candidates_path)
        self._cleanup(logger, env, app_eligible_candidates_path, glue_file_path)

        target_date_metrics = datetime.strftime(datetime.now() - timedelta(days=8), '%Y%m%d')
        target_date_metrics = '20210304'
        metrics_path = "notifications/new_listings/nl_metrics/target_date={}".format(target_date_metrics)
        glue_file_path = "{}_$folder$".format(metrics_path)
        self._cleanup(logger, env, metrics_path, glue_file_path)

        app_dom_zip_eligible_path = "notifications/app_eligible_users/target_date={}".format(target_date)
        glue_file_path = "{}_$folder$".format(app_dom_zip_eligible_path)
        self._cleanup(logger, env, app_dom_zip_eligible_path, glue_file_path)

        app_dom_zip_bucketed_path = "notifications/app_bucketed_users/target_date={}".format(target_date)
        glue_file_path = "{}_$folder$".format(app_dom_zip_bucketed_path)
        self._cleanup(logger, env, app_dom_zip_bucketed_path, glue_file_path)
