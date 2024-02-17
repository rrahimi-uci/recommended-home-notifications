import logging
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

    def cleanup_recommended_homes(self, logger, env, target_date):
        """
        Cleanup the paths for the target date to avoid failures due to any previous runs'
        which might have any corrupted data for the target date
        :return:
        """
        users_path = "notifications/recommended_homes/rh_users/target_date={}".format(target_date)
        glue_file_path = "{}_$folder$".format(users_path)
        self._cleanup(logger, env, users_path, glue_file_path)

        recommended_listings_path = "notifications/recommended_homes/rh_listings/target_date={}".format(target_date)
        glue_file_path = "{}_$folder$".format(recommended_listings_path)
        self._cleanup(logger, env, recommended_listings_path, glue_file_path)

        recommendations_path = "notifications/recommended_homes/rh_recommendations/target_date={}".format(target_date)
        glue_file_path = "{}_$folder$".format(recommendations_path)
        self._cleanup(logger, env, recommendations_path, glue_file_path)

        recommended_homes_path = "notifications/recommended_homes/rh_candidates_ranked/target_date={}".format(target_date)
        glue_file_path = "{}_$folder$".format(recommended_homes_path)
        self._cleanup(logger, env, recommended_homes_path, glue_file_path)

        target_date_metrics = datetime.strftime(datetime.now() - timedelta(days=8), '%Y%m%d')
        target_date_metrics = '20210304'
        recommended_homes_path = "notifications/recommended_homes/rh_metrics/target_date={}".format(target_date_metrics)
        glue_file_path = "{}_$folder$".format(recommended_homes_path)
        self._cleanup(logger, env, recommended_homes_path, glue_file_path)

        recommended_homes_path = "notifications/recommended_homes/rh_eligible_users/target_date={}".format(target_date)
        glue_file_path = "{}_$folder$".format(recommended_homes_path)
        self._cleanup(logger, env, recommended_homes_path, glue_file_path)

        recommended_homes_path = "notifications/recommended_homes/rh_eligible_candidates/target_date={}".format(target_date)
        glue_file_path = "{}_$folder$".format(recommended_homes_path)
        self._cleanup(logger, env, recommended_homes_path, glue_file_path)

        recommended_homes_path = "notifications/recommended_homes/rh_bucketed_candidates/target_date={}".format(target_date)
        glue_file_path = "{}_$folder$".format(recommended_homes_path)
        self._cleanup(logger, env, recommended_homes_path, glue_file_path)

        app_eligible_candidates_path = "notifications/recommended_homes/rh_eligible_candidates_app/target_date={}".format(target_date)
        glue_file_path = "{}_$folder$".format(app_eligible_candidates_path)
        self._cleanup(logger, env, app_eligible_candidates_path, glue_file_path)