import logging
import os
import sys
from datetime import datetime, timedelta
import etl_cleanup

logger = logging.getLogger('rdc_recommended_notifications')
logging.getLogger().addHandler(logging.StreamHandler(sys.stderr))
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger.setLevel(logging.INFO)

env = os.environ['ENV']
region = os.environ['REGION']
activity = os.environ['ACTIVITY_ARN']


def lambda_handler(event, context):
    cl = etl_cleanup.Cleanup()
    target_date = datetime.strftime(datetime.now() - timedelta(days=1), '%Y%m%d')
    logger.info("Cleanup initiated!")
    cl.cleanup_new_listings(logger, env, target_date)
    logger.info("New Listings Pipeline files cleaned up!")

