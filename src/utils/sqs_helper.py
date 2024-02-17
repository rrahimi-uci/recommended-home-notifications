import boto3
import json
from botocore.exceptions import ClientError
from botocore.client import Config

from src.utils import logger
from src.utils.schema import Schema
from src.utils import constants

log, _ = logger.getlogger()

config = Config(connect_timeout=180, max_pool_connections=5000)
sqs_client = boto3.resource('sqs', region_name=constants.DeploymentConstants.AWS_REGION, config=config)


def send_message(message_body, event_template, queue_name, message_attributes=None):
    """
    Send a message to an AWS SQS queue

    :param message_body: Body of the message
    :param event_template: Event template for the type of the recommendation like new listings, recommended homes etc
    :param queue_name: Name of the sqs queue
    :param message_attributes: Custom attributes of the message
    :return: Response object of sending message on the Queue
    """
    if not message_attributes:
        message_attributes = {}

    try:
        message_body = json.loads(message_body)
        event_template['payload']['content']['recommendations'] = message_body["recommendations"]
        event_template['meta']['experiment']['variation'] = message_body["variation"]
        event_template['meta']['experiment']['key'] = message_body["experiment_name"]
        if len(message_body["user_id"]) < 25:
            event_template['payload']['recipients'][0]['memberId'] = message_body["user_id"]
            event_template['payload']['recipients'][0]['visitorId'] = None
        else:
            event_template['payload']['recipients'][0]['visitorId'] = message_body["user_id"]
            event_template['payload']['recipients'][0]['memberId'] = None
        queue = sqs_client.get_queue_by_name(QueueName=queue_name)
        response = queue.send_message(
            MessageBody=json.dumps(event_template),
            MessageAttributes=message_attributes
        )

        return response['MessageId'], 1
    except ClientError as error:
        log.error("Send message failed: {}".format(error.__str__()))
        return "NoID", 0


def create_queue(self, attributes=None):
    """
    Creates an AWS SQS queue

    :param attributes: The attributes of the queue
    :return: Queue object
    """
    if not attributes:
        attributes = {}

    try:
        self.queue = sqs_client.create_queue(
            QueueName=self.name,
            Attributes=attributes
        )
        log.info("Created queue '{}' with URL={}".format(self.name, self.queue.url))
    except ClientError as error:
        log.exception("Couldn't create queue named '{}'.".format(self.name))
        raise error
    else:
        return self.queue


def get_queue(self):
    """
    Gets an AWS SQS queue by name

    :return: queue object
    """
    try:
        self.queue = sqs_client.get_queue_by_name(QueueName=self.name)
        log.info("Got queue '{}' with URL={}".format(self.name, self.queue.url))
    except ClientError as error:
        log.exception("Couldn't get queue named {}.".format(self.name))
        raise error
    else:
        return self.queue


def get_schema(event_type):
    if event_type == "recommended_homes":
        return Schema.sqs_recommended_homes_schema
    elif event_type == "new_listings":
        return Schema.sqs_new_listings_schema
    elif event_type == "recommended_homes_app":
        return Schema.sqs_recommended_homes_app_schema
    elif event_type == "new_listings_app":
        return Schema.sqs_new_listings_app_schema
    elif event_type == "dom_zip_app":
        return Schema.sqs_dom_zip_app_schema
    elif event_type == "recommended_homes_email":
        return Schema.sqs_recommended_homes_email_schema
    elif event_type == "new_listings_email":
        return Schema.sqs_new_listings_email_schema
    else:
        return Schema.sqs_recommended_homes_schema
