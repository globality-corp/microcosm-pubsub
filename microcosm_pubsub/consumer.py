"""
Message consumer.

"""
from json import loads
from hashlib import md5

from boto3 import client
from microcosm.api import defaults


class SQSMessage(object):
    """
    SQS message wrapper.

    """
    def __init__(self, consumer, content, message_id, receipt_handle):
        self.consumer = consumer
        self.content = content
        self.message_id = message_id
        self.receipt_handle = receipt_handle

    def ack(self):
        """
        Acknowledge this message was processed successfully.

        """
        self.consumer.ack(self)

    def nack(self):
        """
        Acknowledge this message was NOT processed successfully.

        """
        self.consumer.nack(self)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        if type is None:
            self.ack()
        else:
            self.nack()

    @classmethod
    def from_sqs(cls, consumer, sqs_message, validate_md5=False):
        """
        Create a message from SQS data.

        """
        message_id = sqs_message["MessageId"]
        receipt_handle = sqs_message["ReceiptHandle"]
        body = sqs_message["Body"]

        if validate_md5:
            cls.validate_body(sqs_message, body)

        message = loads(body)["Message"]
        content = cls.parse_content(consumer, message)

        return cls(
            consumer=consumer,
            content=content,
            message_id=message_id,
            receipt_handle=receipt_handle,
        )

    @classmethod
    def parse_content(cls, consumer, message):
        """
        Parse and validate the message.

        """
        base_message = consumer.pubsub_message_codecs["_"].decode(message)
        media_type = base_message["mediaType"]
        content = consumer.pubsub_message_codecs[media_type].decode(message)
        return content

    @classmethod
    def validate_body(cls, sqs_message, body):
        """
        Validate the message body.

        Just checks for tampering; schema validation occurs once we know the type of message.

        """
        expected_md5_of_body = sqs_message["MD5OfBody"]
        actual_md5_of_body = md5(body).hexdigest()
        if expected_md5_of_body != actual_md5_of_body:
            raise Exception("MD5 validation failed. Expected: {} Actual: {}".format(
                expected_md5_of_body,
                actual_md5_of_body,
            ))


class SQSConsumer(object):
    """
    Consume message from a (single) SQS queue.

    """
    def __init__(self, sqs_client, sqs_queue_url, pubsub_message_codecs, limit, wait_seconds):
        self.sqs_client = sqs_client
        self.sqs_queue_url = sqs_queue_url
        self.pubsub_message_codecs = pubsub_message_codecs
        self.limit = limit
        self.wait_seconds = wait_seconds

    def consume(self):
        """
        Consume a batch of messages.

        :returns: a list of `SQSMessage`
        """
        return [
            SQSMessage.from_sqs(self, message)
            for message in self.sqs_client.receive_message(
                QueueUrl=self.sqs_queue_url,
                MaxNumberOfMessages=self.limit,
                WaitTimeSeconds=self.wait_seconds,
            ).get("Messages", [])
        ]

    def ack(self, message):
        """
        Acknowledge that a message was processed successfully.

        Deletes the message from the queue.

        """
        self.sqs_client.delete_message(
            QueueUrl=self.sqs_queue_url,
            ReceiptHandle=message.receipt_handle,
        )

    def nack(self, message):
        """
        Acknowledge that a message was NOT processed successfully.

        Does nothing, allowing queue dead-lettering to take effect.

        """
        pass


@defaults(
    # SQS will not return more than ten messages at a time
    limit=10,
    # SQS will only return a few messages at time unless long polling is enabled (>0)
    wait_seconds=1,
)
def configure_sqs_consumer(graph):
    """
    Configure an SQS consumer.

    """
    sqs_queue_url = graph.config.sqs_consumer.sqs_queue_url
    limit = graph.config.sqs_consumer.limit
    wait_seconds = graph.config.sqs_consumer.wait_seconds

    if graph.metadata.testing:
        from mock import MagicMock
        sqs_client = MagicMock()
    else:
        sqs_client = client("sqs")

    return SQSConsumer(
        sqs_client=sqs_client,
        sqs_queue_url=sqs_queue_url,
        pubsub_message_codecs=graph.pubsub_message_codecs,
        limit=limit,
        wait_seconds=wait_seconds,
    )
