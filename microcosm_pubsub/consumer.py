"""
Message consumer.

"""
from os.path import exists
from urllib.parse import urlparse

from boto3 import Session
from microcosm.api import defaults, typed
from microcosm_logging.decorators import logger

from microcosm_pubsub.backoff import BackoffPolicy
from microcosm_pubsub.reader import SQSFileReader, SQSStdInReader


STDIN = "STDIN"


def is_file(url):
    if exists(url):
        return True

    return exists(urlparse(url).path)


@logger
class SQSConsumer:
    """
    Consume message from a (single) SQS queue.

    """
    def __init__(
        self,
        sqs_client,
        sqs_envelope,
        sqs_queue_url,
        limit,
        wait_seconds,
        backoff_policy,
    ):
        self.sqs_client = sqs_client
        self.sqs_envelope = sqs_envelope
        self.sqs_queue_url = sqs_queue_url
        self.limit = limit
        self.wait_seconds = wait_seconds
        self.backoff_policy = backoff_policy

    def consume(self):
        """
        Consume a batch of messages.

        :returns: a list of `SQSMessage`
        """
        return [
            self.sqs_envelope.parse_raw_message(self, raw_message)
            for raw_message in self.sqs_client.receive_message(
                AttributeNames=[
                    "ApproximateReceiveCount",
                ],
                MaxNumberOfMessages=self.limit,
                QueueUrl=self.sqs_queue_url,
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

    def nack(self, message, visibility_timeout_seconds=None):
        """
        Acknowledge that a message was NOT processed successfully.

        (Re)sets the retry visibility timeout on the message.

        There are three cases here:
         1.  We raised `Nack`; in this case, we explicitly wish to reprocess the message in the
             near future; setting the visibility timeout is the right thing to do.

         2a. We raised some non-`Nack` error and the `BackoffPolicy` HAS a configured value for its
             `message_retry_visibility_timeout_seconds` meaning an operator has chosen to reprocess
             all messages within that time limit; setting the visibility timeout is the right thing to do.

         2b. We raised some non-`Nack` error and the `BackoffPolicy` DOES NOT HAVE a configured value for its
             `message_retry_visibility_timeout_seconds` config meaning that the system will fallback to
             its default behavior. We can either fallback to the SQS queue default (usually 30s) -- meaning
             not setting the visibility timeout -- or we can enforce a default in this library -- using
             the sqs consumer's config and override the SQS queue's visibility with a known, smallish value.

             We choose the latter under the assumption that 30s is too long to reprocess most messages
             as a defult and that long-running handlers will be configured accordingly (see: 2a).

        Therefore: we always invoke `change_message_visibility`

        """
        timeout = self.backoff_policy.compute_backoff_timeout(message, visibility_timeout_seconds)
        self.sqs_client.change_message_visibility(
            QueueUrl=self.sqs_queue_url,
            ReceiptHandle=message.receipt_handle,
            VisibilityTimeout=timeout,
        )


def configure_sqs_client(graph):
    endpoint_url = graph.config.sqs_consumer.endpoint_url
    profile_name = graph.config.sqs_consumer.profile_name
    region_name = graph.config.sqs_consumer.region_name
    session = Session(profile_name=profile_name)
    return session.client(
        "sqs",
        endpoint_url=endpoint_url,
        region_name=region_name,
    )


@defaults(
    endpoint_url=None,
    profile_name=None,
    region_name=None,
    # backoff policy
    backoff_policy="NaiveBackoffPolicy",
    # SQS will not return more than ten messages at a time
    limit=typed(int, default_value=10),
    # SQS will only return a few messages at time unless long polling is enabled (>0)
    wait_seconds=typed(int, default_value=1),
    # On error, change the visibility timeout when nacking
    message_retry_visibility_timeout_seconds=typed(int, default_value=5),
)
def configure_sqs_consumer(graph):
    """
    Configure an SQS consumer.

    """
    sqs_queue_url = graph.config.sqs_consumer.sqs_queue_url

    if graph.metadata.testing or sqs_queue_url == "test":
        from unittest.mock import MagicMock
        sqs_client = MagicMock()
    elif sqs_queue_url == STDIN:
        sqs_client = SQSStdInReader()
    elif is_file(sqs_queue_url):
        sqs_client = SQSFileReader(sqs_queue_url)
    else:
        sqs_client = configure_sqs_client(graph)

    backoff_policy_class = BackoffPolicy.choose_backoff_policy(
        graph.config.sqs_consumer.backoff_policy,
    )

    backoff_policy = backoff_policy_class(
        message_retry_visibility_timeout_seconds=graph.config.sqs_consumer.message_retry_visibility_timeout_seconds,
    )

    return SQSConsumer(
        backoff_policy=backoff_policy,
        limit=graph.config.sqs_consumer.limit,
        sqs_client=sqs_client,
        sqs_envelope=graph.sqs_envelope,
        sqs_queue_url=sqs_queue_url,
        wait_seconds=graph.config.sqs_consumer.wait_seconds,
    )
