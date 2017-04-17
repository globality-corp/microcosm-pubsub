"""
Consumer tests.

"""
from json import dumps

from hamcrest import (
    assert_that,
    equal_to,
    is_,
    has_length,
)
from mock import patch

from microcosm_pubsub.errors import Nack
from microcosm_pubsub.message import SQSMessage
from microcosm_pubsub.tests.fixtures import DerivedSchema, ExampleDaemon


MESSAGE_ID = "message-id"
RECEIPT_HANDLE = "receipt-handle"


def test_consume():
    """
    Consumer delegates to SQS client.

    """
    graph = ExampleDaemon.create_for_testing().graph
    # simulate the response structure
    graph.sqs_consumer.sqs_client.receive_message.return_value = dict(Messages=[dict(
        MessageId=MESSAGE_ID,
        ReceiptHandle=RECEIPT_HANDLE,
        MD5OfBody="7efaa8404863d47c51ed0e20b9014aec",
        Body=dumps(dict(
            Message=dumps(dict(
                data="data",
                mediaType=DerivedSchema.MEDIA_TYPE,
            )),
        ))),
    ])

    messages = graph.sqs_consumer.consume()

    # SQS should have been called
    graph.sqs_consumer.sqs_client.receive_message.assert_called_with(
        QueueUrl="queue",
        MaxNumberOfMessages=10,
        WaitTimeSeconds=1,
    )

    # and response translated properly
    assert_that(messages, has_length(1))
    assert_that(messages[0].consumer, is_(equal_to(graph.sqs_consumer)))
    assert_that(messages[0].message_id, is_(equal_to(MESSAGE_ID)))
    assert_that(messages[0].receipt_handle, is_(equal_to(RECEIPT_HANDLE)))
    assert_that(messages[0].content, is_(equal_to(dict(
        data="data",
        media_type=DerivedSchema.MEDIA_TYPE,
    ))))


def test_nack_without_visibility_timeout():
    """
    Consumer passes

    """
    graph = ExampleDaemon.create_for_testing().graph
    message = SQSMessage(
        consumer=graph.sqs_consumer,
        content=None,
        media_type=DerivedSchema.MEDIA_TYPE,
        message_id=MESSAGE_ID,
        receipt_handle=RECEIPT_HANDLE,
    )
    message.nack()
    graph.sqs_consumer.sqs_client.change_message_visibility.assert_not_called()


def test_nack_with_visibility_timeout():
    """
    Consumer delegates to SQS client with visibility timeout

    """
    visibility_timeout_seconds = 2
    graph = ExampleDaemon.create_for_testing().graph
    message = SQSMessage(
        consumer=graph.sqs_consumer,
        content=None,
        media_type=DerivedSchema.MEDIA_TYPE,
        message_id=MESSAGE_ID,
        receipt_handle=RECEIPT_HANDLE,
    )
    with patch.object(graph.sqs_consumer, "visibility_timeout_seconds", visibility_timeout_seconds):
        message.nack()
        graph.sqs_consumer.sqs_client.change_message_visibility.assert_called_with(
            QueueUrl="queue",
            ReceiptHandle=RECEIPT_HANDLE,
            VisibilityTimeout=visibility_timeout_seconds,
        )


def test_nack_with_visibility_timeout_via_exception():
    """
    Consumer raises Nack; calls nack with visibility timeout

    """
    visibility_timeout_seconds = 2
    graph = ExampleDaemon.create_for_testing().graph
    message = SQSMessage(
        consumer=graph.sqs_consumer,
        content=None,
        media_type=DerivedSchema.MEDIA_TYPE,
        message_id=MESSAGE_ID,
        receipt_handle=RECEIPT_HANDLE,
    )
    with patch.object(graph.sqs_consumer, "visibility_timeout_seconds", visibility_timeout_seconds):
        try:
            with message:
                raise Nack(visibility_timeout_seconds)
        except Nack:
            pass

    graph.sqs_consumer.sqs_client.change_message_visibility.assert_called_with(
        QueueUrl="queue",
        ReceiptHandle=RECEIPT_HANDLE,
        VisibilityTimeout=visibility_timeout_seconds,
    )


def test_ack():
    """
    Consumer delegates to SQS client.

    """
    graph = ExampleDaemon.create_for_testing().graph
    message = SQSMessage(
        consumer=graph.sqs_consumer,
        content=None,
        media_type=DerivedSchema.MEDIA_TYPE,
        message_id=MESSAGE_ID,
        receipt_handle=RECEIPT_HANDLE,
    )
    message.ack()
    graph.sqs_consumer.sqs_client.delete_message.assert_called_with(
        QueueUrl="queue",
        ReceiptHandle=RECEIPT_HANDLE,
    )
