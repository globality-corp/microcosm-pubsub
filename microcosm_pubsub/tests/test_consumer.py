"""
Consumer tests.

"""
from json import dumps

from hamcrest import (
    assert_that,
    equal_to,
    has_length,
    is_,
)

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
        AttributeNames=[
            "ApproximateReceiveCount",
        ],
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


def test_raw_consume():
    """
    Test that messages sent via raw message delivery can be handled

    """
    graph = ExampleDaemon.create_for_testing().graph
    # simulate the response structure
    graph.sqs_consumer.sqs_client.receive_message.return_value = dict(Messages=[
        dict(
            MessageId=MESSAGE_ID,
            ReceiptHandle=RECEIPT_HANDLE,
            MD5OfBody="7efaa8404863d47c51ed0e20b9014aec",
            Body=dumps(dict(
                data="data",
                mediaType=DerivedSchema.MEDIA_TYPE,
            )),
        ),
    ])

    messages = graph.sqs_consumer.consume()

    # SQS should have been called
    graph.sqs_consumer.sqs_client.receive_message.assert_called_with(
        AttributeNames=[
            "ApproximateReceiveCount",
        ],
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
