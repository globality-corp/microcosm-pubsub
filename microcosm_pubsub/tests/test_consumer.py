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
from microcosm.caching import NaiveCache

from microcosm_pubsub.reader import SQSJsonReader
from microcosm_pubsub.tests.fixtures import DerivedSchema, ExampleDaemon


MESSAGE_ID = "message-id"
RECEIPT_HANDLE = "receipt-handle"


def create_daemon():
    return ExampleDaemon.create_for_testing().graph


def create_daemon_with_naive_cache():
    # Use NaiveCache here to avoid reusing the graph components between tests,
    # as we're modifying the `sqs_consumer.sqs_client` in some tests
    return ExampleDaemon.create_for_testing(cache=NaiveCache()).graph


def test_consume():
    """
    Consumer delegates to SQS client.

    """
    graph = create_daemon()
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
    graph = create_daemon()
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


def test_json_consume():
    """
    Test that message sent as JSON could be delivered

    """
    graph = create_daemon_with_naive_cache()
    # simulate the response structure
    # replacing MagicMock with real reader
    graph.sqs_consumer.sqs_client = SQSJsonReader(dict(
            MessageId=MESSAGE_ID,
            ReceiptHandle=RECEIPT_HANDLE,
            MD5OfBody="7efaa8404863d47c51ed0e20b9014aec",
            Body=dumps(dict(
                data="data",
                mediaType=DerivedSchema.MEDIA_TYPE,
            )),
        ),
    )

    messages = graph.sqs_consumer.consume()

    # and response translated properly
    assert_that(messages, has_length(1))
    assert_that(messages[0].consumer, is_(equal_to(graph.sqs_consumer)))
    assert_that(messages[0].message_id, is_(equal_to(MESSAGE_ID)))
    assert_that(messages[0].receipt_handle, is_(equal_to(RECEIPT_HANDLE)))
    assert_that(messages[0].content, is_(equal_to(dict(
        data="data",
        media_type=DerivedSchema.MEDIA_TYPE,
    ))))
