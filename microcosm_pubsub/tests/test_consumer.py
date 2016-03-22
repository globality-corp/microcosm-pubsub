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
from microcosm.api import create_object_graph

from microcosm_pubsub.consumer import SQSMessage
from microcosm_pubsub.tests.fixtures import (
    FOO_QUEUE_URL,
    FOO_MEDIA_TYPE,
    FooSchema,
    MESSAGE_ID,
    RECEIPT_HANDLE,
)


def test_consume():
    """
    Consumer delegates to SQS client.

    """
    def loader(metadata):
        return dict(
            sqs_consumer=dict(
                sqs_queue_url=FOO_QUEUE_URL,
            ),
            pubsub_message_codecs=dict(
                default=FooSchema,
            ),
        )

    graph = create_object_graph("example", testing=True, loader=loader)
    # simulate the response structure
    graph.sqs_consumer.sqs_client.receive_message.return_value = dict(Messages=[dict(
        MessageId=MESSAGE_ID,
        ReceiptHandle=RECEIPT_HANDLE,
        MD5OfBody="7efaa8404863d47c51ed0e20b9014aec",
        Body=dumps(dict(
            Message=dumps(dict(
                bar="baz",
                mediaType=FOO_MEDIA_TYPE,
            )),
        ))),
    ])

    messages = graph.sqs_consumer.consume()

    # SQS should have been called
    graph.sqs_consumer.sqs_client.receive_message.assert_called_with(
        QueueUrl='foo-queue-url',
        MaxNumberOfMessages=10,
        WaitTimeSeconds=0,
    )

    # and response translated properly
    assert_that(messages, has_length(1))
    assert_that(messages[0].consumer, is_(equal_to(graph.sqs_consumer)))
    assert_that(messages[0].message_id, is_(equal_to(MESSAGE_ID)))
    assert_that(messages[0].receipt_handle, is_(equal_to(RECEIPT_HANDLE)))
    assert_that(messages[0].content, is_(equal_to(dict(
        bar="baz",
        media_type=FOO_MEDIA_TYPE,
    ))))


def test_ack():
    """
    Consumer delegates to SQS client.

    """
    def loader(metadata):
        return dict(
            sqs_consumer=dict(
                sqs_queue_url=FOO_QUEUE_URL,
            ),
            pubsub_message_codecs=dict(
                default=FooSchema,
            ),
        )

    graph = create_object_graph("example", testing=True, loader=loader)
    message = SQSMessage(
        consumer=graph.sqs_consumer,
        message_id=MESSAGE_ID,
        receipt_handle=RECEIPT_HANDLE,
        content=None,
    )
    message.ack()
    graph.sqs_consumer.sqs_client.delete_message.assert_called_with(
        QueueUrl='foo-queue-url',
        ReceiptHandle=RECEIPT_HANDLE,
    )
