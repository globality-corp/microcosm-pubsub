"""
SQS envelope tests.

"""
from json import dumps

from hamcrest import (
    assert_that,
    equal_to,
    is_,
)

from microcosm.api import create_object_graph
from microcosm_pubsub.conventions import created
from microcosm_pubsub.envelope import (
    CodecSQSEnvelope,
    RawSQSEnvelope,
)


def test_raw_sqs_envelope():
    graph = create_object_graph("example", testing=True)
    consumer = None
    message_id = "message_id"
    receipt_handle = "receipt_handle"
    envelope = RawSQSEnvelope(graph)

    sqs_message = envelope.parse_raw_message(consumer, dict(
        MessageId=message_id,
        ReceiptHandle=receipt_handle,
        Body=dumps(dict(
            foo="bar",
        )),
    ))

    assert_that(sqs_message.content, is_(equal_to(dict(foo="bar"))))
    assert_that(sqs_message.media_type, is_(equal_to("application/json")))
    assert_that(sqs_message.message_id, is_(equal_to(message_id)))
    assert_that(sqs_message.receipt_handle, is_(equal_to(receipt_handle)))


def test_codec_sqs_envelope():
    graph = create_object_graph("example", testing=True)
    consumer = None
    message_id = "message_id"
    receipt_handle = "receipt_handle"
    envelope = CodecSQSEnvelope(graph)

    media_type = created("foo")
    uri = "http://foo/id"

    sqs_message = envelope.parse_raw_message(consumer, dict(
        MessageId=message_id,
        ReceiptHandle=receipt_handle,
        Body=dumps(dict(
            Message=dumps(dict(
                mediaType=media_type,
                foo="bar",
                uri=uri,
            )),
        )),
    ))

    assert_that(sqs_message.content, is_(equal_to(dict(
        # NB: no foo key here because it's not part of the schema
        media_type=media_type,
        uri=uri,
    ))))
    assert_that(sqs_message.media_type, is_(equal_to(media_type)))
    assert_that(sqs_message.message_id, is_(equal_to(message_id)))
    assert_that(sqs_message.receipt_handle, is_(equal_to(receipt_handle)))
