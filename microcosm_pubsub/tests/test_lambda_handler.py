"""
Testing Lambda handler
"""
from json import dumps

from hamcrest import assert_that, is_

from microcosm_pubsub.tests.fixtures import DerivedSchema, ExampleDaemon


MESSAGE_ID = "message-id"
RECEIPT_HANDLE = "receipt-handle"


def test_lambda_handler_warmup_event():
    """
    Tests if handler is created
    """
    handler = ExampleDaemon.make_lambda_handler()
    evt = {"warm": 1}
    assert_that(handler(evt, {}), is_("warming up"))


def test_lambda_handler_sqs_event():
    """
    Tests if handler is created
    """
    handler = ExampleDaemon.make_lambda_handler()
    message = dict(
        messageId=MESSAGE_ID,
        receiptHandle=RECEIPT_HANDLE,
        md5OfBody="7efaa8404863d47c51ed0e20b9014aec",
        body=dumps(
            dict(
                data="data",
                mediaType=DerivedSchema.MEDIA_TYPE,
            )
        ),
    )
    event = {
        "Records": [message]
    }

    assert_that(handler(event, {}), is_("event processed"))
