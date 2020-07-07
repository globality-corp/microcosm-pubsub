"""
Testing Lambda handler
"""
from json import dumps, loads
from hamcrest import (
    assert_that,
    is_,
    instance_of,
)

from microcosm_pubsub.reader import SQSJsonReader
from microcosm_pubsub.tests.fixtures import DerivedSchema, ExampleDaemon


MESSAGE_ID = "message-id"
RECEIPT_HANDLE = "receipt-handle"


def test_lambda_handler():
    """
    Tests if handler is created
    """
    handler = ExampleDaemon.make_lambda_handler()
    evt = {"warm": 1}
    assert_that(handler(evt, {}), is_("warming up"))
