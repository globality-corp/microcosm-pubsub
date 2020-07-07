"""
Testing Lambda handler
"""
from hamcrest import (
    assert_that,
    is_,
)

from microcosm_pubsub.tests.fixtures import ExampleDaemon


MESSAGE_ID = "message-id"
RECEIPT_HANDLE = "receipt-handle"


def test_lambda_handler():
    """
    Tests if handler is created
    """
    handler = ExampleDaemon.make_lambda_handler()
    evt = {"warm": 1}
    assert_that(handler(evt, {}), is_("warming up"))
