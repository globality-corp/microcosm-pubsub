"""
SQSMessageContext tests.

"""
from hamcrest import (
    assert_that,
    has_entries,
)

from microcosm_pubsub.message import SQSMessage
from microcosm_pubsub.tests.fixtures import ExampleDaemon


MESSAGE_ID = "message_id"
MESSAGE_URI = "message_uri"


class TestSQSMessageContext:

    def setup(self):
        self.graph = ExampleDaemon.create_for_testing().graph
        self.message = SQSMessage(
            consumer=self.graph.sqs_consumer,
            content=dict(
                opaque_data=dict(
                    foo="bar",
                ),
                uri=MESSAGE_URI,
            ),
            media_type=None,
            message_id=MESSAGE_ID,
            receipt_handle=None,
        )

    def test_includes_opaque_data(self):
        with self.graph.opaque.initialize(self.graph.sqs_message_context, self.message):
            assert_that(
                self.graph.opaque.as_dict(),
                has_entries(
                    foo="bar",
                ),
            )

    def test_includes_message_id(self):
        with self.graph.opaque.initialize(self.graph.sqs_message_context, self.message):
            assert_that(
                self.graph.opaque.as_dict(),
                has_entries(
                    message_id=MESSAGE_ID,
                ),
            )

    def test_includes_message_uri(self):
        with self.graph.opaque.initialize(self.graph.sqs_message_context, self.message):
            assert_that(
                self.graph.opaque.as_dict(),
                has_entries(
                    uri=MESSAGE_URI,
                ),
            )

    def test_sets_initial_ttl(self):
        with self.graph.opaque.initialize(self.graph.sqs_message_context, self.message):
            assert_that(
                self.graph.opaque.as_dict(),
                has_entries({
                    "X-Request-Ttl": "31",
                }),
            )

    def test_updates_existing_ttl(self):
        self.message.opaque_data["X-Request-Ttl"] = "10"

        with self.graph.opaque.initialize(self.graph.sqs_message_context, self.message):
            assert_that(
                self.graph.opaque.as_dict(),
                has_entries({
                    "X-Request-Ttl": "9",
                }),
            )

    def test_handle_existing_opaque_message_id(self):
        self.message = SQSMessage(
            consumer=self.graph.sqs_consumer,
            content=dict(
                opaque_data=dict(
                    foo="bar",
                    message_id="opaque_message_id",
                ),
                uri=MESSAGE_URI,
            ),
            media_type=None,
            message_id=MESSAGE_ID,
            receipt_handle=None,
        )

        with self.graph.opaque.initialize(self.graph.sqs_message_context, self.message):
            assert_that(
                self.graph.opaque.as_dict(),
                has_entries(
                    message_id=MESSAGE_ID,
                ),
            )
