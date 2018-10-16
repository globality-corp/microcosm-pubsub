"""
Context tests.

"""
from hamcrest import (
    assert_that,
    calling,
    has_entries,
    raises,
)
from microcosm_pubsub.errors import TTLExpired
from microcosm_pubsub.tests.fixtures import ExampleDaemon


MESSAGE_ID = "message_id"
MESSAGE_URI = "message_uri"


class TestSQSMessageContext:

    def setup(self):
        self.graph = ExampleDaemon.create_for_testing().graph

    def test_handle(self):
        message = dict(
            opaque_data=dict(foo="bar"),
            uri=MESSAGE_URI,
        )

        with self.graph.opaque.initialize(self.graph.sqs_message_context, message, message_id=MESSAGE_ID):
            assert_that(
                self.graph.opaque.as_dict(),
                has_entries(
                    foo="bar",
                    message_id=MESSAGE_ID,
                    uri=MESSAGE_URI,
                ),
            )

    def test_sets_ttl(self):
        with self.graph.opaque.initialize(self.graph.sqs_message_context, {}, message_id=MESSAGE_ID):
            assert_that(
                self.graph.opaque.as_dict(),
                has_entries({
                    "X-Request-Ttl": "31",
                }),
            )

    def test_updates_ttl(self):
        message = {"opaque_data": {"X-Request-Ttl": "10"}}

        with self.graph.opaque.initialize(self.graph.sqs_message_context, message, message_id=MESSAGE_ID):
            assert_that(
                self.graph.opaque.as_dict(),
                has_entries({
                    "X-Request-Ttl": "9",
                }),
            )

    def test_zero_ttl(self):
        message = {"opaque_data": {"X-Request-Ttl": "0"}}

        assert_that(
            calling(self.graph.sqs_message_context).with_args(message),
            raises(TTLExpired),
        )
