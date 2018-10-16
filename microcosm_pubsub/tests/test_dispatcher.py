"""
Dispatcher tests.

"""
from hamcrest import (
    assert_that,
    calling,
    contains,
    greater_than,
    has_entries,
    has_properties,
    raises,
)

from microcosm_pubsub.context import TTL_KEY
from microcosm_pubsub.conventions import created
from microcosm_pubsub.errors import IgnoreMessage
from microcosm_pubsub.message import SQSMessage
from microcosm_pubsub.result import MessageHandlingResultType
from microcosm_pubsub.tests.fixtures import (
    ExampleDaemon,
    DerivedSchema,
)


MESSAGE_ID = "message-id"


class TestDispatcher:

    def setup(self):
        self.daemon = ExampleDaemon.create_for_testing()
        self.graph = self.daemon.graph

        self.dispatcher = self.graph.sqs_message_dispatcher

        self.content = dict(bar="baz", uri="http://example.com")
        self.message = SQSMessage(
            consumer=self.graph.sqs_consumer,
            content=self.content,
            media_type=DerivedSchema.MEDIA_TYPE,
            message_id=MESSAGE_ID,
            receipt_handle=None,
        )
        self.graph.sqs_consumer.sqs_client.reset_mock()

    def test_handle_or_ignore_message_handles_message(self):
        result = self.dispatcher.handle_or_ignore_message(
            message=self.message,
            bound_handlers=self.daemon.bound_handlers,
        )
        assert_that(
            result,
            has_properties(
                result=MessageHandlingResultType.SUCCEEDED,
            ),
        )

    def test_handle_or_ignore_message_ignores_unsupported_media_type(self):
        """
        Unsupported media types are ignored.

        """
        assert_that(
            calling(self.dispatcher.handle_or_ignore_message).with_args(
                message=SQSMessage(
                    consumer=None,
                    content=self.content,
                    media_type=created("bar"),
                    message_id=MESSAGE_ID,
                    receipt_handle=None,
                ),
                bound_handlers=self.daemon.bound_handlers,
            ),
            raises(IgnoreMessage),
        )

    def test_handle_message_succeeded(self):
        result = self.dispatcher.handle_message(
            message=self.message,
            content=self.content,
            handler=self.graph.noop_handler,
        )
        assert_that(
            result,
            contains(
                True,
                greater_than(0.0),
            ),
        )

    def test_message_context_default_values(self):
        with self.dispatcher.message_context(
            message=self.message,
            content=self.content,
            handler=self.graph.noop_handler,
        ) as extra:
            assert_that(
                extra,
                has_entries({
                    "uri": "http://example.com",
                    TTL_KEY: "31",
                    "message_id": MESSAGE_ID,
                }),
            )
