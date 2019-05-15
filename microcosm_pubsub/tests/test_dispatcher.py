"""
Dispatcher tests.

"""
from json import dumps

from hamcrest import assert_that, greater_than, has_properties

from microcosm_pubsub.conventions import created
from microcosm_pubsub.message import SQSMessage
from microcosm_pubsub.result import MessageHandlingResultType
from microcosm_pubsub.tests.fixtures import DerivedSchema, ExampleDaemon


MESSAGE_ID = "message-id"


class TestDispatcher:

    def setup(self):
        self.daemon = ExampleDaemon.create_for_testing()
        self.graph = self.daemon.graph

        self.dispatcher = self.graph.sqs_message_dispatcher

        self.content = dict(bar="baz", uri="http://example.com")
        self.message = SQSMessage(
            approximate_receive_count=0,
            consumer=self.graph.sqs_consumer,
            content=self.content,
            media_type=DerivedSchema.MEDIA_TYPE,
            message_id=MESSAGE_ID,
            receipt_handle=None,
        )
        self.graph.sqs_consumer.sqs_client.reset_mock()

    def test_handle_message_succeeded(self):
        result = self.dispatcher.handle_message(
            message=self.message,
            bound_handlers=self.daemon.bound_handlers,
        )
        assert_that(
            result,
            has_properties(
                elapsed_time=greater_than(0.0),
                result=MessageHandlingResultType.SUCCEEDED,
            ),
        )

    def test_handle_message_ignored(self):
        """
        Unsupported media types are ignored.

        """
        self.message.media_type = created("bar")
        assert_that(
            self.dispatcher.handle_message(
                message=self.message,
                bound_handlers=self.daemon.bound_handlers,
            ),
            has_properties(
                elapsed_time=greater_than(0.0),
                result=MessageHandlingResultType.IGNORED,
            ),
        )

    def test_handle_message_expired(self):
        """
        Messages whose TTL have reached 0 are ignored

        """
        self.message.content = dict(
            opaque_data={
                "X-Request-Ttl": "0",
            },
        )
        assert_that(
            self.dispatcher.handle_message(
                message=self.message,
                bound_handlers=self.daemon.bound_handlers,
            ),
            has_properties(
                elapsed_time=greater_than(0.0),
                result=MessageHandlingResultType.EXPIRED,
            ),
        )

    def test_handle_message_published_time(self):
        """
        Messages that have a published time header calculate that time

        """
        self.message.content = dict(
            opaque_data={
                "X-Request-Published": "0",
            },
        )
        assert_that(
            self.dispatcher.handle_message(
                message=self.message,
                bound_handlers=self.daemon.bound_handlers,
            ),
            has_properties(
                handle_start_time=greater_than(0.0),
                elapsed_time=greater_than(0.0),
                result=MessageHandlingResultType.SUCCEEDED,
            ),
        )

    def test_handle_message_reached_processing_limit(self):
        """
        Messages that have reached the processing limit are ignored

        """
        self.dispatcher.max_processing_attempts = 1
        self.message.approximate_receive_count = 2
        assert_that(
            self.dispatcher.handle_message(
                message=self.message,
                bound_handlers=self.daemon.bound_handlers,
            ),
            has_properties(
                elapsed_time=greater_than(0.0),
                result=MessageHandlingResultType.SKIPPED,
            ),
        )

    def test_handle_batch_with_message_succeeded(self):
        batch = [
            dict(
                MessageId=MESSAGE_ID,
                ReceiptHandle="receipt-handle",
                Body=dumps(dict(
                    Message=dumps(dict(
                        mediaType=DerivedSchema.MEDIA_TYPE,
                        bar="baz",
                        uri="http://example.com",
                    )),
                )))
        ]
        self.dispatcher.sqs_consumer.sqs_client.receive_message.value = dict(Messages=batch)
        result = self.dispatcher.handle_batch(
            bound_handlers=self.daemon.bound_handlers,
        )[0]
        assert_that(
            result,
            has_properties(
                elapsed_time=greater_than(0.0),
                result=MessageHandlingResultType.SUCCEEDED,
            ),
        )
