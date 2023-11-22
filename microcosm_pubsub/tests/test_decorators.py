"""
Decorator tests.

"""
from hamcrest import (
    assert_that,
    equal_to,
    instance_of,
    is_,
)

from microcosm_pubsub.tests.fixtures import DuckTypeSchema, ExampleDaemon, noop_handler


class TestDecorators:

    def setup_method(self):
        self.daemon = ExampleDaemon.create_for_testing()
        self.graph = self.daemon.graph

    def test_schema_decorators(self):
        assert_that(
            self.graph.pubsub_message_schema_registry.find(DuckTypeSchema.MEDIA_TYPE).schema,
            is_(instance_of(DuckTypeSchema)),
        )

    def test_handles_decorators(self):
        assert_that(
            self.graph.sqs_message_handler_registry.find(
                DuckTypeSchema.MEDIA_TYPE,
                self.daemon.bound_handlers,
            ),
            is_(equal_to(noop_handler)),
        )
