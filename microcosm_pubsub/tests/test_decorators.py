"""
Decorator tests.

"""
from hamcrest import (
    assert_that,
    equal_to,
    instance_of,
    is_,
)
from microcosm.api import create_object_graph

from microcosm_pubsub.tests.fixtures import noop_handler, TestSchema


class TestDecorators(object):

    def setup(self):
        self.graph = create_object_graph("test", testing=True)
        self.graph.use(
            "pubsub_message_schema_registry",
            "sqs_message_handler_registry",
        )

    def test_schema_decorators(self):
        assert_that(
            self.graph.pubsub_message_schema_registry.find(TestSchema.MEDIA_TYPE).schema,
            is_(instance_of(TestSchema)),
        )

    def test_handles_decorators(self):
        assert_that(
            self.graph.sqs_message_handler_registry.find(TestSchema.MEDIA_TYPE),
            is_(equal_to(noop_handler)),
        )
