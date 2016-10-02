"""
Decorator tests.

"""
from hamcrest import (
    assert_that,
    contains,
    instance_of,
    is_,
)
from marshmallow import fields, Schema
from microcosm.api import create_object_graph

from microcosm_pubsub.decorators import handles, schema


@schema
class TestSchema(Schema):
    MEDIA_TYPE = "test"

    test = fields.String()


@handles(TestSchema)
def noop_handler(message):
    return True


class TestDecorators(object):

    def setup(self):
        self.graph = create_object_graph("test")

    def test_schema_decorators(self):
        assert_that(
            self.graph.pubsub_message_schema_registry[TestSchema.MEDIA_TYPE].schema,
            is_(instance_of(TestSchema)),
        )

    def test_handles_decorators(self):
        assert_that(
            list(self.graph.sqs_message_handler_registry.iterate(TestSchema.MEDIA_TYPE)),
            contains(noop_handler)
        )
