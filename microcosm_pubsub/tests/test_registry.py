"""
Test registry.

"""
from hamcrest import (
    assert_that,
    calling,
    empty,
    has_length,
    is_,
    raises,
)
from microcosm.api import create_object_graph

from microcosm_pubsub.codecs import DEFAULT_MEDIA_TYPE, PubSubMessageSchema
import microcosm_pubsub.registry  # noqa: F401
from microcosm_pubsub.tests.fixtures import DerivedSchema, noop_handler


def another_handler(message):
    return True


class AnotherSchema(PubSubMessageSchema):
    MEDIA_TYPE = "application/vnd.microcosm.another"


class DerivedPubSubMessageCodecRegistry(object):

    def setup(self):
        self.graph = create_object_graph("test")
        self.registry = self.graph.pubsub_message_schema_registry

    def test_iterate_not_found(self):
        schemas = list(self.registry.iterate(DEFAULT_MEDIA_TYPE))
        assert_that(schemas, is_(empty()))

    def test_iterate_found(self):
        schemas = list(self.registry.iterate(DerivedSchema.MEDIA_TYPE))
        assert_that(schemas, has_length(1))

    def test_iterate_multiple_not_allowed(self):
        assert_that(
            calling(self.registry.register).with_args(DerivedSchema.MEDIA_TYPE, AnotherSchema),
            raises(Exception),
        )

    def test_iterate_duplicate(self):
        assert_that(
            calling(self.registry.register).with_args(DerivedSchema.MEDIA_TYPE, DerivedSchema),
            raises(Exception),
        )


class DerivedSQSMessageHandlerRegistry(object):

    def setup(self):
        self.graph = create_object_graph("test")
        self.registry = self.graph.sqs_message_handler_registry

    def test_iterate_not_found(self):
        handlers = list(self.registry.iterate(DEFAULT_MEDIA_TYPE))
        assert_that(handlers, is_(empty()))

    def test_iterate_found(self):
        handlers = list(self.registry.iterate(DerivedSchema.MEDIA_TYPE))
        assert_that(handlers, has_length(1))

    def test_iterate_multiple(self):
        self.registry.register(DerivedSchema.MEDIA_TYPE, another_handler)
        handlers = list(self.registry.iterate(DerivedSchema.MEDIA_TYPE))
        assert_that(handlers, has_length(2))

    def test_iterate_duplicate(self):
        assert_that(
            calling(self.registry.register).with_args(DerivedSchema.MEDIA_TYPE, noop_handler),
            raises(Exception),
        )
