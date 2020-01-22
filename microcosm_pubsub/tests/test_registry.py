"""
Test registry.

"""
from hamcrest import (
    assert_that,
    calling,
    equal_to,
    instance_of,
    is_,
    raises,
)
from microcosm.api import binding, create_object_graph

from microcosm_pubsub.codecs import DEFAULT_MEDIA_TYPE, PubSubMessageCodec, PubSubMessageSchema
from microcosm_pubsub.conventions import changed
from microcosm_pubsub.conventions.messages import ChangedURIMessageSchema
from microcosm_pubsub.decorators import schema
from microcosm_pubsub.registry import AlreadyRegisteredError
from microcosm_pubsub.tests.fixtures import DerivedSchema, ExampleDaemon, noop_handler


class AnotherHandler:

    def __init__(self, graph=None):
        pass

    def __call__(self, message):
        return True

    def __name__(self):
        return "another_handler"


@binding("another_handler")
def configure_another_handler(graph):
    return AnotherHandler()


class AnotherSchema(PubSubMessageSchema):
    MEDIA_TYPE = "application/vnd.microcosm.another"


class ChangedSchema(PubSubMessageSchema):
    MEDIA_TYPE = changed("Foo")


@schema
class WildcardSchema(PubSubMessageSchema):
    MEDIA_TYPE = "application/vnd.microcosm.wildcard.*"


class TestDerivedPubSubMessageCodecRegistry:

    def setup(self):
        self.graph = create_object_graph("test")
        self.registry = self.graph.pubsub_message_schema_registry

    def test_register_duplicate_ok(self):
        """
        OK to register the same schema multiple times.

        """
        self.registry.register(DerivedSchema.MEDIA_TYPE, DerivedSchema)

    def test_register_duplicate(self):
        """
        Cannot registery multiple schemas for the same media type.

        """
        assert_that(
            calling(self.registry.register).with_args(DerivedSchema.MEDIA_TYPE, AnotherSchema),
            raises(AlreadyRegisteredError),
        )

    def test_find_not_found(self):
        """
        Missing media type raise not found.

        """
        assert_that(
            calling(self.registry.find).with_args(DEFAULT_MEDIA_TYPE),
            raises(KeyError),
        )

    def test_find(self):
        """
        Find returns schema.

        """
        schema = self.registry.find(DerivedSchema.MEDIA_TYPE)
        assert_that(schema, is_(instance_of(PubSubMessageCodec)))
        assert_that(schema.schema, is_(instance_of(DerivedSchema)))

    def test_find_wildcard(self):
        schema = self.registry.find("application/vnd.microcosm.wildcard.foo_12")
        assert_that(schema, is_(instance_of(PubSubMessageCodec)))
        assert_that(schema.schema, is_(instance_of(WildcardSchema)))

    def test_wildcard_does_not_match_extended(self):
        # The postfix `*` wildcard doesn't match on `.` separators
        assert_that(
            calling(self.registry.find).with_args("application/vnd.microcosm.wildcard.foo.bar"),
            raises(KeyError),
        )

    def test_find_changed(self):
        schema = self.registry.find(ChangedSchema.MEDIA_TYPE)
        assert_that(schema, is_(instance_of(PubSubMessageCodec)))
        assert_that(schema.schema, is_(instance_of(ChangedURIMessageSchema)))

    def test_serialize_unknown_field(self):
        schema = self.registry.find(ChangedSchema.MEDIA_TYPE)
        # No exception raised
        schema.encode(dict(
            foo="bar",
            media_type=changed("Foo"),
            uri="http://uri",
        ))


class TestDerivedSQSMessageHandlerRegistry:

    def setup(self):
        self.daemon = ExampleDaemon.create_for_testing()
        self.graph = self.daemon.graph
        self.registry = self.graph.sqs_message_handler_registry

    def test_find(self):
        """
        Can find handler by media type.

        """
        handler = self.registry.find(DerivedSchema.MEDIA_TYPE, self.daemon.bound_handlers)
        assert_that(handler, is_(equal_to(noop_handler)))

    def test_find_not_found(self):
        """
        Error for unknown media type.

        """
        assert_that(
            calling(self.registry.find).with_args(DEFAULT_MEDIA_TYPE, self.daemon.bound_handlers),
            raises(KeyError),
        )

    def test_register_duplicate(self):
        """
        No exception raises when re-registering an existing handler.

        """
        self.registry.register(DerivedSchema.MEDIA_TYPE, noop_handler)

    def test_duplicate_bound_handler_found(self):
        self.graph.unlock()
        self.graph.use("another_handler")
        self.graph.lock()

        self.registry.register(DerivedSchema.MEDIA_TYPE, self.graph.another_handler)

        assert_that(
            calling(self.registry.compute_bound_handlers).with_args(
                self.daemon.components + ["another_handler"]
            ),
            raises(
                AlreadyRegisteredError,
            )
        )

    def test_register_another(self):
        """
        OK to register another handler.

        """
        another_handler = AnotherHandler()

        bound_handlers = {
            DerivedSchema.MEDIA_TYPE: another_handler,
        }

        self.registry.register(DerivedSchema.MEDIA_TYPE, another_handler)
        handler = self.registry.find(DerivedSchema.MEDIA_TYPE, bound_handlers)
        assert_that(handler, is_(instance_of(AnotherHandler)))

    def test_find_by_class(self):
        """
        Can find handler by media type.

        """
        another_handler = AnotherHandler()

        bound_handlers = {
            DerivedSchema.MEDIA_TYPE: AnotherHandler,
        }

        self.registry.register(DerivedSchema.MEDIA_TYPE, another_handler)
        handler = self.registry.find(DerivedSchema.MEDIA_TYPE, bound_handlers)
        assert_that(handler, is_(instance_of(AnotherHandler)))
