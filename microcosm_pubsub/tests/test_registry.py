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
from microcosm.api import create_object_graph

from microcosm_pubsub.codecs import (
    DEFAULT_MEDIA_TYPE,
    PubSubMessageCodec,
    PubSubMessageSchema,
)
from microcosm_pubsub.registry import AlreadyRegisteredError
from microcosm_pubsub.tests.fixtures import DerivedSchema, ExampleDaemon, noop_handler


class AnotherHandler:

    def __init__(self, graph=None):
        pass

    def __call__(self, message):
        return True


class AnotherSchema(PubSubMessageSchema):
    MEDIA_TYPE = "application/vnd.microcosm.another"


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
        No exception raises when re-registering ab existing handler.

        """
        self.registry.register(DerivedSchema.MEDIA_TYPE, noop_handler)

    def test_regiser_another(self):
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
