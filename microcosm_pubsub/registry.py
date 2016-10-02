"""
Registry of SQS message handlers.

"""
from abc import ABCMeta, abstractproperty
from microcosm.api import defaults
from microcosm.errors import NotBoundError
from microcosm_logging.decorators import logger

from microcosm_pubsub.codecs import PubSubMessageCodec


class AlreadyRegisteredError(Exception):
    pass


class Registry(object):
    """
    A decorator-friendly registry of per-media type objects.

    Supports static configuration from binding keys and explicit registration from decorators
    (using a singleton).

    """
    __metaclass__ = ABCMeta

    def __init__(self, graph):
        """
        Create registry, auto-registering items found using the legacy graph binding key.

        """
        try:
            for media_type, value in getattr(graph, self.legacy_binding_key).items():
                self.register(media_type, value)
        except NotBoundError:
            pass

    @abstractproperty
    def legacy_binding_key(self):
        pass

    @classmethod
    def register(cls, media_type, value):
        """
        Register a valuex for a media type.

        It is an error to register the same value multiple times.

        """
        existing_value = cls.MAPPINGS.get(media_type)
        if existing_value:
            if value == existing_value:
                return
            raise AlreadyRegisteredError("A mapping already exists  media type: {}".format(
                media_type,
            ))
        cls.MAPPINGS[media_type] = value

    @classmethod
    def find(cls, media_type):
        return cls.MAPPINGS[media_type]


@logger
class PubSubMessageSchemaRegistry(Registry):
    """
    Keeps track of available message schemas.

    """
    # singleton registry
    MAPPINGS = dict()

    def __init__(self, graph):
        super(PubSubMessageSchemaRegistry, self).__init__(graph)
        self.strict = graph.config.pubsub_message_schema_registry.strict

    @property
    def legacy_binding_key(self):
        return "pubsub_message_codecs"

    @property
    def allows_multiple(self):
        return False

    def __getitem__(self, media_type):
        """
        Create a codec or raise KeyError.

        """
        schema_cls = self.__class__.find(media_type)
        return PubSubMessageCodec(schema_cls(strict=self.strict))


@logger
class SQSMessageHandlerRegistry(Registry):
    """
    Keeps track of available handlers.

    """
    # singleton registry
    MAPPINGS = dict()

    @property
    def legacy_binding_key(self):
        return "sqs_message_handlers"

    def allows_multiple(self):
        return True

    def iterate(self, media_type):
        """
        Iterate through available values for the given media type.

        """
        value = self.__class__.find(media_type)
        if not value:
            self.logger.debug("No mapping found for media type: {}".format(media_type))
            return

        yield value


@defaults(
    strict=True,
)
def configure_schema_registry(graph):
    return PubSubMessageSchemaRegistry(graph)


def configure_handler_registry(graph):
    return SQSMessageHandlerRegistry(graph)


def register_schema(schema_cls):
    PubSubMessageSchemaRegistry.register(schema_cls.MEDIA_TYPE, schema_cls)


def register_handler(schema_cls, handler):
    SQSMessageHandlerRegistry.register(schema_cls.MEDIA_TYPE, handler)
