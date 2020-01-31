"""
Registry of SQS message handlers.

"""
from collections import defaultdict
from inspect import isclass

from marshmallow import EXCLUDE
from microcosm.api import defaults
from microcosm_logging.decorators import logger

from microcosm_pubsub.codecs import PubSubMessageCodec
from microcosm_pubsub.conventions.messages import (
    ChangedURIMessageSchema,
    IdentityMessageSchema,
    URIMessageSchema,
)


class AlreadyRegisteredError(Exception):
    pass


@logger
class PubSubMessageSchemaRegistry:
    """
    Keeps track of available message schemas.

    """
    def __init__(self, graph):
        self._media_types = set()
        self._mappings = dict()
        self.graph = graph
        self.lifecycle_change = graph.pubsub_lifecycle_change
        self.auto_register = graph.config.pubsub_message_schema_registry.auto_register

    def register(self, media_type, value):
        """
        Register a value for a media type.

        It is an error to register more than one value for the same media type.

        """
        self._media_types.add(media_type)

        if isinstance(value, str):
            # When using the @handles convention, there may not be a concrete schema class declared.
            # In this case, the media type itself is passed as the value.
            # thereby allowing handlers to be declared using convention-driven
            return

        existing_value = self._mappings.get(media_type)
        if existing_value:
            if value == existing_value:
                return
            raise AlreadyRegisteredError("A schema already exists for media type: {}".format(
                media_type,
            ))
        self._mappings[media_type] = value

    def find_matching_media_type(self, media_type):
        """
        Find if a media type is present in the registry, either as an exact match,
        or as a pattern match

        The only non-exact pattern currently supported is a trailing *, which can match any character except `.`
        e.g. "application/vnd.globality.pubsub._.created.foo.*"

        """
        # `media_type` is present as an exact match
        if media_type in self._media_types:
            return media_type
        # Otherwise look for a pattern match
        media_type_prefix = ".".join(media_type.split(".")[0:-1])
        wildcard_media_type = f"{media_type_prefix}.*"
        if wildcard_media_type in self._media_types:
            return wildcard_media_type

        return None

    def find(self, media_type):
        """
        Create a codec or raise KeyError. If autoregistration is enabled, falls
        back to the URIMessageSchema.

        """
        matching_media_type = self.find_matching_media_type(media_type)
        if matching_media_type is None:
            if self.auto_register and self.lifecycle_change.matches(media_type):
                # When using convention-based media types, we may need to auto-register
                self._media_types.add(media_type)
            else:
                raise KeyError("Unregistered media type: {}".format(media_type))

        try:
            # use a concrete schema class if any
            schema_cls = self._mappings[matching_media_type]
            schema = schema_cls(unknown=EXCLUDE)
        except KeyError:
            # use convention otherwise
            if self.lifecycle_change.Deleted in media_type.split("."):
                schema = IdentityMessageSchema()
            elif self.lifecycle_change.Changed in media_type.split("."):
                schema = ChangedURIMessageSchema()
            else:
                schema = URIMessageSchema()

        return PubSubMessageCodec(schema)


@logger
class SQSMessageHandlerRegistry:
    """
    Keeps track of available handlers.

    """
    def __init__(self, graph):
        self._mappings = defaultdict(set)
        self.graph = graph

    def register(self, media_type, handler):
        self._mappings[media_type].add(handler)

    def compute_bound_handlers(self, bindings):
        """
        Compute which handlers are bound to the graph.

        In cases where multiple daemons exist in the same code base, it's possible that
        the registry will include handlers that aren't currently bound.

        """
        bound_components = [
            getattr(self.graph, binding)
            for binding in bindings
        ]
        bound_component_types = [
            type(bound_component)
            for bound_component in bound_components
        ]

        bound_handlers = dict()
        for media_type, handler in self.iter_handlers():
            if handler in bound_components or handler in bound_component_types:
                if bound_handlers.get(media_type):
                    raise AlreadyRegisteredError(
                        "Handler {} already registered for media type: {}".format(
                            bound_handlers.get(media_type).__name__,
                            media_type,
                        )
                    )
                bound_handlers[media_type] = handler
        return bound_handlers

    def find(self, media_type, bound_handlers):
        handler = bound_handlers[media_type]

        if isclass(handler):
            return handler(self.graph)
        else:
            return handler

    def iter_handlers(self):
        for media_type, handlers in self._mappings.items():
            for handler in handlers:
                yield media_type, handler

    def keys(self):
        return self._mappings.keys()


@defaults(
    auto_register=True,
)
def configure_schema_registry(graph):
    return PubSubMessageSchemaRegistry(graph)


def configure_handler_registry(graph):
    return SQSMessageHandlerRegistry(graph)


def media_type_for(schema_cls):
    if hasattr(schema_cls, "MEDIA_TYPE"):
        return schema_cls.MEDIA_TYPE
    if hasattr(schema_cls, "infer_media_type"):
        return schema_cls.infer_media_type()
    if isinstance(schema_cls, str):
        return schema_cls
    raise Exception("Cannot infer media type for schema class: {}".format(schema_cls))
