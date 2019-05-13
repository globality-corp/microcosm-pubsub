"""
Fluent decorators for resources and handlers.

"""
from microcosm.hooks import on_resolve

from microcosm_pubsub.registry import (
    PubSubMessageSchemaRegistry,
    SQSMessageHandlerRegistry,
    media_type_for,
)


def register_schema(registry, schema_cls):
    registry.register(media_type_for(schema_cls), schema_cls)


def register_handler(registry, schema_cls, handler):
    registry.register(media_type_for(schema_cls), handler)


def handles(schema_cls):
    """
    Register a handler, tying it to a specific resource.

    Also registers its schema.

    """
    def decorator(func):
        on_resolve(PubSubMessageSchemaRegistry, register_schema, schema_cls)
        on_resolve(SQSMessageHandlerRegistry, register_handler, schema_cls, func)
        return func
    return decorator


def schema(schema_cls):
    """
    Register a schema.

    """
    on_resolve(PubSubMessageSchemaRegistry, register_schema, schema_cls)
    return schema_cls
