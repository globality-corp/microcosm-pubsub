"""
Fluent decorators for resources and handlers.

"""
from microcosm_pubsub.registry import (
    register_handler,
    register_schema,
)


def handles(schema_cls):
    """
    Register a handler, tying it to a specific resource.

    Also registers its schema.

    """
    def decorator(func):
        register_schema(schema_cls)
        register_handler(schema_cls, func)
        return func
    return decorator


def schema(schema_cls):
    """
    Register a schema.

    """
    register_schema(schema_cls)
    return schema_cls
