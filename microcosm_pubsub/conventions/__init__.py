"""
Convention-driven pubsub.

"""
from functools import partial

from microcosm_pubsub.conventions.lifecycle import LifecycleChange
from microcosm_pubsub.conventions.messages import IdentityMessageSchema, URIMessageSchema  # noqa: F401
from microcosm_pubsub.conventions.naming import make_media_type, name_for  # noqa: F401


def media_type(lifecycle_change):
    """
    Fluent wrapper around message publishing for convention-driven schemas.

    """
    return partial(make_media_type, lifecycle_change=lifecycle_change)


def changed(resource, **kwargs):
    return media_type(LifecycleChange.Changed)(resource, **kwargs)


def created(resource, **kwargs):
    return media_type(LifecycleChange.Created)(resource, **kwargs)


def deleted(resource, **kwargs):
    return media_type(LifecycleChange.Deleted)(resource, **kwargs)
