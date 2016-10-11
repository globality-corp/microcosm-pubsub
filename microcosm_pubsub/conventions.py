"""
Convention-driven pubsub.

"""
from enum import Enum, unique
from inspect import isclass
from six import string_types

from inflection import underscore
from marshmallow import fields

from microcosm_pubsub.codecs import PubSubMessageSchema


@unique
class LifecycleChange(Enum):
    """
    CRUD lifecycle changes that may be announced via pubsub.

    Note that `Changed` is strongly discouraged; use immutable resources instead.

    """
    Changed = u"changed"
    Created = u"created"
    Deleted = u"deleted"

    def matches(self, media_type):
        return self.value in media_type.split(".")


def name_for(obj):
    """
    Get a name for something.

    Allows overriding of default names using the `__alias__` attribute.

    """
    if isinstance(obj, string_types):
        return underscore(obj)

    cls = obj if isclass(obj) else obj.__class__

    if hasattr(cls, "__alias__"):
        return underscore(cls.__alias__)
    else:
        return underscore(cls.__name__)


def make_media_type(resource, lifecycle_change=None, organization=None, public=False):
    """
    Generate a canonical media type for a message that announces a lifecycle change to a resource.

    :param resource: the resource (name)
    :param lifecycle_change: the change that happened to the resource
    :param organization: the vendor organization to which this resource belongs
    :param public: whether the resource is meant for public/external consumption

    """
    if lifecycle_change is None:
        lifecycle_change = LifecycleChange.Created
    if organization is None:
        organization = "globality"

    return "application/vnd.{}.{}.{}.{}".format(
        # use a vendor specific media type
        organization,
        # differentiate messages sent over a pubsub channel from other messages
        "pubsub" if public else "pubsub._",
        # qualify the message with the kind of lifecycle change
        lifecycle_change.value,
        # specify the resource name
        name_for(resource),
    )


class URIMessageSchema(PubSubMessageSchema):
    """
    Define a baseline message schema that points to (the URI of) a resource that experienced a lifecycle change.

    By convention, pubsub messages should be *references* to something that happened within some other
    source-of-truth (e.g. a CRUD microservice). Because there are inherent race conditions between publishing
    a message and commit a persistent transaction, message consumers are expected to call back to source-of-truth
    (e.g. via HTTP) and fetch the current resource value. In the event that message has not been committed yet,
    the consumer can retry a few times before giving up (e.g. via SQS dead-lettering).

    """
    def __init__(self, media_type, **kwargs):
        super(PubSubMessageSchema, self).__init__(**kwargs)
        self.MEDIA_TYPE = media_type

    uri = fields.String(required=True)

    def deserialize_media_type(self, obj):
        return self.MEDIA_TYPE


def changed(resource, **kwargs):
    """
    Fluent wrapper around message publishing for convention-driven schemas.

    """
    return make_media_type(resource, lifecycle_change=LifecycleChange.Changed, **kwargs)


def created(resource, **kwargs):
    """
    Fluent wrapper around message publishing for convention-driven schemas.

    """
    return make_media_type(resource, lifecycle_change=LifecycleChange.Created, **kwargs)


def deleted(resource, **kwargs):
    """
    Fluent wrapper around message publishing for convention-driven schemas.

    """
    return make_media_type(resource, lifecycle_change=LifecycleChange.Deleted, **kwargs)
