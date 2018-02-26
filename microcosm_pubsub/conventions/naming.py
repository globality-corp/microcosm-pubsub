from inspect import isclass

from inflection import underscore

from microcosm_pubsub.conventions.lifecycle import LifecycleChange


def name_for(obj):
    """
    Get a name for something.

    Allows overriding of default names using the `__alias__` attribute.

    """
    if isinstance(obj, str):
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
        str(lifecycle_change),
        # specify the resource name
        name_for(resource),
    )
