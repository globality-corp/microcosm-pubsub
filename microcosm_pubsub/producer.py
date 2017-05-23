"""
Message producer.

"""
from collections import defaultdict
from distutils.util import strtobool
from functools import wraps
from six import string_types

from boto3 import client
from microcosm.api import defaults
from microcosm.errors import NotBoundError
from microcosm_logging.decorators import logger
from microcosm_logging.timing import elapsed_time

from microcosm_pubsub.conventions import LifecycleChange, make_media_type
from microcosm_pubsub.errors import TopicNotDefinedError


@logger
class SNSProducer(object):
    """
    Produces messages to SNS topics.

    """
    def __init__(self, opaque, pubsub_message_schema_registry, sns_client, sns_topic_arns, skip):
        self.opaque = opaque
        self.pubsub_message_schema_registry = pubsub_message_schema_registry
        self.sns_client = sns_client
        self.sns_topic_arns = sns_topic_arns
        self.skip = skip

    def produce(self, media_type, dct=None, **kwargs):
        """
        Produce a message.

        :returns: the message id

        """
        if self.skip:
            return
        message, topic_arn, opaque_data = self.create_message(media_type, dct, **kwargs)
        return self.publish_message(media_type, message, topic_arn, opaque_data)

    def create_message(self, media_type, dct, opaque_data=None, **kwargs):
        if opaque_data is None:
            opaque_data = dict()

        if self.opaque is not None:
            opaque_data.update(self.opaque.as_dict())

        topic_arn = self.choose_topic_arn(media_type)
        message = self.pubsub_message_schema_registry.find(media_type).encode(
            dct,
            opaque_data=opaque_data,
            **kwargs
        )
        return message, topic_arn, opaque_data

    def publish_message(self, media_type, message, topic_arn, opaque_data):
        extra = dict(
            media_type=media_type,
            **opaque_data
        )
        self.logger.debug("Publishing message with media type {media_type}", extra=extra)

        with elapsed_time(extra):
            result = self.sns_client.publish(
                TopicArn=topic_arn,
                Message=message,
            )

        self.logger.info("Published message with media type {media_type}", extra=extra)

        return result["MessageId"]

    def choose_topic_arn(self, media_type):
        """
        Choose a topic for this type of message.

        """
        try:
            topic_arn = self.sns_topic_arns[media_type]
        except KeyError:
            topic_arn = None

        if topic_arn is None:
            raise TopicNotDefinedError("No topic arn was registered for messages of type: {}".format(
                media_type,
            ))
        return topic_arn


class DeferredProducer(object):
    """
    A context manager to defer message production until the end of a block.

    """
    def __init__(self, producer):
        self.producer = producer
        self.messages = []

    def produce(self, media_type, dct=None, **kwargs):
        message, topic_arn, opaque_data = self.producer.create_message(media_type, dct, **kwargs)
        self.messages.append((media_type, message, topic_arn, opaque_data))

    def __enter__(self):
        self.messages = []
        return self

    def __exit__(self, type, value, traceback):
        if type is not None:
            return

        for media_type, message, topic_arn, opaque_data in self.messages:
            self.producer.publish_message(media_type, message, topic_arn, opaque_data)


def deferred(component, key="sns_producer"):
    """
    A decorator to defer message production until after the decorated function has completed

    """
    graph = component.graph
    sns_producer = getattr(graph, key)

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                deferred_producer = DeferredProducer(sns_producer)
                setattr(component, key, deferred_producer)
                with deferred_producer:
                    return func(*args, **kwargs)
            finally:
                setattr(component, key, sns_producer)
        return wrapper
    return decorator


def collapse_dict(dct, prefix="", separator="."):
    """
    Collapse a nested dictionary into a single-level dictionary.

    Since "." is not legal in an environment variable, we can't easily express our
    pubsub overrides in environment variable form. The only legal special characters
    is underscore and our configuration loader already uses double underscore as
    a dictionary separator. So we (re)collapse nested dictionaries here.

    """
    for key, value in dct.items():
        if isinstance(value, dict):
            for nested_key, nested_value in collapse_dict(value, key, separator):
                yield separator.join([prefix, nested_key]), nested_value
        else:
            yield separator.join([prefix, key]), value


def iter_topic_mappings(dct):
    for key, value in dct.items():
        if isinstance(value, string_types):
            yield key, value
        else:
            for nested_key, nested_value in collapse_dict(value, key):
                yield nested_key, nested_value


@defaults(
    default=None,
    mappings={},
)
def configure_sns_topic_arns(graph):
    """
    Configure a mapping from message types to topic ARNs.

    """
    if graph.config.sns_topic_arns.default is None:
        sns_topic_arns = dict()
    else:
        sns_topic_arns = defaultdict(lambda: graph.config.sns_topic_arns.default)

    sns_topic_arns.update(graph.config.sns_topic_arns.mappings)

    for lifecycle_change in LifecycleChange:
        resource_dict = graph.config.sns_topic_arns.get(lifecycle_change.value, {})
        for resource_name, topic in iter_topic_mappings(resource_dict):
            media_type = make_media_type(resource_name, lifecycle_change)
            sns_topic_arns[media_type] = topic

    return sns_topic_arns


@defaults(
    endpoint_url=None,
    mock_sns=True,
    skip=None,
)
def configure_sns_producer(graph):
    """
    Configure an SNS producer.

    The SNS Producer requires the following collaborators:
        - Opaque from microcosm.opaque for capturing context information
        - an aws sns client, i.e. from boto.
        - pubsub message codecs: see tests for examples.
        - sns topic arns: see tests for examples.

    """
    if graph.metadata.testing:
        from mock import MagicMock

        if not graph.config.sns_producer.mock_sns:
            return MagicMock()

        sns_client = MagicMock()
    else:
        endpoint_url = graph.config.sns_producer.endpoint_url
        sns_client = client("sns", endpoint_url=endpoint_url)
    try:
        opaque = graph.opaque
    except NotBoundError:
        opaque = None

    if graph.config.sns_producer.skip is None:
        # In development mode, default to not publishing because there's typically
        # not anywhere to publish to (e.g. no SNS topic)
        skip = graph.metadata.debug
    else:
        # If configured explicitly, respect the flag
        skip = strtobool(graph.config.sns_producer.skip)

    return SNSProducer(
        opaque=opaque,
        pubsub_message_schema_registry=graph.pubsub_message_schema_registry,
        sns_client=sns_client,
        sns_topic_arns=graph.sns_topic_arns,
        skip=skip,
    )
