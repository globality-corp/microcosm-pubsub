"""
Message producer.

"""
from collections import defaultdict
from dataclasses import dataclass
from distutils.util import strtobool
from functools import wraps
from logging import Logger
from time import time
from typing import Dict, List

from boto3 import Session
from botocore.client import Config
from microcosm.api import defaults, typed
from microcosm.errors import NotBoundError
from microcosm_logging.decorators import logger
from microcosm_logging.timing import elapsed_time

from microcosm_pubsub.batch import MessageBatchSchema
from microcosm_pubsub.constants import PUBLISHED_KEY
from microcosm_pubsub.conventions.naming import make_media_type
from microcosm_pubsub.errors import TopicNotDefinedError


@dataclass
class PubsubMessage:
    """
    Container class encapsulating all of the necessary components for
    publishing a given message

    """
    media_type: str
    message: str
    message_attributes: Dict[str, Dict[str, str]]
    opaque_data: dict
    topic_arn: str


@logger
class SNSProducer:
    """
    Produces messages to SNS topics.

    """
    logger: Logger

    def __init__(
        self,
        opaque,
        pubsub_message_schema_registry,
        sns_client,
        sns_topic_arns,
        skip,
        deferred_batch_size,
        pubsub_producer_metrics
    ):
        self.opaque = opaque
        self.pubsub_message_schema_registry = pubsub_message_schema_registry
        self.sns_client = sns_client
        self.sns_topic_arns = sns_topic_arns
        self.skip = skip
        self.deferred_batch_size = deferred_batch_size
        self.pubsub_producer_metrics = pubsub_producer_metrics

    def produce(self, media_type, dct=None, uri=None, **kwargs):
        """
        Produce a message.

        :returns: the message id

        """

        if self.skip:
            return
        pubsub_message = self.create_message(media_type, dct, uri, **kwargs)
        return self.publish_message(pubsub_message)

    def create_message(self, media_type, dct, uri=None, opaque_data=None, **kwargs) -> PubsubMessage:
        if opaque_data is None:
            opaque_data = dict()

        if self.opaque is not None:
            opaque_data.update(self.opaque.as_dict())

        opaque_data[PUBLISHED_KEY] = str(time())

        topic_arn = self.choose_topic_arn(media_type)

        message_attributes = self.choose_message_attributes(media_type)

        message = self.pubsub_message_schema_registry.find(media_type).encode(
            dct,
            opaque_data=opaque_data,
            media_type=media_type,
            uri=uri,
            **kwargs
        )
        return PubsubMessage(
            media_type=media_type,
            message=message,
            message_attributes=message_attributes,
            opaque_data=opaque_data,
            topic_arn=topic_arn,
        )

    def publish_message(self, pubsub_message: PubsubMessage):
        extra = dict(
            media_type=pubsub_message.media_type,
            **pubsub_message.opaque_data
        )
        self.logger.debug("Publishing message with media type {media_type}", extra=extra)
        publish_result = "SUCCESS"
        publish_exception = None

        with elapsed_time(extra):
            try:
                result = self.sns_client.publish(
                    TopicArn=pubsub_message.topic_arn,
                    Message=pubsub_message.message,
                    MessageAttributes=pubsub_message.message_attributes,
                )
            except Exception as e:
                publish_exception = e
                publish_result = "FAILURE"

        self.pubsub_producer_metrics(
            publish_result=publish_result,
            **extra
        )

        if publish_exception:
            raise Exception(f"Could not publish message, SNS producer error: {publish_exception}")

        self.logger.debug("Published message with media type {media_type}", extra=extra)

        # This is another layer of protection for the message send
        # If result is not defined, it will exit with an error here
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

    def choose_message_attributes(self, media_type: str) -> Dict[str, Dict[str, str]]:
        """
        Choose message attributes for this message

        """
        return {
            "media_type": {
                "DataType": "String",
                "StringValue": media_type,
            }
        }


class DeferredProducer:
    """
    A context manager to defer message production until the end of a block.

    """
    def __init__(self, producer):
        self.producer = producer
        self.messages = []

    def produce(self, media_type, dct=None, **kwargs):
        if self.producer.skip:
            return

        pubsub_message = self.producer.create_message(media_type, dct, **kwargs)
        self.messages.append(pubsub_message)

    def __enter__(self):
        self.messages = []
        return self

    def __exit__(self, type, value, traceback):
        if type is not None:
            return

        for pubsub_message in self.messages:
            self.producer.publish_message(pubsub_message)


class DeferredBatchProducer(DeferredProducer):

    def generate_message_batches(self, messages, deferred_batch_size):
        for i in range(0, len(messages), deferred_batch_size):
            yield messages[i:i + deferred_batch_size]

    def construct_batch_pubsub_message(self, message_batch: List[PubsubMessage]):
        return [
            dict(
                media_type=pubsub_message.media_type,
                message=pubsub_message.message,
                message_attributes=pubsub_message.message_attributes,
                opaque_data=pubsub_message.opaque_data,
                topic_arn=pubsub_message.topic_arn,
            )
            for pubsub_message in message_batch
        ]

    def __exit__(self, type, value, traceback):
        if type is not None:
            return

        for message_batch in self.generate_message_batches(self.messages, self.producer.deferred_batch_size):
            if len(message_batch) > 1:
                self.producer.produce(
                    MessageBatchSchema.MEDIA_TYPE,
                    messages=self.construct_batch_pubsub_message(message_batch),
                )
            elif len(message_batch) == 1:
                self.producer.publish_message(message_batch[0])


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


def deferred_batch(component, key="sns_producer"):
    """
    A decorator to defer batch message production until after the decorated function has completed

    """
    graph = component.graph
    sns_producer = getattr(graph, key)

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                deferred_producer = DeferredBatchProducer(sns_producer)
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
        if isinstance(value, str):
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
        # NB: Do not use the default for the batch schema
        sns_topic_arns[MessageBatchSchema.MEDIA_TYPE] = None

    sns_topic_arns.update(graph.config.sns_topic_arns.mappings)

    for lifecycle_change in graph.pubsub_lifecycle_change:
        resource_dict = graph.config.sns_topic_arns.get(lifecycle_change, {})
        for resource_name, topic in iter_topic_mappings(resource_dict):
            media_type = make_media_type(resource_name, lifecycle_change)
            sns_topic_arns[media_type] = topic

    return sns_topic_arns


@defaults(
    profile_name=None,
    region_name=None,
    endpoint_url=None,
    mock_sns=True,
    skip=None,
    # the size used to determine batching in the deferred batch producer
    deferred_batch_size=typed(int, default_value=100),
    # SNS endpoint timeout configuration
    connect_timeout=typed(int, default_value=60),
    read_timeout=typed(int, default_value=60),
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
        from unittest.mock import MagicMock

        if not graph.config.sns_producer.mock_sns:
            return MagicMock()

        sns_client = MagicMock()
    else:
        endpoint_url = graph.config.sns_producer.endpoint_url
        profile_name = graph.config.sns_producer.profile_name
        region_name = graph.config.sns_producer.region_name
        session = Session(profile_name=profile_name)
        config = Config(
            read_timeout=graph.config.sns_producer.read_timeout,
            connect_timeout=graph.config.sns_producer.connect_timeout,
        )
        sns_client = session.client(
            "sns",
            endpoint_url=endpoint_url,
            region_name=region_name,
            config=config,
        )
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
        deferred_batch_size=graph.config.sns_producer.deferred_batch_size,
        pubsub_producer_metrics=graph.pubsub_producer_metrics,
    )
