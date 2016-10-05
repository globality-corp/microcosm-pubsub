"""
Parse the SQS envelope.

SQS defines an envelope with a message id, receipt handle, body, and so forth,
with the underlying message embedded in the body. There are multiple ways to
process this envelope, depending on the degree of validation and metadata desired.

"""
from abc import ABCMeta, abstractmethod
from json import loads
from hashlib import md5

from microcosm.api import defaults

from microcosm_pubsub.codecs import MediaTypeSchema, PubSubMessageCodec
from microcosm_pubsub.message import SQSMessage


class SQSEnvelope(object):
    """
    Enveloping base class.

    """
    __metaclass__ = ABCMeta

    def __init__(self, graph):
        self.validate_md5 = graph.config.sqs_envelope.validate_md5

    def parse_raw_message(self, consumer, raw_message):
        """
        Create an `SQSMessage` from SQS data.

        """
        message_id = raw_message["MessageId"]
        receipt_handle = raw_message["ReceiptHandle"]
        body = raw_message["Body"]

        if self.validate_md5:
            self.validate_md5(raw_message, body)

        message = loads(body)["Message"]
        media_type, content = self.parse_media_type_and_content(message)

        return SQSMessage(
            consumer=consumer,
            content=content,
            media_type=media_type,
            message_id=message_id,
            receipt_handle=receipt_handle,
        )

    @abstractmethod
    def parse_media_type_and_content(self, message):
        """
        Parse and validate SQS message media type and content.

        :returns: a media_type, content tuple

        """
        pass

    def validate_md5(self, raw_message, body):
        """
        Validate the message body.

        Just checks for tampering; schema validation occurs once we know the type of message.

        """
        expected_md5_of_body = raw_message["MD5OfBody"]
        actual_md5_of_body = md5(body).hexdigest()
        if expected_md5_of_body != actual_md5_of_body:
            raise Exception("MD5 validation failed. Expected: {} Actual: {}".format(
                expected_md5_of_body,
                actual_md5_of_body,
            ))


class RawSQSEnvelope(SQSEnvelope):
    """
    Enveloping strategy that just passes raw JSON.

    """
    def parse_media_type_and_content(self, message):
        content = loads(message)
        media_type = "application/json"
        return media_type, content


class CodecSQSEnvelope(SQSEnvelope):
    """
    Enveloping strategy that uses a media type-driven message codec.

    """
    def __init__(self, graph):
        super(CodecSQSEnvelope, self).__init__(graph)
        self.media_type_codec = PubSubMessageCodec(MediaTypeSchema())
        self.pubsub_message_schema_registry = graph.pubsub_message_schema_registry

    def parse_media_type_and_content(self, message):
        """
        Decode the message once to extract its media type and then again with the correct codec.

        """
        base_message = self.media_type_codec.decode(message)
        media_type = base_message["mediaType"]
        try:
            content = self.pubsub_message_schema_registry[media_type].decode(message)
        except KeyError:
            return media_type, None
        else:
            return media_type, content


@defaults(
    strategy_name="CodecSQSEnvelope",
    validate_md5=False,
)
def configure_sqs_envelope(graph):
    strategy_name = graph.config.sqs_envelope.strategy_name
    # It should be possible to inject custom enveloping strategies as long as the
    # subclass is imported before the graph is initialized; alternative, this block
    # can be switched to using setuptools entry points.
    strategies = {
        strategy.__name__: strategy
        for strategy in SQSEnvelope.__subclasses__()
    }
    try:
        strategy = strategies[strategy_name]
    except KeyError:
        raise Exception("Unknown SQS enveloping strategy: {}".format(
            strategy_name,
        ))
    else:
        return strategy(graph)
