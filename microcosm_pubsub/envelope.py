"""
Parse the SQS envelope.

SQS defines an envelope with a message id, receipt handle, body, and so forth,
with the underlying message embedded in the body. There are multiple ways to
process this envelope, depending on the degree of validation and metadata desired.

"""
from abc import ABCMeta, abstractmethod
from json import loads
from hashlib import md5
from six import add_metaclass

from microcosm.api import defaults

from microcosm_pubsub.codecs import MediaTypeSchema, PubSubMessageCodec
from microcosm_pubsub.message import SQSMessage


@add_metaclass(ABCMeta)
class MessageBodyParser(object):
    """
    Mixin for parsing the data from an SQS message.

    """
    @abstractmethod
    def parse_message(self, body):
        """
        Extract the user-space portions of the message from the message body.

        :returns: a dictionary

        """
        pass


@add_metaclass(ABCMeta)
class MediaTypeAndContentParser(object):
    """
    Mixin for parsing a media type and content from a message body.

    """
    @abstractmethod
    def parse_media_type_and_content(self, message):
        """
        Extract the media type and content dictionary for a message body.

        :returns: a media type (string), and content (dict) tuple

        """
        pass


class RawMessageBodyParser(MessageBodyParser):

    def parse_message(self, body):
        """
        Assume the top-level message *IS* the message body.

        """
        return body


class RawMediaTypeAndContentParser(MediaTypeAndContentParser):

    def parse_media_type_and_content(self, message):
        content = message
        media_type = "application/json"
        return media_type, loads(content)


class SNSMessageBodyParser(MessageBodyParser):

    def parse_message(self, body):
        """
        Extract the user-space portions of the message from the message body.

        When an SQS queue subscribes to an SNS topic, the user-space message is packaged up
        as JSON within the `Message` key of the top-level envelope.

        """
        return loads(body)["Message"]


class CodecMediaTypeAndContentParser(MediaTypeAndContentParser):

    def __init__(self, graph):
        super(CodecMediaTypeAndContentParser, self).__init__(graph)
        self.media_type_codec = PubSubMessageCodec(MediaTypeSchema())
        self.pubsub_message_schema_registry = graph.pubsub_message_schema_registry

    def parse_media_type_and_content(self, message):
        """
        Decode the message once to extract its media type and then again with the correct codec.

        """
        base_message = self.media_type_codec.decode(message)
        media_type = base_message["mediaType"]
        try:
            content = self.pubsub_message_schema_registry.find(media_type).decode(message)
        except KeyError:
            return media_type, None
        else:
            return media_type, content


class SQSEnvelope(MessageBodyParser, MediaTypeAndContentParser):
    """
    Enveloping base class.

    """
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

        message = self.parse_message(body)
        media_type, content = self.parse_media_type_and_content(message)

        return SQSMessage(
            consumer=consumer,
            content=content,
            media_type=media_type,
            message_id=message_id,
            receipt_handle=receipt_handle,
        )

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


class RawSQSEnvelope(RawMessageBodyParser, RawMediaTypeAndContentParser, SQSEnvelope):
    """
    Enveloping strategy that just passes raw JSON.

    """
    pass


class CodecSQSEnvelope(SNSMessageBodyParser, CodecMediaTypeAndContentParser, SQSEnvelope):
    """
    Enveloping strategy that uses a media type-driven message codec.

    """
    pass


class LocalStackSQSEnvelope(RawMessageBodyParser, CodecSQSEnvelope, SQSEnvelope):
    """
    Enveloping strategy that uses a media type-driven message codec with localstack conventions.

    """
    pass


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
