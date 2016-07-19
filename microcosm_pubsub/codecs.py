"""
Message encoding and decoding.

"""
from collections import defaultdict
from json import dumps, loads

from marshmallow import fields, Schema, ValidationError
from microcosm.api import defaults


DEFAULT_MEDIA_TYPE = "application/vnd.globality.pubsub.default"


class PubSubMessageSchema(Schema):
    """
    Base schema for messages, including a media type.

    """
    media_type = fields.Method(
        # called by dump
        serialize="serialize_media_type",
        # called by load
        deserialize="deserialize_media_type",
        attribute="mediaType",
        # need to set missing to non-None or marshmallow won't call the deserialize function
        missing=DEFAULT_MEDIA_TYPE,
    )

    def serialize_media_type(self, message):
        """
        Fetch the media type from the message.

        """
        try:
            return message["mediaType"]
        except KeyError:
            raise ValidationError("Message did not define a media type")

    def deserialize_media_type(self, obj):
        """
        Return a custom media type.

        Should be overridden in subclasses.

        """
        return DEFAULT_MEDIA_TYPE


class MediaTypeSchema(Schema):
    """
    Custom schema for extracting media type.

    """
    mediaType = fields.String(required=True)


class PubSubMessageCodec(object):
    """
    Message encoder/decoder.

    """

    def __init__(self, schema):
        self.schema = schema

    def encode(self, dct=None, **kwargs):
        """
        Encode a message.

        Uses the appropriate codec to write JSON.

        """
        message = dct.copy() if dct else dict()
        message.update(kwargs)
        encoded = self.schema.load(message)
        return dumps(encoded.data)

    def decode(self, message):
        """
        Decode a message.

        Uses the appropriate coded to read JSON.

        """
        dct = loads(message)
        # we need to explicitly validate because dump() doesn't
        self.schema.validate(dct)
        decoded = self.schema.dump(dct)
        return decoded.data


@defaults(
    default=None,
    strict=True,
    mappings=dict(),
)
def configure_pubsub_message_codecs(graph):
    """
    Configure a mapping from message types to codecs.

    """
    default_schema_cls = graph.config.pubsub_message_codecs.default
    strict = graph.config.pubsub_message_codecs.strict

    if default_schema_cls:
        message_codecs = defaultdict(lambda: PubSubMessageCodec(default_schema_cls(strict=strict)))
    else:
        message_codecs = dict()

    for key, schema_cls in graph.config.pubsub_message_codecs.mappings.items():
        message_codecs[key] = PubSubMessageCodec(schema_cls(strict=strict))

    return message_codecs
