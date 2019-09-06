"""
Message encoding and decoding.

"""
from json import dumps, loads

from marshmallow import (
    EXCLUDE,
    Schema,
    ValidationError,
    fields,
)


DEFAULT_MEDIA_TYPE = "application/json"


def enrich(error: ValidationError, schema: Schema):
    """
    Enrich a schema validation error with a human-interpretable
    description of a marshmallow schema.

    """
    messages = error.messages
    if isinstance(messages, dict):
        messages.update(dict(
            schema=str(schema),
        ))
    else:
        messages.append(
            str(schema),
        )

    return error


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
    opaque_data = fields.Dict(
        attribute="opaqueData",
        required=False,
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


class PubSubMessageCodec:
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
        # We allow unknown fields to pass through (but not included here)
        # to accommodate unconditional parameter passing during produce()
        # e.g. passing in `uri` for IdentityMessages
        return dumps(self.schema.load(message, unknown=EXCLUDE))

    def decode(self, message):
        """
        Decode a message.

        Uses the appropriate codec to read JSON.

        """
        if not isinstance(message, dict):
            dct = loads(message)
        else:
            dct = message

        try:
            # load performs a validation and raises on error
            # Similarly, we exclude unknown fields to avoid errors on decode
            self.schema.load(dct, unknown=EXCLUDE)
        except ValidationError as error:
            # Add more useful information to logs for debugging validation errors.
            raise enrich(error, self.schema)

        return self.schema.dump(dct)
