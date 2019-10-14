"""
Message encoding and decoding.

"""
from json import dumps, loads
from typing import Any, Dict, Optional

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
    MEDIA_TYPE = DEFAULT_MEDIA_TYPE

    mediaType = fields.String(
        attribute="media_type",
        required=True,
    )
    opaqueData = fields.Dict(
        attribute="opaque_data",
        required=False,
    )


class MediaTypeSchema(Schema):
    """
    Custom schema for extracting media type.

    """
    mediaType = fields.String(required=True)


class PubSubMessageCodec:
    """
    Message encoder/decoder.

    """

    def __init__(self, schema: PubSubMessageSchema):
        self.schema = schema

    def encode(self, dct: Optional[Dict[str, Any]] = None, **kwargs) -> str:
        """
        Encode a message.

        Uses the appropriate codec to write JSON.

        """
        message = dct.copy() if dct else dict()
        message.update(kwargs)

        dumped = self.schema.dump(message)

        # NB: Schema dump doesn't run validation
        errors = self.schema.validate(dumped)
        if errors:
            raise ValidationError(message=errors)

        # Dump to string
        return dumps(dumped)

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
            return self.schema.load(dct, unknown=EXCLUDE)
        except ValidationError as error:
            # Add more useful information to logs for debugging validation errors.
            raise enrich(error, self.schema)
